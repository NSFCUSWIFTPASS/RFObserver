"""Continuous capture -> process -> detect -> store -> publish loop.

Double-buffer pipeline:
- Capture thread fills buffer A for DURATION_SEC
- Processing thread works on buffer B (previous capture)
- Swap buffers and repeat
- If processing exceeds capture time, the excess is measured and reported
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from rfobserver.processing.burst import BurstDetectionConfig, detect_bursts
from rfobserver.processing.iq_utils import calculate_iq_statistics, convert_bytes_to_complex
from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid, compute_summary_psd

if TYPE_CHECKING:
    from rfobserver.capture.receiver import CaptureResult, IReceiver
    from rfobserver.config import AppSettings
    from rfobserver.models import BurstFingerprint, IQStatistics, ProcessedDataEnvelope, PSDData
    from rfobserver.pipeline.attribution import AttributionItem, AttributionWorker
    from rfobserver.pipeline.beacon import ProgressBeacon
    from rfobserver.storage.database import SensorDatabase
    from rfobserver.storage.local import LocalStorage
    from rfobserver.transport.nats_producer import NatsProducer
    from rfobserver.web.websocket import LiveBroadcast
    from rfobserver.zms.monitor import ZmsMonitor

logger = logging.getLogger(__name__)


def select_bursts_for_attribution(
    bursts: list[BurstFingerprint],
    noise_floor_db: float,
    snr_db: float,
    max_n: int,
) -> list[BurstFingerprint]:
    """SNR gate + top-N-by-power. Returns the strongest bursts that clear the
    gate, most-powerful first, capped at max_n."""
    gated = [b for b in bursts if (b.peak_power_db - noise_floor_db) >= snr_db]
    gated.sort(key=lambda b: b.peak_power_db, reverse=True)
    return gated[:max_n]


def _build_attribution_items(
    iq_bytes: bytes,
    sample_rate_hz: float,
    center_freq_hz: float,
    picked: list[BurstFingerprint],
) -> list[AttributionItem]:
    """CPU-bound: IQ conversion + per-burst channelize. No asyncio in here --
    run this off the event loop (asyncio.to_thread) since it can take seconds
    for a wideband chunk with several picked bursts."""
    from rfobserver.pipeline.attribution import AttributionItem
    from rfobserver.processing.channelize import channelize_to_cs16, select_rate_and_protocols

    data = convert_bytes_to_complex(iq_bytes)
    items: list[AttributionItem] = []
    for burst in picked:
        offset = burst.peak_freq_hz - float(center_freq_hz)
        rate, passes = select_rate_and_protocols(burst.bandwidth_hz)
        cs16 = channelize_to_cs16(data, float(sample_rate_hz), offset, rate)
        items.append(
            AttributionItem(
                burst_id=burst.burst_id,
                cs16=cs16,
                target_rate_hz=rate,
                passes=passes,
                power_db=burst.peak_power_db,
            )
        )
    return items


class ContinuousProcessor:
    """Double-buffered capture/processing pipeline.

    Capture and processing run on separate threads via a ThreadPoolExecutor.
    Each iteration:
      1. Start capture N+1 in capture thread
      2. If processing of capture N is still running, wait and measure excess
      3. When capture N+1 finishes, kick off processing N+1
      4. Repeat -- capture N+2 overlaps with processing N+1
    """

    def __init__(
        self,
        receiver: IReceiver,
        database: SensorDatabase,
        local_storage: LocalStorage,
        settings: AppSettings,
        broadcast: LiveBroadcast | None = None,
        zms_monitor: ZmsMonitor | None = None,
        nats_producer: NatsProducer | None = None,
        beacon: ProgressBeacon | None = None,
    ) -> None:
        self._receiver = receiver
        self._db = database
        self._storage = local_storage
        self._settings = settings
        self._broadcast = broadcast
        self._zms_monitor = zms_monitor
        self._nats_producer = nats_producer
        self._beacon = beacon

        # 1 thread for capture, N-3 cores for processing, 2 cores left free for OS/web
        total_cores = os.cpu_count() or 4
        self._process_workers = max(1, total_cores - 3)
        self._capture_executor = ThreadPoolExecutor(max_workers=1)
        self._process_executor = ThreadPoolExecutor(max_workers=1)
        self._capture_count = 0
        self._running = False
        self._excess_ms: float = 0.0

        self._attrib_worker: AttributionWorker | None = None
        self._attrib_task: asyncio.Task[None] | None = None
        if settings.ATTRIBUTION_ENABLED:
            from rfobserver.pipeline.attribution import (
                AttributionWorker,
                StrongestQueue,
                find_rtl433,
            )

            rtl = find_rtl433(settings.ATTRIBUTION_RTL433_PATH or None)
            if rtl is None:
                logger.warning("ATTRIBUTION_ENABLED but rtl_433 not found; attribution disabled")
            else:
                q = StrongestQueue(maxsize=settings.ISOLATION_QUEUE_MAX)
                self._attrib_worker = AttributionWorker(database, rtl, queue=q)

        logger.info(
            "Pipeline: 1 capture thread, %d PSD worker threads (%d cores, 2 reserved)",
            self._process_workers,
            total_cores,
        )

    async def run(self) -> None:
        """Run the double-buffered capture/processing loop."""
        self._running = True
        s = self._settings
        freqs = self._build_frequency_list()

        logger.info(
            "Starting double-buffer pipeline: %d frequencies, %s Hz bandwidth",
            len(freqs),
            s.BANDWIDTH,
        )

        process_future: asyncio.Future[_ProcessResult] | None = None
        broadcast_task: asyncio.Task[None] | None = None

        if self._attrib_worker is not None:
            self._attrib_task = asyncio.create_task(self._attrib_worker.run())

        while self._running:
            for center_freq in freqs:
                if not self._running:
                    break
                try:
                    # -- Capture (blocks for DURATION_SEC on real hardware) --
                    capture_t0 = time.monotonic()
                    result = await self._receiver.receive_samples(center_freq)
                    capture_elapsed = time.monotonic() - capture_t0

                    # -- Wait for previous processing if still running --
                    excess_ms = 0.0
                    if process_future is not None:
                        if not process_future.done():
                            wait_t0 = time.monotonic()
                            await process_future
                            excess_ms = (time.monotonic() - wait_t0) * 1000.0
                            logger.info("Processing lagged capture by %.1f ms", excess_ms)
                        else:
                            await process_future  # collect any exception

                        # Fire off store+broadcast as background task (non-blocking)
                        prev_result = process_future.result()
                        if broadcast_task is not None:
                            await broadcast_task  # ensure previous broadcast finished
                        broadcast_task = asyncio.create_task(
                            self._store_and_broadcast(prev_result, excess_ms)
                        )
                        process_future = None

                    # -- Kick off processing for this capture immediately --
                    self._capture_count += 1
                    if self._beacon is not None:
                        self._beacon.mark()
                    logger.debug(
                        "Capture #%d at %d Hz (%.1f ms, %d bytes)",
                        self._capture_count,
                        center_freq,
                        capture_elapsed * 1000,
                        len(result.raw_capture.iq_data_bytes),
                    )

                    loop = asyncio.get_running_loop()
                    process_future = loop.run_in_executor(
                        self._process_executor,
                        _process_capture_blocking,
                        result,
                        self._settings,
                        self._capture_count,
                        self._receiver.serial,
                        self._process_workers,
                    )

                except Exception:
                    logger.exception("Error in capture at %d Hz", center_freq)
                    await asyncio.sleep(1.0)

        # Drain remaining work
        if process_future is not None:
            await process_future
            prev_result = process_future.result()
            await self._store_and_broadcast(prev_result, 0.0)
        if broadcast_task is not None:
            await broadcast_task

        # Shut down the attribution worker (its queue.get() only unblocks on
        # cancel, so stop() alone is not enough).
        if self._attrib_worker is not None:
            self._attrib_worker.stop()
        if self._attrib_task is not None:
            self._attrib_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._attrib_task

    def stop(self) -> None:
        self._running = False

    async def _store_and_broadcast(self, pr: _ProcessResult, excess_ms: float) -> None:
        """Save raw file, store detections in SQLite, broadcast to WebSocket."""
        self._excess_ms = excess_ms

        # Save raw IQ file
        self._storage.save_capture(pr.filename, pr.iq_bytes)

        # Store detections
        for burst in pr.bursts:
            await self._db.insert_detection(
                burst_id=burst.burst_id,
                start_time=burst.start_time,
                stop_time=burst.stop_time,
                center_freq_hz=burst.center_freq_hz,
                bandwidth_hz=burst.bandwidth_hz,
                peak_power_db=burst.peak_power_db,
                duration_ms=burst.duration_ms,
                detection_timestamp=burst.detection_timestamp,
                peak_freq_hz=burst.peak_freq_hz,
            )

        if pr.bursts:
            logger.info(
                "Detected %d bursts at %d Hz (capture #%d)",
                len(pr.bursts),
                pr.center_freq_hz,
                pr.capture_num,
            )

        # rtl_433 attribution: gate (cheap, stays on the loop), then channelize
        # off-loop (CPU-bound: full-chunk IQ conversion + per-burst mixer/
        # resample, up to ISOLATION_MAX_PER_SEC times), then enqueue
        # (never blocks) back on the loop.
        if self._attrib_worker is not None and pr.bursts:
            picked = select_bursts_for_attribution(
                pr.bursts,
                pr.noise_floor_db,
                self._settings.ISOLATION_SNR_DB,
                self._settings.ISOLATION_MAX_PER_SEC,
            )
            if picked:
                items = await asyncio.to_thread(
                    _build_attribution_items,
                    pr.iq_bytes,
                    float(self._settings.BANDWIDTH),
                    float(pr.center_freq_hz),
                    picked,
                )
                for item in items:
                    self._attrib_worker.queue.put_nowait(item)

        # Build the processed envelope once; fan out to ZMS + NATS.
        if self._zms_monitor is not None or self._nats_producer is not None:
            try:
                envelope = self._build_envelope(pr)
                await self._fanout_envelope(envelope, pr.capture_num)
            except Exception:
                logger.exception("Processed envelope fan-out failed")

        # Broadcast to WebSocket
        if self._broadcast is not None:
            await self._broadcast.publish(
                {
                    "type": "psd",
                    "center_freq_hz": pr.center_freq_hz,
                    "bandwidth_hz": self._settings.BANDWIDTH,
                    "powers": pr.summary_psd.powers,
                    "frequencies": pr.summary_psd.frequencies,
                    "num_bins": pr.summary_psd.num_bins,
                    "avg_power_db": pr.iq_stats.average,
                    "max_power_db": pr.iq_stats.max,
                    "kurtosis": pr.iq_stats.kurtosis,
                    "burst_count": len(pr.bursts),
                    "capture_num": pr.capture_num,
                    "process_ms": pr.process_ms,
                    "excess_ms": excess_ms,
                }
            )

    def _build_envelope(self, pr: _ProcessResult) -> ProcessedDataEnvelope:
        """Construct a ProcessedDataEnvelope from a per-capture result.

        Fills in the metadata fields rf-shared / rf-processor read from the
        DB schema (length, interval, sampling_rate, bit_depth) from settings
        rather than leaving them at model defaults.
        """
        from pathlib import Path

        from rfobserver.models import MetadataRecord, ProcessedDataEnvelope

        s = self._settings
        meta = MetadataRecord(
            hostname=s.HOSTNAME,
            organization=s.ORGANIZATION,
            serial=self._receiver.serial,
            frequency=pr.center_freq_hz,
            timestamp=datetime.now(timezone.utc),
            source_path=Path(pr.filename),
            gain=s.GAIN,
            sampling_rate=s.BANDWIDTH,
            length=s.DURATION_SEC,
            interval=s.INTERVAL_SEC,
            bit_depth=16,
        )
        return ProcessedDataEnvelope(
            metadata=meta,
            statistics=pr.iq_stats,
            psd_data=pr.summary_psd,
        )

    async def _fanout_envelope(self, envelope: ProcessedDataEnvelope, capture_num: int) -> None:
        """Submit the processed envelope to ZMS (HTTP) and NATS (rfobs.stats)."""
        if self._zms_monitor is not None:
            try:
                ok = await self._zms_monitor.submit_observation(envelope)
                if ok:
                    logger.debug("ZMS observation submitted (capture #%d)", capture_num)
            except Exception:
                logger.exception("ZMS observation submission failed")

        if self._nats_producer is not None:
            try:
                await self._nats_producer.publish_stats(envelope, self._settings.HOSTNAME)
            except Exception:
                logger.exception("NATS stats publish failed")

    def _build_frequency_list(self) -> list[int]:
        s = self._settings
        if s.FREQUENCY_STEP <= 0 or s.FREQUENCY_END <= s.FREQUENCY_START:
            return [s.FREQUENCY_START]
        freqs = []
        f = s.FREQUENCY_START
        while f <= s.FREQUENCY_END:
            freqs.append(f)
            f += s.FREQUENCY_STEP
        return freqs


class _ProcessResult:
    """Container for results from the processing thread."""

    __slots__ = (
        "iq_stats",
        "summary_psd",
        "bursts",
        "center_freq_hz",
        "capture_num",
        "process_ms",
        "filename",
        "iq_bytes",
        "noise_floor_db",
    )

    def __init__(
        self,
        iq_stats: IQStatistics,
        summary_psd: PSDData,
        bursts: list[BurstFingerprint],
        center_freq_hz: int,
        capture_num: int,
        process_ms: float,
        filename: str,
        iq_bytes: bytes,
        noise_floor_db: float = 0.0,
    ) -> None:
        self.iq_stats = iq_stats
        self.summary_psd = summary_psd
        self.bursts = bursts
        self.center_freq_hz = center_freq_hz
        self.capture_num = capture_num
        self.process_ms = process_ms
        self.filename = filename
        self.iq_bytes = iq_bytes
        self.noise_floor_db = noise_floor_db


def _process_capture_blocking(
    result: CaptureResult,
    settings: AppSettings,
    capture_num: int,
    serial: str,
    num_workers: int = 1,
) -> _ProcessResult:
    """CPU-bound processing on a worker thread.

    Receives the CaptureResult and returns all computed data.
    File saving is also done here to keep IO off the event loop.
    """
    t0 = time.monotonic()

    s = settings
    iq_bytes = result.raw_capture.iq_data_bytes
    center_freq_hz = result.raw_capture.center_freq_hz
    capture_time = result.raw_capture.capture_timestamp

    # Save raw capture file
    filename = f"{serial}-{s.HOSTNAME}-{capture_time.strftime('%Y%m%dT%H%M%S')}.sc16"

    # IQ conversion + stats
    data = convert_bytes_to_complex(iq_bytes)
    iq_stats = calculate_iq_statistics(data)

    # PSD grid + summary
    grid_config = PSDGridConfig(
        num_bins=s.NUM_FFT_BINS,
        time_resolution_ms=s.PSD_TIME_RESOLUTION_MS,
        num_workers=num_workers,
    )
    psd_grid = compute_psd_grid(data, s.BANDWIDTH, config=grid_config)
    summary_psd = compute_summary_psd(psd_grid, center_freq_hz, s.BANDWIDTH)

    # Burst detection
    burst_config = BurstDetectionConfig(
        threshold_high_db=s.BURST_THRESHOLD_HIGH_DB,
        threshold_low_ratio=s.BURST_THRESHOLD_LOW_RATIO,
        merge_freq_bins=s.BURST_MERGE_FREQ_BINS,
        merge_time_sec=s.BURST_MERGE_TIME_MS / 1000.0,
        noise_floor_percentile=s.BURST_NOISE_FLOOR_PERCENTILE,
    )
    detection_result = detect_bursts(
        psd_grid,
        config=burst_config,
        center_freq_hz=float(center_freq_hz),
        capture_time=capture_time,
    )

    process_ms = (time.monotonic() - t0) * 1000.0

    return _ProcessResult(
        iq_stats=iq_stats,
        summary_psd=summary_psd,
        bursts=detection_result.bursts,
        center_freq_hz=center_freq_hz,
        capture_num=capture_num,
        process_ms=process_ms,
        filename=filename,
        iq_bytes=iq_bytes,
        noise_floor_db=detection_result.noise_floor_db,
    )
