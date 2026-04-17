"""Streaming capture -> process -> detect -> store -> publish pipeline.

Replaces the batch ``ContinuousProcessor`` when streaming mode is active.
Three threads coordinate via bounded queues:

* **Receiver thread** – calls ``recv_chunk()`` in a tight loop, feeds the
  pre-trigger circular buffer and power trigger, enqueues SC16 buffers.
* **Processing thread** – converts SC16→complex, computes PSD rows via
  ``compute_psd_grid``, feeds the ``RollingBurstDetector``, pushes results
  to the async event loop.
* **Event loop** – broadcasts PSD + stats to WebSocket, stores bursts to
  SQLite, periodically submits to ZMS.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import queue
import threading
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import numpy as np

from rfobserver.capture.buffer import CircularBuffer
from rfobserver.processing.burst import BurstDetectionConfig
from rfobserver.processing.iq_utils import calculate_iq_statistics, convert_sc16_to_complex
from rfobserver.processing.rolling_burst import RollingBurstDetector
from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid, compute_summary_psd

if TYPE_CHECKING:
    from rfobserver.capture.receiver import IReceiver
    from rfobserver.config import AppSettings
    from rfobserver.models import BurstFingerprint, IQStatistics, PSDData
    from rfobserver.processing.spectral import PSDGridResult
    from rfobserver.storage.database import SensorDatabase
    from rfobserver.storage.local import LocalStorage
    from rfobserver.web.websocket import LiveBroadcast
    from rfobserver.zms.monitor import ZmsMonitor

logger = logging.getLogger(__name__)

# Sentinel used to signal threads to shut down.
_STOP = None


class _StreamResult:
    """Container for results produced by the processing thread."""

    __slots__ = (
        "summary_psd",
        "iq_stats",
        "bursts",
        "psd_grid",
        "center_freq_hz",
        "capture_num",
        "process_ms",
        "latency_ms",
    )

    def __init__(
        self,
        summary_psd: PSDData,
        iq_stats: IQStatistics,
        bursts: list[BurstFingerprint],
        psd_grid: PSDGridResult,
        center_freq_hz: int,
        capture_num: int,
        process_ms: float,
        latency_ms: float,
    ) -> None:
        self.summary_psd = summary_psd
        self.iq_stats = iq_stats
        self.bursts = bursts
        self.psd_grid = psd_grid
        self.center_freq_hz = center_freq_hz
        self.capture_num = capture_num
        self.process_ms = process_ms
        self.latency_ms = latency_ms


class StreamingProcessor:
    """Streaming pipeline: continuous recv → incremental PSD → rolling burst detection."""

    def __init__(
        self,
        receiver: IReceiver,
        database: SensorDatabase,
        local_storage: LocalStorage,
        settings: AppSettings,
        broadcast: LiveBroadcast | None = None,
        zms_monitor: ZmsMonitor | None = None,
    ) -> None:
        self._receiver = receiver
        self._db = database
        self._storage = local_storage
        self._settings = settings
        self._broadcast = broadcast
        self._zms_monitor = zms_monitor
        self._running = False

        s = settings
        total_cores = os.cpu_count() or 4
        self._process_workers = max(1, total_cores - 3)

        # Chunk sizing: how many PSD time-slices per recv chunk
        nperseg = s.NUM_FFT_BINS
        overlap = 0.5
        hop = int(nperseg * (1 - overlap))
        slice_samples = int(s.BANDWIDTH * s.PSD_TIME_RESOLUTION_MS / 1000.0)
        if slice_samples < nperseg:
            slice_samples = nperseg
        ffts_per_slice = max(1, (slice_samples - nperseg) // hop + 1)
        actual_slice_samples = nperseg + (ffts_per_slice - 1) * hop

        chunk_slices = s.STREAMING_CHUNK_SLICES
        self._chunk_samples = chunk_slices * actual_slice_samples
        self._chunk_duration = self._chunk_samples / s.BANDWIDTH

        # Buffer pool: 8 pre-allocated SC16 (int32) buffers
        # Larger pool so the receiver thread is never starved even when
        # processing falls behind.
        self._buf_pool: queue.Queue[np.ndarray] = queue.Queue(maxsize=8)
        for _ in range(8):
            self._buf_pool.put(np.zeros(self._chunk_samples, dtype=np.int32))

        # Inter-thread queues — pass raw SC16 (int32) to processing thread
        # Processing is best-effort: chunks are dropped if the queue is full.
        self._chunk_queue: queue.Queue[tuple[np.ndarray, float] | None] = queue.Queue(maxsize=4)
        self._dropped_chunks = 0
        self._result_queue: asyncio.Queue[_StreamResult | None] = asyncio.Queue(maxsize=8)
        self._loop: asyncio.AbstractEventLoop | None = None

        # Pre-trigger circular buffer (int32 = SC16, half the memory of complex64)
        pre_trigger_samples = int(s.TRIGGER_PRE_SEC * s.BANDWIDTH)
        self._pre_trigger_buf = CircularBuffer(max(1, pre_trigger_samples), dtype=np.int32)

        # Trigger state
        self._manual_trigger = threading.Event()
        self._trigger_active = False
        self._trigger_file: str | None = None
        self._below_threshold_count = 0

        self._capture_count = 0

        logger.info(
            "StreamingProcessor: chunk=%d samples (%.1f ms), %d PSD workers, "
            "pre-trigger=%.2fs (%d samples)",
            self._chunk_samples,
            self._chunk_duration * 1000,
            self._process_workers,
            s.TRIGGER_PRE_SEC,
            pre_trigger_samples,
        )

    # -- Public API --

    async def run(self) -> None:
        """Start the streaming pipeline."""
        self._running = True
        self._loop = asyncio.get_running_loop()

        recv_thread = threading.Thread(target=self._receiver_loop, name="recv", daemon=True)
        proc_thread = threading.Thread(target=self._processing_loop, name="proc", daemon=True)

        recv_thread.start()
        proc_thread.start()

        try:
            await self._result_consumer_loop()
        finally:
            self._running = False
            # Unblock threads
            self._chunk_queue.put(_STOP)
            recv_thread.join(timeout=5)
            proc_thread.join(timeout=5)

    def stop(self) -> None:
        self._running = False

    def manual_trigger(self) -> None:
        """Activate the manual IQ capture trigger."""
        self._manual_trigger.set()
        logger.info("Manual trigger activated")

    def stop_trigger(self) -> None:
        """Deactivate the manual IQ capture trigger."""
        self._manual_trigger.clear()
        logger.info("Manual trigger deactivated")

    # -- Receiver thread --

    def _receiver_loop(self) -> None:
        """Runs on a dedicated thread.  Calls recv_chunk() and feeds queues."""
        s = self._settings
        freqs = self._build_frequency_list()
        recv_count = 0

        try:
            while self._running:
                for center_freq in freqs:
                    if not self._running:
                        return

                    self._receiver.start_streaming(center_freq)

                    # Dwell at this frequency for DURATION_SEC
                    chunks_per_dwell = max(1, int(s.DURATION_SEC / self._chunk_duration))

                    for _ in range(chunks_per_dwell):
                        if not self._running:
                            break

                        # Get a buffer — never block the receiver thread
                        try:
                            buf = self._buf_pool.get_nowait()
                        except queue.Empty:
                            # All buffers held by processing — allocate a temporary one
                            buf = np.zeros(self._chunk_samples, dtype=np.int32)

                        recv_time = time.monotonic()
                        n = self._receiver.recv_chunk(buf)
                        t_recv_done = time.monotonic()

                        if n < len(buf):
                            logger.warning("recv_chunk short: %d/%d samples", n, len(buf))

                        # Store raw SC16 in pre-trigger buffer (fast array copy)
                        self._pre_trigger_buf.write(buf[:n])

                        # Check trigger (fast subsampled power estimate)
                        self._check_trigger_sc16(buf[:n])

                        # Enqueue for processing — best-effort, drop if behind
                        try:
                            self._chunk_queue.put_nowait((buf, recv_time))
                        except queue.Full:
                            self._dropped_chunks += 1
                            # Return buf to pool since processing won't use it
                            with contextlib.suppress(queue.Full):
                                self._buf_pool.put_nowait(buf)

                        recv_count += 1
                        if recv_count % 50 == 0:
                            recv_ms = (t_recv_done - recv_time) * 1000
                            logger.info(
                                "TIMING recv#%d: recv=%.1fms dropped=%d (IQ=%.1fms)",
                                recv_count,
                                recv_ms,
                                self._dropped_chunks,
                                self._chunk_duration * 1000,
                            )

                    self._receiver.stop_streaming()

        except Exception:
            logger.exception("Receiver loop crashed")
        finally:
            self._chunk_queue.put(_STOP)

    def _check_trigger_sc16(self, sc16_buf: np.ndarray) -> None:
        """Check power trigger from raw SC16 data (no complex conversion)."""
        s = self._settings

        if not s.TRIGGER_ENABLED and not self._manual_trigger.is_set():
            if self._trigger_active:
                self._end_triggered_capture()
            return

        # Fast mean power estimate from int16 pairs — avoids complex conversion
        raw16 = sc16_buf.view(np.int16).reshape(-1, 2)
        # Subsample for speed (~4K samples is enough for power estimate)
        step = max(1, len(raw16) // 4096)
        sub = raw16[::step].astype(np.float32) / 32768.0
        power_sq = sub[:, 0] ** 2 + sub[:, 1] ** 2
        mean_power_db = float(10.0 * np.log10(np.mean(power_sq) + 1e-30))

        above_threshold = mean_power_db > s.TRIGGER_THRESHOLD_DB
        manual = self._manual_trigger.is_set()

        if not self._trigger_active:
            if above_threshold or manual:
                self._start_triggered_capture(sc16_buf)
        else:
            self._append_triggered_capture(sc16_buf)

            if not manual and not above_threshold:
                self._below_threshold_count += 1
                if self._below_threshold_count >= s.TRIGGER_HYSTERESIS:
                    self._end_triggered_capture()
            else:
                self._below_threshold_count = 0

    def _start_triggered_capture(self, first_sc16: np.ndarray) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        self._trigger_file = (
            f"{self._receiver.serial}-{self._settings.HOSTNAME}-{ts}-triggered.sc16"
        )
        logger.info("Triggered capture started: %s", self._trigger_file)

        # Flush pre-trigger buffer (already SC16 int32) to disk
        pre_data = self._pre_trigger_buf.read()
        pre_bytes = pre_data.tobytes()

        self._storage.save_capture(self._trigger_file, pre_bytes + first_sc16.tobytes())
        self._trigger_active = True
        self._below_threshold_count = 0

    def _append_triggered_capture(self, sc16_buf: np.ndarray) -> None:
        if self._trigger_file is None:
            return
        filepath = self._storage.storage_path / self._trigger_file
        with open(filepath, "ab") as f:
            f.write(sc16_buf.tobytes())

    def _end_triggered_capture(self) -> None:
        logger.info("Triggered capture ended: %s", self._trigger_file)
        self._trigger_active = False
        self._trigger_file = None
        self._below_threshold_count = 0

    # -- Processing thread --

    def _processing_loop(self) -> None:
        """Runs on a dedicated thread.  Converts SC16, computes PSD, detects bursts."""
        s = self._settings
        freqs = self._build_frequency_list()
        center_freq = freqs[0] if freqs else s.FREQUENCY_START

        grid_config = PSDGridConfig(
            num_bins=s.NUM_FFT_BINS,
            time_resolution_ms=s.PSD_TIME_RESOLUTION_MS,
            num_workers=self._process_workers,
        )

        burst_config = BurstDetectionConfig(
            threshold_high_db=s.BURST_THRESHOLD_HIGH_DB,
            threshold_low_ratio=s.BURST_THRESHOLD_LOW_RATIO,
            merge_freq_bins=s.BURST_MERGE_FREQ_BINS,
            merge_time_sec=s.BURST_MERGE_TIME_MS / 1000.0,
        )

        # We need the freq_axis from the first PSD computation to init the detector.
        rolling_detector: RollingBurstDetector | None = None

        try:
            while self._running:
                item = self._chunk_queue.get(timeout=2.0)
                if item is _STOP:
                    break

                sc16_buf, recv_time = item
                t0 = time.monotonic()

                # SC16 → complex conversion (done here, off the receiver thread)
                complex_chunk = convert_sc16_to_complex(sc16_buf)
                t_convert = time.monotonic()

                # Return SC16 buffer to pool
                self._buf_pool.put(sc16_buf)

                # PSD grid for this chunk
                psd_grid = compute_psd_grid(complex_chunk, s.BANDWIDTH, config=grid_config)
                t_psd = time.monotonic()

                # Initialize rolling detector on first chunk (need freq_axis)
                if rolling_detector is None:
                    if len(psd_grid.time_axis) > 1:
                        time_res_s = psd_grid.time_axis[1] - psd_grid.time_axis[0]
                    else:
                        time_res_s = s.PSD_TIME_RESOLUTION_MS / 1000.0
                    rolling_detector = RollingBurstDetector(
                        window_rows=s.BURST_WINDOW_ROWS,
                        eval_interval_rows=s.BURST_EVAL_INTERVAL_ROWS,
                        num_bins=s.NUM_FFT_BINS,
                        burst_config=burst_config,
                        center_freq_hz=float(center_freq),
                        freq_axis=psd_grid.freq_axis,
                        time_resolution_s=time_res_s,
                    )

                # Feed rolling detector
                completed_bursts = rolling_detector.feed(psd_grid)
                t_burst = time.monotonic()

                # IQ stats for this chunk
                iq_stats = calculate_iq_statistics(complex_chunk)
                t_stats = time.monotonic()

                # Summary PSD for this chunk
                summary_psd = compute_summary_psd(psd_grid, center_freq, s.BANDWIDTH)

                self._capture_count += 1
                process_ms = (time.monotonic() - t0) * 1000.0
                latency_ms = (time.monotonic() - recv_time) * 1000.0

                # Periodic processing timing report
                if self._capture_count % 50 == 0:
                    logger.info(
                        "PROC chunk#%d: convert=%.1fms psd=%.1fms "
                        "burst=%.1fms stats=%.1fms TOTAL=%.1fms "
                        "latency=%.1fms (IQ=%.1fms)",
                        self._capture_count,
                        (t_convert - t0) * 1000,
                        (t_psd - t_convert) * 1000,
                        (t_burst - t_psd) * 1000,
                        (t_stats - t_burst) * 1000,
                        process_ms,
                        latency_ms,
                        self._chunk_duration * 1000,
                    )

                result = _StreamResult(
                    summary_psd=summary_psd,
                    iq_stats=iq_stats,
                    bursts=completed_bursts,
                    psd_grid=psd_grid,
                    center_freq_hz=center_freq,
                    capture_num=self._capture_count,
                    process_ms=process_ms,
                    latency_ms=latency_ms,
                )

                # Push to event loop (non-blocking, drop if full)
                if self._loop is not None:
                    self._loop.call_soon_threadsafe(self._result_queue.put_nowait, result)

        except queue.Empty:
            pass  # timeout on chunk_queue.get — loop around and check _running
        except Exception:
            logger.exception("Processing loop crashed")
        finally:
            if self._loop is not None:
                self._loop.call_soon_threadsafe(self._result_queue.put_nowait, _STOP)

    # -- Async result consumer (event loop) --

    async def _result_consumer_loop(self) -> None:
        """Broadcast results to WebSocket, store bursts, submit to ZMS."""
        last_zms_time = time.monotonic()

        while self._running:
            try:
                result = await asyncio.wait_for(self._result_queue.get(), timeout=2.0)
            except TimeoutError:
                continue
            if result is _STOP:
                break

            # Store completed bursts
            for burst in result.bursts:
                await self._db.insert_detection(
                    burst_id=burst.burst_id,
                    start_time=burst.start_time,
                    stop_time=burst.stop_time,
                    center_freq_hz=burst.center_freq_hz,
                    bandwidth_hz=burst.bandwidth_hz,
                    peak_power_db=burst.peak_power_db,
                    duration_ms=burst.duration_ms,
                    detection_timestamp=burst.detection_timestamp,
                )

            if result.bursts:
                logger.info(
                    "Detected %d bursts at %d Hz (chunk #%d)",
                    len(result.bursts),
                    result.center_freq_hz,
                    result.capture_num,
                )

            # Broadcast to WebSocket
            if self._broadcast is not None:
                await self._broadcast.publish(
                    {
                        "type": "psd",
                        "center_freq_hz": result.center_freq_hz,
                        "bandwidth_hz": self._settings.BANDWIDTH,
                        "powers": result.summary_psd.powers,
                        "frequencies": result.summary_psd.frequencies,
                        "num_bins": result.summary_psd.num_bins,
                        "avg_power_db": result.iq_stats.average,
                        "max_power_db": result.iq_stats.max,
                        "kurtosis": result.iq_stats.kurtosis,
                        "burst_count": len(result.bursts),
                        "capture_num": result.capture_num,
                        "process_ms": result.process_ms,
                        "excess_ms": result.latency_ms,
                    }
                )

            # Periodic ZMS submission (~every 500ms)
            now = time.monotonic()
            if self._zms_monitor is not None and (now - last_zms_time) >= 0.5:
                last_zms_time = now
                try:
                    from pathlib import Path

                    from rfobserver.models import MetadataRecord, ProcessedDataEnvelope

                    meta = MetadataRecord(
                        hostname=self._settings.HOSTNAME,
                        organization=self._settings.ORGANIZATION,
                        serial=self._receiver.serial,
                        frequency=result.center_freq_hz,
                        timestamp=datetime.now(timezone.utc),
                        source_path=Path("/tmp/rfobserver/streaming"),
                        gain=self._settings.GAIN,
                        sampling_rate=self._settings.BANDWIDTH,
                    )
                    envelope = ProcessedDataEnvelope(
                        metadata=meta,
                        statistics=result.iq_stats,
                        psd_data=result.summary_psd,
                    )
                    ok = await self._zms_monitor.submit_observation(envelope)
                    if ok:
                        logger.debug("ZMS observation submitted (chunk #%d)", result.capture_num)
                except Exception:
                    logger.exception("ZMS observation submission failed")

    # -- Helpers --

    def _build_frequency_list(self) -> list[int]:
        s = self._settings
        if s.FREQUENCY_STEP <= 0 or s.FREQUENCY_END <= s.FREQUENCY_START:
            return [s.FREQUENCY_START]
        freqs: list[int] = []
        f = s.FREQUENCY_START
        while f <= s.FREQUENCY_END:
            freqs.append(f)
            f += s.FREQUENCY_STEP
        return freqs
