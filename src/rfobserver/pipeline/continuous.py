"""Continuous capture -> process -> detect -> store -> publish loop.

Each iteration:
1. Capture IQ samples from receiver
2. Convert to complex, compute IQ statistics
3. Compute PSD grid (high-res) and summary PSD
4. Run burst detection on PSD grid
5. Store detections in local SQLite
6. Broadcast PSD to WebSocket subscribers
"""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from rfobserver.processing.burst import BurstDetectionConfig, detect_bursts
from rfobserver.processing.iq_utils import calculate_iq_statistics, convert_bytes_to_complex
from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid, compute_summary_psd

if TYPE_CHECKING:
    from datetime import datetime

    from rfobserver.capture.receiver import CaptureResult, IReceiver
    from rfobserver.config import AppSettings
    from rfobserver.models import BurstFingerprint, IQStatistics, PSDData
    from rfobserver.storage.database import SensorDatabase
    from rfobserver.storage.local import LocalStorage
    from rfobserver.web.websocket import LiveBroadcast

logger = logging.getLogger(__name__)


class ContinuousProcessor:
    """Main processing loop."""

    def __init__(
        self,
        receiver: IReceiver,
        database: SensorDatabase,
        local_storage: LocalStorage,
        settings: AppSettings,
        broadcast: LiveBroadcast | None = None,
    ) -> None:
        self._receiver = receiver
        self._db = database
        self._storage = local_storage
        self._settings = settings
        self._broadcast = broadcast
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._capture_count = 0
        self._running = False

    async def run(self) -> None:
        """Run the continuous processing loop with overlapped capture/processing."""
        self._running = True
        s = self._settings
        freqs = self._build_frequency_list()

        logger.info(
            "Starting continuous loop: %d frequencies, %s Hz bandwidth",
            len(freqs),
            s.BANDWIDTH,
        )

        pending_process: asyncio.Task[None] | None = None

        while self._running:
            for center_freq in freqs:
                if not self._running:
                    break
                try:
                    # 1. Capture (blocks for DURATION_SEC on real hardware)
                    result = await self._receiver.receive_samples(center_freq)

                    # 2. Wait for previous processing to finish before starting new one
                    if pending_process is not None:
                        await pending_process
                        pending_process = None

                    # 3. Kick off processing while next capture can proceed
                    pending_process = asyncio.create_task(
                        self._process_and_broadcast(result, center_freq)
                    )
                except Exception:
                    logger.exception("Error in capture at %d Hz", center_freq)
                    await asyncio.sleep(1.0)

        # Drain any pending work
        if pending_process is not None:
            await pending_process

    def stop(self) -> None:
        self._running = False

    async def _process_and_broadcast(self, result: CaptureResult, center_freq_hz: int) -> None:
        """Process a completed capture: compute, detect, store, broadcast."""
        s = self._settings
        capture_time = result.raw_capture.capture_timestamp
        iq_bytes = result.raw_capture.iq_data_bytes

        self._capture_count += 1
        logger.debug(
            "Capture #%d at %d Hz (%d bytes)",
            self._capture_count,
            center_freq_hz,
            len(iq_bytes),
        )

        # 1. Save raw file
        filename = (
            f"{self._receiver.serial}-{s.HOSTNAME}-{capture_time.strftime('%Y%m%dT%H%M%S')}.sc16"
        )
        self._storage.save_capture(filename, iq_bytes)

        # 2. Process (CPU-bound, run in executor)
        loop = asyncio.get_running_loop()
        iq_stats, summary_psd, bursts = await loop.run_in_executor(
            self._executor,
            _process_capture_blocking,
            iq_bytes,
            s.BANDWIDTH,
            center_freq_hz,
            s.NUM_FFT_BINS,
            s.PSD_TIME_RESOLUTION_MS,
            s.BURST_THRESHOLD_HIGH_DB,
            s.BURST_THRESHOLD_LOW_RATIO,
            s.BURST_MERGE_FREQ_BINS,
            s.BURST_MERGE_TIME_MS,
            capture_time,
        )

        # 4. Store detections in SQLite
        for burst in bursts:
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

        if bursts:
            logger.info(
                "Detected %d bursts at %d Hz (capture #%d)",
                len(bursts),
                center_freq_hz,
                self._capture_count,
            )

        # 5. Broadcast PSD to WebSocket subscribers
        if self._broadcast is not None:
            await self._broadcast.publish(
                {
                    "type": "psd",
                    "center_freq_hz": center_freq_hz,
                    "bandwidth_hz": s.BANDWIDTH,
                    "powers": summary_psd.powers,
                    "frequencies": summary_psd.frequencies,
                    "num_bins": summary_psd.num_bins,
                    "avg_power_db": iq_stats.average,
                    "max_power_db": iq_stats.max,
                    "kurtosis": iq_stats.kurtosis,
                    "burst_count": len(bursts),
                    "capture_num": self._capture_count,
                }
            )

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


def _process_capture_blocking(
    iq_bytes: bytes,
    sampling_rate: int,
    center_freq_hz: int,
    num_fft_bins: int,
    time_resolution_ms: float,
    threshold_high_db: float,
    threshold_low_ratio: float,
    merge_freq_bins: int,
    merge_time_ms: float,
    capture_time: datetime,
) -> tuple[IQStatistics, PSDData, list[BurstFingerprint]]:
    """CPU-bound processing: IQ stats, PSD grid, burst detection."""
    data = convert_bytes_to_complex(iq_bytes)
    iq_stats = calculate_iq_statistics(data)

    grid_config = PSDGridConfig(
        num_bins=num_fft_bins,
        time_resolution_ms=time_resolution_ms,
    )
    psd_grid = compute_psd_grid(data, sampling_rate, config=grid_config)
    summary_psd = compute_summary_psd(psd_grid, center_freq_hz, sampling_rate)

    burst_config = BurstDetectionConfig(
        threshold_high_db=threshold_high_db,
        threshold_low_ratio=threshold_low_ratio,
        merge_freq_bins=merge_freq_bins,
        merge_time_sec=merge_time_ms / 1000.0,
    )
    detection_result = detect_bursts(
        psd_grid,
        config=burst_config,
        center_freq_hz=float(center_freq_hz),
        capture_time=capture_time,
    )

    return iq_stats, summary_psd, detection_result.bursts
