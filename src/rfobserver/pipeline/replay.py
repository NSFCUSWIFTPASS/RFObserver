"""Replay a recorded SigMF capture through the streaming detection pipeline.

Assembles a minimal pipeline (replay receiver -> StreamingProcessor -> SQLite),
runs the whole capture through it losslessly, and returns the detections so a
recorded real-world capture can be validated against its known transmissions.
"""

from __future__ import annotations

import asyncio
import logging
import tempfile
from pathlib import Path
from typing import Any

from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.capture.replay_receiver import FileReplayReceiver
from rfobserver.capture.sigmf_reader import load_raw, load_sigmf
from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import StreamingProcessor
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.local import LocalStorage

logger = logging.getLogger(__name__)


def _auto_num_bins(sample_rate_hz: float, tres_ms: float, cap: int = 2048) -> int:
    """Largest power-of-two FFT size that fits in one PSD time slice (<= cap).

    A slice holds sample_rate * tres samples; the FFT can't be larger than that.
    So low-rate captures (e.g. 1 MS/s) use a smaller FFT than the 28-56 MHz field.
    """
    slice_samples = int(sample_rate_hz * tres_ms / 1000.0)
    n = 1
    while n * 2 <= min(cap, max(2, slice_samples)):
        n *= 2
    return max(n, 64)


async def run_replay(
    capture_path: str | Path,
    *,
    num_bins: int | None = None,
    threshold_db: float | None = None,
    time_resolution_ms: float = 0.2,
    limit: int = 5000,
    sample_rate_hz: float | None = None,
    center_freq_hz: float = 0.0,
    datatype: str = "ci16_le",
    max_seconds: float | None = None,
) -> dict[str, Any]:
    """Run a recorded capture through the pipeline and return its detections.

    Pass a SigMF path (``.sigmf-data``/``.sigmf-meta``/base) for a capture with a
    sidecar; for a headerless raw ``.dat`` supply ``sample_rate_hz`` (and usually
    ``center_freq_hz``/``datatype``), whose parameters live only in the filename.
    ``max_seconds`` caps how much of the head is replayed (for long captures).

    Returns ``{capture: {...}, num_bins, detections: [...]}``.
    """
    if sample_rate_hz is not None:
        cap = load_raw(
            capture_path,
            datatype=datatype,
            sample_rate_hz=sample_rate_hz,
            center_freq_hz=center_freq_hz,
        )
    else:
        cap = load_sigmf(capture_path)
    n_bins = num_bins or _auto_num_bins(cap.sample_rate_hz, time_resolution_ms)
    center = int(cap.center_freq_hz)

    max_samples = None
    if max_seconds is not None:
        max_samples = max(1, int(max_seconds * cap.sample_rate_hz))
    replay_samples = cap.num_samples if max_samples is None else min(cap.num_samples, max_samples)

    # Size the rolling window/eval to the capture. The field defaults (4096/2048
    # rows) assume continuous operation; on a short capture the detector would
    # barely evaluate on real data. For a long capture these clamp back to the
    # field values.
    defaults = AppSettings(_env_file=None)
    slice_samples = max(1, int(cap.sample_rate_hz * time_resolution_ms / 1000.0))
    capture_rows = max(1, replay_samples // slice_samples)
    eval_rows = min(defaults.BURST_EVAL_INTERVAL_ROWS, max(64, capture_rows // 4))
    window_rows = min(defaults.BURST_WINDOW_ROWS, max(2 * eval_rows + 32, capture_rows))
    logger.info(
        "Replay %s: %.3f MS/s, center %.6f MHz, %s, %.2f Ms samples, num_bins=%d",
        Path(capture_path).name,
        cap.sample_rate_hz / 1e6,
        center / 1e6,
        cap.datatype,
        replay_samples / 1e6,
        n_bins,
    )

    with tempfile.TemporaryDirectory() as tmp:
        storage_dir = Path(tmp) / "storage"
        storage_dir.mkdir()
        settings = AppSettings(
            FREQUENCY_START=center,
            FREQUENCY_END=center,
            BANDWIDTH=int(cap.sample_rate_hz),
            NUM_FFT_BINS=n_bins,
            PSD_TIME_RESOLUTION_MS=time_resolution_ms,
            BURST_WINDOW_ROWS=window_rows,
            BURST_EVAL_INTERVAL_ROWS=eval_rows,
            MOCK_RECEIVER=True,
            SENSOR_ACTIVE=True,
            STORAGE_PATH=str(storage_dir),
            DB_PATH=str(Path(tmp) / "replay.db"),
            ARCHIVE_MAX_GB=0.01,
            _env_file=None,
        )
        if threshold_db is not None:
            object.__setattr__(settings, "BURST_THRESHOLD_HIGH_DB", threshold_db)

        db = SensorDatabase(settings.DB_PATH)
        await db.connect()
        try:
            receiver = FileReplayReceiver(
                cap,
                ReceiverConfig(
                    gain_db=settings.GAIN,
                    bandwidth_hz=settings.BANDWIDTH,
                    duration_sec=settings.DURATION_SEC,
                ),
                max_samples=max_samples,
            )
            receiver.initialize()
            storage = LocalStorage(
                storage_path=settings.STORAGE_PATH, max_gb=settings.ARCHIVE_MAX_GB
            )
            processor = StreamingProcessor(
                receiver=receiver,
                database=db,
                local_storage=storage,
                settings=settings,
                drop_on_overflow=False,  # lossless: process every sample of the capture
            )
            await _drive_to_end(processor, receiver, settings)
            detections = await db.query_detections(limit=limit)
        finally:
            await db.close()

    return {
        "capture": {
            "path": str(capture_path),
            "sample_rate_hz": cap.sample_rate_hz,
            "center_freq_hz": cap.center_freq_hz,
            "datatype": cap.datatype,
            "num_samples": replay_samples,
            "duration_sec": replay_samples / cap.sample_rate_hz,
        },
        "num_bins": n_bins,
        "detections": detections,
    }


async def _drive_to_end(
    processor: StreamingProcessor, receiver: FileReplayReceiver, settings: AppSettings
) -> None:
    """Run until the capture is exhausted + the rolling window has flushed, then stop."""
    # Rows of trailing drain needed for the rolling detector to flush a burst at
    # the very end past its margin, converted to receiver chunks.
    drain_rows = settings.BURST_WINDOW_ROWS + settings.BURST_EVAL_INTERVAL_ROWS + 64
    chunk_slices = max(1, settings.STREAMING_CHUNK_SLICES)
    drain_chunks = drain_rows // chunk_slices + 3

    async def stopper() -> None:
        while not receiver.exhausted:
            await asyncio.sleep(0.02)
        target = processor._capture_count + drain_chunks
        while processor._capture_count < target:
            await asyncio.sleep(0.02)
        await asyncio.sleep(0.2)
        processor.stop()

    # Generous ceiling; a 5-minute capture at 40 MS/s is a lot of FFTs.
    await asyncio.wait_for(asyncio.gather(processor.run(), stopper()), timeout=3600.0)
