"""Burst detection integration tests against synthetic 2-second IQ buffers.

Drives the real ``StreamingProcessor`` with ``SyntheticBurstReceiver`` (a mock
source that serves a pre-built buffer with planted bursts at known time,
frequency, and amplitude). Asserts the detector finds the planted bursts at
the expected frequencies, and that ``BURST_THRESHOLD_HIGH_DB`` controls
detection of marginal bursts as advertised.

Amplitudes were calibrated against the live streaming pipeline (not just
``compute_psd_grid``): per-window noise-floor estimation in
``RollingBurstDetector`` plus chunked PSD evaluation gives a tighter
detection margin than the single-grid path, so the marginal-burst pair
uses thresholds (15 dB low / 35 dB high) that bracket the planted signal
robustly under streaming.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import StreamingProcessor
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.local import LocalStorage

from ._synth import Burst, SyntheticBurstReceiver, iq_to_sc16_int32, make_iq_with_bursts

if TYPE_CHECKING:
    from pathlib import Path


def _build_settings(tmp_path: Path, *, bandwidth: int, num_fft_bins: int) -> AppSettings:
    storage = tmp_path / "storage"
    storage.mkdir()
    return AppSettings(
        FREQUENCY_START=915_000_000,
        FREQUENCY_END=915_000_000,
        BANDWIDTH=bandwidth,
        DURATION_SEC=0.5,
        GAIN=35,
        NUM_FFT_BINS=num_fft_bins,
        PSD_TIME_RESOLUTION_MS=0.5,
        STREAMING_CHUNK_SLICES=10,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage),
        DB_PATH=str(tmp_path / "test.db"),
        ARCHIVE_MAX_GB=0.01,
        _env_file=None,
    )


async def _run_until_exhausted_then_drain(
    processor: StreamingProcessor,
    receiver: SyntheticBurstReceiver,
    drain_chunks: int = 600,
) -> None:
    """Run ``processor.run()`` until ``receiver`` exhausts its buffer, then
    feed zero chunks for ``drain_chunks`` more so the rolling window flushes
    completed bursts past the trailing margin.
    """

    async def stopper() -> None:
        while not receiver.exhausted:
            await asyncio.sleep(0.02)
        end_target = processor._capture_count + drain_chunks
        while processor._capture_count < end_target:
            await asyncio.sleep(0.02)
        processor.stop()

    await asyncio.wait_for(asyncio.gather(processor.run(), stopper()), timeout=30.0)


def _make_processor(
    settings: AppSettings,
    receiver: SyntheticBurstReceiver,
    db: SensorDatabase,
) -> StreamingProcessor:
    storage = LocalStorage(storage_path=settings.STORAGE_PATH, max_gb=settings.ARCHIVE_MAX_GB)
    return StreamingProcessor(
        receiver=receiver,
        database=db,
        local_storage=storage,
        settings=settings,
        # Lossless replay: the pipeline blocks instead of dropping chunks when
        # processing falls behind, so every burst-carrying chunk is processed.
        # This makes detection deterministic on slow/shared CI runners (a single
        # processing worker there used to overrun the queue and drop bursts).
        drop_on_overflow=False,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("bandwidth", "num_fft_bins"),
    [
        (1_000_000, 256),
        (10_000_000, 512),
        (25_000_000, 1024),
    ],
)
async def test_detects_known_bursts_at_multiple_bandwidths(
    tmp_path: Path, bandwidth: int, num_fft_bins: int
) -> None:
    """Three well-separated bursts in a 2-second buffer, all detected.

    Threshold is set well above the 10 dB default so noise blips don't
    pollute the assertion. Burst amplitudes calibrated to clear ~30 dB
    headroom on the PSD grid at every tested bandwidth.
    """
    bursts = [
        Burst(start_sec=0.4, duration_sec=0.05, freq_offset_hz=bandwidth * 0.20, amplitude=0.20),
        Burst(start_sec=0.9, duration_sec=0.05, freq_offset_hz=-bandwidth * 0.15, amplitude=0.20),
        Burst(start_sec=1.4, duration_sec=0.05, freq_offset_hz=bandwidth * 0.30, amplitude=0.20),
    ]
    iq = make_iq_with_bursts(duration_sec=2.0, bandwidth_hz=bandwidth, bursts=bursts)
    sc16 = iq_to_sc16_int32(iq)

    settings = _build_settings(tmp_path, bandwidth=bandwidth, num_fft_bins=num_fft_bins)
    object.__setattr__(settings, "BURST_THRESHOLD_HIGH_DB", 30.0)

    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        receiver = SyntheticBurstReceiver(
            receiver_config=ReceiverConfig(
                gain_db=settings.GAIN,
                bandwidth_hz=settings.BANDWIDTH,
                duration_sec=settings.DURATION_SEC,
            ),
            iq_int32=sc16,
            # Pacing only bounds the wall-clock feed rate; correctness no longer
            # depends on it because the processor runs lossless (see
            # _make_processor). 2x realtime keeps the test brief.
            pacing_factor=2.0,
        )
        receiver.initialize()
        processor = _make_processor(settings, receiver, db)
        await _run_until_exhausted_then_drain(processor, receiver)

        detections = await db.query_detections(limit=1000)
        # Each planted burst must have at least one detection within ±100 kHz of
        # its planted offset (looser than the bin spacing at 25 MHz so the
        # tolerance scales with BW). Frequencies are absolute (center+offset).
        center = settings.FREQUENCY_START
        for b in bursts:
            expected = center + b.freq_offset_hz
            matches = [d for d in detections if abs(d["center_freq_hz"] - expected) < 100_000]
            assert matches, (
                f"no detection within 100 kHz of planted burst at "
                f"{expected / 1e6:.3f} MHz (BW={bandwidth / 1e6:.0f} MHz); "
                f"got {len(detections)} detections at "
                f"{[round(d['center_freq_hz'] / 1e3, 1) for d in detections[:10]]}"
            )
    finally:
        await db.close()


@pytest.fixture
def marginal_burst_iq() -> tuple[bytes, int]:
    """Single moderate-headroom burst at +200 kHz in a 1 MHz / 2 s buffer.

    Amplitude calibrated against the streaming pipeline: detected at
    ``BURST_THRESHOLD_HIGH_DB=15.0``, rejected at
    ``BURST_THRESHOLD_HIGH_DB=35.0``. Returned as bytes so the fixture is
    cheap to share between two tests without regenerating noise.
    """
    iq = make_iq_with_bursts(
        duration_sec=2.0,
        bandwidth_hz=1_000_000,
        bursts=[Burst(start_sec=0.5, duration_sec=0.5, freq_offset_hz=200_000, amplitude=0.01)],
        seed=11,
    )
    sc16 = iq_to_sc16_int32(iq)
    return sc16.tobytes(), len(sc16)


async def _run_threshold_test(
    tmp_path: Path,
    sc16_bytes: tuple[bytes, int],
    threshold_high_db: float,
) -> int:
    import numpy as np

    raw, n = sc16_bytes
    sc16 = np.frombuffer(raw, dtype=np.int32).reshape(n).copy()

    settings = _build_settings(tmp_path, bandwidth=1_000_000, num_fft_bins=256)
    object.__setattr__(settings, "BURST_THRESHOLD_HIGH_DB", threshold_high_db)

    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        receiver = SyntheticBurstReceiver(
            receiver_config=ReceiverConfig(
                gain_db=settings.GAIN,
                bandwidth_hz=settings.BANDWIDTH,
                duration_sec=settings.DURATION_SEC,
            ),
            iq_int32=sc16,
            pacing_factor=2.0,
        )
        receiver.initialize()
        processor = _make_processor(settings, receiver, db)
        await _run_until_exhausted_then_drain(processor, receiver)

        detections = await db.query_detections(limit=1000)
        # Count detections near the planted offset (+200 kHz from center).
        center = settings.FREQUENCY_START
        return sum(1 for d in detections if abs(d["center_freq_hz"] - (center + 200_000)) < 80_000)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_low_threshold_detects_marginal_burst(
    tmp_path: Path, marginal_burst_iq: tuple[bytes, int]
) -> None:
    """The planted burst must be detected at threshold=15 dB."""
    near_planted = await _run_threshold_test(tmp_path, marginal_burst_iq, threshold_high_db=15.0)
    assert near_planted >= 1, "low threshold should detect the marginal burst"


@pytest.mark.asyncio
async def test_high_threshold_rejects_marginal_burst(
    tmp_path: Path, marginal_burst_iq: tuple[bytes, int]
) -> None:
    """The same buffer with threshold=35 dB must produce zero matching detections."""
    near_planted = await _run_threshold_test(tmp_path, marginal_burst_iq, threshold_high_db=35.0)
    assert near_planted == 0, (
        f"high threshold should reject the marginal burst, got {near_planted} matching"
    )


@pytest.mark.asyncio
async def test_lossless_shutdown_does_not_hang_on_full_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Stopping a lossless processor must not wedge on a full chunk queue.

    Regression: in lossless mode the receiver blocks until there is queue room,
    and a slow single worker leaves the queue full. Shutdown must still unblock
    the receiver and join all threads promptly (a blocking ``put(_STOP)`` after
    the consumer had stopped used to deadlock, freezing the event loop).
    """
    import threading
    import time

    bw = 1_000_000
    iq = make_iq_with_bursts(
        duration_sec=1.0,
        bandwidth_hz=bw,
        bursts=[Burst(start_sec=0.3, duration_sec=0.05, freq_offset_hz=bw * 0.2, amplitude=0.2)],
    )
    sc16 = iq_to_sc16_int32(iq)

    settings = _build_settings(tmp_path, bandwidth=bw, num_fft_bins=256)

    # Slow the per-chunk work and cap to one worker so the bounded queue stays
    # full behind the processor while the receiver keeps producing.
    original = StreamingProcessor._process_one_chunk

    def slow_process(self: StreamingProcessor, *args: object, **kwargs: object) -> object:
        time.sleep(0.1)
        return original(self, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(StreamingProcessor, "_process_one_chunk", slow_process)

    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        receiver = SyntheticBurstReceiver(
            receiver_config=ReceiverConfig(
                gain_db=settings.GAIN,
                bandwidth_hz=settings.BANDWIDTH,
                duration_sec=settings.DURATION_SEC,
            ),
            iq_int32=sc16,
            pacing_factor=1000.0,  # feed fast so the queue saturates immediately
        )
        receiver.initialize()
        processor = _make_processor(settings, receiver, db)
        processor._num_proc_workers = 1

        task = asyncio.create_task(processor.run())
        # Let the queue fill and stay full behind the slow worker.
        while processor._chunk_queue.qsize() < processor._chunk_queue.maxsize:
            await asyncio.sleep(0.02)

        processor.stop()
        # Must return well within the per-thread join timeout; a hang here means
        # the deadlock is back.
        await asyncio.wait_for(task, timeout=10.0)

        lingering = [
            t.name
            for t in threading.enumerate()
            if t.name in ("recv", "dispatch", "burst") and t.is_alive()
        ]
        assert not lingering, f"threads still alive after shutdown: {lingering}"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_tone_check_persists_detection(tmp_path: Path) -> None:
    """With TONE_CHECK enabled, a planted continuous tone yields detected rows."""
    bandwidth = 1_000_000
    center = 915_000_000
    offset = 200_000  # +200 kHz from center
    # A continuous strong tone spanning the whole buffer (not a short burst).
    iq = make_iq_with_bursts(
        duration_sec=2.0,
        bandwidth_hz=bandwidth,
        bursts=[Burst(start_sec=0.0, duration_sec=2.0, freq_offset_hz=offset, amplitude=0.3)],
    )
    sc16 = iq_to_sc16_int32(iq)

    settings = _build_settings(tmp_path, bandwidth=bandwidth, num_fft_bins=256)
    object.__setattr__(settings, "DURATION_SEC", 0.1)  # flush often
    object.__setattr__(settings, "TONE_CHECK_ENABLED", True)
    object.__setattr__(settings, "TONE_CHECK_FREQ_HZ", float(center + offset))
    object.__setattr__(settings, "TONE_CHECK_THRESHOLD_DB", 10.0)

    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        receiver = SyntheticBurstReceiver(
            receiver_config=ReceiverConfig(
                gain_db=settings.GAIN,
                bandwidth_hz=settings.BANDWIDTH,
                duration_sec=settings.DURATION_SEC,
            ),
            iq_int32=sc16,
            pacing_factor=4.0,
        )
        receiver.initialize()
        processor = _make_processor(settings, receiver, db)
        await _run_until_exhausted_then_drain(processor, receiver)

        rows = await db.query_tone_checks(limit=100)
        assert rows, "tone check produced no rows"
        detected = [r for r in rows if r["detected"]]
        assert detected, f"tone not detected in any interval; rows={rows[:3]}"
        r = detected[0]
        assert r["in_band"]
        assert abs(r["tone_freq_hz"] - (center + offset)) < 1e-6
        assert r["snr_db"] >= 10.0
    finally:
        await db.close()
