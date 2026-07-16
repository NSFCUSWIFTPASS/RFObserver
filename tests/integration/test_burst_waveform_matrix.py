"""Wideband-burst detection matrix against the full StreamingProcessor.

The goal is deployment confidence: for the burst modes the sensor sees in the
field (a grid of pulse lengths x occupied bandwidths, at the two field sample
rates, placed at various offsets within the band), verify the DEPLOYED
configuration -- the real field ``AppSettings`` grid defaults -- detects each
burst and measures its duration, center frequency, and bandwidth within the
resolution that config actually provides.

Each burst is a constant-envelope multitone comb (see make_iq_with_wideband_burst)
driven through the real receiver -> dispatch -> rolling burst detection ->
SQLite path. Tolerances are derived from the field grid's own resolution
(FFT bin spacing and PSD time resolution): tight where the config can resolve
the burst, deliberately loose at the corners it physically cannot (a 50 kHz
burst is only ~2 FFT bins wide in a 28-56 MHz span, so its bandwidth is
resolution-limited, not a detector error).
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

from ._synth import (
    SyntheticBurstReceiver,
    iq_to_sc16_int32,
    make_iq_with_wideband_burst,
)

if TYPE_CHECKING:
    from pathlib import Path

SDR_CENTER_HZ = 915_000_000
# Calibrated comb defaults (see make_iq_with_wideband_burst): a per-tone
# amplitude of 0.02 puts every combo's per-bin power ~30 dB above the noise
# floor without the widest comb clipping the SC16 range; threshold 15 dB then
# yields one clean detection per combo at the field FFT resolution.
DEFAULT_PER_TONE_AMP = 0.02
DEFAULT_THRESHOLD_HIGH_DB = 15.0
# Upper bound on how long to wait for the planted burst to be persisted before
# giving up (so a genuine miss still ends and fails its assertion). Generous for
# the heaviest long-burst combos; the poll returns as soon as the burst lands.
SETTLE_TIMEOUT_SEC = 120.0

SAMPLE_RATES = [28_000_000, 56_000_000]
OCCUPIED_BWS = [50_000, 150_000, 500_000, 2_000_000, 20_000_000]
DURATIONS_MS = [1.3, 2.7, 10.24, 83.2, 393.1]


def max_offset_hz(fs: int, occupied_bw_hz: float) -> float:
    """Largest |offset| keeping the occupied band clear of the +/-Fs/2 edge."""
    return 0.45 * fs - occupied_bw_hz / 2.0


def field_settings(tmp_path: Path, fs: int, threshold_high_db: float) -> AppSettings:
    """Deployment config: the real field grid defaults, tuned only to this SDR.

    Pulls NUM_FFT_BINS / PSD_TIME_RESOLUTION_MS / window / eval / chunk from the
    actual ``AppSettings`` defaults so the test validates what the sensor runs.
    """
    defaults = AppSettings(_env_file=None)
    storage = tmp_path / "storage"
    storage.mkdir()
    settings = AppSettings(
        FREQUENCY_START=SDR_CENTER_HZ,
        FREQUENCY_END=SDR_CENTER_HZ,
        BANDWIDTH=fs,
        DURATION_SEC=0.5,
        GAIN=35,
        NUM_FFT_BINS=defaults.NUM_FFT_BINS,
        PSD_TIME_RESOLUTION_MS=defaults.PSD_TIME_RESOLUTION_MS,
        STREAMING_CHUNK_SLICES=defaults.STREAMING_CHUNK_SLICES,
        BURST_WINDOW_ROWS=defaults.BURST_WINDOW_ROWS,
        BURST_EVAL_INTERVAL_ROWS=defaults.BURST_EVAL_INTERVAL_ROWS,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage),
        DB_PATH=str(tmp_path / "test.db"),
        ARCHIVE_MAX_GB=0.01,
        _env_file=None,
    )
    object.__setattr__(settings, "BURST_THRESHOLD_HIGH_DB", threshold_high_db)
    return settings


def select_burst(detections: list[dict], *, center_hz: float, occupied_bw_hz: float) -> dict | None:
    """Pick the detection best matching the planted burst (freq overlap, then power).

    The floor exceeds assert_burst_measured's center tolerance (4 * bin spacing,
    up to ~219 kHz at 56 MHz / 1024 bins) so a detection the center assertion
    would accept is never filtered out here and misreported as "no detection".
    """
    tol = max(occupied_bw_hz, 250_000.0)
    candidates = [d for d in detections if abs(d["center_freq_hz"] - center_hz) <= tol]
    if not candidates:
        return None
    return max(candidates, key=lambda d: d["peak_power_db"])


async def drive_until_detected(
    processor: StreamingProcessor,
    receiver: SyntheticBurstReceiver,
    db: SensorDatabase,
    *,
    center_hz: float,
    occupied_bw_hz: float,
    timeout_sec: float = SETTLE_TIMEOUT_SEC,
) -> None:
    """Run the pipeline until the planted burst is persisted, then stop it.

    Stopping is condition-based, not count-based: after the receiver exhausts
    its planted buffer it keeps returning (noise-floor-matched) drain noise, so
    the rolling detector advances past the burst's trailing margin and flushes
    the completed burst while we poll the DB. We stop as soon as it appears (or
    after ``timeout_sec`` so a genuine miss still ends and fails its assertion).
    """

    async def stopper() -> None:
        while not receiver.exhausted:
            await asyncio.sleep(0.02)
        max_polls = int(timeout_sec / 0.05)
        for _ in range(max_polls):
            detections = await db.query_detections(limit=5000)
            if select_burst(detections, center_hz=center_hz, occupied_bw_hz=occupied_bw_hz):
                break
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.2)  # small settle so co-completed rows drain too
        processor.stop()

    await asyncio.wait_for(asyncio.gather(processor.run(), stopper()), timeout=timeout_sec + 30.0)


async def run_combo(
    tmp_path: Path,
    *,
    fs: int,
    occupied_bw_hz: float,
    duration_ms: float,
    offset_hz: float,
    per_tone_amp: float = DEFAULT_PER_TONE_AMP,
    threshold_high_db: float = DEFAULT_THRESHOLD_HIGH_DB,
) -> list[dict]:
    """Generate one comb burst, stream it through the field pipeline, return detections."""
    settings = field_settings(tmp_path, fs, threshold_high_db)
    duration_sec = duration_ms / 1000.0
    # Margins scale with the burst so it occupies well under half the buffer's
    # time; otherwise a long burst dominates the per-bin noise floor.
    margin_sec = max(0.02, 0.75 * duration_sec)
    buffer_sec = margin_sec + duration_sec + margin_sec

    iq = make_iq_with_wideband_burst(
        duration_sec=buffer_sec,
        sample_rate_hz=fs,
        burst_start_sec=margin_sec,
        burst_duration_sec=duration_sec,
        burst_bw_hz=occupied_bw_hz,
        burst_offset_hz=offset_hz,
        num_bins=settings.NUM_FFT_BINS,
        per_tone_amp=per_tone_amp,
    )
    sc16 = iq_to_sc16_int32(iq)

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
            pacing_factor=50.0,  # feed fast; lossless mode keeps it correct
        )
        receiver.initialize()
        storage = LocalStorage(storage_path=settings.STORAGE_PATH, max_gb=settings.ARCHIVE_MAX_GB)
        processor = StreamingProcessor(
            receiver=receiver,
            database=db,
            local_storage=storage,
            settings=settings,
            drop_on_overflow=False,
        )
        await drive_until_detected(
            processor,
            receiver,
            db,
            center_hz=SDR_CENTER_HZ + offset_hz,
            occupied_bw_hz=occupied_bw_hz,
        )
        return await db.query_detections(limit=5000)
    finally:
        await db.close()


def assert_burst_measured(
    burst: dict,
    *,
    fs: int,
    occupied_bw_hz: float,
    duration_ms: float,
    offset_hz: float,
) -> None:
    """Assert duration / center / bandwidth within field-resolution tolerances."""
    bin_spacing = fs / AppSettings(_env_file=None).NUM_FFT_BINS
    tres_ms = AppSettings(_env_file=None).PSD_TIME_RESOLUTION_MS

    # Duration: PSD time quantization dominates the short bursts; a percentage
    # floor covers the longer ones.
    dur_tol_ms = max(6.0 * tres_ms, 0.10 * duration_ms)
    assert abs(burst["duration_ms"] - duration_ms) <= dur_tol_ms, (
        f"duration {burst['duration_ms']:.3f} vs {duration_ms} (tol {dur_tol_ms:.3f} ms)"
    )

    # Center frequency: within a few FFT bins.
    ctr_tol = 4.0 * bin_spacing
    expected_center = SDR_CENTER_HZ + offset_hz
    assert abs(burst["center_freq_hz"] - expected_center) <= ctr_tol, (
        f"center {burst['center_freq_hz']:.0f} vs {expected_center:.0f} (tol {ctr_tol:.0f} Hz)"
    )

    # Bandwidth: bin quantization + the detection skirt. The 8-bin floor is what
    # makes the narrow-in-wide corners (50 kHz ~= 2 bins) pass -- their bandwidth
    # is resolution-limited by the field config, not a detector error.
    bw_tol = max(8.0 * bin_spacing, 0.20 * occupied_bw_hz)
    assert abs(burst["bandwidth_hz"] - occupied_bw_hz) <= bw_tol, (
        f"bandwidth {burst['bandwidth_hz']:.0f} vs {occupied_bw_hz} (tol {bw_tol:.0f} Hz)"
    )


def _is_slow(duration_ms: float) -> bool:
    """The ~400 ms modes stream the largest buffers; gate them behind --runslow."""
    return duration_ms >= 393.0


def _combo_offset(fs: int, bw: float, index: int) -> float:
    """A deterministic, non-zero, sign-alternating offset that fits the band."""
    fracs = [0.3, -0.35, 0.5, -0.25, 0.4]
    return fracs[index % len(fracs)] * max_offset_hz(fs, bw)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fs", "occupied_bw_hz", "duration_ms"),
    [
        pytest.param(
            fs,
            bw,
            dur,
            id=f"fs{fs // 1_000_000}M-bw{int(bw)}-dur{dur}ms",
            marks=pytest.mark.slow if _is_slow(dur) else [],
        )
        for fs in SAMPLE_RATES
        for bw in OCCUPIED_BWS
        for dur in DURATIONS_MS
    ],
)
async def test_burst_matrix(
    tmp_path: Path, fs: int, occupied_bw_hz: float, duration_ms: float
) -> None:
    index = OCCUPIED_BWS.index(occupied_bw_hz) + DURATIONS_MS.index(duration_ms)
    offset = _combo_offset(fs, occupied_bw_hz, index)

    detections = await run_combo(
        tmp_path,
        fs=fs,
        occupied_bw_hz=occupied_bw_hz,
        duration_ms=duration_ms,
        offset_hz=offset,
    )
    burst = select_burst(
        detections, center_hz=SDR_CENTER_HZ + offset, occupied_bw_hz=occupied_bw_hz
    )
    assert burst is not None, (
        f"no detection near planted burst (fs={fs}, bw={occupied_bw_hz}, "
        f"dur={duration_ms}); got {len(detections)} detections"
    )
    assert_burst_measured(
        burst,
        fs=fs,
        occupied_bw_hz=occupied_bw_hz,
        duration_ms=duration_ms,
        offset_hz=offset,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("offset_frac", [-0.4, 0.0, 0.4])
async def test_offset_sweep_center_arithmetic(tmp_path: Path, offset_frac: float) -> None:
    """One combo swept across offsets proves center = SDR_center + offset."""
    fs, bw, dur = 28_000_000, 500_000, 10.24
    offset = offset_frac * max_offset_hz(fs, bw)

    detections = await run_combo(
        tmp_path, fs=fs, occupied_bw_hz=bw, duration_ms=dur, offset_hz=offset
    )
    burst = select_burst(detections, center_hz=SDR_CENTER_HZ + offset, occupied_bw_hz=bw)
    assert burst is not None, f"no detection at offset {offset:.0f} Hz"
    assert_burst_measured(burst, fs=fs, occupied_bw_hz=bw, duration_ms=dur, offset_hz=offset)


@pytest.mark.asyncio
async def test_smoke_wideband_combo(tmp_path: Path) -> None:
    """Fast representative combo for the default (non-slow) suite."""
    fs, bw, dur = 28_000_000, 500_000, 10.24
    offset = 0.3 * max_offset_hz(fs, bw)
    detections = await run_combo(
        tmp_path, fs=fs, occupied_bw_hz=bw, duration_ms=dur, offset_hz=offset
    )
    burst = select_burst(detections, center_hz=SDR_CENTER_HZ + offset, occupied_bw_hz=bw)
    assert burst is not None, f"no detection near planted burst; got {len(detections)} detections"
    assert_burst_measured(burst, fs=fs, occupied_bw_hz=bw, duration_ms=dur, offset_hz=offset)
