"""Recording PSD grids are streamed by the RAM/disk flag, not hoarded in RAM."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import numpy as np
import pytest

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import StreamingProcessor
from rfobserver.storage import psd_grid
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.local import LocalStorage


def _settings(tmp_path: Path, **kw) -> AppSettings:
    storage = tmp_path / "st"
    storage.mkdir()
    base = dict(
        FREQUENCY_START=915_000_000,
        FREQUENCY_END=915_000_000,
        BANDWIDTH=2_000_000,
        DURATION_SEC=0.5,
        GAIN=30,
        NUM_FFT_BINS=256,
        PSD_TIME_RESOLUTION_MS=0.5,
        STREAMING_CHUNK_SLICES=10,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage),
        DB_PATH=str(tmp_path / "t.db"),
        ARCHIVE_MAX_GB=0.01,
        _env_file=None,
    )
    base.update(kw)
    return AppSettings(**base)


async def _record_briefly(settings: AppSettings, db: SensorDatabase) -> Path:
    receiver = MockReceiver(
        ReceiverConfig(
            gain_db=settings.GAIN,
            bandwidth_hz=settings.BANDWIDTH,
            duration_sec=settings.DURATION_SEC,
        )
    )
    receiver.initialize()
    storage = LocalStorage(storage_path=settings.STORAGE_PATH, max_gb=settings.ARCHIVE_MAX_GB)
    proc = StreamingProcessor(
        receiver=receiver, database=db, local_storage=storage, settings=settings
    )

    async def driver() -> None:
        for _ in range(500):  # wait for streaming to start
            if proc._capture_count > 2:
                break
            await asyncio.sleep(0.02)
        proc.start_recording()
        while proc._capture_count < 14:  # let some chunks accumulate grids
            await asyncio.sleep(0.02)
        proc.stop_recording()
        await asyncio.sleep(0.1)
        proc.stop()

    await asyncio.wait_for(asyncio.gather(proc.run(), driver()), timeout=30.0)
    # Manual start_recording() writes into the manual/ subdir.
    return Path(settings.STORAGE_PATH) / "manual"


async def _record_with_preroll(
    settings: AppSettings, db: SensorDatabase, *, preroll_to: int, record_to: int
) -> Path:
    """Let ``preroll_to`` chunks accumulate (filling the pre-trigger buffers)
    BEFORE starting the recording, then record only up to ``record_to`` — so the
    pre-roll dominates and a missing pre-roll PSD is obvious."""
    receiver = MockReceiver(
        ReceiverConfig(
            gain_db=settings.GAIN,
            bandwidth_hz=settings.BANDWIDTH,
            duration_sec=settings.DURATION_SEC,
        )
    )
    receiver.initialize()
    storage = LocalStorage(storage_path=settings.STORAGE_PATH, max_gb=settings.ARCHIVE_MAX_GB)
    proc = StreamingProcessor(
        receiver=receiver, database=db, local_storage=storage, settings=settings
    )

    async def driver() -> None:
        for _ in range(2000):
            if proc._capture_count >= preroll_to:
                break
            await asyncio.sleep(0.01)
        proc.start_recording()
        while proc._capture_count < record_to:
            await asyncio.sleep(0.01)
        proc.stop_recording()
        await asyncio.sleep(0.1)
        proc.stop()

    await asyncio.wait_for(asyncio.gather(proc.run(), driver()), timeout=30.0)
    return Path(settings.STORAGE_PATH) / "manual"


def _assert_psd_covers_iq(storage_dir: Path, bandwidth_hz: int) -> None:
    """The .psd grid time span should cover ~all of the recorded IQ, pre-roll
    included. Without the pre-trigger PSD buffer the grid would start only at
    the recording trigger and be far shorter than the (pre-roll + recorded) IQ."""
    sc16 = next(storage_dir.glob("*.sc16"))
    # load_grid memmaps at the sidecar's declared shape, so a sidecar claiming
    # more rows than the file holds raises "mmap length is greater than file
    # size" and the Captures page fails outright. Check the file agrees before
    # anything else.
    raw_path, _ = psd_grid.grid_paths(sc16)
    raw_meta = json.loads(sc16.with_suffix(".psd.json").read_text())
    declared = int(raw_meta["rows"]) * int(raw_meta["num_bins"]) * 4
    assert raw_path.stat().st_size == declared, (
        f".psd is {raw_path.stat().st_size} bytes but the sidecar declares {declared}"
    )

    loaded = psd_grid.load_grid(sc16)
    assert loaded is not None
    mm, meta = loaded
    grid_span = mm.shape[0] * float(meta["time_resolution_s"])
    iq_span = (sc16.stat().st_size // 4) / bandwidth_hz
    assert mm.shape[0] == meta["rows"] > 0
    # Two-sided: the one-sided >= 0.85 bound this used to carry let a capture
    # whose grid was 1.25x the IQ span (and 4 chunks out of step with it) pass.
    # Grid rows are now trimmed to the recorded sample range, so the spans match.
    assert 0.9 * iq_span <= grid_span <= 1.1 * iq_span, (
        f"grid {grid_span:.4f}s vs IQ {iq_span:.4f}s"
    )

    # The reported capture duration spans the full recorded signal (pre-trigger
    # pre-roll + post-trigger), so it matches the .sc16 length rather than only
    # the post-trigger wall-clock.
    cap_meta = json.loads(sc16.with_suffix(".json").read_text())
    reported = float(cap_meta["duration_sec"])
    file_span = float(cap_meta["total_samples"]) / float(cap_meta["sample_rate_hz"])
    assert abs(reported - file_span) < 0.02, f"duration {reported:.4f}s vs file {file_span:.4f}s"


@pytest.mark.asyncio
async def test_disk_mode_psd_covers_pretrigger(tmp_path: Path) -> None:
    settings = _settings(tmp_path, RECORDING_RAM_BUFFER=False, TRIGGER_PRE_SEC=1.0)
    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        storage_dir = await _record_with_preroll(settings, db, preroll_to=20, record_to=25)
    finally:
        await db.close()
    _assert_psd_covers_iq(storage_dir, settings.BANDWIDTH)


@pytest.mark.asyncio
async def test_ram_mode_psd_covers_pretrigger(tmp_path: Path) -> None:
    settings = _settings(
        tmp_path, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=30.0, TRIGGER_PRE_SEC=1.0
    )
    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        storage_dir = await _record_with_preroll(settings, db, preroll_to=20, record_to=25)
    finally:
        await db.close()
    _assert_psd_covers_iq(storage_dir, settings.BANDWIDTH)


@pytest.mark.asyncio
async def test_recording_inserts_iq_capture_row(tmp_path: Path) -> None:
    from datetime import datetime, timedelta

    settings = _settings(tmp_path, RECORDING_RAM_BUFFER=False, TRIGGER_PRE_SEC=0.5)
    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        storage_dir = await _record_with_preroll(settings, db, preroll_to=15, record_to=25)
        await asyncio.sleep(0.3)  # let the loop-scheduled insert run
        sc16 = next(storage_dir.glob("*.sc16"))
        cap_meta = json.loads(sc16.with_suffix(".json").read_text())
        start = datetime.fromisoformat(cap_meta["start_time"])
        rows = await db.query_iq_captures(
            since=start - timedelta(seconds=5), until=start + timedelta(seconds=10)
        )
        assert len(rows) == 1, "recording must insert exactly one iq_captures row"
        r = rows[0]
        assert r["filename"] == sc16.name
        assert r["origin"] == "manual"  # start_recording() is a manual capture
        assert abs(r["duration_sec"] - float(cap_meta["duration_sec"])) < 0.01
        # Tuning matches the settings-derived values avg_windows carry.
        assert r["sample_rate_hz"] == float(settings.BANDWIDTH)
        assert r["gain_db"] == float(settings.GAIN)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_disk_mode_writes_psd_not_npz(tmp_path: Path) -> None:
    settings = _settings(tmp_path, RECORDING_RAM_BUFFER=False)
    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        storage_dir = await _record_briefly(settings, db)
    finally:
        await db.close()
    sc16 = next(storage_dir.glob("*.sc16"))
    raw, meta = psd_grid.grid_paths(sc16)
    assert raw.exists() and meta.exists(), "disk-mode recording must write .psd + .psd.json"
    assert not list(storage_dir.glob("*.npz")), "no legacy .npz for new recordings"
    loaded = psd_grid.load_grid(sc16)
    assert loaded is not None
    mm, m = loaded
    assert mm.shape[0] == m["rows"] > 0
    assert mm.shape[1] == 256


@pytest.mark.asyncio
async def test_ram_mode_also_writes_psd(tmp_path: Path) -> None:
    settings = _settings(tmp_path, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=30.0)
    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        storage_dir = await _record_briefly(settings, db)
    finally:
        await db.close()
    sc16 = next(storage_dir.glob("*.sc16"))
    loaded = psd_grid.load_grid(sc16)
    assert loaded is not None, "RAM-mode recording must also write the .psd companion"
    mm, m = loaded
    assert mm.shape[0] == m["rows"] > 0
    assert not list(storage_dir.glob("*.npz"))


@pytest.mark.asyncio
async def test_psd_rows_align_with_the_iq_they_describe(tmp_path: Path) -> None:
    """The .psd must describe the same samples as the .sc16.

    Regression test for the ~820 ms misalignment fixed in b281ef2: PSD grids
    emerge from behind the chunk queue and worker pool while the IQ pre-roll is
    written synchronously, so selecting grids by arrival order put the two
    files out of step. A span-length check cannot see this (the spans were
    within 25% of each other while the content was 4 chunks apart); only
    correlating the content can.

    STREAMING_CHUNK_SLICES=200 and TRIGGER_PRE_SEC=0.2 are both load-bearing.
    The defect exists only where the pipeline latency (about 4 chunks) exceeds
    the pre-roll window. This file's other tests use 10 slices (a 4.5 ms chunk,
    so ~18 ms of latency) and a 1.0 s pre-roll: either of those alone hides the
    bug completely. See docs/debugging/2026-09-22_trigger-psd-iq-misalignment.md.
    """
    settings = _settings(
        tmp_path,
        RECORDING_RAM_BUFFER=False,
        STREAMING_CHUNK_SLICES=200,
        TRIGGER_PRE_SEC=0.2,
    )
    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        storage_dir = await _record_with_preroll(settings, db, preroll_to=8, record_to=16)
    finally:
        await db.close()

    sc16 = next(storage_dir.glob("*.sc16"))
    pmeta = json.loads(sc16.with_suffix(".psd.json").read_text())
    # Derive the fallbacks so this test still RUNS against pre-fix code, where
    # the sidecar carries neither field, and fails on alignment rather than on
    # a KeyError.
    slice_samples = int(
        pmeta.get("slice_samples") or round(float(pmeta["time_resolution_s"]) * settings.BANDWIDTH)
    )
    offset = int(pmeta.get("start_sample_offset", 0))

    iq = np.fromfile(sc16, dtype=np.int32).view(np.int16).astype(np.float32).reshape(-1, 2)
    iq = iq[offset:] / 32768.0
    nrows = min(len(iq) // slice_samples, int(pmeta["rows"]))
    assert nrows > 20, f"capture too short to test alignment ({nrows} rows)"

    usable = nrows * slice_samples
    power = iq[:usable, 0] ** 2 + iq[:usable, 1] ** 2
    a = 10 * np.log10(power.reshape(nrows, slice_samples).mean(axis=1) + 1e-30)
    grid = np.fromfile(sc16.with_suffix(".psd"), dtype=np.float32)
    b = grid.reshape(-1, int(pmeta["num_bins"]))[:nrows].mean(axis=1)

    a = (a - a.mean()) / (a.std() + 1e-12)
    b = (b - b.mean()) / (b.std() + 1e-12)
    best_lag, best_corr = 0, -2.0
    for lag in range(-nrows + 5, nrows - 5):
        if lag >= 0:
            x, y = a[: nrows - lag], b[lag:nrows]
        else:
            x, y = a[-lag:nrows], b[: nrows + lag]
        # Require a substantial overlap. A few dozen rows correlate above 0.7 by
        # chance, so without this the argmax can land on a near-total shift with
        # a 25-row window and report a misalignment that is not there. A quarter
        # of the capture still admits the pre-fix offset (1154 rows of 2046,
        # leaving 892 overlapping) while excluding the degenerate windows.
        if len(x) < max(50, nrows // 4) or x.std() < 1e-9 or y.std() < 1e-9:
            continue
        c = float(np.corrcoef(x, y)[0, 1])
        if c > best_corr:
            best_lag, best_corr = lag, c

    # Check correlation first: a flat signal must fail as "cannot test" rather
    # than silently satisfying the lag assertion on noise.
    assert best_corr > 0.5, f"PSD does not describe the IQ at any lag (best {best_corr:.2f})"
    assert abs(best_lag) <= 1, f"PSD is {best_lag} rows out of step with the IQ"
