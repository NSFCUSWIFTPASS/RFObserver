"""Recording PSD grids are streamed by the RAM/disk flag, not hoarded in RAM."""

from __future__ import annotations

import asyncio
from pathlib import Path

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
    return Path(settings.STORAGE_PATH)


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
