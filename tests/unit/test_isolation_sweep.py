"""The sweep pipeline survives an isolation build failure and always tears the
stage and the attribution worker down (I-5)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.continuous import ContinuousProcessor
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.local import LocalStorage

if TYPE_CHECKING:
    from pathlib import Path


async def _processor(tmp_path: Path, **kw):
    storage = tmp_path / "storage"
    storage.mkdir()
    s = AppSettings(
        FREQUENCY_START=915_000_000,
        FREQUENCY_END=915_000_000,
        BANDWIDTH=1_000_000,
        DURATION_SEC=0.001,
        GAIN=35,
        NUM_FFT_BINS=64,
        PSD_TIME_RESOLUTION_MS=0.5,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage),
        DB_PATH=str(tmp_path / "t.db"),
        ARCHIVE_MAX_GB=0.01,
        ISOLATION_ENABLED=True,
        _env_file=None,
        **kw,
    )
    rx = MockReceiver(
        receiver_config=ReceiverConfig(gain_db=35, bandwidth_hz=s.BANDWIDTH, duration_sec=0.001)
    )
    rx.initialize()
    db = SensorDatabase(s.DB_PATH)
    await db.connect()
    proc = ContinuousProcessor(
        receiver=rx,
        database=db,
        local_storage=LocalStorage(storage_path=s.STORAGE_PATH, max_gb=s.ARCHIVE_MAX_GB),
        settings=s,
    )
    return proc, db


@pytest.mark.asyncio
async def test_isolation_build_failure_leaves_the_sweep_running(tmp_path, monkeypatch):
    def boom(*a, **k):
        raise PermissionError("storage path is not writable")

    monkeypatch.setattr("rfobserver.pipeline.isolation.build_isolation", boom)
    proc, db = await _processor(tmp_path)
    try:

        async def stop_after() -> None:
            while proc._capture_count < 2:
                await asyncio.sleep(0.01)
            proc.stop()

        await asyncio.wait_for(asyncio.gather(proc.run(), stop_after()), timeout=10.0)
        st = proc.isolation_status()
        assert proc._capture_count >= 2
        assert st["enabled"] is False
        assert "not writable" in st["disabled_reason"]
    finally:
        await db.close()


class _FakeStage:
    def __init__(self) -> None:
        self.started = self.stopped = False
        self.stats = None

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True


class _FakeWorker:
    def __init__(self) -> None:
        self.stopped = False
        self.queue = None

    def stop(self) -> None:
        self.stopped = True

    async def run(self) -> None:
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_a_cancelled_sweep_still_stops_the_stage_and_the_worker(tmp_path, monkeypatch):
    stage, worker = _FakeStage(), _FakeWorker()
    monkeypatch.setattr(
        "rfobserver.pipeline.isolation.build_isolation", lambda *a, **k: (stage, worker, None)
    )
    proc, db = await _processor(tmp_path)
    try:
        task = asyncio.create_task(proc.run())
        while proc._capture_count < 1:
            await asyncio.sleep(0.01)
        attrib_task = proc._attrib_task
        assert stage.started and attrib_task is not None
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stage.stopped and worker.stopped
        assert attrib_task.done()
    finally:
        await db.close()
