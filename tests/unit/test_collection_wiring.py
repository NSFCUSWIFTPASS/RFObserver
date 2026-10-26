"""Collection statistics: what the storage evictions, the recorder and the
receiver loop add."""

from __future__ import annotations

import os
import threading
import time
from typing import TYPE_CHECKING

import pytest

from rfobserver.storage.collection import CollectionStats
from rfobserver.storage.local import LocalStorage
from tests.unit.test_recording_storage import _proc

if TYPE_CHECKING:
    from pathlib import Path

MB = 1024**2


def _cap(ls: LocalStorage, name: str, size: int, mtime: float) -> Path:
    """An auto/ capture: an .sc16 of ``size`` bytes plus a 100-byte .json."""
    sc16 = ls.auto_dir / f"{name}.sc16"
    sc16.write_bytes(b"\0" * size)
    (ls.auto_dir / f"{name}.json").write_bytes(b"x" * 100)
    os.utime(sc16, (mtime, mtime))
    ls.track(sc16)
    return sc16


def _recorder(ls: LocalStorage) -> list[tuple[str, int]]:
    calls: list[tuple[str, int]] = []
    ls.on_deleted = lambda reason, nbytes: calls.append((reason, nbytes))
    return calls


def test_cap_eviction_reports_sc16_bytes(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=5 / 1024)  # 5 MB
    calls = _recorder(ls)
    _cap(ls, "a", 3 * MB, 1)
    _cap(ls, "b", 3 * MB, 2)  # over the cap by one capture
    ls.enforce_cap()
    assert calls == [("cap", 3 * MB)]  # the .sc16 only, not its .json


def test_free_space_eviction_reports_space(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    calls = _recorder(ls)
    _cap(ls, "a", 1000, 1)
    _cap(ls, "b", 2000, 2)
    ls.evict_until_free(10**12, free_bytes=lambda: 0)
    assert calls == [("space", 1000), ("space", 2000)]


def test_sweep_limit_eviction_is_not_reported(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=1 / 1024)  # 1 MB
    calls = _recorder(ls)
    for i in range(3):
        p = tmp_path / f"s{i}.sc16"
        p.write_bytes(b"\0" * (400 * 1024))
        os.utime(p, (i + 1, i + 1))
    ls._enforce_limit(400 * 1024)
    assert not (tmp_path / "s0.sc16").exists()
    assert calls == []


def test_no_hook_still_evicts(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=5 / 1024)
    _cap(ls, "a", 3 * MB, 1)
    _cap(ls, "b", 3 * MB, 2)
    ls.enforce_cap()
    assert not (ls.auto_dir / "a.sc16").exists()


@pytest.mark.parametrize(("trigger", "origin"), [(True, "auto"), (False, "manual")])
def test_finalize_adds_the_capture(tmp_path, trigger, origin):
    proc = _proc(tmp_path)
    stats = CollectionStats()
    proc._collection = stats
    proc._trigger_initiated = trigger
    proc._recording_dir = proc._storage.auto_dir if trigger else proc._storage.manual_dir
    rate = proc._settings.BANDWIDTH
    proc._recording_bytes = int(rate) * 4 // 2  # half a second of sc16
    proc._recording_wall_start = 1_792_800_100.0
    proc._write_recording_metadata("x.sc16", 0.5)
    (row,) = stats.take().values()
    assert row[f"{origin}_captures"] == 1
    assert row[f"{origin}_bytes"] == int(rate) * 4 // 2
    assert row[f"{origin}_seconds"] == pytest.approx(0.5)


def test_replay_adds_nothing(tmp_path):
    proc = _proc(tmp_path)
    stats = CollectionStats()
    proc._collection = stats
    proc._replay_mode = True
    proc._recording_dir = proc._storage.manual_dir
    proc._recording_bytes = 4000
    proc._write_recording_metadata("x.sc16", 0.01)
    proc._count_live_chunk(1000)
    assert stats.take() == {}


def test_live_chunk_adds_its_seconds(tmp_path):
    proc = _proc(tmp_path)
    stats = CollectionStats()
    proc._collection = stats
    rate = proc._settings.BANDWIDTH
    proc._count_live_chunk(int(rate // 4))
    proc._count_live_chunk(0)
    (row,) = stats.take().values()
    assert row == {"active_seconds": pytest.approx(0.25)}


def test_receiver_loop_counts_live_chunks(tmp_path):
    proc = _proc(tmp_path)
    stats = CollectionStats()
    proc._collection = stats
    proc._running = True
    t = threading.Thread(target=proc._receiver_loop, daemon=True)
    t.start()
    deadline = time.monotonic() + 10
    seen = 0.0
    while time.monotonic() < deadline and seen <= 0:
        time.sleep(0.05)
        seen += sum(r.get("active_seconds", 0.0) for r in stats.take().values())
    proc._running = False
    t.join(timeout=10)
    assert seen > 0


async def test_collection_loop_flushes_and_logs_failures(tmp_path, monkeypatch, caplog):
    import asyncio

    from rfobserver.pipeline import app as pipeline_app
    from rfobserver.storage.database import COLLECTION_SINCE_KEY, SensorDatabase

    monkeypatch.setattr(pipeline_app, "COLLECTION_FLUSH_SEC", 0.01)
    db = SensorDatabase(str(tmp_path / "c.db"))
    await db.connect()
    stats = CollectionStats()
    stats.add_active(2.0, time.time())
    real = db.add_collection_hours
    calls = 0

    async def flaky(rows):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("database is locked")
        await real(rows)

    monkeypatch.setattr(db, "add_collection_hours", flaky)
    task = asyncio.create_task(pipeline_app._collection_loop(stats, db))
    try:
        for _ in range(200):
            await asyncio.sleep(0.01)
            hour = int(time.time()) // 3600 * 3600
            rows = await db.collection_hours(hour - 3600, hour + 3600)
            if rows:
                break
        assert sum(r["active_seconds"] for r in rows) == pytest.approx(2.0)
        assert await db.get_config(COLLECTION_SINCE_KEY) is not None
        assert "Collection statistics flush failed" in caplog.text
    finally:
        task.cancel()
        await db.close()
