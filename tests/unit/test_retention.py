"""Row retention (STATS_RETENTION_DAYS), chunked so the writer is never starved."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta

import pytest

from rfobserver.config import AppSettings
from rfobserver.pipeline.app import _cleanup_loop, _retention_days, _run_retention
from rfobserver.storage.database import SensorDatabase


@pytest.fixture
async def db(tmp_path):
    d = SensorDatabase(str(tmp_path / "r.db"))
    await d.connect()
    yield d
    await d.close()


async def _window(db, when: datetime, powers=(1.0, 2.0)):
    await db.insert_avg_window(
        start_time=when,
        duration_sec=1.0,
        sdr_center_freq_hz=915e6,
        sample_rate_hz=2e6,
        gain_db=30.0,
        num_bins=len(powers) if powers is not None else 2,
        freq_start_hz=914e6,
        freq_step_hz=1e6,
        pwr_avg=-50.0,
        pwr_max=-40.0,
        pwr_median=-50.0,
        pwr_std=1.0,
        kurtosis=3.0,
        powers=None if powers is None else list(powers),
    )


async def _detection(db, bid: str, when: datetime):
    await db.insert_detection(
        burst_id=bid,
        start_time=when,
        stop_time=when,
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1.0,
        detection_timestamp=when,
    )


async def _count(db, sql: str) -> int:
    async with db._db.execute(sql) as cur:
        return (await cur.fetchone())[0]


async def test_delete_older_than_removes_only_old_rows_across_chunks(db):
    now = datetime.utcnow()
    for i in range(23):
        await _detection(db, f"old{i}", now - timedelta(days=800, minutes=i))
    for i in range(4):
        await _detection(db, f"new{i}", now - timedelta(days=10, minutes=i))
    removed = await db.delete_older_than("detections", 730, chunk=5, pause_sec=0)
    assert removed == 23
    left = {r["burst_id"] for r in await db.query_detections()}
    assert left == {f"new{i}" for i in range(4)}


async def test_delete_older_than_handles_windows_and_minutes(db):
    now = datetime.utcnow()
    await _window(db, now - timedelta(days=800))
    await _window(db, now - timedelta(days=1))
    await db._db.execute(
        "INSERT INTO avg_minutes (minute_start, sdr_center_freq_hz, n) VALUES (?, ?, 1), (?, ?, 1)",
        (
            (now - timedelta(days=800)).strftime("%Y-%m-%dT%H:%M"),
            915e6,
            (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M"),
            915e6,
        ),
    )
    await db._db.commit()
    assert await db.delete_older_than("avg_windows", 730, chunk=1, pause_sec=0) == 1
    assert await db.delete_older_than("avg_minutes", 730, chunk=1, pause_sec=0) == 1
    assert await _count(db, "SELECT COUNT(*) FROM avg_windows") == 1
    assert await _count(db, "SELECT COUNT(*) FROM avg_minutes") == 1


async def test_delete_older_than_rejects_unknown_tables(db):
    with pytest.raises(KeyError):
        await db.delete_older_than("tone_checks", 1)


async def test_chunked_blob_prune_nulls_only_old_blobs_and_resumes(db):
    now = datetime.utcnow()
    for i in range(12):
        await _window(db, now - timedelta(days=40, minutes=i))
    await _window(db, now - timedelta(days=1))
    assert await db.prune_avg_psd_blobs(30, chunk=5, pause_sec=0) == 12
    assert await _count(db, "SELECT COUNT(*) FROM avg_windows WHERE psd_powers IS NULL") == 12
    assert await _count(db, "SELECT COUNT(*) FROM avg_windows") == 13  # rows kept
    assert await db.prune_avg_psd_blobs(30, chunk=5, pause_sec=0) == 0
    # A tighter cutoff later continues past the watermark.
    await _window(db, now - timedelta(days=10))
    assert await db.prune_avg_psd_blobs(7, chunk=5, pause_sec=0) == 1


async def test_blob_prune_handles_rows_sharing_a_timestamp(db):
    t = datetime.utcnow() - timedelta(days=40)
    for _ in range(7):
        await _window(db, t)
    assert await db.prune_avg_psd_blobs(30, chunk=3, pause_sec=0) == 7


async def test_insert_avg_window_without_powers_stores_a_null_blob(db):
    await _window(db, datetime.utcnow(), powers=None)
    assert await _count(db, "SELECT COUNT(*) FROM avg_windows WHERE psd_powers IS NULL") == 1


async def test_file_stats_reports_file_and_reusable_bytes(db):
    size, reusable = await db.file_stats()
    assert size > 0 and reusable >= 0


def test_retention_days_under_pressure():
    assert _retention_days(30, 7, False) == 30
    assert _retention_days(30, 7, True) == 7
    assert _retention_days(3, 7, True) == 3
    assert _retention_days(0, 7, False) == 0  # disabled
    assert _retention_days(0, 7, True) == 7  # pressure prunes even when disabled


class _RecDB:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    async def prune_avg_psd_blobs(self, days: int) -> int:
        self.calls.append(("blobs", days))
        return 0

    async def delete_older_than(self, table: str, days: int) -> int:
        self.calls.append((table, days))
        return 0


async def test_run_retention_normal_and_pressure():
    s = AppSettings(_env_file=None, DB_RETENTION_DAYS=30, STATS_RETENTION_DAYS=730)
    d = _RecDB()
    await _run_retention(s, d, pressure=False)
    assert d.calls == [
        ("blobs", 30),
        ("detections", 730),
        ("avg_windows", 730),
        ("avg_minutes", 730),
    ]
    d.calls.clear()
    await _run_retention(s, d, pressure=True)
    assert d.calls == [
        ("blobs", 7),
        ("detections", 90),
        ("avg_windows", 730),  # stats rows are never cut by pressure
        ("avg_minutes", 730),
    ]


async def test_run_retention_skips_disabled_parts_and_survives_errors():
    s = AppSettings(_env_file=None, DB_RETENTION_DAYS=0, STATS_RETENTION_DAYS=0)
    d = _RecDB()
    await _run_retention(s, d, pressure=False)
    assert d.calls == []

    class _Boom(_RecDB):
        async def prune_avg_psd_blobs(self, days: int) -> int:
            raise RuntimeError("x")

    b = _Boom()
    s2 = AppSettings(_env_file=None, DB_RETENTION_DAYS=30, STATS_RETENTION_DAYS=730)
    await _run_retention(s2, b, pressure=False)
    assert ("detections", 730) in b.calls  # one failure does not stop the rest


async def test_cleanup_loop_wakes_early_on_the_event():
    s = AppSettings(_env_file=None, DB_RETENTION_DAYS=30, DB_CLEANUP_INTERVAL_SEC=3600)
    d = _RecDB()
    wake = asyncio.Event()
    task = asyncio.create_task(_cleanup_loop(s, d, wake=wake))
    for _ in range(5):
        await asyncio.sleep(0)
    first = len(d.calls)
    wake.set()
    for _ in range(10):
        await asyncio.sleep(0)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert first >= 1 and len(d.calls) > first
