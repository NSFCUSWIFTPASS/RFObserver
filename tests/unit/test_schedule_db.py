"""Scheduled captures: schedules and runs in the sensor database, and the
scheduled columns of collection_hours."""

from __future__ import annotations

import sqlite3

import pytest

from rfobserver.schedules.model import parse_schedule
from rfobserver.storage.collection import CollectionStats
from rfobserver.storage.database import SensorDatabase

T = 1793212200250
H = 1_792_800_000


def sched(sid: str, **extra: object):
    body = {"id": sid, "start_ms": T, "duration_ms": 500, **extra}
    return parse_schedule(body, now_ms=T - 1000, grace_ms=0, source="api", new_id=lambda: sid)


@pytest.fixture
async def db(tmp_path):
    database = SensorDatabase(str(tmp_path / "s.db"))
    await database.connect()
    yield database
    await database.close()


async def test_schedule_round_trip_and_delete(db: SensorDatabase) -> None:
    a = sched("a", every_ms=1000, count=3, label="one")
    b = sched("b")
    await db.insert_schedule(a)
    await db.insert_schedule(b)
    assert await db.get_schedule("a") == a
    assert [s.id for s in await db.list_schedules()] == ["a", "b"]
    assert await db.delete_schedule("a", T) is True
    assert await db.delete_schedule("a", T) is False
    assert await db.delete_schedule("nope", T) is False
    assert [s.id for s in await db.list_schedules()] == ["b"]
    assert [s.id for s in await db.list_schedules(include_deleted=True)] == ["a", "b"]
    assert await db.get_schedule("a") == a  # still readable for its runs


async def test_runs_upsert_list_last_and_prune(db: SensorDatabase) -> None:
    await db.insert_schedule(sched("a", every_ms=1000, count=9))
    await db.upsert_run("a", 0, "recording", start_ms=T)
    await db.upsert_run("a", 0, "recorded", start_ms=T, filename="f.sc16", info={"x": 1})
    await db.upsert_run("a", 1, "missed", start_ms=T + 1000, reason="busy")
    await db.upsert_run("a", 2, "partial", start_ms=T + 2000, reason="retuned")
    runs = await db.list_runs("a")
    assert [r["run"] for r in runs] == [2, 1, 0]
    assert runs[2] == {
        "run": 0,
        "status": "recorded",
        "start_ms": T,
        "filename": "f.sc16",
        "reason": None,
        "info": {"x": 1},
    }
    assert await db.last_run("a") == 2
    assert await db.last_run("b") is None
    await db.prune_runs("a", keep=2)
    assert [r["run"] for r in await db.list_runs("a")] == [2, 1]
    assert [r["run"] for r in await db.list_runs("a", limit=1)] == [2]


async def test_scheduled_collection_columns(db: SensorDatabase) -> None:
    cs = CollectionStats()
    cs.add_recorded("scheduled", H + 10, H + 12, 8_000)
    await cs.flush(db, H + 20)
    (row,) = await db.collection_hours(H, H + 3600)
    assert row["scheduled_captures"] == 1
    assert row["scheduled_bytes"] == 8_000
    assert row["scheduled_seconds"] == pytest.approx(2.0)


async def test_old_collection_table_gains_scheduled_columns(tmp_path) -> None:
    path = tmp_path / "old.db"
    con = sqlite3.connect(path)
    con.execute(
        "CREATE TABLE collection_hours (hour INTEGER PRIMARY KEY, "
        "auto_captures INTEGER NOT NULL DEFAULT 0, manual_captures INTEGER NOT NULL DEFAULT 0, "
        "auto_bytes INTEGER NOT NULL DEFAULT 0, manual_bytes INTEGER NOT NULL DEFAULT 0, "
        "auto_seconds REAL NOT NULL DEFAULT 0, manual_seconds REAL NOT NULL DEFAULT 0, "
        "deleted_cap_captures INTEGER NOT NULL DEFAULT 0, "
        "deleted_cap_bytes INTEGER NOT NULL DEFAULT 0, "
        "deleted_space_captures INTEGER NOT NULL DEFAULT 0, "
        "deleted_space_bytes INTEGER NOT NULL DEFAULT 0, "
        "active_seconds REAL NOT NULL DEFAULT 0)"
    )
    con.execute("INSERT INTO collection_hours (hour, auto_captures) VALUES (?, 3)", (H,))
    con.commit()
    con.close()
    db = SensorDatabase(str(path))
    await db.connect()
    try:
        (row,) = await db.collection_hours(H, H + 3600)
        assert row["auto_captures"] == 3 and row["scheduled_captures"] == 0
    finally:
        await db.close()
