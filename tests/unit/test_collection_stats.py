"""Hourly IQ collection counters: the in-memory accumulator and its table."""

from __future__ import annotations

import threading

import pytest

from rfobserver.storage.collection import CollectionStats
from rfobserver.storage.database import COLLECTION_SINCE_KEY, SensorDatabase

H = 1_792_800_000  # an hour start (a multiple of 3600)


@pytest.fixture
async def db(tmp_path):
    database = SensorDatabase(str(tmp_path / "c.db"))
    await database.connect()
    yield database
    await database.close()


def test_capture_within_an_hour():
    cs = CollectionStats()
    cs.add_recorded("auto", H + 60, H + 70, 4_000)
    assert cs.take() == {H: {"auto_captures": 1, "auto_bytes": 4_000, "auto_seconds": 10.0}}


def test_capture_spanning_two_hours_splits_by_time():
    cs = CollectionStats()
    cs.add_recorded("manual", H + 3590, H + 3610, 2_000)
    d = cs.take()
    assert d[H] == {"manual_captures": 1, "manual_bytes": 1_000, "manual_seconds": 10.0}
    assert d[H + 3600] == {"manual_bytes": 1_000, "manual_seconds": 10.0}


def test_split_bytes_sum_exactly():
    cs = CollectionStats()
    cs.add_recorded("auto", H + 3599.3, H + 7200.9, 1_000_003)
    d = cs.take()
    assert sum(v.get("auto_bytes", 0) for v in d.values()) == 1_000_003
    assert sorted(d) == [H, H + 3600, H + 7200]


def test_take_clears_pending():
    cs = CollectionStats()
    cs.add_active(1.0, H)
    cs.take()
    assert cs.take() == {}


def test_deleted_and_active_go_to_their_hour_and_unknown_reason_is_ignored():
    cs = CollectionStats()
    cs.add_deleted("cap", 500, H + 5)
    cs.add_deleted("space", 300, H + 3605)
    cs.add_deleted("user", 999, H + 5)
    cs.add_active(0.25, H + 5)
    d = cs.take()
    assert d[H] == {"deleted_cap_captures": 1, "deleted_cap_bytes": 500, "active_seconds": 0.25}
    assert d[H + 3600] == {"deleted_space_captures": 1, "deleted_space_bytes": 300}


def test_unknown_origin_is_ignored():
    cs = CollectionStats()
    cs.add_recorded("replay", H, H + 1, 10)
    assert cs.take() == {}


def test_concurrent_adds_lose_nothing():
    cs = CollectionStats()

    def work():
        for _ in range(10_000):
            cs.add_active(0.001, H + 1)

    threads = [threading.Thread(target=work) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert cs.take()[H]["active_seconds"] == pytest.approx(80.0)


async def test_flush_sets_since_once_and_adds_to_rows(db):
    a = CollectionStats()
    a.add_recorded("auto", H + 10, H + 20, 400)
    await a.flush(db, H + 100)
    assert await db.get_config(COLLECTION_SINCE_KEY) == str(H)

    b = CollectionStats()  # a restart: a new accumulator, the same hour
    b.add_recorded("auto", H + 30, H + 40, 600)
    b.add_active(5.0, H + 50)
    await b.flush(db, H + 7300)
    assert await db.get_config(COLLECTION_SINCE_KEY) == str(H)

    (row,) = await db.collection_hours(H, H + 3600)
    assert row["hour"] == H
    assert row["auto_captures"] == 2
    assert row["auto_bytes"] == 1_000
    assert row["auto_seconds"] == pytest.approx(20.0)
    assert row["active_seconds"] == pytest.approx(5.0)
    assert row["manual_bytes"] == 0


async def test_collection_hours_range_is_half_open_and_ordered(db):
    cs = CollectionStats()
    for k in (2, 0, 1):
        cs.add_active(1.0, H + k * 3600)
    await cs.flush(db, H)
    rows = await db.collection_hours(H, H + 7200)
    assert [r["hour"] for r in rows] == [H, H + 3600]


async def test_flush_keeps_deltas_when_the_write_fails(db, monkeypatch):
    cs = CollectionStats()
    cs.add_recorded("manual", H, H + 1, 40)
    real = db.add_collection_hours
    calls = 0

    async def flaky(rows):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("database is locked")
        await real(rows)

    monkeypatch.setattr(db, "add_collection_hours", flaky)
    with pytest.raises(RuntimeError):
        await cs.flush(db, H)
    cs.add_recorded("manual", H + 2, H + 3, 60)
    await cs.flush(db, H)
    (row,) = await db.collection_hours(H, H + 3600)
    assert row["manual_captures"] == 2 and row["manual_bytes"] == 100
