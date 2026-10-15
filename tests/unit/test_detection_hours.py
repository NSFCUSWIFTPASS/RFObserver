"""Hourly detection counts: the stored table, the finalize pass, and
/api/detections/hourly (stored hours plus a live tail)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from rfobserver.config import AppSettings
from rfobserver.pipeline import app as pipeline_app
from rfobserver.storage.database import (
    DET_HOURS_DONE_KEY,
    DET_HOURS_SINCE_KEY,
    SensorDatabase,
)
from tests.unit.test_detections_page import _add, _client

UTC = timezone.utc
H0 = datetime(2026, 10, 15, 10, 0, 0, tzinfo=UTC)  # an hour boundary
E0 = int(H0.timestamp())


@pytest.fixture
async def db(tmp_path):
    database = SensorDatabase(str(tmp_path / "det.db"))
    await database.connect()
    yield database
    await database.close()


async def _seed(db: SensorDatabase) -> None:
    """Hour 0: 2 decoded (two models), 1 tried, 1 never tried. Hour 1: 1 decoded."""
    await _add(db, "a", H0 + timedelta(minutes=1), model="SilverSpring-Mesh")
    await _add(db, "b", H0 + timedelta(minutes=2, microseconds=123456), model="Acurite-Tower")
    await _add(db, "c", H0 + timedelta(minutes=3), model="")
    await _add(db, "d", H0 + timedelta(minutes=59, seconds=59, microseconds=999999))
    await _add(db, "e", H0 + timedelta(hours=1, minutes=5), model="SilverSpring-Mesh")


async def test_count_by_hour_groups_models_and_tried(db):
    await _seed(db)
    rows = sorted(await db.count_detections_by_hour(E0, E0 + 7200))
    assert rows == [
        (E0, "", 0, 1),
        (E0, "", 1, 1),
        (E0, "Acurite-Tower", 1, 1),
        (E0, "SilverSpring-Mesh", 1, 1),
        (E0 + 3600, "SilverSpring-Mesh", 1, 1),
    ]
    # [since, until): the second hour alone.
    assert await db.count_detections_by_hour(E0 + 3600, E0 + 7200) == [
        (E0 + 3600, "SilverSpring-Mesh", 1, 1)
    ]


async def test_store_replaces_the_hours_it_covers(db):
    await db.store_detection_hours(E0, E0 + 3600, [(E0, "", 0, 5)])
    await db.store_detection_hours(E0, E0 + 3600, [(E0, "x", 1, 2)])
    assert await db.detection_hours(E0, E0 + 3600) == [(E0, "x", 1, 2)]


async def test_first_pass_starts_counting_at_the_current_hour(db):
    await _seed(db)
    now = H0 + timedelta(hours=5, minutes=20)
    await pipeline_app._detection_hours_pass(db, None, now)
    start = str(E0 + 5 * 3600)
    assert await db.get_config(DET_HOURS_SINCE_KEY) == start  # no backfill
    assert await db.get_config(DET_HOURS_DONE_KEY) == start
    assert await db.detection_hours(0, 2**40) == []


async def test_pass_stores_only_settled_hours(db, monkeypatch):
    monkeypatch.setattr(pipeline_app, "_DET_HOURS_PAUSE_SEC", 0)
    await _seed(db)
    await db.set_config(DET_HOURS_SINCE_KEY, str(E0))
    await db.set_config(DET_HOURS_DONE_KEY, str(E0))
    # 1:14: hour 0 ended 14 min ago, not settled yet.
    await pipeline_app._detection_hours_pass(db, None, H0 + timedelta(hours=1, minutes=14))
    assert await db.get_config(DET_HOURS_DONE_KEY) == str(E0)
    # 1:15: hour 0 is stored; hour 1 is still open.
    await pipeline_app._detection_hours_pass(db, None, H0 + timedelta(hours=1, minutes=15))
    assert await db.get_config(DET_HOURS_DONE_KEY) == str(E0 + 3600)
    assert sum(r[3] for r in await db.detection_hours(E0, E0 + 7200)) == 4
    # Hour 1 ends at 2:00; settled at 2:15, not before.
    await pipeline_app._detection_hours_pass(db, None, H0 + timedelta(hours=2, minutes=14))
    assert await db.get_config(DET_HOURS_DONE_KEY) == str(E0 + 3600)
    await pipeline_app._detection_hours_pass(db, None, H0 + timedelta(hours=2, minutes=15))
    assert await db.get_config(DET_HOURS_DONE_KEY) == str(E0 + 7200)


async def test_pass_catches_up_in_bounded_steps(db, monkeypatch):
    monkeypatch.setattr(pipeline_app, "_DET_HOURS_PAUSE_SEC", 0)
    monkeypatch.setattr(pipeline_app, "_DET_HOURS_PER_PASS", 3)
    await db.set_config(DET_HOURS_SINCE_KEY, str(E0))
    await db.set_config(DET_HOURS_DONE_KEY, str(E0))
    await pipeline_app._detection_hours_pass(db, None, H0 + timedelta(days=1))
    assert await db.get_config(DET_HOURS_DONE_KEY) == str(E0 + 3 * 3600)


async def test_hourly_endpoint_mixes_stored_and_live_hours(db):
    await _seed(db)
    await db.set_config(DET_HOURS_SINCE_KEY, str(E0))
    await db.set_config(DET_HOURS_DONE_KEY, str(E0 + 3600))
    # Hour 0 stored (with a count that differs from the rows, to tell them apart).
    await db.store_detection_hours(E0, E0 + 3600, [(E0, "", 0, 7), (E0, "M", 1, 3)])
    async with await _client(AppSettings(_env_file=None), db) as client:
        r = await client.get(
            "/api/detections/hourly",
            params={
                "since": (H0 - timedelta(hours=3)).isoformat(),
                "until": (H0 + timedelta(hours=1, minutes=30)).isoformat(),
            },
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert (body["covered_since"], body["finalized_until"], body["bucket_sec"]) == (
        E0,
        E0 + 3600,
        3600,
    )
    h0, h1 = body["hours"]
    assert h0 == {
        "t": E0,
        "all": 10,
        "attributed": 3,
        "tried": 0,
        "not_tried": 7,
        "models": {"M": 3},
    }
    assert h1 == {  # live, from the rows
        "t": E0 + 3600,
        "all": 1,
        "attributed": 1,
        "tried": 0,
        "not_tried": 0,
        "models": {"SilverSpring-Mesh": 1},
    }


async def test_hourly_endpoint_before_counting_started(db):
    await _seed(db)
    async with await _client(AppSettings(_env_file=None), db) as client:
        r = await client.get(
            "/api/detections/hourly",
            params={"since": H0.isoformat(), "until": (H0 + timedelta(hours=2)).isoformat()},
        )
        assert r.json()["covered_since"] is None and r.json()["hours"] == []
        bad = await client.get("/api/detections/hourly", params={"since": "nope", "until": "x"})
        assert bad.status_code == 400
        missing = await client.get("/api/detections/hourly")
        assert missing.status_code == 400


async def test_hourly_live_tail_is_bounded(db):
    """A stalled finalize pass must not turn into a long live count."""
    await _seed(db)
    await db.set_config(DET_HOURS_SINCE_KEY, str(E0))
    await db.set_config(DET_HOURS_DONE_KEY, str(E0))  # nothing stored
    async with await _client(AppSettings(_env_file=None), db) as client:
        r = await client.get(
            "/api/detections/hourly",
            params={"since": H0.isoformat(), "until": (H0 + timedelta(hours=5)).isoformat()},
        )
    # Only the last three hours (2:00-5:00) are counted live; they hold nothing.
    assert r.json()["hours"] == []


async def test_detections_json_filters_by_attribution(db):
    await _seed(db)
    async with await _client(AppSettings(_env_file=None), db) as client:
        got = lambda r: sorted(d["burst_id"] for d in r.json()["detections"])  # noqa: E731
        assert got(await client.get("/api/detections.json", params={"attributed": "true"})) == [
            "a",
            "b",
            "e",
        ]
        assert got(await client.get("/api/detections.json", params={"attributed": "false"})) == [
            "c",
            "d",
        ]
        assert got(await client.get("/api/detections.json", params={"model": "Acurite-Tower"})) == [
            "b"
        ]
        # until is exclusive (the fleet pages with it).
        until = (H0 + timedelta(minutes=2, microseconds=123456)).isoformat()
        assert got(await client.get("/api/detections.json", params={"until": until})) == ["a"]
