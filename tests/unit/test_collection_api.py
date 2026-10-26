"""Collection statistics API: hourly, daily, range, 7-day average, storage."""

from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pytest

from rfobserver.config import AppSettings
from rfobserver.storage.collection import CollectionStats
from rfobserver.storage.database import COLLECTION_SINCE_KEY, SensorDatabase
from rfobserver.storage.local import LocalStorage
from rfobserver.web.app import create_app
from rfobserver.web.routes import collection as routes

UTC = timezone.utc


def _ts(*a: int) -> int:
    return int(datetime(*a, tzinfo=UTC).timestamp())


NOW = _ts(2026, 10, 26, 18)  # Monday 2026-10-26 18:00Z (12:00 in Denver)


@pytest.fixture
async def make_client(tmp_path, monkeypatch):
    monkeypatch.setattr(routes, "_now", lambda: float(NOW))
    dbs: list[SensorDatabase] = []

    async def make(tz: str = "UTC", since: int | None = None, hours=None):
        db = SensorDatabase(str(tmp_path / f"c{len(dbs)}.db"))
        await db.connect()
        dbs.append(db)
        if since is not None:
            await db.set_config(COLLECTION_SINCE_KEY, str(since))
        if hours:
            await db.add_collection_hours(hours)
        app = create_app(AppSettings(_env_file=None, TIMEZONE=tz, STORAGE_PATH=str(tmp_path)))
        app.state.database = db
        app.state.local_storage = LocalStorage(str(tmp_path), max_gb=1)
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")

    yield make
    for db in dbs:
        await db.close()


def _every_hour(lo: int, hi: int, **fields: float) -> dict[int, dict[str, float]]:
    return {h: dict(fields) for h in range(lo, hi, 3600)}


async def test_daily_sums_local_hours_in_denver(make_client):
    # 2026-10-25 in Denver is 06:00Z on the 25th to 06:00Z on the 26th.
    hours = _every_hour(_ts(2026, 10, 25, 5), _ts(2026, 10, 26, 7), auto_bytes=10, auto_captures=1)
    c = await make_client("America/Denver", since=_ts(2026, 10, 1), hours=hours)
    async with c:
        d = (await c.get("/api/collection/daily", params={"date": "2026-10-25"})).json()
    assert d["timezone"] == "America/Denver"
    assert d["date"] == "2026-10-25"
    assert d["auto_bytes"] == 240 and d["bytes"] == 240 and d["captures"] == 24
    assert d["complete"] is True and d["covered"] is True and d["hours"] == 24


async def test_dst_day_has_23_hours(make_client):
    hours = _every_hour(_ts(2026, 3, 8, 6), _ts(2026, 3, 9, 8), active_seconds=3600.0)
    c = await make_client("America/Denver", since=_ts(2026, 3, 1), hours=hours)
    async with c:
        d = (await c.get("/api/collection/daily", params={"date": "2026-03-08"})).json()
    assert d["hours"] == 23
    assert d["active_seconds"] == pytest.approx(23 * 3600)


async def test_range_total_and_days(make_client):
    hours = {
        _ts(2026, 10, 20, 1): {"manual_bytes": 5, "manual_captures": 1, "manual_seconds": 1.5},
        _ts(2026, 10, 21, 1): {"auto_bytes": 7, "auto_captures": 2},
        _ts(2026, 10, 22, 23): {"deleted_cap_bytes": 3, "deleted_cap_captures": 1},
        _ts(2026, 10, 23, 0): {"auto_bytes": 1000},  # outside the range
    }
    c = await make_client(since=_ts(2026, 10, 1), hours=hours)
    async with c:
        r = (
            await c.get(
                "/api/collection/daily", params={"start": "2026-10-20", "end": "2026-10-22"}
            )
        ).json()
    assert [d["date"] for d in r["days"]] == ["2026-10-20", "2026-10-21", "2026-10-22"]
    assert [d["bytes"] for d in r["days"]] == [5, 7, 0]
    t = r["total"]
    assert (t["bytes"], t["captures"], t["deleted_cap_bytes"]) == (12, 3, 3)
    assert t["seconds"] == pytest.approx(1.5)


async def test_before_coverage_is_zero_and_not_covered(make_client):
    c = await make_client(since=_ts(2026, 10, 21, 12))
    async with c:
        before = (await c.get("/api/collection/daily", params={"date": "2026-10-20"})).json()
        partial = (await c.get("/api/collection/daily", params={"date": "2026-10-21"})).json()
        full = (await c.get("/api/collection/daily", params={"date": "2026-10-22"})).json()
    assert (before["covered"], before["bytes"]) == (False, 0)
    assert partial["covered"] is False
    assert full["covered"] is True
    assert before["covered_since"] == "2026-10-21T12:00:00+00:00"


async def test_nothing_counted_yet(make_client):
    c = await make_client()
    async with c:
        d = (await c.get("/api/collection/daily", params={"date": "2026-10-25"})).json()
    assert d["covered_since"] is None and d["covered"] is False and d["bytes"] == 0


async def test_today_is_not_complete(make_client):
    c = await make_client(since=_ts(2026, 10, 1))
    async with c:
        d = (await c.get("/api/collection/daily", params={"date": "2026-10-26"})).json()
    assert d["complete"] is False


async def test_average_null_until_seven_complete_days(make_client):
    hours = _every_hour(_ts(2026, 10, 15), _ts(2026, 10, 26), auto_bytes=100)
    six = await make_client(since=_ts(2026, 10, 20), hours=hours)  # 20..25: 6 days
    seven = await make_client(since=_ts(2026, 10, 19), hours=hours)  # 19..25: 7 days
    async with six, seven:
        a6 = (await six.get("/api/collection/average")).json()
        a7 = (await seven.get("/api/collection/average", params={"days": 7})).json()
    assert a6["average"] is None and a6["days_counted"] == 6
    assert a7["days_counted"] == 7
    assert a7["average"]["auto_bytes"] == pytest.approx(2400)
    assert a7["average"]["bytes"] == pytest.approx(2400)
    assert [d["date"] for d in a7["days"]][0] == "2026-10-19"


@pytest.mark.parametrize(
    "q",
    [
        {"date": "2026-13-01"},
        {"start": "2026-10-05", "end": "2026-10-01"},
        {"start": "2025-01-01", "end": "2026-10-01"},
        {"start": "2026-10-01"},
        {},
        {"date": "2026-10-01", "start": "2026-10-01", "end": "2026-10-02"},
    ],
)
async def test_bad_daily_queries_are_400(make_client, q):
    c = await make_client()
    async with c:
        r = await c.get("/api/collection/daily", params=q)
    assert r.status_code == 400 and r.json()["detail"]


@pytest.mark.parametrize("days", [0, 61, "x"])
async def test_average_days_out_of_range_is_400(make_client, days):
    c = await make_client()
    async with c:
        r = await c.get("/api/collection/average", params={"days": days})
    assert r.status_code in (400, 422)


async def test_hourly_lists_hours_with_local_time(make_client):
    hours = {_ts(2026, 10, 25, 6): {"active_seconds": 12.5}}
    c = await make_client("America/Denver", since=_ts(2026, 10, 1), hours=hours)
    async with c:
        r = await c.get(
            "/api/collection/hourly",
            params={"since": "2026-10-25T00:00:00Z", "until": "2026-10-26T00:00:00Z"},
        )
        bad = await c.get(
            "/api/collection/hourly",
            params={"since": "2026-09-01T00:00:00Z", "until": "2026-10-26T00:00:00Z"},
        )
    (h,) = r.json()["hours"]
    assert h["hour_utc"] == "2026-10-25T06:00:00+00:00"
    assert h["hour_local"] == "2026-10-25T00:00:00-06:00"
    assert h["active_seconds"] == pytest.approx(12.5)
    assert bad.status_code == 400


async def test_storage_snapshot(make_client, tmp_path):
    c = await make_client()
    ls = LocalStorage(str(tmp_path), max_gb=1)
    (ls.auto_dir / "a.sc16").write_bytes(b"\0" * 4000)
    (ls.auto_dir / "a.json").write_bytes(b"x" * 100)
    (ls.manual_dir / "m.sc16").write_bytes(b"\0" * 800)
    ls.track(ls.auto_dir / "a.sc16")
    ls.track(ls.manual_dir / "m.sc16")
    c._transport.app.state.local_storage = ls  # type: ignore[attr-defined]
    async with c:
        s = (await c.get("/api/collection/storage")).json()
    assert (s["auto_captures"], s["manual_captures"]) == (1, 1)
    assert (s["auto_bytes"], s["manual_bytes"]) == (4100, 800)
    assert s["cap_bytes"] == 1024**3
    assert s["volume_total_bytes"] >= s["volume_free_bytes"] > 0


async def test_no_database_is_503(tmp_path):
    app = create_app(AppSettings(_env_file=None))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/api/collection/daily", params={"date": "2026-10-25"})
    assert r.status_code == 503


async def test_flushed_figures_reach_the_api(make_client):
    c = await make_client()
    stats = CollectionStats()
    stats.add_recorded("auto", _ts(2026, 10, 25, 10), _ts(2026, 10, 25, 10) + 2, 8000)
    db = c._transport.app.state.database  # type: ignore[attr-defined]
    await stats.flush(db, _ts(2026, 10, 24))
    async with c:
        d = (await c.get("/api/collection/daily", params={"date": "2026-10-25"})).json()
    assert (d["auto_captures"], d["auto_bytes"]) == (1, 8000)
    assert d["covered"] is True
