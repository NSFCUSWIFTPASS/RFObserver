"""Tests for the Dashboard peak finder endpoint."""

from datetime import datetime, timedelta, timezone

import numpy as np
import pytest
from httpx import ASGITransport, AsyncClient

from rfobserver.config import AppSettings
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.rollup import ROLLUP_OLDEST_KEY, fold_windows
from rfobserver.web.app import create_app
from rfobserver.web.routes.api import _PEAKS_CACHE

UTC = timezone.utc
NOW = datetime.now(UTC).replace(second=0, microsecond=0)


@pytest.fixture(autouse=True)
def _clear_peaks_cache():
    # NOW is a fixed module constant, and several tests below share the same
    # (since, until, window_sec, count, metric) key by default, so the
    # process-global endpoint cache must not survive between tests -- same
    # reason test_heavy_query_cap.py varies `until` per request for the
    # waterfall cache.
    _PEAKS_CACHE.clear()
    yield


@pytest.fixture
async def client(tmp_path):
    path = str(tmp_path / "t.db")
    settings = AppSettings(_env_file=None, DB_PATH=path)
    database = SensorDatabase(path)
    await database.connect()
    app = create_app(settings)
    app.state.database = database
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c, database
    await database.close()


async def _seed(db, minutes_ago: float, pwr_max: float):
    await db.insert_avg_window(
        start_time=NOW - timedelta(minutes=minutes_ago),
        duration_sec=0.5,
        sdr_center_freq_hz=2.437e9,
        sample_rate_hz=56e6,
        gain_db=40.0,
        num_bins=4,
        freq_start_hz=2.409e9,
        freq_step_hz=1e6,
        powers=np.array([pwr_max, -60.0, -60.0, -60.0], dtype="<f4"),
        pwr_avg=-50.0,
        pwr_max=pwr_max,
        pwr_median=-60.0,
        pwr_std=1.0,
        kurtosis=2.0,
        interference=0,
    )


async def _roll(db):
    rows = []
    async for chunk in db.iter_rollup_windows(
        since=NOW - timedelta(days=40), until=NOW + timedelta(minutes=1)
    ):
        rows.extend(chunk)
    await db.upsert_avg_minutes(fold_windows(rows))


def _range(days: int = 7) -> dict:
    return {
        "since": (NOW - timedelta(days=days)).isoformat(),
        "until": NOW.isoformat(),
    }


async def test_returns_separated_peaks_strongest_first(client):
    c, db = client
    await _seed(db, 100, -25.0)
    await _seed(db, 500, -18.0)
    await _seed(db, 900, -30.0)
    await _roll(db)
    r = await c.get("/api/averaged/peaks", params={**_range(), "window_sec": 1800, "count": 10})
    assert r.status_code == 200
    body = r.json()
    assert [p["rank"] for p in body["peaks"]] == [1, 2, 3]
    assert round(body["peaks"][0]["value"], 1) == -18.0
    assert body["truncated"] is False


async def test_window_bounds_are_centred_on_the_peak(client):
    c, db = client
    await _seed(db, 500, -18.0)
    await _roll(db)
    r = await c.get("/api/averaged/peaks", params={**_range(), "window_sec": 1800})
    peak = r.json()["peaks"][0]
    t = datetime.fromisoformat(peak["peak_time"])
    assert datetime.fromisoformat(peak["since"]) == t - timedelta(seconds=900)
    assert datetime.fromisoformat(peak["until"]) == t + timedelta(seconds=900)


async def test_peaks_closer_than_the_window_collapse_to_one(client):
    c, db = client
    for i in range(6):
        await _seed(db, 500 + i, -20.0 - i)
    await _roll(db)
    r = await c.get("/api/averaged/peaks", params={**_range(), "window_sec": 1800})
    assert len(r.json()["peaks"]) == 1


async def test_old_peaks_are_flagged_when_their_psd_is_gone(client):
    c, db = client
    # DB_RETENTION_DAYS defaults to 7, so a 10-day-old peak has no blob left.
    await _seed(db, 60 * 24 * 10, -18.0)
    await _seed(db, 60, -19.0)
    await _roll(db)
    r = await c.get("/api/averaged/peaks", params={**_range(days=30), "window_sec": 1800})
    flags = {p["rank"]: p["psd_available"] for p in r.json()["peaks"]}
    assert flags[1] is False
    assert flags[2] is True


async def test_empty_range_returns_an_empty_list_not_a_wider_search(client):
    c, db = client
    await _seed(db, 60 * 24 * 20, -18.0)
    await _roll(db)
    r = await c.get("/api/averaged/peaks", params={**_range(days=3), "window_sec": 1800})
    assert r.status_code == 200
    assert r.json()["peaks"] == []


@pytest.mark.parametrize(
    "params",
    [
        {"window_sec": 1234},
        {"count": 0},
        {"count": 21},
        {"metric": "nonsense"},
    ],
)
async def test_invalid_parameters_are_rejected(client, params):
    c, _ = client
    r = await c.get("/api/averaged/peaks", params={**_range(), **params})
    assert r.status_code == 400


async def test_inverted_range_is_rejected(client):
    c, _ = client
    r = await c.get(
        "/api/averaged/peaks",
        params={"since": NOW.isoformat(), "until": (NOW - timedelta(days=1)).isoformat()},
    )
    assert r.status_code == 400


async def test_covered_since_reports_the_backfill_depth(client):
    c, db = client
    await _seed(db, 60, -18.0)
    await _roll(db)
    await db.set_config(ROLLUP_OLDEST_KEY, (NOW - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M"))
    r = await c.get("/api/averaged/peaks", params=_range(days=30))
    covered = datetime.fromisoformat(r.json()["covered_since"])
    assert covered > NOW - timedelta(days=3)
