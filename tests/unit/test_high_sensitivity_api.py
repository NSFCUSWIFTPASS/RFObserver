"""High-sensitivity API on a sensor: /api/high-sensitivity."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from rfobserver.config import AppSettings
from rfobserver.pipeline.high_sensitivity import HighSensitivity
from rfobserver.storage.database import SensorDatabase
from rfobserver.web.app import create_app

NOW = 1795037000.0


class _Clock:
    t = NOW

    def __call__(self) -> float:
        return self.t


class _Proc:
    """The parts of StreamingProcessor the routes use."""

    def __init__(self, settings: AppSettings, clock: _Clock) -> None:
        self.high_sensitivity = HighSensitivity(settings, clock=clock)


@pytest.fixture
async def client(tmp_path):
    made: list[Any] = []

    async def make(pipeline: bool = True) -> tuple[httpx.AsyncClient, Any, _Clock]:
        settings = AppSettings(_env_file=None, STORAGE_PATH=str(tmp_path))
        app = create_app(settings)
        db = SensorDatabase(str(tmp_path / "s.db"))
        await db.connect()
        app.state.database = db
        clock = _Clock()
        proc = _Proc(settings, clock) if pipeline else None
        app.state.processor = proc
        c = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")
        made.append((c, db))
        return c, proc, clock

    yield make
    for c, db in made:
        await c.aclose()
        await db.close()


async def test_start_get_delete(client) -> None:
    c, proc, clock = await client()
    r = await c.post("/api/high-sensitivity", json={"seconds": 120, "id": "daily"})
    assert r.status_code == 200
    run = r.json()["run"]
    assert run["id"] == "daily" and run["threshold_db"] == 15.0 and run["seconds_left"] == 120
    assert run["start"].endswith("Z") and run["end"].endswith("Z")
    clock.t += 20
    g = (await c.get("/api/high-sensitivity")).json()
    assert g["run"]["id"] == "daily" and g["run"]["seconds_left"] == 100
    d = await c.delete("/api/high-sensitivity/daily")
    assert d.status_code == 200 and d.json()["run"]["end_reason"] == "deleted"
    assert (await c.get("/api/high-sensitivity")).json()["run"] is None


async def test_errors(client) -> None:
    c, proc, clock = await client()
    assert (await c.post("/api/high-sensitivity", json={"seconds": 301})).status_code == 400
    assert (await c.post("/api/high-sensitivity", json={"seconds": "x"})).status_code == 400
    bad_thr = {"seconds": 60, "threshold_db": 5}
    assert (await c.post("/api/high-sensitivity", json=bad_thr)).status_code == 400
    assert (await c.post("/api/high-sensitivity", json=[1])).status_code == 400
    await c.post("/api/high-sensitivity", json={"seconds": 60, "id": "a"})
    r = await c.post("/api/high-sensitivity", json={"seconds": 60, "id": "b"})
    assert r.status_code == 409
    assert (await c.delete("/api/high-sensitivity/zzz")).status_code == 404
    assert (await c.get("/api/high-sensitivity/zzz")).status_code == 404


async def test_no_pipeline_is_503(client) -> None:
    c, _, _ = await client(pipeline=False)
    assert (await c.post("/api/high-sensitivity", json={"seconds": 60})).status_code == 503


async def test_run_summaries_are_listed_and_fetched(client, tmp_path) -> None:
    c, proc, clock = await client()
    db = c._transport.app.state.database  # type: ignore[attr-defined]
    await db.hs_run_save(
        {
            "id": "r1",
            "source": "api",
            "start": NOW - 100,
            "end": NOW - 40,
            "ended": NOW - 40,
            "end_reason": "expired",
            "threshold_db": 15.0,
            "summary": {"detections": 9},
        }
    )
    g = (await c.get("/api/high-sensitivity")).json()
    assert [r["id"] for r in g["runs"]] == ["r1"]
    one = (await c.get("/api/high-sensitivity/r1")).json()
    assert one["summary"] == {"detections": 9} and one["end_reason"] == "expired"


async def test_health_reports_the_mode(client) -> None:
    c, proc, clock = await client()
    proc.high_sensitivity.start(60, run_id="h")
    h = (await c.get("/api/health")).json()
    assert h["high_sensitivity"]["active"] is True
    assert h["high_sensitivity"]["run"]["id"] == "h"


async def test_ids_are_plain_and_not_reused(client) -> None:
    c, proc, clock = await client()
    for bad in ("daily/0dB", "a?b", "x#y", "has space"):
        r = await c.post("/api/high-sensitivity", json={"seconds": 10, "id": bad})
        assert r.status_code == 400, bad
    assert (
        await c.post("/api/high-sensitivity", json={"seconds": 10, "id": "d.1"})
    ).status_code == 200
    await c.delete("/api/high-sensitivity/d.1")
    db = c._transport.app.state.database  # type: ignore[attr-defined]
    await db.hs_run_save(
        {
            "id": "d.1",
            "source": "api",
            "start": NOW,
            "end": NOW + 10,
            "ended": NOW,
            "end_reason": "deleted",
            "threshold_db": 15.0,
            "summary": None,
        }
    )
    r = await c.post("/api/high-sensitivity", json={"seconds": 10, "id": "d.1"})
    assert r.status_code == 409
