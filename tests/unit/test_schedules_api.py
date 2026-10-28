"""Scheduled captures API: /api/schedules on a sensor."""

from __future__ import annotations

from collections import namedtuple
from typing import Any

import httpx
import pytest

from rfobserver.config import AppSettings
from rfobserver.schedules.book import ScheduleBook
from rfobserver.storage.database import SensorDatabase
from rfobserver.web.app import create_app
from rfobserver.web.routes import schedules as routes

NOW = 1793212140.0  # 2026-10-28T18:29:00Z
START = "2026-10-28T18:30:00.250Z"
HCRO = {"LATITUDE": 40.8165, "LONGITUDE": -121.4639}
Usage = namedtuple("Usage", "total used free")


@pytest.fixture
async def client(tmp_path, monkeypatch):
    monkeypatch.setattr(routes, "_now", lambda: NOW)
    monkeypatch.setattr(routes.shutil, "disk_usage", lambda p: Usage(10**12, 10**11, 9 * 10**11))
    clients: list[Any] = []
    dbs: list[SensorDatabase] = []

    async def make(book: bool = True, **settings: Any) -> httpx.AsyncClient:
        db = SensorDatabase(str(tmp_path / f"s{len(dbs)}.db"))
        await db.connect()
        dbs.append(db)
        app = create_app(
            AppSettings(_env_file=None, STORAGE_PATH=str(tmp_path), **{**HCRO, **settings})
        )
        if book:
            b = ScheduleBook(db, now=lambda: NOW)
            await b.load()
            app.state.schedules = b
        c = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")
        clients.append(c)
        return c

    yield make
    for c in clients:
        await c.aclose()
    for db in dbs:
        await db.close()


async def test_create_list_get_delete(client) -> None:
    c = await client()
    r = await c.post(
        "/api/schedules",
        json={"id": "w1", "start": START, "duration_ms": 2000, "label": "pass", "source": "ui"},
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["schedule"]["id"] == "w1" and body["schedule"]["source"] == "ui"
    assert body["schedule"]["start"] == START
    assert body["next_runs"] == [{"run": 0, "start": START, "start_local": "11:30:00.250 PDT"}]
    assert body["warnings"] == []

    lst = (await c.get("/api/schedules")).json()
    assert lst["now"] == "2026-10-28T18:29:00.000Z"
    assert lst["now_local"] == "11:29:00.000 PDT"
    assert lst["timezone"] == "America/Los_Angeles"
    (item,) = lst["schedules"]
    assert item["schedule"]["id"] == "w1"
    assert item["next_run"] == {"run": 0, "start": START, "start_local": "11:30:00.250 PDT"}
    assert item["last_run"] is None and lst["recent"] == []

    one = (await c.get("/api/schedules/w1")).json()
    assert one["schedule"]["label"] == "pass" and one["runs"] == []

    assert (await c.delete("/api/schedules/w1")).json() == {"deleted": "w1"}
    assert (await c.get("/api/schedules")).json()["schedules"] == []
    assert (await c.delete("/api/schedules/w1")).status_code == 404
    assert (await c.get("/api/schedules/nope")).status_code == 404


async def test_repeat_is_idempotent_and_conflict_is_409(client) -> None:
    c = await client()
    body = {"id": "r", "start": START, "duration_ms": 100, "every_ms": 60000, "count": 10}
    assert (await c.post("/api/schedules", json=body)).status_code == 201
    again = await c.post("/api/schedules", json=body)
    assert again.status_code == 200 and again.json()["schedule"]["id"] == "r"
    assert len(again.json()["next_runs"]) == 5
    clash = await c.post("/api/schedules", json={**body, "duration_ms": 200})
    assert clash.status_code == 409 and "already used" in clash.json()["detail"]


async def test_generated_id(client) -> None:
    c = await client()
    r = await c.post("/api/schedules", json={"start": START, "duration_ms": 10})
    assert r.status_code == 201 and len(r.json()["schedule"]["id"]) == 12


@pytest.mark.parametrize(
    ("body", "detail"),
    [
        ({"start": "2026-10-28T18:00:00Z", "duration_ms": 10}, "in the past"),
        ({"start": START}, "duration_ms"),
        ([1, 2], "JSON object"),
    ],
)
async def test_invalid_is_400(client, body: Any, detail: str) -> None:
    c = await client()
    r = await c.post("/api/schedules", json=body)
    assert r.status_code == 400 and detail in r.json()["detail"]


async def test_too_large_for_free_space_is_409(client) -> None:
    c = await client(BANDWIDTH=26_000_000)
    # 600 s at 26 MS/s is 62.4 GB a run; 20 runs need 1.25 TB, more than 900 GB free.
    r = await c.post(
        "/api/schedules",
        json={"start": START, "duration_ms": 600_000, "every_ms": 3_600_000, "count": 20},
    )
    assert r.status_code == 409 and "free" in r.json()["detail"]


async def test_sweep_warning(client) -> None:
    c = await client(FREQUENCY_STEP=1_000_000, DURATION_SEC=0.5)
    r = await c.post("/api/schedules", json={"start": START, "duration_ms": 2000})
    assert r.status_code == 201
    assert r.json()["warnings"] == [
        "This sensor is sweeping; windows longer than the dwell (0.5 s) will be cut at the retune."
    ]


async def test_no_book_is_503(client) -> None:
    c = await client(book=False)
    assert (await c.get("/api/schedules")).status_code == 503
    assert (await c.post("/api/schedules", json={})).status_code == 503


async def test_runs_and_recent(client) -> None:
    c = await client()
    await c.post(
        "/api/schedules",
        json={"id": "a", "start": START, "duration_ms": 100, "every_ms": 1000, "count": 3},
    )
    book = c._transport.app.state.schedules  # type: ignore[attr-defined]
    start_ms = 1793212200250
    book.report_run("a", 0, "recorded", start_ms=start_ms, filename="f0.sc16", info={"samples": 1})
    book.report_run("a", 1, "missed", start_ms=start_ms + 1000, reason="busy")
    one = (await c.get("/api/schedules/a")).json()
    assert [r["run"] for r in one["runs"]] == [1, 0]
    assert one["runs"][1]["start"] == START and one["runs"][1]["filename"] == "f0.sc16"
    assert one["runs"][0]["start_local"] == "11:30:01.250 PDT"
    lst = (await c.get("/api/schedules")).json()
    assert lst["schedules"][0]["last_run"]["status"] == "missed"
    assert [(r["id"], r["run"]) for r in lst["recent"]] == [("a", 1), ("a", 0)]


async def test_local_time_shows_the_date_when_it_differs(client) -> None:
    c = await client()
    r = await c.post(
        "/api/schedules", json={"start": "2026-10-29T03:00:00.000Z", "duration_ms": 10}
    )
    assert r.json()["next_runs"][0]["start_local"] == "2026-10-28 20:00:00.000 PDT"


async def test_retry_after_the_start_passed_is_a_no_op(client, monkeypatch) -> None:
    c = await client()
    body = {"id": "late", "start": START, "duration_ms": 100}
    assert (await c.post("/api/schedules", json=body)).status_code == 201
    monkeypatch.setattr(routes, "_now", lambda: NOW + 120)  # 60 s after the start
    again = await c.post("/api/schedules", json=body)
    assert again.status_code == 200, again.text
    other = await c.post("/api/schedules", json={**body, "id": "late2"})
    assert other.status_code == 400 and "past" in other.json()["detail"]


async def test_join_a_repeating_schedule_late(client) -> None:
    c = await client()
    r = await c.post(
        "/api/schedules",
        json={
            "id": "rep",
            "start": "2026-10-28T18:20:00.000Z",
            "duration_ms": 100,
            "every_ms": 60000,
            "count": 30,
        },
    )
    assert r.status_code == 201, r.text
    assert r.json()["next_runs"][0] == {
        "run": 9,
        "start": "2026-10-28T18:29:00.000Z",
        "start_local": "11:29:00.000 PDT",
    }
