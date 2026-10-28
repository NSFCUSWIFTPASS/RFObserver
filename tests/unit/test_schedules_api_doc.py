"""docs/schedules-api.md: every example request runs, in order, against a
sensor holding the doc's example schedule, and its response must match the
documented one (documented objects may leave keys out, documented lists may
show only their first items). The field reference must list every field the
API returns."""

from __future__ import annotations

import json
import re
import shlex
from collections import namedtuple
from pathlib import Path
from typing import Any

import httpx
import pytest

from rfobserver.config import AppSettings
from rfobserver.schedules.book import ScheduleBook
from rfobserver.schedules.model import parse_schedule
from rfobserver.storage.database import SensorDatabase
from rfobserver.web.app import create_app
from rfobserver.web.routes import schedules as routes

DOC = Path(__file__).resolve().parents[2] / "docs" / "schedules-api.md"

# The doc's example sensor: at Hat Creek (America/Los_Angeles), "now" is
# 2026-10-28 18:29:00 UTC (11:29 PDT), 900 GB free.
NOW = 1793212140.0
HOURLY_START = 1793206800000  # 2026-10-28T17:00:00.000Z
Usage = namedtuple("Usage", "total used free")


@pytest.fixture
async def client(tmp_path, monkeypatch):
    monkeypatch.setattr(routes, "_now", lambda: NOW)
    monkeypatch.setattr(routes.shutil, "disk_usage", lambda p: Usage(10**12, 10**11, 9 * 10**11))
    db = SensorDatabase(str(tmp_path / "d.db"))
    await db.connect()
    app = create_app(
        AppSettings(
            _env_file=None,
            STORAGE_PATH=str(tmp_path),
            LATITUDE=40.8165,
            LONGITUDE=-121.4639,
            BANDWIDTH=26_000_000,
        )
    )
    book = ScheduleBook(db, now=lambda: NOW)
    await book.load()
    hourly = parse_schedule(
        {
            "id": "hourly",
            "start_ms": HOURLY_START,
            "duration_ms": 2000,
            "every_ms": 3_600_000,
            "count": 24,
            "label": "Hourly sample",
        },
        now_ms=HOURLY_START - 60_000,
        grace_ms=0,
        source="api",
        new_id=lambda: "hourly",
    )
    await book.add(hourly)
    book.report_run(
        "hourly",
        0,
        "recorded",
        start_ms=HOURLY_START,
        filename="323E282-hcro-jet-001-20261028T170000000-sched-hourly-0.sc16",
        info={
            "first_sample_time": "2026-10-28T17:00:00.000000031Z",
            "start_error_ns": 31,
            "late_start_samples": 0,
            "lost_samples": 0,
            "writer_refused_samples": 0,
            "clock_source": "ntp",
            "clock_server": "0.fr.pool.ntp.org",
            "samples": 52000000,
        },
    )
    book.report_run(
        "hourly",
        1,
        "partial",
        start_ms=HOURLY_START + 3_600_000,
        filename="323E282-hcro-jet-001-20261028T180000000-sched-hourly-1.sc16",
        reason="retuned",
        info={
            "first_sample_time": "2026-10-28T18:00:00.000000012Z",
            "start_error_ns": 12,
            "late_start_samples": 0,
            "lost_samples": 0,
            "writer_refused_samples": 0,
            "clock_source": "ntp",
            "clock_server": "0.fr.pool.ntp.org",
            "samples": 31200000,
        },
    )
    app.state.schedules = book
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://sensor"
    ) as c:
        yield c
    await db.close()


_BLOCK = re.compile(r"```(\w+)\n(.*?)```", re.S)


def _request(body: str) -> tuple[str, str, Any] | None:
    """(method, path, json body) of a ``curl ... "$SENSOR/api/schedules..."``."""
    text = body.replace("\\\n", " ")
    if "$SENSOR/api/schedules" not in text:
        return None
    words = shlex.split(text, posix=True)
    method, path, data = "GET", None, None
    for i, w in enumerate(words):
        if w == "-X":
            method = words[i + 1]
        elif w == "-d":
            data = json.loads(words[i + 1])
        elif w.startswith("$SENSOR"):
            path = w[len("$SENSOR") :]
    assert path is not None
    return method, path, data


def _examples() -> list[tuple[str, str, Any, Any]]:
    blocks = _BLOCK.findall(DOC.read_text())
    out = []
    for (lang, body), (nxt_lang, nxt_body) in zip(blocks, blocks[1:], strict=False):
        req = _request(body) if lang == "bash" else None
        if req and nxt_lang == "json":
            out.append((*req, json.loads(nxt_body)))
    return out


def _contains(got: Any, want: Any, where: str = "") -> None:
    if isinstance(want, dict):
        assert isinstance(got, dict), where
        for k, v in want.items():
            assert k in got, f"{where}.{k} missing"
            _contains(got[k], v, f"{where}.{k}")
    elif isinstance(want, list):
        assert isinstance(got, list) and len(got) >= len(want), where
        for i, v in enumerate(want):
            _contains(got[i], v, f"{where}[{i}]")
    else:
        assert got == want, f"{where}: {got!r} != {want!r}"


def test_the_doc_covers_every_method():
    methods = {(m, p.split("/")[3] if p.count("/") >= 3 else "") for m, p, _, _ in _examples()}
    assert {m for m, _ in methods} == {"GET", "POST", "DELETE"}


async def test_documented_responses_in_order(client):
    examples = _examples()
    assert len(examples) >= 6
    for method, path, data, want in examples:
        r = await client.request(method, path, json=data)
        if set(want) == {"detail"}:
            assert r.status_code in (400, 404, 409), (method, path, r.text)
        else:
            assert r.status_code in (200, 201), (method, path, r.text)
        _contains(r.json(), want, f"{method} {path}")


async def test_field_reference_lists_every_field(client):
    documented = set(re.findall(r"^\| `(\w+)` \|", DOC.read_text(), re.M))
    created = (
        await client.post(
            "/api/schedules",
            json={"id": "f", "start": "2026-10-28T18:40:00Z", "duration_ms": 10},
        )
    ).json()
    lst = (await client.get("/api/schedules")).json()
    one = (await client.get("/api/schedules/hourly")).json()
    gone = (await client.delete("/api/schedules/f")).json()
    item = lst["schedules"][0]
    returned = (
        set(created)
        | set(created["schedule"])
        | set(created["next_runs"][0])
        | set(lst)
        | set(item)
        | set(lst["recent"][0])
        | set(one)
        | set(one["runs"][0])
        | set(one["runs"][0]["info"])
        | set(gone)
    )
    assert returned == documented
