"""docs/collection-api.md: every example request is run against a sensor
holding the doc's example data, and its response must match the documented
one. A documented object may leave keys out (the doc trims long responses),
and a documented list may show only its first items; everything shown must
be exact. The field reference must list every field the API returns."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from rfobserver.config import AppSettings
from rfobserver.storage.collection import FIELDS
from rfobserver.storage.database import COLLECTION_SINCE_KEY, SensorDatabase
from rfobserver.web.app import create_app
from rfobserver.web.routes import collection as routes

DOC = Path(__file__).resolve().parents[2] / "docs" / "collection-api.md"

UTC = timezone.utc


def _ts(*a: int) -> int:
    return int(datetime(*a, tzinfo=UTC).timestamp())


# The doc's example sensor: in Boulder, so America/Denver (UTC-6 in October), counting
# since local midnight on 2026-10-18, and "now" is 12:00 local on 2026-10-26.
SINCE = _ts(2026, 10, 18, 6)
NOW = _ts(2026, 10, 26, 18)
CAPTURE = 224_000_000  # one second of IQ at 56 MS/s, 4 bytes per sample


def example_hours() -> dict[int, dict[str, float]]:
    """Every hour counted: one 1 s auto capture and a full hour of live RF.
    On 2026-10-25 (local) there is also a 4 s manual capture at 09:00 and two
    captures deleted by the storage cap at 15:00."""
    hours: dict[int, dict[str, float]] = {
        h: {
            "auto_captures": 1,
            "auto_bytes": CAPTURE,
            "auto_seconds": 1.0,
            "active_seconds": 3600.0,
        }
        for h in range(SINCE, NOW, 3600)
    }
    hours[_ts(2026, 10, 25, 15)].update(
        manual_captures=1, manual_bytes=4 * CAPTURE, manual_seconds=4.0
    )
    hours[_ts(2026, 10, 25, 21)].update(deleted_cap_captures=2, deleted_cap_bytes=2 * CAPTURE)
    return hours


SNAPSHOT = {
    "auto_captures": 180,
    "manual_captures": 1,
    "auto_bytes": 40_320_036_000,
    "manual_bytes": 896_000_200,
    "cap_bytes": 53_687_091_200,
}


@pytest.fixture
async def client(tmp_path, monkeypatch):
    monkeypatch.setattr(routes, "_now", lambda: float(NOW))
    monkeypatch.setattr(
        routes.shutil,
        "disk_usage",
        lambda _p: SimpleNamespace(free=412_316_860_416, total=982_141_468_672),
    )
    db = SensorDatabase(str(tmp_path / "c.db"))
    await db.connect()
    await db.set_config(COLLECTION_SINCE_KEY, str(SINCE))
    await db.add_collection_hours(example_hours())
    app = create_app(AppSettings(_env_file=None, LATITUDE=40.01, LONGITUDE=-105.27))
    app.state.database = db
    app.state.local_storage = SimpleNamespace(
        snapshot=lambda: dict(SNAPSHOT), storage_path=str(tmp_path)
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://sensor"
    ) as c:
        yield c
    await db.close()


_BLOCK = re.compile(r"```(\w+)\n(.*?)```", re.S)
_CURL = re.compile(r'curl -s "\$SENSOR(/api/collection/[^"]+)"')


def _examples() -> list[tuple[str, Any]]:
    """(path, documented response) for each curl block followed by a json block."""
    blocks = _BLOCK.findall(DOC.read_text())
    out = []
    for (lang, body), (nxt_lang, nxt_body) in zip(blocks, blocks[1:], strict=False):
        m = _CURL.search(body)
        if lang == "bash" and m and nxt_lang == "json":
            out.append((m.group(1), json.loads(nxt_body)))
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
    elif isinstance(want, float):
        assert got == pytest.approx(want), where
    else:
        assert got == want, where


def test_the_doc_has_examples_for_every_endpoint():
    paths = {p.split("?")[0] for p, _ in _examples()}
    assert paths == {f"/api/collection/{e}" for e in ("daily", "average", "hourly", "storage")}


@pytest.mark.parametrize(("path", "want"), _examples(), ids=[p for p, _ in _examples()])
async def test_documented_response(client, path, want):
    r = await client.get(path)
    assert r.status_code == (400 if set(want) == {"detail"} else 200), r.text
    _contains(r.json(), want, path)


async def test_field_reference_lists_every_field(client):
    text = DOC.read_text()
    documented = set(re.findall(r"^\| `(\w+)` \|", text, re.M))
    day = (await client.get("/api/collection/daily?date=2026-10-25")).json()
    hour = (
        await client.get(
            "/api/collection/hourly?since=2026-10-26T00:00:00Z&until=2026-10-26T01:00:00Z"
        )
    ).json()
    avg = (await client.get("/api/collection/average")).json()
    rng = (await client.get("/api/collection/daily?start=2026-10-24&end=2026-10-25")).json()
    store = (await client.get("/api/collection/storage")).json()
    returned = set(day) | set(hour) | set(hour["hours"][0]) | set(avg) | set(rng) | set(store)
    assert set(FIELDS) <= documented
    assert returned == documented
