"""Heavy Dashboard aggregations run one at a time per kind, bounding event-loop load."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest

from rfobserver.config import AppSettings
from rfobserver.web.app import create_app

_RANGE = {"since": "2026-09-13T00:00:00Z", "until": "2026-09-14T00:00:00Z"}


class _SlowDB:
    def __init__(self) -> None:
        self.active = {"wf": 0, "stats": 0}
        self.max_active = {"wf": 0, "stats": 0, "total": 0}

    async def _run(self, kind: str) -> None:
        self.active[kind] += 1
        self.max_active[kind] = max(self.max_active[kind], self.active[kind])
        self.max_active["total"] = max(self.max_active["total"], sum(self.active.values()))
        await asyncio.sleep(0.1)
        self.active[kind] -= 1

    async def query_avg_waterfall(self, **_: Any) -> dict[str, Any]:
        await self._run("wf")
        return {
            "bucket_sec": 0.0,
            "num_bins": 0,
            "min_db": 0.0,
            "max_db": 0.0,
            "total_windows": 0,
            "freq_start_hz": 0.0,
            "freq_step_hz": 0.0,
            "mode": 0,
            "buckets": [],
            "psd_rows": [],
        }

    async def query_avg_stats(self, **_: Any) -> dict[str, Any]:
        await self._run("stats")
        return {"bucket_sec": 0.0, "min_pwr": 0.0, "max_pwr": 0.0, "points": []}


@pytest.mark.asyncio
async def test_heavy_aggregations_capped_one_per_kind() -> None:
    app = create_app(AppSettings(_env_file=None))
    db = _SlowDB()
    app.state.database = db
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        reqs = []
        for i in range(4):
            # Distinct until per request so the waterfall cache never hits.
            params = {"since": _RANGE["since"], "until": f"2026-09-14T00:00:0{i}Z"}
            reqs.append(client.get("/api/averaged/waterfall", params=params))
            reqs.append(client.get("/api/averaged/stats", params=params))
        responses = await asyncio.gather(*reqs)
    assert all(r.status_code == 200 for r in responses), [r.status_code for r in responses]
    assert db.max_active["wf"] == 1, "waterfall aggregations must not overlap"
    assert db.max_active["stats"] == 1, "stats aggregations must not overlap"
    assert db.max_active["total"] == 2, "one waterfall and one stats may run together"
