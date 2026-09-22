"""A malformed numeric query param is the client's mistake, so it must read as 400.

Without this every one of these endpoints raises ValueError out of a bare int()
and Starlette turns it into a 500, which sends an operator looking for a server
fault over a typo in a URL.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from rfobserver.config import AppSettings
from rfobserver.web.app import create_app

_RANGE = {"since": "2026-09-13T00:00:00Z", "until": "2026-09-14T00:00:00Z"}


class _StubDB:
    """Answers the three endpoints so the test exercises parsing, not queries."""

    async def query_avg_waterfall(self, **_: Any) -> dict[str, Any]:
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
        return {"bucket_sec": 0.0, "min_pwr": 0.0, "max_pwr": 0.0, "points": []}

    async def query_avg_minute_peaks(self, **_: Any) -> list[Any]:
        return []

    async def get_config(self, _key: str) -> str | None:
        return None


async def _get(path: str, params: dict[str, str]) -> httpx.Response:
    app = create_app(AppSettings(_env_file=None))
    app.state.database = _StubDB()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        return await client.get(path, params={**_RANGE, **params})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/api/averaged/waterfall", {"max_rows": "abc"}),
        ("/api/averaged/waterfall", {"max_bins": "abc"}),
        ("/api/averaged/stats", {"max_points": "abc"}),
        ("/api/averaged/peaks", {"count": "abc"}),
        ("/api/averaged/peaks", {"window_sec": "abc"}),
    ],
)
async def test_non_numeric_value_is_a_client_error(path: str, params: dict[str, str]) -> None:
    resp = await _get(path, params)
    assert resp.status_code == 400, f"{path} {params} gave {resp.status_code}"
    assert "integer" in resp.json()["detail"].lower()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/api/averaged/waterfall", {"max_rows": "1.5"}),
        ("/api/averaged/stats", {"max_points": "1e3"}),
    ],
)
async def test_non_integer_numeric_is_also_rejected(path: str, params: dict[str, str]) -> None:
    # int("1.5") raises just as int("abc") does; both are the client's mistake.
    resp = await _get(path, params)
    assert resp.status_code == 400


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/api/averaged/waterfall", {"max_rows": "250"}),
        ("/api/averaged/waterfall", {}),
        ("/api/averaged/stats", {"max_points": "250"}),
        ("/api/averaged/stats", {}),
    ],
)
async def test_valid_and_absent_values_still_work(path: str, params: dict[str, str]) -> None:
    resp = await _get(path, params)
    assert resp.status_code == 200
