"""Captures read paths must span the auto/ and manual/ storage subdirs."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from rfobserver.config import AppSettings
from rfobserver.web.app import create_app


@pytest.fixture
def settings(tmp_path):
    return AppSettings(_env_file=None, STORAGE_PATH=str(tmp_path / "storage"))


@pytest.fixture
def app(settings):
    return create_app(settings)


def _seed(settings, sub, base):
    d = Path(settings.STORAGE_PATH) / sub
    d.mkdir(parents=True, exist_ok=True)
    sc16 = d / f"{base}.sc16"
    sc16.write_bytes(b"\x00" * 16)
    sc16.with_suffix(".json").write_text(json.dumps({"center_freq_hz": 915_000_000}))
    return sc16


async def _get(app, path):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        return await client.get(path)


@pytest.mark.asyncio
async def test_list_spans_auto_and_manual(app, settings):
    _seed(settings, "auto", "trig-cap")
    _seed(settings, "manual", "man-cap")

    r = await _get(app, "/captures/list")
    assert r.status_code == 200
    by_name = {e["filename"]: e for e in r.json()}
    assert "trig-cap.sc16" in by_name
    assert "man-cap.sc16" in by_name
    # Each entry reports which set it came from.
    assert by_name["trig-cap.sc16"]["origin"] == "auto"
    assert by_name["man-cap.sc16"]["origin"] == "manual"


@pytest.mark.asyncio
async def test_detail_resolves_capture_in_subdir(app, settings):
    _seed(settings, "auto", "trig-cap")
    r = await _get(app, "/captures/detail/trig-cap.sc16")
    assert r.status_code == 200
    assert r.json()["filename"] == "trig-cap.sc16"
