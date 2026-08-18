"""Tests for POST /captures/redetect/{filename} (re-run detection on a stored grid)."""

from __future__ import annotations

import json

import numpy as np
import pytest

from rfobserver.config import AppSettings
from rfobserver.storage import psd_grid
from rfobserver.web.app import create_app

CENTER = 915_000_000
BANDWIDTH = 26_000_000
ROWS = 200
NUM_BINS = 64
TRES = 0.001


@pytest.fixture
def settings(tmp_path):
    return AppSettings(_env_file=None, STORAGE_PATH=str(tmp_path / "storage"))


@pytest.fixture
def app(settings):
    return create_app(settings)


def _seed_capture_meta(storage, base):
    sc16 = storage / f"{base}.sc16"
    sc16.write_bytes(b"\x00" * 8)
    sc16.with_suffix(".json").write_text(
        json.dumps(
            {
                "start_time": "2026-08-18T00:00:00+00:00",
                "duration_sec": ROWS * TRES,
                "center_freq_hz": CENTER,
                "sample_rate_hz": BANDWIDTH,
                "gain_db": 35,
            }
        )
    )
    return sc16


def _seed_grid_capture(settings, base):
    """Seed <base>.sc16 + .json + .psd + .psd.json with a synthetic burst."""
    from pathlib import Path

    storage = Path(settings.STORAGE_PATH)
    storage.mkdir(parents=True, exist_ok=True)
    sc16 = _seed_capture_meta(storage, base)

    # Flat -100 dB noise floor with one strong burst (60 dB above floor).
    grid = np.full((ROWS, NUM_BINS), -100.0, dtype=np.float32)
    grid[50:71, 30:34] = -40.0
    raw_path, meta_path = psd_grid.grid_paths(sc16)
    grid.tofile(raw_path)
    freq_axis = (np.arange(NUM_BINS) - NUM_BINS / 2) * (BANDWIDTH / NUM_BINS)
    psd_grid.write_meta(
        meta_path,
        rows=ROWS,
        num_bins=NUM_BINS,
        time_resolution_s=TRES,
        center_freq_hz=CENTER,
        bandwidth_hz=BANDWIDTH,
        freq_axis=freq_axis,
        grid_min=-100.0,
        grid_max=-40.0,
        cal_offset_db=None,
    )
    return sc16


def _seed_capture_without_grid(settings, base):
    from pathlib import Path

    storage = Path(settings.STORAGE_PATH)
    storage.mkdir(parents=True, exist_ok=True)
    return _seed_capture_meta(storage, base)


async def _post(app, path, json_body=None):
    from httpx import ASGITransport, AsyncClient

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        if json_body is None:
            return await client.post(path)
        return await client.post(path, json=json_body)


@pytest.mark.asyncio
async def test_redetect_rewrites_sidecar(app, settings):
    _seed_grid_capture(settings, "cap")

    r = await _post(app, "/captures/redetect/cap.sc16", {"threshold_high_db": 20.0})
    assert r.status_code == 200
    assert len(r.json()["detections"]) >= 1

    r2 = await _post(app, "/captures/redetect/cap.sc16", {"threshold_high_db": 70.0})
    assert r2.status_code == 200
    assert r2.json()["detections"] == []


@pytest.mark.asyncio
async def test_redetect_empty_body_uses_defaults(app, settings):
    _seed_grid_capture(settings, "cap")

    r = await _post(app, "/captures/redetect/cap.sc16")
    assert r.status_code == 200
    assert "detections" in r.json()


@pytest.mark.asyncio
async def test_redetect_404_without_grid(app, settings):
    _seed_capture_without_grid(settings, "cap")

    r = await _post(app, "/captures/redetect/cap.sc16", {})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_redetect_404_unknown_capture(app, settings):
    from pathlib import Path

    Path(settings.STORAGE_PATH).mkdir(parents=True, exist_ok=True)

    r = await _post(app, "/captures/redetect/does-not-exist.sc16", {})
    assert r.status_code == 404
