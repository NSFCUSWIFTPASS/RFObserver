"""Tests for GET /captures/detections/{filename} (sidecar lazy-generate route)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from rfobserver.config import AppSettings
from rfobserver.storage import detections_sidecar as ds
from rfobserver.storage import psd_grid
from rfobserver.storage.database import SensorDatabase
from rfobserver.web.app import create_app

CENTER = 915e6
SAMPLE_RATE = 56e6
GAIN = 40.0
TRES = 0.5
DURATION_SEC = 10.0
# Grid rows chosen so the effective tres (duration / rows) equals the nominal
# TRES, keeping the row assertions below independent of the tres-vs-effective
# distinction (that distinction is exercised in test_detections_sidecar).
ROWS = 20


@pytest.fixture
def settings(tmp_path):
    return AppSettings(
        _env_file=None,
        STORAGE_PATH=str(tmp_path / "storage"),
        DB_PATH=str(tmp_path / "test.db"),
    )


@pytest.fixture
async def app_and_db(settings):
    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    app = create_app(settings)
    app.state.database = db
    yield app, db, settings
    await db.close()


def _seed_capture(settings, base, cap_start):
    from pathlib import Path

    storage = Path(settings.STORAGE_PATH)
    storage.mkdir(parents=True, exist_ok=True)
    sc16 = storage / f"{base}.sc16"
    sc16.write_bytes(b"\x00" * 100)
    meta = {
        "start_time": cap_start.isoformat(),
        "duration_sec": DURATION_SEC,
        "center_freq_hz": CENTER,
        "sample_rate_hz": SAMPLE_RATE,
        "gain_db": GAIN,
    }
    sc16.with_suffix(".json").write_text(json.dumps(meta))
    _raw, psd_meta_path = psd_grid.grid_paths(sc16)
    psd_meta_path.write_text(json.dumps({"time_resolution_s": TRES, "rows": ROWS}))
    return sc16


async def _insert(db, burst_id, start, stop, **overrides):
    kwargs = dict(
        burst_id=burst_id,
        start_time=start,
        stop_time=stop,
        center_freq_hz=CENTER,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=(stop - start).total_seconds() * 1000.0,
        detection_timestamp=start,
        sdr_center_freq_hz=CENTER,
        sample_rate_hz=SAMPLE_RATE,
        gain_db=GAIN,
        peak_freq_hz=CENTER + 1000.0,
    )
    kwargs.update(overrides)
    await db.insert_detection(**kwargs)


async def _get(app, path):
    from httpx import ASGITransport, AsyncClient

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        return await client.get(path)


@pytest.mark.asyncio
async def test_old_capture_lazily_generates_and_writes_sidecar(app_and_db):
    app, db, settings = app_and_db
    cap_start = datetime.now(timezone.utc) - timedelta(hours=1)  # ended long ago
    sc16 = _seed_capture(settings, "old-cap", cap_start)

    await _insert(
        db, "in-window", cap_start + timedelta(seconds=2.0), cap_start + timedelta(seconds=3.0)
    )
    await _insert(
        db,
        "wrong-tuning",
        cap_start + timedelta(seconds=2.0),
        cap_start + timedelta(seconds=3.0),
        sdr_center_freq_hz=2437e6,
    )

    # Sidecar does not exist yet.
    assert not ds.sidecar_path(sc16).exists()

    r = await _get(app, "/captures/detections/old-cap.sc16")
    assert r.status_code == 200
    payload = r.json()
    assert payload.get("pending") is not True
    assert len(payload["detections"]) == 1
    det = payload["detections"][0]
    assert det["row_start"] == 4  # round(2.0 / 0.5)
    assert det["row_stop"] == 6  # round(3.0 / 0.5)

    # File was written.
    assert ds.sidecar_path(sc16).exists()

    # Second call reads the file, not the DB: overwrite the sidecar with a
    # sentinel payload and assert it is returned verbatim.
    ds.sidecar_path(sc16).write_text(json.dumps({"detections": [], "sentinel": True}))
    r2 = await _get(app, "/captures/detections/old-cap.sc16")
    assert r2.status_code == 200
    assert r2.json() == {"detections": [], "sentinel": True}


@pytest.mark.asyncio
async def test_recent_capture_without_sidecar_is_pending(app_and_db):
    app, db, settings = app_and_db
    cap_start = datetime.now(timezone.utc)  # just ended, within grace
    _seed_capture(settings, "fresh-cap", cap_start)

    r = await _get(app, "/captures/detections/fresh-cap.sc16")
    assert r.status_code == 200
    payload = r.json()
    assert payload["detections"] == []
    assert payload["pending"] is True
