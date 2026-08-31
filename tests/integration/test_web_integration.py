"""Integration test: WebUI reads from SQLite and renders pages."""

from __future__ import annotations

import pytest

from rfobserver.config import AppSettings
from rfobserver.storage.database import SensorDatabase
from rfobserver.web.app import create_app


@pytest.fixture
async def app_with_db(tmp_path):
    db_path = tmp_path / "web_test.db"
    settings = AppSettings(DB_PATH=str(db_path))

    db = SensorDatabase(str(db_path))
    await db.connect()

    app = create_app(settings)
    app.state.database = db

    yield app, db

    await db.close()


@pytest.mark.asyncio
async def test_dashboard_renders(app_with_db):
    from httpx import ASGITransport, AsyncClient

    app, db = app_with_db
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/")
        assert r.status_code == 200
        assert "RFObserver" in r.text


@pytest.mark.asyncio
async def test_api_status_with_db(app_with_db):
    from httpx import ASGITransport, AsyncClient

    app, db = app_with_db
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/api/status")
        assert r.status_code == 200
        data = r.json()
        assert "version" in data


@pytest.mark.asyncio
async def test_history_page_renders(app_with_db):
    from httpx import ASGITransport, AsyncClient

    app, db = app_with_db
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/history/")
        assert r.status_code == 200


@pytest.fixture
async def _seed_avg(app_with_db):
    from datetime import datetime, timedelta, timezone

    app, db = app_with_db
    start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    await db.insert_avg_window(
        start_time=start,
        duration_sec=2.0,
        sdr_center_freq_hz=915e6,
        sample_rate_hz=56e6,
        gain_db=40.0,
        num_bins=4,
        freq_start_hz=0.0,
        freq_step_hz=1.0,
        pwr_avg=-70.0,
        pwr_max=-50.0,
        pwr_median=-72.0,
        pwr_std=3.0,
        kurtosis=1.0,
        powers=[-80.0, -70.0, -60.0, -50.0],
    )
    await db.insert_detection(
        burst_id="b1",
        start_time=start + timedelta(seconds=0.5),
        stop_time=start + timedelta(seconds=0.6),
        center_freq_hz=915.1e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=100.0,
        detection_timestamp=start + timedelta(seconds=0.5),
        sdr_center_freq_hz=915e6,
        sample_rate_hz=56e6,
        gain_db=40.0,
    )
    return app, db


@pytest.mark.asyncio
async def test_api_averaged_list(_seed_avg):
    from httpx import ASGITransport, AsyncClient

    app, _ = _seed_avg
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.get("/api/averaged")
        assert r.status_code == 200
        windows = r.json()["windows"]
        assert len(windows) == 1
        assert windows[0]["sdr_center_freq_hz"] == 915e6
        assert "psd_powers" not in windows[0]


@pytest.mark.asyncio
async def test_api_averaged_detail_and_detections(_seed_avg):
    from httpx import ASGITransport, AsyncClient

    app, _ = _seed_avg
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        wid = (await c.get("/api/averaged")).json()["windows"][0]["id"]
        detail = await c.get(f"/api/averaged/{wid}")
        assert detail.status_code == 200
        body = detail.json()
        assert body["powers"] == pytest.approx([-80.0, -70.0, -60.0, -50.0], abs=1e-3)
        assert len(body["frequencies"]) == 4

        dets = await c.get(f"/api/averaged/{wid}/detections")
        assert dets.status_code == 200
        assert [d["burst_id"] for d in dets.json()["detections"]] == ["b1"]

        assert (await c.get("/api/averaged/9999")).status_code == 404


@pytest.mark.asyncio
async def test_publish_processed_persists_and_is_queryable(app_with_db):
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from httpx import ASGITransport, AsyncClient

    from rfobserver.config import AppSettings
    from rfobserver.models import IQStatistics
    from rfobserver.pipeline.streaming import StreamingProcessor

    app, db = app_with_db
    settings = AppSettings(_env_file=None)
    storage = MagicMock()
    storage.auto_dir = MagicMock()
    storage.manual_dir = MagicMock()
    receiver = MagicMock()
    receiver.serial = "sim0"
    proc = StreamingProcessor(
        receiver=receiver,
        database=db,
        local_storage=storage,
        settings=settings,
        broadcast=None,
        zms_monitor=None,
        nats_producer=None,
        replay_mode=False,
    )
    summary = SimpleNamespace(
        powers=[-80.0, -70.0],
        frequencies=[915e6, 916e6],
        center_freq=915e6,
        sample_rate=56_000_000,
        num_bins=2,
    )
    result = SimpleNamespace(summary_psd=summary, center_freq_hz=915_000_000, capture_num=1)
    stats = IQStatistics(average=-70.0, max=-50.0, median=-72.0, std=3.0, kurtosis=1.0)

    await proc._publish_processed([-80.0, -70.0], result, stats)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        windows = (await c.get("/api/averaged")).json()["windows"]
        assert len(windows) == 1
        assert windows[0]["sdr_center_freq_hz"] == 915_000_000.0
