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
