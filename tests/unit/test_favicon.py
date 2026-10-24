"""Every page links the tab icon, and the sensor and fleet apps serve it."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi.testclient import TestClient

from rfobserver.config import AppSettings
from rfobserver.fleet.app import create_fleet_app
from rfobserver.web.app import create_app

if TYPE_CHECKING:
    from pathlib import Path

ICONS = ("/static/favicon.ico", "/static/favicon-32.png", "/static/apple-touch-icon.png")


def _check(client: TestClient, page: str) -> None:
    html = client.get(page).text
    for path in ICONS:
        assert f'href="{path}"' in html, path
        r = client.get(path)
        assert r.status_code == 200 and r.headers["content-type"].startswith("image/"), path


def test_sensor_pages_link_and_serve_the_icon() -> None:
    _check(TestClient(create_app(AppSettings(_env_file=None))), "/live/")


def test_fleet_pages_link_and_serve_the_icon(tmp_path: Path) -> None:
    _check(TestClient(create_fleet_app(tmp_path, mdns=False)), "/sensors")
