"""Collection statistics: the sensor's time zone and its local day bounds."""

from __future__ import annotations

import logging
from datetime import date
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from rfobserver.config import AppSettings
from rfobserver.storage.collection import local_day_bounds, sensor_zone, whole_hour_zone
from rfobserver.web.app import create_app


def test_day_bounds_utc_and_denver():
    assert local_day_bounds(date(2026, 10, 26), ZoneInfo("UTC")) == (1_792_972_800, 1_793_059_200)
    lo, hi = local_day_bounds(date(2026, 10, 25), ZoneInfo("America/Denver"))
    assert (lo, hi - lo) == (1_792_908_000, 24 * 3600)  # 2026-10-25T06:00Z
    lo, hi = local_day_bounds(date(2026, 3, 8), ZoneInfo("America/Denver"))
    assert hi - lo == 23 * 3600
    lo, hi = local_day_bounds(date(2026, 11, 1), ZoneInfo("America/Denver"))
    assert hi - lo == 25 * 3600


def test_whole_hour_zone_accepts_whole_hours():
    assert whole_hour_zone("America/Denver") == ZoneInfo("America/Denver")
    assert whole_hour_zone("UTC") == ZoneInfo("UTC")


@pytest.mark.parametrize("bad", ["Asia/Kolkata", "Not/AZone", "", "../etc/passwd"])
def test_whole_hour_zone_rejects_fractional_and_unknown(bad):
    with pytest.raises(ValueError):
        whole_hour_zone(bad)


def test_default_is_utc():
    assert AppSettings(_env_file=None).TIMEZONE == "UTC"
    assert sensor_zone(AppSettings(_env_file=None)) == ZoneInfo("UTC")


def test_sensor_zone_uses_the_setting():
    s = AppSettings(_env_file=None, TIMEZONE="America/Denver")
    assert sensor_zone(s) == ZoneInfo("America/Denver")


def test_a_bad_setting_falls_back_to_utc_with_a_warning(caplog):
    s = AppSettings(_env_file=None, TIMEZONE="Bad/Zone")  # the sensor still starts
    with caplog.at_level(logging.WARNING):
        assert sensor_zone(s) == ZoneInfo("UTC")
    assert "Bad/Zone" in caplog.text


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # Config apply persists to ./.env
    app = create_app(AppSettings(_env_file=None))
    return TestClient(app), app


def test_config_apply_sets_the_timezone(client):
    c, app = client
    r = c.post("/config/apply", json={"timezone": "America/Denver"})
    assert r.status_code == 200, r.text
    assert app.state.settings.TIMEZONE == "America/Denver"


@pytest.mark.parametrize("bad", ["Asia/Kolkata", "Not/AZone"])
def test_config_apply_rejects_a_bad_timezone(client, bad):
    c, app = client
    r = c.post("/config/apply", json={"timezone": bad})
    assert r.status_code == 400
    assert app.state.settings.TIMEZONE == "UTC"
