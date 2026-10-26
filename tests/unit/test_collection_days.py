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


DENVER = (40.01, -105.27)  # Boulder
HCRO = (40.82, -121.47)
DELHI = (28.61, 77.21)  # Asia/Kolkata: UTC+5:30


def _settings(loc: tuple[float, float] | None) -> AppSettings:
    lat, lon = loc if loc else (None, None)
    return AppSettings(_env_file=None, LATITUDE=lat, LONGITUDE=lon)


def test_zone_comes_from_the_sensor_location():
    assert sensor_zone(_settings(DENVER)) == (ZoneInfo("America/Denver"), "location")
    assert sensor_zone(_settings(HCRO)) == (ZoneInfo("America/Los_Angeles"), "location")


def test_no_location_is_pacific():
    assert sensor_zone(_settings(None)) == (ZoneInfo("America/Los_Angeles"), "default")
    half = AppSettings(_env_file=None, LATITUDE=40.0, LONGITUDE=None)
    assert sensor_zone(half) == (ZoneInfo("America/Los_Angeles"), "default")


def test_a_fractional_zone_at_the_location_falls_back_to_pacific(caplog):
    with caplog.at_level(logging.WARNING):
        assert sensor_zone(_settings(DELHI)) == (ZoneInfo("America/Los_Angeles"), "default")
    assert "Asia/Kolkata" in caplog.text


def test_a_requested_zone_wins():
    assert sensor_zone(_settings(DENVER), "Asia/Tokyo") == (ZoneInfo("Asia/Tokyo"), "request")
    with pytest.raises(ValueError):
        sensor_zone(_settings(DENVER), "Asia/Kolkata")
    with pytest.raises(ValueError):
        sensor_zone(_settings(DENVER), "Not/AZone")


def test_the_location_lookup_is_cached(monkeypatch):
    from rfobserver.storage import collection as coll

    calls = _counting_lookup(monkeypatch)
    for _ in range(3):
        sensor_zone(_settings(DENVER))
    assert calls == [DENVER]
    coll.location_zone.cache_clear()


def test_the_location_wins_over_the_timezone_setting():
    s = AppSettings(_env_file=None, TIMEZONE="Asia/Tokyo", LATITUDE=40.01, LONGITUDE=-105.27)
    assert sensor_zone(s) == (ZoneInfo("America/Denver"), "location")
    assert sensor_zone(s, "Asia/Tokyo") == (ZoneInfo("Asia/Tokyo"), "request")


def test_timezone_setting_is_used_without_a_location():
    s = AppSettings(_env_file=None, TIMEZONE="Asia/Tokyo")
    assert sensor_zone(s) == (ZoneInfo("Asia/Tokyo"), "setting")
    assert AppSettings(_env_file=None).TIMEZONE == ""  # not set


def test_timezone_setting_from_the_environment(monkeypatch):
    monkeypatch.setenv("RFOBS_TIMEZONE", "America/Chicago")
    assert sensor_zone(AppSettings(_env_file=None)) == (ZoneInfo("America/Chicago"), "setting")


@pytest.mark.parametrize("bad", ["Not/AZone", "Asia/Kolkata"])
def test_an_unusable_timezone_setting_falls_back_to_pacific(caplog, bad):
    s = AppSettings(_env_file=None, TIMEZONE=bad)
    with caplog.at_level(logging.WARNING):
        assert sensor_zone(s) == (ZoneInfo("America/Los_Angeles"), "default")
    assert bad in caplog.text


def test_a_fractional_zone_at_the_location_uses_the_setting():
    s = AppSettings(_env_file=None, TIMEZONE="Asia/Dubai", LATITUDE=DELHI[0], LONGITUDE=DELHI[1])
    assert sensor_zone(s) == (ZoneInfo("Asia/Dubai"), "setting")


def _counting_lookup(monkeypatch):
    from rfobserver.storage import collection as coll

    coll.location_zone.cache_clear()
    calls = []
    real = coll._lookup_zone_name

    def counting(lat, lon):
        calls.append((lat, lon))
        return real(lat, lon)

    monkeypatch.setattr(coll, "_lookup_zone_name", counting)
    return calls


def test_the_zone_is_resolved_at_startup(monkeypatch, caplog):
    calls = _counting_lookup(monkeypatch)
    with caplog.at_level(logging.INFO):
        app = create_app(_settings(DENVER))
    assert calls == [DENVER]  # before any request
    assert app.state.collection_zone.current() == (ZoneInfo("America/Denver"), "location")
    assert app.state.settings.TIMEZONE == "America/Denver"  # set from the location
    assert "America/Denver" in caplog.text


def test_a_location_change_resolves_the_zone_at_once(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # Config apply persists to ./.env
    calls = _counting_lookup(monkeypatch)
    app = create_app(_settings(DENVER))
    c = TestClient(app)
    r = c.post("/config/apply", json={"latitude": HCRO[0], "longitude": HCRO[1]})
    assert r.status_code == 200, r.text
    assert calls == [DENVER, HCRO]  # looked up by the apply, not a later request
    assert app.state.collection_zone.current() == (ZoneInfo("America/Los_Angeles"), "location")
    assert app.state.settings.TIMEZONE == "America/Los_Angeles"
    assert "RFOBS_TIMEZONE=America/Los_Angeles" in (tmp_path / ".env").read_text()
    # Without a location, TIMEZONE (the last one found, or set by hand) is used.
    c.post("/config/apply", json={"latitude": "", "longitude": ""})
    assert app.state.collection_zone.current() == (ZoneInfo("America/Los_Angeles"), "setting")


def test_requests_use_the_resolved_zone(monkeypatch):
    calls = _counting_lookup(monkeypatch)
    app = create_app(_settings(DENVER))
    zone = app.state.collection_zone
    for _ in range(3):
        assert zone.get(app.state.settings)[1] == "location"
    assert calls == [DENVER]
