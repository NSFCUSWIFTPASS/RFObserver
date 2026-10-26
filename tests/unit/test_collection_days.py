"""Collection statistics: the sensor's time zone and its local day bounds."""

from __future__ import annotations

import logging
from datetime import date
from zoneinfo import ZoneInfo

import pytest

from rfobserver.config import AppSettings
from rfobserver.storage.collection import local_day_bounds, sensor_zone, whole_hour_zone


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

    coll.location_zone.cache_clear()
    calls = []
    real = coll._lookup_zone_name

    def counting(lat, lon):
        calls.append((lat, lon))
        return real(lat, lon)

    monkeypatch.setattr(coll, "_lookup_zone_name", counting)
    for _ in range(3):
        sensor_zone(_settings(DENVER))
    assert calls == [DENVER]
    coll.location_zone.cache_clear()


def test_the_timezone_setting_is_gone():
    assert not hasattr(AppSettings(_env_file=None), "TIMEZONE")
