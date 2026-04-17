"""Tests for rfobserver.config."""

from rfobserver.config import AppSettings


def test_default_settings():
    settings = AppSettings(
        _env_file=None,  # don't read .env during tests
    )
    assert settings.FREQUENCY_START == 2_437_000_000
    assert settings.BANDWIDTH == 56_000_000
    assert settings.GAIN == 40
    assert settings.MOCK_RECEIVER is False
    assert settings.NUM_FFT_BINS == 1024
    assert settings.PSD_TIME_RESOLUTION_MS == 0.2


def test_nats_url():
    settings = AppSettings(
        NATS_HOST="nats.example.com",
        NATS_PORT=4223,
        _env_file=None,
    )
    assert settings.NATS_URL == "nats://nats.example.com:4223"


def test_env_prefix(monkeypatch):
    monkeypatch.setenv("RFOBS_GAIN", "50")
    monkeypatch.setenv("RFOBS_LOG_LEVEL", "DEBUG")
    settings = AppSettings(_env_file=None)
    assert settings.GAIN == 50
    assert settings.LOG_LEVEL == "DEBUG"


def test_zms_none_when_incomplete():
    settings = AppSettings(_env_file=None)
    assert settings.zms is None


def test_zms_constructed_when_complete():
    settings = AppSettings(
        ZMS_ZMC_HTTP="http://zmc.test",
        ZMS_IDENTITY_HTTP="http://id.test",
        ZMS_TOKEN="secret-token",
        ZMS_MONITOR_ID="mon-1",
        _env_file=None,
    )
    assert settings.zms is not None
    assert settings.zms.monitor_id == "mon-1"
    assert settings.zms.token == "secret-token"
    assert settings.zms.dst_or_zmc == "http://zmc.test"


def test_zms_with_dst_http():
    settings = AppSettings(
        ZMS_ZMC_HTTP="http://zmc.test",
        ZMS_DST_HTTP="http://dst.test",
        ZMS_IDENTITY_HTTP="http://id.test",
        ZMS_TOKEN="tok",
        ZMS_MONITOR_ID="m1",
        _env_file=None,
    )
    assert settings.zms.dst_or_zmc == "http://dst.test"
