"""Tests for rfobserver.config."""

from rfobserver.config import AppSettings
from rfobserver.web.routes.config import _persist_settings


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


def test_sensor_active_defaults_false():
    # Fresh install starts in Standby so it does not claim the SDR until enabled.
    settings = AppSettings(_env_file=None)
    assert settings.SENSOR_ACTIVE is False


def test_sensor_active_env_override(monkeypatch):
    monkeypatch.setenv("RFOBS_SENSOR_ACTIVE", "true")
    settings = AppSettings(_env_file=None)
    assert settings.SENSOR_ACTIVE is True


def test_toggle_persists_to_env_and_reloads(monkeypatch, tmp_path):
    """A UI toggle (persist to .env in cwd) must survive a restart.

    This is what makes runtime toggles durable for the systemd service, whose
    WorkingDirectory is the writable state dir holding this .env.
    """
    monkeypatch.chdir(tmp_path)

    # Enable non-default toggles, as a UI toggle would, then persist.
    settings = AppSettings(_env_file=None)
    settings.SENSOR_ACTIVE = True
    settings.NATS_ENABLED = True
    settings.ZMS_ENABLED = True
    _persist_settings(settings)

    assert (tmp_path / ".env").exists()

    # A fresh process reads the same .env from cwd and sees the enabled state.
    reloaded = AppSettings()
    assert reloaded.SENSOR_ACTIVE is True
    assert reloaded.NATS_ENABLED is True
    assert reloaded.ZMS_ENABLED is True

    # Toggling back to the default is likewise durable.
    reloaded.SENSOR_ACTIVE = False
    _persist_settings(reloaded)
    assert AppSettings().SENSOR_ACTIVE is False


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


def test_burst_window_covers_long_bursts() -> None:
    """Field default window must hold ~400 ms bursts with room to spare.

    A 393.1 ms burst is ~1966 rows at 0.2 ms; it must sit well inside the
    window (not merely fit) to be measured at its true duration, so the window
    is 4096 rows (~819 ms), not just over 1966.
    """
    s = AppSettings(_env_file=None)
    assert s.BURST_WINDOW_ROWS >= 4096
    assert s.BURST_EVAL_INTERVAL_ROWS == s.BURST_WINDOW_ROWS // 2
