"""Replay control endpoints."""

from __future__ import annotations

import numpy as np
import pytest

from rfobserver.config import AppSettings
from rfobserver.web.app import create_app


class _FakeSupervisor:
    def __init__(self):
        self.active = False
        self._replay = None
        self.started_with = None
        self.receiver = None

    async def start_replay(self, receiver):
        self.started_with = receiver
        self._replay = receiver
        self.receiver = receiver
        # Mirrors PipelineSupervisor._start(): the replay pipeline flips
        # `active` True while it runs, even though it isn't the live SDR.
        self.active = True

    async def stop_replay(self):
        self._replay = None
        self.receiver = None
        self.active = False

    def replay_status(self):
        if self._replay is None:
            return None
        rx = self._replay
        return {"source": rx.source_name, "speed": rx.speed, "looping": rx.loop}

    async def set_active(self, v):
        self.active = v
        return v


@pytest.fixture
def app_ctx(tmp_path):
    src = tmp_path / "replays"
    src.mkdir()
    # a tiny valid ci16_le raw file (100 samples)
    (src / "iq_capture_x_915MHz_1.0Msps_0.1s_30dB_test.dat").write_bytes(
        np.arange(200, dtype=np.int16).tobytes()
    )
    settings = AppSettings(
        _env_file=None,
        STORAGE_PATH=str(tmp_path / "storage"),
        DB_PATH=str(tmp_path / "d.db"),
        REPLAY_SOURCE_DIR=str(src),
    )
    app = create_app(settings)
    app.state.supervisor = _FakeSupervisor()
    return app, src


def _client(app):
    from fastapi.testclient import TestClient

    return TestClient(app)


def test_start_raw_replay_then_status_and_stop(app_ctx):
    app, src = app_ctx
    c = _client(app)
    path = str(src / "iq_capture_x_915MHz_1.0Msps_0.1s_30dB_test.dat")
    r = c.post(
        "/api/replay/start",
        json={
            "path": path,
            "sample_rate_hz": 1e6,
            "center_freq_hz": 915e6,
            "gain_db": 30,
            "datatype": "ci16_le",
            "speed": 2.0,
        },
    )
    assert r.status_code == 200
    s = c.get("/api/sensor").json()
    assert s["replay"]["speed"] == 2.0 and s["replay"]["looping"] is True
    assert c.post("/api/replay/stop").status_code == 200
    assert c.get("/api/sensor").json()["replay"] is None


def test_speed_requires_active_replay(app_ctx):
    app, _src = app_ctx
    c = _client(app)
    assert c.post("/api/replay/speed", json={"speed": 4.0}).status_code == 409


def test_path_outside_allowlist_rejected(app_ctx):
    app, _src = app_ctx
    c = _client(app)
    r = c.post("/api/replay/start", json={"path": "/etc/passwd", "sample_rate_hz": 1e6})
    assert r.status_code in (400, 404)


def test_double_start_preserves_original_pre_replay_snapshot(app_ctx):
    """A second /replay/start without an intervening /replay/stop must not
    clobber the snapshot with the already-mutated tuning from the first
    replay. /replay/stop must restore the ORIGINAL pre-replay tuning and
    active state, not either replay's tuning.
    """
    app, src = app_ctx
    c = _client(app)
    path = str(src / "iq_capture_x_915MHz_1.0Msps_0.1s_30dB_test.dat")
    settings = app.state.settings

    orig_bandwidth = settings.BANDWIDTH
    orig_freq_start = settings.FREQUENCY_START
    orig_freq_end = settings.FREQUENCY_END
    orig_gain = settings.GAIN
    assert app.state.supervisor.active is False

    # Replay A.
    r = c.post(
        "/api/replay/start",
        json={"path": path, "sample_rate_hz": 1e6, "center_freq_hz": 915e6, "gain_db": 30},
    )
    assert r.status_code == 200
    assert settings.BANDWIDTH == 1_000_000
    assert settings.GAIN == 30
    # start_replay flips the fake's `active` True, exactly like the real
    # PipelineSupervisor -- this is what makes the clobbering bug possible.
    assert app.state.supervisor.active is True

    # Replay B, started WITHOUT stopping A first.
    r = c.post(
        "/api/replay/start",
        json={"path": path, "sample_rate_hz": 2e6, "center_freq_hz": 433e6, "gain_db": 20},
    )
    assert r.status_code == 200
    assert settings.BANDWIDTH == 2_000_000
    assert settings.GAIN == 20

    assert c.post("/api/replay/stop").status_code == 200

    # Restored to the ORIGINAL pre-replay values, not A's or B's tuning.
    assert orig_bandwidth == settings.BANDWIDTH
    assert orig_freq_start == settings.FREQUENCY_START
    assert orig_freq_end == settings.FREQUENCY_END
    assert orig_gain == settings.GAIN
    # The prior-active state restored must be the ORIGINAL (False), so the
    # live SDR is not unintentionally reactivated by stop.
    assert app.state.supervisor.active is False


def test_replay_start_missing_body_fields_returns_400(app_ctx):
    app, src = app_ctx
    c = _client(app)

    # No filename and no path at all.
    r = c.post("/api/replay/start", json={})
    assert r.status_code == 400

    # path present, sample_rate_hz missing.
    path = str(src / "iq_capture_x_915MHz_1.0Msps_0.1s_30dB_test.dat")
    r = c.post("/api/replay/start", json={"path": path})
    assert r.status_code == 400


def test_replay_speed_missing_body_field_returns_400(app_ctx):
    app, src = app_ctx
    c = _client(app)
    path = str(src / "iq_capture_x_915MHz_1.0Msps_0.1s_30dB_test.dat")
    r = c.post(
        "/api/replay/start",
        json={"path": path, "sample_rate_hz": 1e6, "center_freq_hz": 915e6},
    )
    assert r.status_code == 200

    r = c.post("/api/replay/speed", json={})
    assert r.status_code == 400
