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

    async def start_replay(self, receiver):
        self.started_with = receiver
        self._replay = receiver

    async def stop_replay(self):
        self._replay = None

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
