"""BURST_DETECTION_ENABLED: off skips detection in both pipelines without
holding anything up, and the sensor reports it (and the PSD backend)."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import numpy as np
from fastapi.testclient import TestClient

from rfobserver.capture.receiver import CaptureResult, RawCapture
from rfobserver.config import AppSettings
from rfobserver.pipeline import continuous
from rfobserver.pipeline.streaming import _STOP
from rfobserver.processing import psd_cuda
from rfobserver.processing.rolling_burst import RollingBurstDetector
from rfobserver.web.app import create_app
from tests.unit.test_isolation_lead import _fake_stage, _grid, _lossless
from tests.unit.test_recording_gaps import _proc


def _run(proc, grids):
    """Feed grids (chunk_start, grid) through the burst thread, inline."""
    for i, start in enumerate(grids):
        proc._burst_queue.put((_grid(proc, proc._settings.NUM_FFT_BINS), 915e6, i, start))
    proc._burst_queue.put(_STOP)
    proc._burst_detection_loop()


def test_off_skips_detection_and_isolation_but_marks_grids_done(tmp_path, monkeypatch):
    proc = _lossless(tmp_path, ISOLATION_ENABLED=True, BURST_DETECTION_ENABLED=False)
    proc._running = True
    stage = _fake_stage()
    proc._isolation = stage
    proc._active_bursts = [{"id": "old"}]
    fed = []
    monkeypatch.setattr(RollingBurstDetector, "feed", lambda self, *a, **k: fed.append(1) or [])
    monkeypatch.setattr(proc, "_submit_isolation", lambda *a: stage.calls.append("submit"))
    ss = proc._slice_samples
    _run(proc, [0, 10 * ss])
    assert fed == [] and stage.calls == []
    assert proc._active_bursts == []
    # The lossless lead wait sees every grid done.
    assert proc._burst_done_end == 20 * ss
    assert proc._burst_done_gen == proc._config_generation
    assert proc._burst_grids_done == 2


def test_switching_back_on_starts_a_fresh_window(tmp_path, monkeypatch):
    proc = _proc(tmp_path)
    proc._running = True
    built = []
    real_init = RollingBurstDetector.__init__

    def counting_init(self, *a, **k):
        built.append(1)
        real_init(self, *a, **k)

    monkeypatch.setattr(RollingBurstDetector, "__init__", counting_init)
    monkeypatch.setattr(RollingBurstDetector, "feed", lambda self, *a, **k: [])
    ss = proc._slice_samples
    seq = iter([True, False, True])

    # Flip the setting as each grid is taken, as a Config change would.
    real_get = proc._burst_queue.get

    def get(*a, **k):
        item = real_get(*a, **k)
        if item is not _STOP:
            proc._settings.BURST_DETECTION_ENABLED = next(seq)
        return item

    monkeypatch.setattr(proc._burst_queue, "get", get)
    _run(proc, [0, 10 * ss, 20 * ss])
    assert len(built) == 2  # one detector before the off grid, a new one after


def _capture(settings):
    n = 1 << 16
    iq = (np.random.default_rng(1).standard_normal(2 * n) * 100).astype(np.int16).tobytes()
    raw = RawCapture(iq, int(settings.FREQUENCY_START), datetime.now(timezone.utc))
    return CaptureResult(raw, SimpleNamespace())


def test_sweep_pipeline_off_returns_no_bursts(tmp_path, monkeypatch):
    settings = AppSettings(
        _env_file=None, BURST_DETECTION_ENABLED=False, STORAGE_PATH=str(tmp_path)
    )
    called = []
    monkeypatch.setattr(continuous, "detect_bursts", lambda *a, **k: called.append(1))
    pr = continuous._process_capture_blocking(_capture(settings), settings, 1, "S")
    assert called == [] and pr.bursts == []
    assert np.isfinite(pr.noise_floor_db)


def test_identity_reports_the_switch_and_the_psd_backend(monkeypatch):
    settings = AppSettings(_env_file=None, BURST_DETECTION_ENABLED=False)
    body = TestClient(create_app(settings)).get("/api/identity").json()
    assert body["features"]["burst_detection"] is False
    assert body["psd_backend"] == {"configured": "cpu", "in_use": "cpu", "reason": None}

    monkeypatch.setattr(psd_cuda, "unavailable_reason", lambda: "libpsdcuda.so not built")
    settings = AppSettings(_env_file=None, PSD_BACKEND="cuda")
    body = TestClient(create_app(settings)).get("/api/identity").json()
    assert body["psd_backend"] == {
        "configured": "cuda",
        "in_use": "cpu",
        "reason": "libpsdcuda.so not built",
    }
    monkeypatch.setattr(psd_cuda, "unavailable_reason", lambda: None)
    body = TestClient(create_app(settings)).get("/api/identity").json()
    assert body["psd_backend"]["in_use"] == "cuda"


def test_config_page_switches_detection_at_once(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # apply writes .env in the working directory
    settings = AppSettings(_env_file=None)
    client = TestClient(create_app(settings))
    html = client.get("/config/").text
    assert 'name="burst_detection_enabled"' in html and "'burst-detection-enabled'" in html
    r = client.post("/config/apply", json={"burst_detection_enabled": False})
    assert r.status_code == 200, r.text
    assert settings.BURST_DETECTION_ENABLED is False
    assert "RFOBS_BURST_DETECTION_ENABLED" in (tmp_path / ".env").read_text()
