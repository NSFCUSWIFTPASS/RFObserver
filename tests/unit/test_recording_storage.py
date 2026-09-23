"""Recording under storage pressure: refusal, stop reasons, write failures."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import TYPE_CHECKING

import numpy as np
import pytest
from fastapi.testclient import TestClient

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import StreamingProcessor
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.governor import GB, StorageGovernor, StorageSample, VolumeSample
from rfobserver.storage.local import LocalStorage
from rfobserver.web.app import create_app

if TYPE_CHECKING:
    from pathlib import Path

T0 = datetime(2026, 9, 23, tzinfo=timezone.utc)


def _governor_at(step: int) -> StorageGovernor:
    gov = StorageGovernor()
    free = {0: 200, 1: 40, 3: 40, 4: 20}[step]
    evictable = step == 1
    for _ in range(2 if step == 3 else 1):
        gov.tick(
            StorageSample(
                data=VolumeSample(int(free * GB), 1000 * GB),
                db_volume=None,
                db_file_bytes=0,
                db_reusable_bytes=0,
                auto_bytes=0,
                manual_bytes=0,
                evictable_auto=evictable,
            ),
            min_free_gb=0,
            now=T0,
        )
    assert gov.state.step == step
    return gov


def _proc(
    tmp_path: Path, governor: StorageGovernor | None = None, **overrides
) -> StreamingProcessor:
    storage = tmp_path / "storage"
    storage.mkdir(exist_ok=True)
    base = dict(
        FREQUENCY_START=915_000_000,
        FREQUENCY_END=915_000_000,
        BANDWIDTH=1_000_000,
        DURATION_SEC=0.5,
        GAIN=35,
        NUM_FFT_BINS=64,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage),
        DB_PATH=str(tmp_path / "t.db"),
        ARCHIVE_MAX_GB=1.0,
        TRIGGER_PRE_SEC=0.001,
        _env_file=None,
    )
    base.update(overrides)
    s = AppSettings(**base)
    rx = MockReceiver(
        receiver_config=ReceiverConfig(
            gain_db=s.GAIN, bandwidth_hz=s.BANDWIDTH, duration_sec=s.DURATION_SEC
        )
    )
    rx.initialize()
    return StreamingProcessor(
        receiver=rx,
        database=SensorDatabase(s.DB_PATH),
        local_storage=LocalStorage(s.STORAGE_PATH, max_gb=s.ARCHIVE_MAX_GB),
        settings=s,
        storage_governor=governor,
    )


def _only_json(proc: StreamingProcessor) -> dict:
    (p,) = [
        q
        for d in (proc._storage.auto_dir, proc._storage.manual_dir)
        for q in d.glob("*.json")
        if not q.name.endswith((".psd.json", ".detections.json"))
    ]
    return json.loads(p.read_text())


@pytest.mark.parametrize("ram", [True, False])
def test_manual_start_is_refused_at_step_3(tmp_path, ram):
    proc = _proc(tmp_path, _governor_at(3), RECORDING_RAM_BUFFER=ram, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    st = proc.recording_status()
    assert st["state"] == "idle"
    assert "below" in st["refused"] and "floor" in st["refused"]


def test_arm_is_refused_at_step_3(tmp_path):
    proc = _proc(tmp_path, _governor_at(3))
    proc.arm_trigger()
    assert proc.recording_status()["state"] == "idle"
    assert proc.recording_status()["refused"]


def test_armed_trigger_does_not_fire_once_step_3_is_reached(tmp_path):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, TRIGGER_THRESHOLD_DB=-200.0)
    proc.arm_trigger()
    assert proc.recording_status()["state"] == "armed"
    for _ in range(2):
        gov.tick(
            StorageSample(
                data=VolumeSample(40 * GB, 1000 * GB),
                db_volume=None,
                db_file_bytes=0,
                db_reusable_bytes=0,
                auto_bytes=0,
                manual_bytes=0,
                evictable_auto=False,
            ),
            min_free_gb=0,
            now=T0,
        )
    proc._check_trigger_and_record(np.full(4096, 1 << 20, dtype=np.int32), (), 0)
    assert proc.recording_status()["state"] == "armed"  # still waiting, not recording
    assert proc.recording_status()["refused"]


@pytest.mark.parametrize("step", [0, 1])
def test_steps_below_3_allow_recording(tmp_path, step):
    proc = _proc(tmp_path, _governor_at(step), RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    try:
        assert proc.recording_status()["state"] == "recording"
        assert proc.recording_status()["refused"] is None
    finally:
        proc.stop_recording()


def test_no_governor_never_refuses(tmp_path):
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    assert proc.recording_status()["state"] == "recording"
    proc.stop_recording()


def test_manual_stop_is_recorded_as_manual(tmp_path):
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    proc.stop_recording()
    assert _only_json(proc)["stopped_reason"] == "manual"


def test_max_duration_stop_is_recorded(tmp_path):
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    proc._effective_max_sec = 0.0
    proc._check_trigger_and_record(np.zeros(1000, dtype=np.int32), (), None)
    assert _only_json(proc)["stopped_reason"] == "max_duration"


def test_trigger_end_stop_is_recorded(tmp_path):
    proc = _proc(
        tmp_path,
        None,
        RECORDING_RAM_BUFFER=True,
        RECORDING_MAX_SEC=5.0,
        TRIGGER_THRESHOLD_DB=0.0,
        TRIGGER_HYSTERESIS=1,
    )
    proc._trigger_initiated = True
    with proc._rec_lock:
        proc._begin_recording()
    proc._check_trigger_and_record(np.zeros(1000, dtype=np.int32), (), None)
    assert _only_json(proc)["stopped_reason"] == "trigger_end"


def _client_with(proc_status: dict) -> TestClient:
    app = create_app(AppSettings(_env_file=None))
    app.state.processor = SimpleNamespace(
        start_recording=lambda: None,
        arm_trigger=lambda: None,
        manual_trigger=lambda: None,
        recording_status=lambda: proc_status,
    )
    return TestClient(app)


@pytest.mark.parametrize("path", ["/api/recording/start", "/api/recording/arm", "/api/trigger"])
def test_api_answers_409_with_the_reason_when_refused(path):
    c = _client_with({"state": "idle", "file": None, "refused": "Recording refused: x"})
    r = c.post(path)
    assert r.status_code == 409
    assert r.json()["detail"] == "Recording refused: x"


def test_api_start_succeeds_when_not_refused():
    c = _client_with({"state": "recording", "file": "a.sc16", "refused": None})
    assert c.post("/api/recording/start").status_code == 200
