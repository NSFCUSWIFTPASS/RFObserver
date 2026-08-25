"""Continuous-trigger state machine + FIFO disk-cap wiring in StreamingProcessor."""

from __future__ import annotations

from unittest.mock import Mock

import numpy as np

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import StreamingProcessor
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.local import LocalStorage


def _make_proc(tmp_path, **overrides) -> tuple[StreamingProcessor, AppSettings]:
    storage_path = tmp_path / "storage"
    storage_path.mkdir()
    base = dict(
        FREQUENCY_START=915_000_000,
        FREQUENCY_END=915_000_000,
        BANDWIDTH=1_000_000,
        DURATION_SEC=0.5,
        GAIN=35,
        NUM_FFT_BINS=64,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage_path),
        DB_PATH=str(tmp_path / "test.db"),
        ARCHIVE_MAX_GB=1.0,
        _env_file=None,
    )
    base.update(overrides)
    settings = AppSettings(**base)
    rx = MockReceiver(
        receiver_config=ReceiverConfig(
            gain_db=settings.GAIN,
            bandwidth_hz=settings.BANDWIDTH,
            duration_sec=settings.DURATION_SEC,
        )
    )
    rx.initialize()
    proc = StreamingProcessor(
        receiver=rx,
        database=SensorDatabase(settings.DB_PATH),
        local_storage=LocalStorage(settings.STORAGE_PATH, max_gb=settings.ARCHIVE_MAX_GB),
        settings=settings,
    )
    return proc, settings


def _low_power_buf() -> np.ndarray:
    """An SC16 chunk of zeros -> power far below any real threshold."""
    return np.zeros(1024, dtype=np.int32)


def test_continuous_auto_arms_when_idle(tmp_path):
    proc, _ = _make_proc(tmp_path, TRIGGER_CONTINUOUS=True, TRIGGER_THRESHOLD_DB=100.0)
    assert proc._recording_state == "idle"
    proc._check_trigger_and_record(_low_power_buf())
    assert proc._recording_state == "armed"
    assert proc._continuous_armed is True


def test_continuous_off_does_not_auto_arm(tmp_path):
    proc, _ = _make_proc(tmp_path, TRIGGER_CONTINUOUS=False, TRIGGER_THRESHOLD_DB=100.0)
    proc._check_trigger_and_record(_low_power_buf())
    assert proc._recording_state == "idle"


def test_toggle_off_disarms_auto_armed(tmp_path):
    proc, settings = _make_proc(tmp_path, TRIGGER_CONTINUOUS=True, TRIGGER_THRESHOLD_DB=100.0)
    proc._check_trigger_and_record(_low_power_buf())
    assert proc._recording_state == "armed"
    # Operator turns continuous off; the auto-armed waiting state is released.
    settings.TRIGGER_CONTINUOUS = False
    proc._check_trigger_and_record(_low_power_buf())
    assert proc._recording_state == "idle"
    assert proc._continuous_armed is False


def test_manual_arm_survives_continuous_off(tmp_path):
    proc, _ = _make_proc(tmp_path, TRIGGER_CONTINUOUS=False, TRIGGER_THRESHOLD_DB=100.0)
    proc.arm_trigger()
    assert proc._recording_state == "armed"
    assert proc._continuous_armed is False
    # A manual arm is not an auto-arm, so the continuous-off rule must not touch it.
    proc._check_trigger_and_record(_low_power_buf())
    assert proc._recording_state == "armed"


def test_end_recording_enforces_disk_cap(tmp_path):
    proc, _ = _make_proc(tmp_path)
    proc._storage.enforce_cap = Mock()
    proc._recording_state = "recording"
    proc._end_recording()
    proc._storage.enforce_cap.assert_called_once()


def test_end_recording_syncs_cap_from_runtime_settings(tmp_path):
    # LocalStorage snapshots the cap at construction; a runtime ARCHIVE_MAX_GB
    # change (via /config/apply) must reach eviction, or the FIFO uses a stale cap.
    proc, settings = _make_proc(tmp_path, ARCHIVE_MAX_GB=50.0)
    assert proc._storage.max_bytes == int(50.0 * 1024**3)
    settings.ARCHIVE_MAX_GB = 0.02  # operator lowers the cap at runtime
    proc._recording_state = "recording"
    proc._end_recording()
    assert proc._storage.max_bytes == int(0.02 * 1024**3)
