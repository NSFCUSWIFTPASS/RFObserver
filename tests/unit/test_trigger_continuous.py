"""Continuous-trigger state machine + FIFO disk-cap wiring in StreamingProcessor."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING
from unittest.mock import Mock

import numpy as np

if TYPE_CHECKING:
    from collections.abc import Iterator

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


def test_manual_record_writes_to_manual_dir(tmp_path):
    proc, _ = _make_proc(tmp_path)
    proc.start_recording()  # manual -> _trigger_initiated False
    assert proc._recording_state == "recording"
    assert proc._recording_dir == proc._storage.manual_dir
    proc._end_recording()
    # The finalized capture lives under manual/.
    assert list(proc._storage.manual_dir.glob("*.sc16"))
    assert not list(proc._storage.auto_dir.glob("*.sc16"))


def test_triggered_record_writes_to_auto_dir(tmp_path):
    proc, _ = _make_proc(tmp_path, TRIGGER_THRESHOLD_DB=-400.0)
    proc.arm_trigger()
    # A zeros chunk (~ -300 dB) exceeds the -400 threshold -> triggered capture.
    proc._check_trigger_and_record(_low_power_buf())
    assert proc._recording_state == "recording"
    assert proc._recording_dir == proc._storage.auto_dir
    proc._end_recording()
    assert list(proc._storage.auto_dir.glob("*.sc16"))
    assert not list(proc._storage.manual_dir.glob("*.sc16"))


def test_end_recording_syncs_cap_from_runtime_settings(tmp_path):
    # LocalStorage snapshots the cap at construction; a runtime ARCHIVE_MAX_GB
    # change (via /config/apply) must reach eviction, or the FIFO uses a stale cap.
    proc, settings = _make_proc(tmp_path, ARCHIVE_MAX_GB=50.0)
    assert proc._storage.max_bytes == int(50.0 * 1024**3)
    settings.ARCHIVE_MAX_GB = 0.02  # operator lowers the cap at runtime
    proc._recording_state = "recording"
    proc._end_recording()
    assert proc._storage.max_bytes == int(0.02 * 1024**3)


def _constant_power_buf(amplitude: int, n: int = 4096) -> np.ndarray:
    """SC16 chunk with constant I=Q=amplitude (deterministic power).

    amplitude=1000 -> 2*(1000/32768)^2 = -27.3 dBFS = -44.3 dB re 50 ohm.
    """
    raw16 = np.full((n, 2), amplitude, dtype=np.int16)
    return raw16.view(np.int32).reshape(-1)


def test_trigger_threshold_uses_displayed_power_scale(tmp_path):
    """Regression: the trigger used to compare raw dBFS against the threshold,
    ~17 dB above the dB re 50-ohm scale the dashboard/history power shows, so
    continuous recordings fired while the observed power was below the
    threshold. A chunk at -44.3 dB (displayed scale) must not fire a -40 dB
    threshold even though its raw dBFS (-27.3) is above it."""
    proc, _ = _make_proc(tmp_path, TRIGGER_CONTINUOUS=True, TRIGGER_THRESHOLD_DB=-40.0)
    proc._check_trigger_and_record(_constant_power_buf(1000))
    assert proc._recording_state == "armed"
    assert not proc._trigger_initiated
    assert not list(proc._storage.auto_dir.glob("*.sc16"))


def test_trigger_fires_when_displayed_power_exceeds_threshold(tmp_path):
    """Same chunk (-44.3 dB displayed) fires once the threshold is below it."""
    proc, _ = _make_proc(tmp_path, TRIGGER_CONTINUOUS=True, TRIGGER_THRESHOLD_DB=-50.0)
    proc._check_trigger_and_record(_constant_power_buf(1000))
    assert proc._recording_state == "recording"
    assert proc._trigger_initiated
    proc._end_recording()
    assert list(proc._storage.auto_dir.glob("*.sc16"))


@contextmanager
def _recctl_running(proc: StreamingProcessor) -> Iterator[None]:
    """Run the recording-control thread (production mode: finalize jobs are
    async). Without it, jobs execute inline on the caller (the default in
    these tests)."""
    t = threading.Thread(target=proc._recctl_loop, daemon=True)
    proc._recctl_thread = t
    t.start()
    try:
        yield
    finally:
        proc._recctl_queue.put(None)
        t.join(timeout=5)
        proc._recctl_thread = None


def test_auto_stop_finalizes_off_the_caller(tmp_path):
    """The receiver-thread auto-stop must not block on finalization: the
    request flips recording -> finalizing and returns immediately; the
    control thread completes the job and lands on idle."""
    proc, _ = _make_proc(tmp_path, TRIGGER_THRESHOLD_DB=-400.0)
    with _recctl_running(proc):
        proc.arm_trigger()
        proc._check_trigger_and_record(_low_power_buf())
        assert proc._recording_state == "recording"

        # Hold the control thread so the finalize job cannot complete yet.
        blocker = threading.Event()
        proc._recctl_queue.put(lambda: blocker.wait(timeout=5))
        proc._request_end_recording(wait=False)
        assert proc._recording_state == "finalizing"  # returned immediately

        blocker.set()
        assert proc._end_done.wait(timeout=5)
        assert proc._recording_state == "idle"
    assert list(proc._storage.auto_dir.glob("*.sc16"))


def test_continuous_rearm_waits_for_finalizing(tmp_path):
    """Continuous mode re-arms only from idle: while a capture is finalizing
    the auto-arm must not fire (a new begin would clobber the finalize job)."""
    proc, _ = _make_proc(tmp_path, TRIGGER_CONTINUOUS=True, TRIGGER_THRESHOLD_DB=100.0)
    proc._recording_state = "finalizing"
    proc._check_trigger_and_record(_low_power_buf())
    assert proc._recording_state == "finalizing"
    proc._recording_state = "idle"
    proc._check_trigger_and_record(_low_power_buf())
    assert proc._recording_state == "armed"


def test_start_recording_refused_while_finalizing(tmp_path):
    proc, _ = _make_proc(tmp_path)
    proc._recording_state = "finalizing"
    proc.start_recording()
    assert proc._recording_state == "finalizing"
    assert not list(proc._storage.manual_dir.glob("*.sc16"))


def test_manual_stop_waits_for_finalization(tmp_path):
    """A manual stop keeps synchronous API semantics: when it returns, the
    capture file is finalized on disk and the state machine is idle."""
    proc, _ = _make_proc(tmp_path)
    with _recctl_running(proc):
        proc.start_recording()
        assert proc._recording_state == "recording"
        proc.stop_recording()
        assert proc._recording_state == "idle"
        assert list(proc._storage.manual_dir.glob("*.sc16"))
