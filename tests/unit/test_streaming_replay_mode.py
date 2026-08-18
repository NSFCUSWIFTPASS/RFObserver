"""replay_mode suppresses persistence/egress while keeping the live view."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pytest

from rfobserver.config import AppSettings


def _proc(replay_mode: bool, tmp_path):
    from rfobserver.pipeline.streaming import StreamingProcessor

    settings = AppSettings(
        _env_file=None, STORAGE_PATH=str(tmp_path), DB_PATH=str(tmp_path / "d.db")
    )
    db = MagicMock()
    db.insert_detection = AsyncMock()
    storage = MagicMock()
    storage.storage_path = tmp_path
    zms = MagicMock()
    nats = MagicMock()
    nats.publish_stats = AsyncMock()
    receiver = MagicMock()
    receiver.serial = "sim0"
    proc = StreamingProcessor(
        receiver=receiver,
        database=db,
        local_storage=storage,
        settings=settings,
        broadcast=None,
        zms_monitor=zms,
        nats_producer=nats,
        replay_mode=replay_mode,
    )
    return proc, db, zms, nats


@pytest.mark.asyncio
async def test_replay_mode_skips_insert(tmp_path):
    proc, db, _zms, _nats = _proc(True, tmp_path)
    # queue one burst result and drain
    proc._burst_result_queue.put_nowait(([_fake_burst()], 915e6))
    proc._burst_result_queue.put_nowait(None)
    await proc._drain_burst_results()
    db.insert_detection.assert_not_called()


@pytest.mark.asyncio
async def test_normal_mode_inserts(tmp_path):
    proc, db, _zms, _nats = _proc(False, tmp_path)
    proc._burst_result_queue.put_nowait(([_fake_burst()], 915e6))
    proc._burst_result_queue.put_nowait(None)
    await proc._drain_burst_results()
    db.insert_detection.assert_called()


def test_replay_mode_begin_recording_is_noop(tmp_path):
    proc, _db, _zms, _nats = _proc(True, tmp_path)
    proc.start_recording()
    assert proc._recording_state == "idle"


def test_replay_mode_start_recording_requires_opt_in(tmp_path):
    """start_recording() stays inert under replay_mode until the user opts in
    via set_replay_recording(True); after opting in it actually records."""
    proc, _db, _zms, _nats = _proc(True, tmp_path)

    proc.start_recording()
    assert proc._recording_state == "idle"

    proc.set_replay_recording(True)
    proc.start_recording()
    assert proc._recording_state == "recording"

    proc.stop_recording()
    assert proc._recording_state == "idle"


def test_replay_mode_arm_trigger_inert_even_after_record_opt_in(tmp_path):
    """arm_trigger() must stay inert during replay regardless of the opt-in --
    only explicit manual record is allowed during replay."""
    proc, _db, _zms, _nats = _proc(True, tmp_path)
    proc.set_replay_recording(True)

    proc.arm_trigger()
    assert proc._recording_state == "idle"


@pytest.mark.asyncio
async def test_replay_mode_deferred_sidecar_uses_grid_path(tmp_path, monkeypatch):
    """Replay detections never land in the DB, so record-stop must build the
    sidecar from the recorded PSD grid, not query the DB."""
    import rfobserver.storage.detections_sidecar as sidecar_mod

    proc, db, _zms, _nats = _proc(True, tmp_path)

    grid_mock = MagicMock(return_value={})
    db_mock = MagicMock()
    monkeypatch.setattr(sidecar_mod, "write_sidecar_from_grid", grid_mock)
    monkeypatch.setattr(sidecar_mod, "write_sidecar", db_mock)

    sc16 = tmp_path / "cap.sc16"
    await proc._deferred_sidecar(sc16, 0)

    grid_mock.assert_called_once()
    assert grid_mock.call_args[0][0] == sc16
    db_mock.assert_not_called()
    assert db is proc._db  # sanity: db is present but unused on the grid path


def _above_threshold_buf(n: int = 64) -> np.ndarray:
    """SC16 buffer (packed int32) whose power is well above TRIGGER_THRESHOLD_DB."""
    raw16 = np.full((n, 2), 30000, dtype=np.int16)
    result: np.ndarray = raw16.view(np.int32).reshape(-1)
    return result


def test_replay_mode_arm_trigger_never_arms(tmp_path):
    proc, _db, _zms, _nats = _proc(True, tmp_path)

    proc.arm_trigger()
    assert proc._recording_state == "idle"

    buf = _above_threshold_buf()
    before_qsize = proc._recording_queue.qsize()
    proc._check_trigger_and_record(buf)
    assert proc._recording_state == "idle"
    assert proc._recording_queue.qsize() == before_qsize


def test_replay_mode_check_trigger_inert_even_if_armed(tmp_path):
    """Defense-in-depth: even if state were somehow 'armed', the trigger/record
    path must stay fully inert under replay_mode (the reviewer-found leak)."""
    proc, _db, _zms, _nats = _proc(True, tmp_path)
    proc._recording_state = "armed"

    buf = _above_threshold_buf()
    before_qsize = proc._recording_queue.qsize()
    proc._check_trigger_and_record(buf)

    assert proc._recording_state == "armed"
    assert not proc._trigger_initiated
    assert proc._recording_queue.qsize() == before_qsize


@pytest.mark.asyncio
async def test_replay_mode_publish_processed_skips_egress(tmp_path, monkeypatch):
    proc, _db, _zms, _nats = _proc(True, tmp_path)
    mock_create_task = MagicMock()
    monkeypatch.setattr(asyncio, "create_task", mock_create_task)

    await proc._publish_processed([], MagicMock(), MagicMock())

    mock_create_task.assert_not_called()


def _tone_check_result():
    result = MagicMock()
    result.center_freq_hz = 915e6
    result.summary_psd.frequencies = [913e6, 914e6, 915e6, 916e6, 917e6]
    return result


@pytest.mark.asyncio
async def test_replay_mode_skips_tone_check_insert(tmp_path):
    """_run_tone_check is a second DB-write path (independent of _drain_burst_results)
    and must also be gated under replay_mode."""
    proc, db, _zms, _nats = _proc(True, tmp_path)
    db.insert_tone_check = AsyncMock()

    await proc._run_tone_check([-90.0, -80.0, -70.0, -80.0, -90.0], _tone_check_result())

    db.insert_tone_check.assert_not_called()


@pytest.mark.asyncio
async def test_normal_mode_tone_check_inserts(tmp_path):
    proc, db, _zms, _nats = _proc(False, tmp_path)
    db.insert_tone_check = AsyncMock()

    await proc._run_tone_check([-90.0, -80.0, -70.0, -80.0, -90.0], _tone_check_result())

    db.insert_tone_check.assert_called_once()


def _fake_burst():
    from datetime import datetime, timezone

    b = MagicMock()
    b.burst_id = "b1"
    b.start_time = datetime.now(timezone.utc)
    b.stop_time = datetime.now(timezone.utc)
    b.center_freq_hz = 915e6
    b.bandwidth_hz = 1e6
    b.peak_power_db = -30.0
    b.duration_ms = 5.0
    b.detection_timestamp = datetime.now(timezone.utc)
    b.peak_freq_hz = 915e6
    return b
