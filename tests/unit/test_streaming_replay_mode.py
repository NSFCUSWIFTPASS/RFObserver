"""replay_mode suppresses persistence/egress while keeping the live view."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

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
    proc = StreamingProcessor(
        receiver=MagicMock(),
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
