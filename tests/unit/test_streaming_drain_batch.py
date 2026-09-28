"""A drain persists all pending bursts in one batched DB write."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest

from rfobserver.models import BurstFingerprint
from rfobserver.pipeline.streaming import StreamingProcessor


class _BatchDB:
    def __init__(self) -> None:
        self.batches: list[list[dict[str, Any]]] = []

    async def insert_detections(self, detections: Any) -> int:
        self.batches.append(list(detections))
        return len(self.batches[-1])


def _proc(db: Any, *, replay: bool = False) -> StreamingProcessor:
    proc = StreamingProcessor.__new__(StreamingProcessor)
    proc._db = db
    proc._replay_mode = replay
    proc._receiver = object()
    proc._settings = type("S", (), {"BANDWIDTH": 2_000_000, "GAIN": 30})()
    proc._burst_result_queue = asyncio.Queue()
    return proc


def _burst() -> BurstFingerprint:
    now = datetime.now(timezone.utc)
    return BurstFingerprint(
        start_time=now,
        stop_time=now,
        center_freq_hz=915e6,
        peak_freq_hz=915e6,
        bandwidth_hz=1e5,
        peak_power_db=-40.0,
        duration_ms=10.0,
    )


@pytest.mark.asyncio
async def test_drain_writes_all_pending_bursts_in_one_batch() -> None:
    db = _BatchDB()
    proc = _proc(db)
    for n in (3, 1, 4):
        await proc._burst_result_queue.put(([_burst() for _ in range(n)], 915e6))
    await proc._drain_burst_results()
    assert len(db.batches) == 1, "one DB write per drain, not per burst"
    assert len(db.batches[0]) == 8
    row = db.batches[0][0]
    assert row["sdr_center_freq_hz"] == 915e6 and row["antenna"] == "RX2"


@pytest.mark.asyncio
async def test_drain_in_replay_mode_writes_nothing() -> None:
    db = _BatchDB()
    proc = _proc(db, replay=True)
    await proc._burst_result_queue.put(([_burst()], 915e6))
    await proc._drain_burst_results()
    assert db.batches == []


@pytest.mark.asyncio
async def test_drain_with_nothing_queued_writes_nothing() -> None:
    db = _BatchDB()
    await _proc(db)._drain_burst_results()
    assert db.batches == []
