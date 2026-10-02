"""Receiver stall flagging (StreamingProcessor._note_recv_timing)."""

from __future__ import annotations

import logging

from rfobserver.pipeline.streaming import (
    RECV_STALL_EXTRA_MS,
    RECV_STALL_WORK_MS,
    StreamingProcessor,
)


def _proc() -> StreamingProcessor:
    p = object.__new__(StreamingProcessor)
    p._chunk_duration = 0.0394
    p._recv_stalls = 0
    p._last_stall_log = 0.0
    p._recording_state = "idle"
    return p


def _note(p, recv_ms, work_ms, at, trigger_ms=0.0):
    t0 = at
    t_recv = t0 + recv_ms / 1000
    t_trig_start = t_recv + (work_ms - trigger_ms) / 1000
    t_end = t_recv + work_ms / 1000
    p._note_recv_timing(t0, t_recv, t_recv, t_trig_start, t_end, t_end)


def test_normal_iteration_is_not_a_stall(caplog):
    p = _proc()
    with caplog.at_level(logging.WARNING):
        _note(p, recv_ms=39.0, work_ms=3.0, at=100.0)
    assert p._recv_stalls == 0 and not caplog.records


def test_slow_work_and_late_recv_are_counted_and_logged_once_a_second(caplog):
    p = _proc()
    with caplog.at_level(logging.WARNING):
        _note(p, recv_ms=39.0, work_ms=RECV_STALL_WORK_MS + 10, at=100.0, trigger_ms=30.0)
        _note(p, recv_ms=39.4 + RECV_STALL_EXTRA_MS + 5, work_ms=1.0, at=100.5)  # GIL/CPU wait
        _note(p, recv_ms=39.0, work_ms=RECV_STALL_WORK_MS + 10, at=101.6)
    assert p._recv_stalls == 3
    lines = [r.getMessage() for r in caplog.records]
    assert len(lines) == 2  # the 100.5 s one is counted, not logged
    assert "RECV STALL" in lines[0] and "trigger/record=30.0" in lines[0]
