"""The IQ ring grows for isolation, but recordings still pre-roll TRIGGER_PRE_SEC."""

from __future__ import annotations

import numpy as np

from tests.unit.test_recording_gaps import _proc


def test_ring_is_trigger_pre_sec_when_isolation_is_off(tmp_path):
    proc = _proc(tmp_path, TRIGGER_PRE_SEC=0.001)
    assert proc._pre_trigger_buf.capacity == 1000
    assert proc._isolation_disabled_reason is None


def test_ring_grows_to_lookback_when_isolation_is_on(tmp_path):
    proc = _proc(
        tmp_path, TRIGGER_PRE_SEC=0.001, ISOLATION_ENABLED=True, ISOLATION_LOOKBACK_SEC=0.01
    )
    assert proc._pre_trigger_buf.capacity == 10_000
    assert proc._ring_sec == 0.01


def test_attribution_alone_also_grows_the_ring(tmp_path):
    proc = _proc(
        tmp_path, TRIGGER_PRE_SEC=0.001, ATTRIBUTION_ENABLED=True, ISOLATION_LOOKBACK_SEC=0.01
    )
    assert proc._isolation_wanted()
    assert proc._pre_trigger_buf.capacity == 10_000


def test_ring_that_would_not_fit_disables_isolation(tmp_path, monkeypatch):
    monkeypatch.setattr("rfobserver.pipeline.streaming._mem_available_bytes", lambda: 100_000)
    proc = _proc(
        tmp_path, TRIGGER_PRE_SEC=0.001, ISOLATION_ENABLED=True, ISOLATION_LOOKBACK_SEC=0.01
    )
    assert proc._pre_trigger_buf.capacity == 1000
    assert "RAM" in proc._isolation_disabled_reason


def test_preroll_reads_only_trigger_pre_sec_from_a_grown_ring(tmp_path):
    proc = _proc(
        tmp_path,
        TRIGGER_PRE_SEC=0.001,
        ISOLATION_ENABLED=True,
        ISOLATION_LOOKBACK_SEC=0.01,
        RECORDING_RAM_BUFFER=True,
        RECORDING_MAX_SEC=1.0,
    )
    proc._pre_trigger_buf.write(np.arange(8000, dtype=np.int32))
    proc.start_recording()
    try:
        assert proc._recording_buf_pos == 1000  # TRIGGER_PRE_SEC at 1 Msps, not 8000
        assert list(proc._recording_buf[:3]) == [7000, 7001, 7002]
    finally:
        proc.stop_recording()
