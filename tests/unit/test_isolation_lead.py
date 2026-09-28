"""Lossless replay with isolation: the receiver may lead the burst thread by at
most the ring minus the lookback isolation still needs (I-1)."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import numpy as np
import pytest

from rfobserver.capture.buffer import CircularBuffer
from rfobserver.config import AppSettings
from rfobserver.models import BurstFingerprint
from rfobserver.pipeline.isolation import (
    BurstCandidate,
    IsolationBatch,
    IsolationStage,
    RingSource,
)
from rfobserver.pipeline.streaming import _STOP, StreamingProcessor
from rfobserver.processing.spectral import PSDGridResult
from tests.unit.test_recording_gaps import _proc


def _lossless(tmp_path, **kw):
    base = _proc(tmp_path, **kw)
    return StreamingProcessor(
        receiver=base._receiver,
        database=base._db,
        local_storage=base._storage,
        settings=base._settings,
        drop_on_overflow=False,
    )


def _fake_stage():
    calls = []
    return SimpleNamespace(
        submit=lambda batch: calls.append("submit") or True,
        wait_idle=lambda timeout: True,
        stats=SimpleNamespace(snapshot=dict),
        calls=calls,
    )


# -- IsolationStage.wait_idle --


def _cand(i=0):
    now = datetime.now(timezone.utc)
    b = BurstFingerprint(
        burst_id=f"b{i}",
        start_time=now,
        stop_time=now,
        center_freq_hz=915e6,
        peak_freq_hz=915.1e6,
        bandwidth_hz=250e3,
        peak_power_db=-10.0,
        start_sample=1000,
        stop_sample=2000,
    )
    return BurstCandidate(b, 30.0)


def _batch():
    ring = CircularBuffer(10_000, dtype=np.int32)
    ring.write(np.zeros(10_000, dtype=np.int32))
    return IsolationBatch([_cand()], 915e6, 2e6, RingSource(ring))


def test_wait_idle_waits_for_the_batch_in_progress(tmp_path, monkeypatch):
    st = IsolationStage(
        AppSettings(ISOLATION_ENABLED=True, _env_file=None),
        archive=None,
        module_feed=None,
        attribution_handoff=None,
    )
    release = threading.Event()
    orig = st.process_batch

    def slow(batch):
        release.wait(5)
        return orig(batch)

    monkeypatch.setattr(st, "process_batch", slow)
    st.start()
    try:
        assert st.wait_idle(0.05)  # nothing submitted yet
        st.submit(_batch())
        assert not st.wait_idle(0.1)  # still being processed
        release.set()
        assert st.wait_idle(2.0)
        assert st.stats.snapshot()["isolated"] == 1
    finally:
        st.stop()


def test_wait_idle_returns_once_the_stage_is_stopped(tmp_path):
    st = IsolationStage(
        AppSettings(ISOLATION_ENABLED=True, _env_file=None),
        archive=None,
        module_feed=None,
        attribution_handoff=None,
    )
    st.submit(_batch())  # never started: queued, never processed
    assert not st.wait_idle(0.05)
    st.start()
    st.stop()
    t0 = time.monotonic()
    assert st.wait_idle(2.0)
    assert time.monotonic() - t0 < 0.5


# -- lead limit --


def test_lead_limit_is_the_ring_minus_the_isolation_reserve(tmp_path):
    proc = _lossless(tmp_path, ISOLATION_ENABLED=True, ISOLATION_LOOKBACK_SEC=3.0)
    proc._isolation = _fake_stage()
    s = proc._settings
    fs = s.BANDWIDTH
    reserve = (
        (s.BURST_EVAL_INTERVAL_ROWS + 3) * proc._slice_samples
        + int(s.ISOLATION_MAX_BURST_SEC * fs)
        + 2 * int(0.002 * fs)
    )
    assert proc._compute_lead_limit() == proc._pre_trigger_buf.capacity - reserve


def test_lead_limit_is_off_in_live_mode_and_without_isolation(tmp_path):
    (tmp_path / "a").mkdir()
    live = _proc(tmp_path / "a", ISOLATION_ENABLED=True, ISOLATION_LOOKBACK_SEC=3.0)
    live._isolation = _fake_stage()
    assert live._compute_lead_limit() is None
    (tmp_path / "b").mkdir()
    off = _lossless(tmp_path / "b")
    assert off._compute_lead_limit() is None


def test_lead_limit_clamps_to_two_chunks_with_a_warning(tmp_path, caplog):
    proc = _lossless(
        tmp_path, TRIGGER_PRE_SEC=0.001, ISOLATION_ENABLED=True, ISOLATION_LOOKBACK_SEC=0.1
    )
    proc._isolation = _fake_stage()
    with caplog.at_level(logging.WARNING):
        assert proc._compute_lead_limit() == 2 * proc._chunk_samples
    assert "lead" in caplog.text


# -- receiver throttle --


def _throttled(tmp_path):
    proc = _lossless(tmp_path, ISOLATION_ENABLED=True, ISOLATION_LOOKBACK_SEC=3.0)
    proc._isolation = _fake_stage()
    proc._running = True
    proc._lead_limit = 1000
    stop = threading.Event()
    proc._burst_thread = threading.Thread(target=stop.wait, daemon=True)
    proc._burst_thread.start()
    return proc, stop


def test_receiver_waits_until_the_burst_thread_catches_up(tmp_path):
    proc, stop = _throttled(tmp_path)
    try:
        gen = proc._config_generation
        proc._wait_for_burst_lead(800, gen)  # 0 + 800 - 0 <= 1000: no wait
        proc._pre_trigger_buf.write(np.zeros(900, dtype=np.int32))
        t = threading.Thread(target=proc._wait_for_burst_lead, args=(800, gen))
        t.start()
        t.join(0.3)
        assert t.is_alive()  # 900 + 800 - 0 > 1000
        proc._publish_burst_done(gen, 900)
        t.join(2.0)
        assert not t.is_alive()
    finally:
        stop.set()


def test_receiver_ignores_a_done_end_from_another_generation(tmp_path):
    proc, stop = _throttled(tmp_path)
    try:
        proc._pre_trigger_buf.write(np.zeros(900, dtype=np.int32))
        proc._publish_burst_done(proc._config_generation + 7, 10**9)
        t = threading.Thread(target=proc._wait_for_burst_lead, args=(800, proc._config_generation))
        t.start()
        t.join(0.3)
        assert t.is_alive()
        proc._running = False  # shutdown releases it
        t.join(2.0)
        assert not t.is_alive()
    finally:
        stop.set()


def test_receiver_stops_throttling_when_the_burst_thread_dies(tmp_path, caplog):
    proc, stop = _throttled(tmp_path)
    proc._pre_trigger_buf.write(np.zeros(900, dtype=np.int32))
    stop.set()
    proc._burst_thread.join(2.0)
    with caplog.at_level(logging.ERROR):
        t0 = time.monotonic()
        proc._wait_for_burst_lead(800, proc._config_generation)
    assert time.monotonic() - t0 < 1.0
    assert proc._lead_limit is None
    assert "burst thread" in caplog.text


def test_receiver_stops_waiting_on_a_reconfigure(tmp_path):
    proc, stop = _throttled(tmp_path)
    try:
        proc._pre_trigger_buf.write(np.zeros(900, dtype=np.int32))
        t = threading.Thread(target=proc._wait_for_burst_lead, args=(800, proc._config_generation))
        t.start()
        t.join(0.2)
        assert t.is_alive()
        proc.reconfigure()
        t.join(2.0)
        assert not t.is_alive()
    finally:
        stop.set()


# -- burst thread publishes done_end --


def _grid(proc, bins):
    rows = 10
    return PSDGridResult(
        grid=np.full((rows, bins), -90.0, dtype=np.float32),
        time_axis=np.arange(rows) * 2e-4,
        freq_axis=np.linspace(-5e5, 5e5, bins),
        ffts_per_slice=1,
        total_ffts=rows,
    )


def test_burst_thread_publishes_done_end_for_every_grid_including_skipped(tmp_path):
    proc = _lossless(tmp_path, ISOLATION_ENABLED=True)
    proc._running = True
    bins = proc._settings.NUM_FFT_BINS
    ss = proc._slice_samples
    proc._burst_queue.put((_grid(proc, bins + 1), 915e6, 1, 5000))  # stale, skipped
    t = threading.Thread(target=proc._burst_detection_loop, daemon=True)
    t.start()
    deadline = time.monotonic() + 2.0
    while proc._burst_done_end != 5000 + 10 * ss and time.monotonic() < deadline:
        time.sleep(0.01)
    assert proc._burst_done_end == 5000 + 10 * ss
    proc._burst_queue.put((_grid(proc, bins), 915e6, 2, 5000 + 10 * ss))
    deadline = time.monotonic() + 2.0
    while proc._burst_done_end != 5000 + 20 * ss and time.monotonic() < deadline:
        time.sleep(0.01)
    assert proc._burst_done_end == 5000 + 20 * ss
    assert proc._burst_done_gen == proc._config_generation
    proc._burst_queue.put(_STOP)
    t.join(2.0)


def test_lossless_burst_thread_waits_for_the_stage_before_publishing(tmp_path, monkeypatch):
    from rfobserver.processing.rolling_burst import RollingBurstDetector

    proc = _lossless(tmp_path, ISOLATION_ENABLED=True)
    proc._running = True
    seen = []
    idle = iter([False, False, True])

    def wait_idle(timeout):
        seen.append(proc._burst_done_end)
        return next(idle)

    stage = _fake_stage()
    stage.wait_idle = wait_idle
    proc._isolation = stage
    now = datetime.now(timezone.utc)
    b = BurstFingerprint(
        start_time=now, stop_time=now, center_freq_hz=915e6, bandwidth_hz=1e5, peak_power_db=-40
    )
    monkeypatch.setattr(RollingBurstDetector, "feed", lambda self, *a, **k: [b])
    monkeypatch.setattr(proc, "_submit_isolation", lambda *a: stage.calls.append("submit"))
    proc._burst_queue.put((_grid(proc, proc._settings.NUM_FFT_BINS), 915e6, 1, 0))
    proc._burst_queue.put(_STOP)
    proc._burst_detection_loop()
    assert stage.calls == ["submit"]
    assert seen == [0, 0, 0]  # done_end not advanced while the stage was busy
    assert proc._burst_done_end == 10 * proc._slice_samples


def test_live_burst_thread_does_not_wait_for_the_stage(tmp_path, monkeypatch):
    from rfobserver.processing.rolling_burst import RollingBurstDetector

    proc = _proc(tmp_path, ISOLATION_ENABLED=True)
    proc._running = True
    stage = _fake_stage()

    def wait_idle(timeout):
        raise AssertionError("live mode must not wait on the stage")

    stage.wait_idle = wait_idle
    proc._isolation = stage
    now = datetime.now(timezone.utc)
    b = BurstFingerprint(
        start_time=now, stop_time=now, center_freq_hz=915e6, bandwidth_hz=1e5, peak_power_db=-40
    )
    monkeypatch.setattr(RollingBurstDetector, "feed", lambda self, *a, **k: [b])
    monkeypatch.setattr(proc, "_submit_isolation", lambda *a: stage.calls.append("submit"))
    proc._burst_queue.put((_grid(proc, proc._settings.NUM_FFT_BINS), 915e6, 1, 0))
    proc._burst_queue.put(_STOP)
    proc._burst_detection_loop()
    assert stage.calls == ["submit"]


# -- replay driver --


@pytest.mark.asyncio
async def test_replay_driver_stops_when_a_pipeline_thread_has_died(caplog):
    from rfobserver.pipeline.replay import _drive_to_end

    stopped = asyncio.Event()

    class _Proc:
        _capture_count = 0
        _burst_grids_done = 0
        _burst_grids_in = 0

        async def run(self):
            await stopped.wait()

        def stop(self):
            stopped.set()

        def dead_pipeline_threads(self):
            return ["burst"]

        def isolation_status(self):
            return {"counts": {}}

    settings = AppSettings(_env_file=None)
    with caplog.at_level(logging.ERROR):
        status = await asyncio.wait_for(
            _drive_to_end(_Proc(), SimpleNamespace(exhausted=True), settings), timeout=5.0
        )
    assert status == {"counts": {}}
    assert "burst" in caplog.text
