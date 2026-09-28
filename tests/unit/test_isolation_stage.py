"""The isolation stage: gate, states, fan-out, and the attribution switch."""

from __future__ import annotations

import asyncio
import threading
import time
from datetime import datetime, timezone

import numpy as np

from rfobserver.capture.buffer import CircularBuffer
from rfobserver.config import AppSettings
from rfobserver.models import BurstFingerprint
from rfobserver.pipeline.isolation import (
    BurstCandidate,
    IsolationBatch,
    IsolationStage,
    RingSource,
    WholeCaptureSource,
    build_isolation,
)
from rfobserver.storage.burst_archive import BurstArchive

FS = 2_000_000.0


def _settings(**kw):
    base = dict(
        ISOLATION_ENABLED=True,
        ISOLATION_SNR_DB=13.0,
        ISOLATION_MAX_PER_SEC=3,
        ISOLATION_MAX_BURST_SEC=0.5,
        ISOLATION_QUEUE_MAX=2,
        _env_file=None,
    )
    base.update(kw)
    return AppSettings(**base)


def _ring(n=400_000):
    r = CircularBuffer(n, dtype=np.int32)
    r.write(np.random.default_rng(0).integers(-2000, 2000, n, dtype=np.int32))
    return r


def _cand(i, snr, start=10_000, stop=20_000):
    now = datetime.now(timezone.utc)
    b = BurstFingerprint(
        burst_id=f"b{i}",
        start_time=now,
        stop_time=now,
        center_freq_hz=915e6,
        peak_freq_hz=915.1e6,
        bandwidth_hz=250e3,
        peak_power_db=-40 + snr,
        start_sample=start,
        stop_sample=stop,
    )
    return BurstCandidate(b, snr)


class _Clock:
    t = 100.0

    def __call__(self):
        return self.t


def _stage(tmp_path, clock=None, **kw):
    saved, fed, handed = [], [], []
    archive = BurstArchive(tmp_path)
    orig = archive.save

    def save(iso, meta, subdir=None):
        saved.append(iso.burst_id)
        return orig(iso, meta, subdir)

    archive.save = save
    st = IsolationStage(
        _settings(**kw),
        archive=archive,
        module_feed=lambda iq, rate, meta: fed.append(meta["burst_id"]),
        attribution_handoff=lambda item: handed.append(item.burst_id),
        clock=clock or _Clock(),
    )
    return st, saved, fed, handed


def test_gate_takes_strongest_first_and_respects_the_per_second_limit(tmp_path):
    st, saved, fed, handed = _stage(tmp_path)
    cands = [_cand(i, snr) for i, snr in enumerate([20, 5, 40, 30, 25])]
    out = st.process_batch(IsolationBatch(cands, 915e6, FS, RingSource(_ring())))
    # 40, 30, 25 dB; b0 over the limit, b1 below SNR
    assert [bid for bid, _ in out] == ["b2", "b3", "b4"]
    assert all(s == "isolated" for _, s in out)
    assert saved == fed == handed == ["b2", "b3", "b4"]
    assert st.stats.snapshot()["gated_out"] == 2


def test_rate_limit_window_resets_after_a_second(tmp_path):
    clock = _Clock()
    st, *_ = _stage(tmp_path, clock=clock, ISOLATION_MAX_PER_SEC=1)
    src = RingSource(_ring())
    assert len(st.process_batch(IsolationBatch([_cand(0, 30)], 915e6, FS, src))) == 1
    assert st.process_batch(IsolationBatch([_cand(1, 30)], 915e6, FS, src)) == []
    clock.t += 1.01
    assert len(st.process_batch(IsolationBatch([_cand(2, 30)], 915e6, FS, src))) == 1


def test_every_picked_burst_gets_exactly_one_state(tmp_path):
    st, *_ = _stage(tmp_path, ISOLATION_MAX_BURST_SEC=0.001)
    ring = CircularBuffer(100_000, dtype=np.int32)
    ring.write(np.zeros(300_000, dtype=np.int32))  # holds 200k..300k
    cands = [_cand(0, 30, 250_000, 260_000), _cand(1, 30, 10_000, 20_000)]
    out = dict(st.process_batch(IsolationBatch(cands, 915e6, FS, RingSource(ring))))
    assert out == {"b0": "too_long", "b1": "iq_expired"}
    snap = st.stats.snapshot()
    assert snap["too_long"] == 1 and snap["iq_expired"] == 1


def test_an_exception_is_the_error_state_and_does_not_stop_the_batch(tmp_path, monkeypatch):
    st, *_ = _stage(tmp_path)
    calls = {"n": 0}
    import rfobserver.pipeline.isolation as iso_mod

    real = iso_mod.isolate_burst

    def flaky(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("bad burst")
        return real(*a, **k)

    monkeypatch.setattr(iso_mod, "isolate_burst", flaky)
    batch = IsolationBatch([_cand(0, 40), _cand(1, 30)], 915e6, FS, RingSource(_ring()))
    out = dict(st.process_batch(batch))
    assert out == {"b0": "error", "b1": "isolated"}


def test_full_queue_counts_queue_full(tmp_path):
    st, *_ = _stage(tmp_path)  # queue max 2 batches, not started: nothing drains

    def b(i):
        return IsolationBatch([_cand(i, 30)], 915e6, FS, RingSource(_ring(1000)))

    assert st.submit(b(0)) and st.submit(b(1))
    assert not st.submit(b(2))
    assert st.stats.snapshot()["queue_full"] == 1


def test_received_and_picked_counters_satisfy_the_accounting_invariants(tmp_path, monkeypatch):
    import rfobserver.pipeline.isolation as iso_mod

    real = iso_mod.isolate_burst

    def flaky(burst, **kw):
        if burst.burst_id == "b6":
            raise ValueError("bad burst")
        return real(burst, **kw)

    monkeypatch.setattr(iso_mod, "isolate_burst", flaky)

    st, *_ = _stage(
        tmp_path, ISOLATION_MAX_PER_SEC=3, ISOLATION_MAX_BURST_SEC=0.001, ISOLATION_QUEUE_MAX=1
    )
    ring = CircularBuffer(100_000, dtype=np.int32)
    ring.write(np.zeros(300_000, dtype=np.int32))  # holds 200k..300k
    cands = [
        _cand(0, 5),  # below SNR
        _cand(1, 6),  # below SNR
        _cand(2, 20),  # passes SNR, but weaker than the top 3: over the rate limit
        _cand(3, 21),  # passes SNR, but weaker than the top 3: over the rate limit
        _cand(4, 40, 10_000, 20_000),  # picked, outside the ring's held range: iq_expired
        _cand(5, 35, 250_000, 260_000),  # picked, longer than max_burst_sec: too_long
        _cand(6, 30, 270_000, 280_000),  # picked, monkeypatched to raise: error
    ]
    mixed = IsolationBatch(cands, 915e6, FS, RingSource(ring))
    assert st.submit(mixed)  # fills the one queue slot; received += 7

    overflow = IsolationBatch([_cand(7, 30)], 915e6, FS, RingSource(ring))
    assert not st.submit(overflow)  # queue full; received += 1, queue_full += 1

    out = dict(st.process_batch(mixed))
    assert out == {"b4": "iq_expired", "b5": "too_long", "b6": "error"}

    snap = st.stats.snapshot()
    assert snap["received"] == 8
    assert snap["queue_full"] == 1
    assert snap["gated_out"] == 4  # 2 below SNR + 2 over the rate limit
    assert snap["picked"] == 3
    assert snap["iq_expired"] == 1
    assert snap["too_long"] == 1
    assert snap["error"] == 1
    assert snap.get("isolated", 0) == 0
    assert snap["received"] == snap["gated_out"] + snap["queue_full"] + snap["picked"]
    assert snap["picked"] == (
        snap.get("isolated", 0) + snap["iq_expired"] + snap["too_long"] + snap["error"]
    )


def test_saving_stops_when_storage_refuses_but_fanout_continues(tmp_path):
    st, saved, fed, handed = _stage(tmp_path)
    st._refuse_saving = lambda: True
    st.process_batch(IsolationBatch([_cand(0, 30)], 915e6, FS, RingSource(_ring())))
    assert saved == [] and fed == ["b0"] and handed == ["b0"]


def test_module_feed_failure_does_not_block_the_attribution_handoff(tmp_path):
    archive = BurstArchive(tmp_path)
    handed = []

    def raising_module_feed(iq, rate, meta):
        raise RuntimeError("module boom")

    st = IsolationStage(
        _settings(),
        archive=archive,
        module_feed=raising_module_feed,
        attribution_handoff=lambda item: handed.append(item.burst_id),
    )
    out = st.process_batch(IsolationBatch([_cand(0, 30)], 915e6, FS, RingSource(_ring())))
    assert out == [("b0", "isolated")]
    assert handed == ["b0"]
    assert st.stats.snapshot()["module_error"] == 1


def test_whole_capture_source_for_the_sweep_pipeline(tmp_path):
    st, saved, *_ = _stage(tmp_path)
    iq = np.zeros(20_000, dtype=np.int16).tobytes()
    c = _cand(0, 30, None, None)
    out = st.process_batch(IsolationBatch([c], 915e6, FS, WholeCaptureSource(iq)))
    assert out == [("b0", "isolated")] and saved == ["b0"]


def test_thread_drains_submitted_batches(tmp_path):
    st, saved, *_ = _stage(tmp_path, ISOLATION_QUEUE_MAX=8)
    st.start()
    try:
        st.submit(IsolationBatch([_cand(0, 30)], 915e6, FS, RingSource(_ring())))
        for _ in range(200):
            if saved:
                break
            time.sleep(0.01)
        assert saved == ["b0"]
    finally:
        st.stop()


def test_stop_on_a_full_queue_returns_promptly_and_counts_consistently(tmp_path, monkeypatch):
    import rfobserver.pipeline.isolation as iso_mod

    real = iso_mod.isolate_burst

    def slow(*a, **k):
        time.sleep(0.3)
        return real(*a, **k)

    monkeypatch.setattr(iso_mod, "isolate_burst", slow)

    st, *_ = _stage(tmp_path, ISOLATION_QUEUE_MAX=1)
    st.start()
    try:
        ring = _ring()
        st.submit(IsolationBatch([_cand(0, 30)], 915e6, FS, RingSource(ring)))
        time.sleep(0.05)  # the worker has dequeued batch 0 and is inside the slow call
        st.submit(IsolationBatch([_cand(1, 30)], 915e6, FS, RingSource(ring)))  # fills the slot

        start = time.monotonic()
        st.stop()
        elapsed = time.monotonic() - start

        assert elapsed < 5.0
        snap = st.stats.snapshot()
        assert snap["received"] == 2
        assert snap["queue_full"] == 1  # batch 1, drained unprocessed by stop()
        assert snap["picked"] == 1  # batch 0, finished by the worker before it exited
        assert snap["isolated"] == 1
        assert snap["received"] == snap.get("gated_out", 0) + snap["queue_full"] + snap["picked"]
    finally:
        if st._thread is not None:
            st.stop()


def test_a_batch_submitted_during_stop_while_the_worker_is_busy_is_not_lost(tmp_path, monkeypatch):
    """Reproduces the race: stop() drains an empty queue while the worker is
    still inside process_batch(); a batch submitted in that window must not
    vanish uncounted once the worker exits its loop without ever draining."""
    import rfobserver.pipeline.isolation as iso_mod

    real = iso_mod.isolate_burst
    worker_inside = threading.Event()
    release_worker = threading.Event()

    def blocking(*a, **k):
        worker_inside.set()
        release_worker.wait(timeout=5.0)
        return real(*a, **k)

    monkeypatch.setattr(iso_mod, "isolate_burst", blocking)

    st, *_ = _stage(tmp_path, ISOLATION_QUEUE_MAX=2)
    st.start()
    try:
        st.submit(IsolationBatch([_cand(0, 30)], 915e6, FS, RingSource(_ring())))
        assert worker_inside.wait(timeout=5.0)  # worker is now stuck inside process_batch(batch 0)

        stop_thread = threading.Thread(target=st.stop)
        stop_thread.start()
        time.sleep(0.05)  # let stop() set the event and run its drain loop

        # Races stop()'s drain: with the fix this is either rejected
        # outright (stop event already set) or, if it slips into the queue,
        # drained and counted by _loop()'s exit-time drain rather than lost.
        st.submit(IsolationBatch([_cand(1, 30)], 915e6, FS, RingSource(_ring())))

        release_worker.set()  # let batch 0 finish so the worker (and stop()) can exit
        stop_thread.join(timeout=5.0)
        assert not stop_thread.is_alive()

        snap = st.stats.snapshot()
        assert snap["received"] == 2
        assert snap["received"] == (
            snap.get("gated_out", 0) + snap.get("queue_full", 0) + snap.get("picked", 0)
        )
    finally:
        release_worker.set()
        if st._thread is not None:
            st.stop()


def test_stop_waits_for_an_in_flight_submit_before_setting_the_stop_event(tmp_path):
    """Closes the last race: submit() reads stop_event (False), is
    preempted before put_nowait, and stop() finishes its whole teardown
    while the worker is idle -- then the late put lands in a queue nobody
    reads. _submit_lock forces stop() to wait for a submit that is already
    mid-flight before it can even flip the stop event.

    Proven by monkeypatching the queue's put_nowait to block on an Event for
    its first call only (the submitted batch -- stop()'s own later push of
    the _STOP sentinel must not be blocked by this, or the test would be
    proving something else): while a submit is stuck inside it (so
    _submit_lock is held), a concurrent stop() must still be waiting on the
    lock -- it cannot have returned. Releasing the block lets both finish,
    and the received invariant must hold no matter which of {the worker,
    stop()'s drain} ends up handling the now-enqueued batch.
    """
    st, *_ = _stage(tmp_path, ISOLATION_QUEUE_MAX=4)
    st.start()
    try:
        real_put_nowait = st._queue.put_nowait
        put_called = threading.Event()
        release_put = threading.Event()

        def blocking_put_nowait(item):
            if not put_called.is_set():
                put_called.set()
                release_put.wait(timeout=5.0)
            return real_put_nowait(item)

        st._queue.put_nowait = blocking_put_nowait

        submitted = {}

        def do_submit():
            submitted["ok"] = st.submit(
                IsolationBatch([_cand(0, 30)], 915e6, FS, RingSource(_ring()))
            )

        submit_thread = threading.Thread(target=do_submit)
        submit_thread.start()
        # submit() is now blocked inside put_nowait, still holding _submit_lock.
        assert put_called.wait(timeout=5.0)

        stopped = threading.Event()

        def do_stop():
            st.stop()
            stopped.set()

        stop_thread = threading.Thread(target=do_stop)
        stop_thread.start()
        time.sleep(0.1)
        # stop() needs _submit_lock to set the stop event; the in-flight
        # submit still holds it, so stop() must still be blocked on it.
        assert not stopped.is_set()

        release_put.set()  # let the blocked submit finish, then stop() can proceed
        submit_thread.join(timeout=5.0)
        stop_thread.join(timeout=5.0)
        assert not submit_thread.is_alive()
        assert not stop_thread.is_alive()
        assert submitted["ok"] is True

        snap = st.stats.snapshot()
        assert snap["received"] == 1
        assert snap["received"] == (
            snap.get("gated_out", 0) + snap.get("queue_full", 0) + snap.get("picked", 0)
        )
    finally:
        if st._thread is not None:
            st.stop()


async def test_attribution_without_rtl433_keeps_isolation(tmp_path, monkeypatch):
    monkeypatch.setattr("rfobserver.pipeline.isolation.find_rtl433", lambda override=None: None)
    s = _settings(ISOLATION_ENABLED=False, ATTRIBUTION_ENABLED=True, STORAGE_PATH=str(tmp_path))
    stage, worker, rtl = build_isolation(
        s,
        database=None,
        storage_path=str(tmp_path),
        loop=asyncio.get_running_loop(),
        module_feed=None,
        refuse_saving=lambda: False,
        replay_source=None,
        on_label=None,
    )
    assert stage is not None and worker is None
    assert "not found" in rtl


async def test_nothing_is_built_when_both_switches_are_off(tmp_path):
    s = _settings(ISOLATION_ENABLED=False, ATTRIBUTION_ENABLED=False)
    assert build_isolation(
        s,
        database=None,
        storage_path=str(tmp_path),
        loop=asyncio.get_running_loop(),
        module_feed=None,
        refuse_saving=lambda: False,
        replay_source=None,
        on_label=None,
    ) == (None, None, None)


async def test_replay_routes_results_to_a_file_not_the_db(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "rfobserver.pipeline.isolation.find_rtl433", lambda override=None: "/bin/true"
    )
    s = _settings(ATTRIBUTION_ENABLED=True)
    stage, worker, rtl = build_isolation(
        s,
        database=object(),
        storage_path=str(tmp_path),
        loop=asyncio.get_running_loop(),
        module_feed=None,
        refuse_saving=lambda: False,
        replay_source="feb4_19-39-48.dat",
        on_label=None,
    )
    from rfobserver.pipeline.attribution import ReplayFileSink

    assert stage._archive_subdir == "replay-feb4_19-39-48"
    assert any(isinstance(k, ReplayFileSink) for k in worker._sinks)
    # Ruling A: the brief's assertion checked for the absence of a db_sink by
    # its inner closure's __name__, which is not a stable way to identify it.
    # Assert the same intent directly: replay's only sink is the ReplayFileSink
    # (db_sink is never added when replay_source is set).
    assert worker._sinks == [k for k in worker._sinks if isinstance(k, ReplayFileSink)]
