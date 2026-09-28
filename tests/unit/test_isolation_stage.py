"""The isolation stage: gate, states, fan-out, and the attribution switch."""

from __future__ import annotations

import asyncio
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


def test_saving_stops_when_storage_refuses_but_fanout_continues(tmp_path):
    st, saved, fed, handed = _stage(tmp_path)
    st._refuse_saving = lambda: True
    st.process_batch(IsolationBatch([_cand(0, 30)], 915e6, FS, RingSource(_ring())))
    assert saved == [] and fed == ["b0"] and handed == ["b0"]


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
            import time

            time.sleep(0.01)
        assert saved == ["b0"]
    finally:
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
