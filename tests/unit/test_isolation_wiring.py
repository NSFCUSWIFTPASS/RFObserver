"""The streaming pipeline hands completed bursts to the isolation stage."""

from __future__ import annotations

import contextlib
from datetime import datetime, timezone

import numpy as np

from rfobserver.models import BurstFingerprint
from rfobserver.pipeline.streaming import peak_bin_snr
from tests.unit.test_recording_gaps import _proc


def _b(peak):
    now = datetime.now(timezone.utc)
    return BurstFingerprint(
        start_time=now,
        stop_time=now,
        center_freq_hz=peak,
        peak_freq_hz=peak,
        bandwidth_hz=1e5,
        peak_power_db=-40.0,
    )


def test_snr_uses_the_noise_at_the_peak_bin():
    axis = np.linspace(-5e5, 5e5, 11)  # 100 kHz bins, offsets
    noise = np.full(11, -90.0)
    noise[7] = -70.0  # +200 kHz bin is noisier
    assert peak_bin_snr(_b(915e6 + 2e5), noise, axis, 915e6, -80.0) == 30.0
    assert peak_bin_snr(_b(915e6), noise, axis, 915e6, -80.0) == 50.0
    assert peak_bin_snr(_b(915e6), None, axis, 915e6, -80.0) == 40.0  # fallback scalar


def test_status_when_off(tmp_path):
    st = _proc(tmp_path).isolation_status()
    assert st["enabled"] is False and st["counts"] == {}


def test_status_reports_ring_and_disabled_reason(tmp_path, monkeypatch):
    monkeypatch.setattr("rfobserver.pipeline.streaming._mem_available_bytes", lambda: 100_000)
    st = _proc(tmp_path, ISOLATION_ENABLED=True, ISOLATION_LOOKBACK_SEC=0.01).isolation_status()
    assert st["enabled"] is False and "RAM" in st["disabled_reason"]


def _chunk(proc, n):
    buf = np.random.default_rng(n).integers(
        -(2**31), 2**31 - 1, proc._chunk_samples, dtype=np.int32
    )
    return proc._process_one_chunk(
        buf, 0.0, n * proc._chunk_samples, n, 915_000_000, proc._make_grid_config()
    )


def test_lossless_mode_waits_for_the_burst_thread_instead_of_dropping(tmp_path):
    import queue
    import threading

    from tests.unit.test_recording_gaps import StreamingProcessor

    base = _proc(tmp_path)
    proc = StreamingProcessor(
        receiver=base._receiver,
        database=base._db,
        local_storage=base._storage,
        settings=base._settings,
        drop_on_overflow=False,
    )
    proc._running = True
    while not proc._burst_queue.full():
        proc._burst_queue.put_nowait(object())
    t = threading.Thread(target=proc._handle_chunk_result, args=(_chunk(proc, 1),))
    t.start()
    t.join(timeout=0.3)
    assert t.is_alive()  # blocked on the full queue, not dropped
    proc._burst_queue.get_nowait()
    t.join(timeout=2.0)
    assert not t.is_alive()
    items = []
    with contextlib.suppress(queue.Empty):
        while True:
            items.append(proc._burst_queue.get_nowait())
    assert isinstance(items[-1], tuple) and proc._burst_grids_in == 1


def test_live_mode_still_drops_a_grid_when_the_burst_thread_is_behind(tmp_path):
    proc = _proc(tmp_path)
    proc._running = True
    while not proc._burst_queue.full():
        proc._burst_queue.put_nowait(object())
    proc._handle_chunk_result(_chunk(proc, 1))  # returns at once
    assert proc._burst_grids_in == 0


def test_replay_attribution_wait_needs_the_stage_to_have_settled_every_burst():
    from rfobserver.pipeline.replay import _attribution_settled

    # One burst decoded, a second picked but still being isolated: not done.
    assert not _attribution_settled({"received": 2, "picked": 2, "isolated": 1, "attr_decoded": 1})
    assert _attribution_settled(
        {"received": 2, "picked": 2, "isolated": 2, "attr_decoded": 1, "attr_not_decoded": 1}
    )
    # Gated-out bursts never reach attribution; evictions and handoff errors count as done.
    assert _attribution_settled(
        {
            "received": 3,
            "gated_out": 1,
            "picked": 2,
            "too_long": 2,
            "attr_dropped": 1,
            "handoff_error": 1,
        }
    )
    assert not _attribution_settled({"received": 1, "picked": 1, "isolated": 1})


def test_status_counts_include_attribution_queue_drops(tmp_path):
    from types import SimpleNamespace

    from rfobserver.pipeline.isolation import IsolationStats

    proc = _proc(tmp_path)
    stats = IsolationStats()
    stats.count("isolated", 3)
    proc._isolation = SimpleNamespace(stats=stats)
    proc._attrib_worker = SimpleNamespace(queue=SimpleNamespace(dropped=2))
    st = proc.isolation_status()
    assert st["enabled"] and st["attribution"]
    assert st["counts"] == {"isolated": 3, "attr_dropped": 2}
