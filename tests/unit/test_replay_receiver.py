"""Paced + looping behavior of FileReplayReceiver."""

from __future__ import annotations

import numpy as np
import pytest

from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.capture.replay_receiver import FileReplayReceiver
from rfobserver.capture.sigmf_reader import SigmfCapture


def _capture(n_samples: int) -> SigmfCapture:
    # interleaved I/Q int16 -> 2*n int16 values; use a ramp so we can detect wrap
    raw = np.arange(n_samples * 2, dtype=np.int16)
    return SigmfCapture(
        datatype="ci16_le",
        sample_rate_hz=1_000_000.0,
        center_freq_hz=915e6,
        raw=raw,
        meta={},
    )


def _cfg() -> ReceiverConfig:
    return ReceiverConfig(gain_db=30, bandwidth_hz=1_000_000, duration_sec=1.0)


def test_loop_seeks_to_zero_instead_of_draining():
    cap = _capture(100)
    rx = FileReplayReceiver(cap, _cfg(), loop=True)
    buf = np.empty(60, dtype=np.int32)
    rx.recv_chunk(buf)  # samples 0..59
    rx.recv_chunk(buf)  # 60..99 then wrap to 0..19
    assert not rx.exhausted  # looping never sets exhausted
    first = buf[0]
    rx.recv_chunk(buf)  # continues from 20
    # after enough reads it keeps returning real (wrapping) capture data, not a
    # frozen drain: the buffer content changes across reads
    assert buf[0] != first or rx._pos != 0


def test_paced_sleeps_scaled_by_speed(monkeypatch):
    cap = _capture(10_000)
    rx = FileReplayReceiver(cap, _cfg(), paced=True, loop=True, speed=1.0)
    slept: list[float] = []
    monkeypatch.setattr("rfobserver.capture.replay_receiver.time.sleep", lambda s: slept.append(s))
    buf = np.empty(1000, dtype=np.int32)  # 1000 samples @ 1 MS/s = 1 ms at 1x
    rx.recv_chunk(buf)
    assert slept and abs(slept[-1] - 0.001) < 0.0005
    slept.clear()
    rx.set_speed(2.0)
    rx.recv_chunk(buf)
    assert slept and abs(slept[-1] - 0.0005) < 0.00025  # 2x -> half the delay


def test_empty_capture_with_loop_fills_buffer_without_hanging():
    """A zero-sample capture with loop=True must not spin resetting `remaining`
    to 0 forever, nor return a buffer with an uninitialized tail; it should
    serve drain noise for the whole chunk instead."""
    cap = _capture(0)
    rx = FileReplayReceiver(cap, _cfg(), loop=True)
    buf = np.full(32, -1, dtype=np.int32)

    calls: list[tuple[int, int]] = []
    orig_fill_drain = rx._fill_drain

    def _spy_fill_drain(out_buf, start=0):
        calls.append((len(out_buf), start))
        orig_fill_drain(out_buf, start=start)

    rx._fill_drain = _spy_fill_drain  # type: ignore[method-assign]

    n = rx.recv_chunk(buf)

    assert n == 32
    # Filled once, for the whole buffer (start=0) -- not a spin loop.
    assert calls == [(32, 0)]
    assert not rx.exhausted  # looping never sets exhausted


def test_unpaced_default_does_not_sleep(monkeypatch):
    cap = _capture(1000)
    rx = FileReplayReceiver(cap, _cfg())  # paced=False, loop=False (batch default)
    slept: list[float] = []
    monkeypatch.setattr("rfobserver.capture.replay_receiver.time.sleep", lambda s: slept.append(s))
    buf = np.empty(500, dtype=np.int32)
    rx.recv_chunk(buf)
    assert slept == []


class _FakeClock:
    """Deterministic monotonic()/sleep() pair: sleep(s) advances the clock by
    s instead of actually blocking, so pacing math can be checked exactly
    without real wall-clock jitter or a slow test."""

    def __init__(self) -> None:
        self.t = 0.0

    def monotonic(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        assert seconds >= 0.0, "recv_chunk must never request a negative sleep"
        self.t += seconds

    def advance(self, seconds: float) -> None:
        """Simulate time spent outside recv_chunk's control, e.g. conversion."""
        self.t += seconds


def _install_fake_clock(monkeypatch) -> _FakeClock:
    import rfobserver.capture.replay_receiver as replay_receiver_mod

    clock = _FakeClock()
    monkeypatch.setattr(replay_receiver_mod.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(replay_receiver_mod.time, "sleep", clock.sleep)
    return clock


def _slow_convert(monkeypatch, cost_sec: float, clock: _FakeClock) -> None:
    """Make every sample conversion eat `cost_sec` of the fake clock, so a
    chunk's pacing decision has to account for real work already done."""
    import rfobserver.capture.replay_receiver as replay_receiver_mod

    original = replay_receiver_mod.to_sc16_int32

    def wrapped(sl, datatype):
        clock.advance(cost_sec)
        return original(sl, datatype)

    monkeypatch.setattr(replay_receiver_mod, "to_sc16_int32", wrapped)


@pytest.mark.parametrize("speed", [1.0, 2.0])
def test_paced_deadline_scheduling_holds_real_time_rate_despite_conversion_cost(monkeypatch, speed):
    """The bug this fixes: sleeping a full chunk duration AFTER conversion
    makes every period (conversion + sleep) longer than the chunk duration,
    so replay runs slow (~0.80-0.86x observed). Deadline scheduling instead
    sleeps only to a fixed t0-anchored deadline, so conversion time is
    absorbed and samples-emitted / elapsed lands exactly on fs * speed."""
    clock = _install_fake_clock(monkeypatch)
    fs = 1_000_000.0
    chunk_samples = 1000  # 1 ms of samples at fs
    conversion_cost = 0.0003  # 30% of the 1x chunk period -- a "noticeable fraction"
    _slow_convert(monkeypatch, conversion_cost, clock)

    cap = _capture(200_000)
    rx = FileReplayReceiver(cap, _cfg(), paced=True, loop=True, speed=speed)

    buf = np.empty(chunk_samples, dtype=np.int32)
    n_chunks = 200
    rx.recv_chunk(buf)  # anchors t0 -- this chunk's own conversion time is
    # necessarily not paced against anything yet, so it is excluded below.
    t_after_first = clock.t
    total_samples = 0
    for _ in range(n_chunks - 1):
        rx.recv_chunk(buf)
        total_samples += chunk_samples

    elapsed = clock.t - t_after_first
    assert elapsed > 0
    rate = total_samples / elapsed
    expected_rate = fs * speed
    assert abs(rate - expected_rate) / expected_rate < 1e-6


def test_paced_speed_change_reanchors_without_bursting(monkeypatch):
    clock = _install_fake_clock(monkeypatch)
    fs = 1_000_000.0
    chunk_samples = 1000
    cap = _capture(200_000)
    rx = FileReplayReceiver(cap, _cfg(), paced=True, loop=True, speed=1.0)
    buf = np.empty(chunk_samples, dtype=np.int32)

    for _ in range(50):
        rx.recv_chunk(buf)
    t_at_1x = clock.t
    assert abs(t_at_1x - 50 * chunk_samples / fs) < 1e-9

    rx.set_speed(2.0)
    for _ in range(50):
        rx.recv_chunk(buf)
    elapsed_at_2x = clock.t - t_at_1x
    assert abs(elapsed_at_2x - 50 * chunk_samples / (fs * 2.0)) < 1e-9


def test_paced_lag_reanchors_instead_of_bursting(monkeypatch, caplog):
    """A slow host that falls > 1s behind must not have the deadline clock
    burst it back to real time with zero-delay chunks; pacing re-anchors to
    "now" and resumes normal per-chunk sleeps instead."""
    clock = _install_fake_clock(monkeypatch)
    fs = 1_000_000.0
    chunk_samples = 1000
    cap = _capture(200_000)
    rx = FileReplayReceiver(cap, _cfg(), paced=True, loop=True, speed=1.0)
    buf = np.empty(chunk_samples, dtype=np.int32)

    rx.recv_chunk(buf)  # anchors t0

    # Simulate a 2s stall (e.g. a slow host / GC pause) before the next chunk.
    clock.advance(2.0)

    import logging

    with caplog.at_level(logging.WARNING, logger="rfobserver.capture.replay_receiver"):
        rx.recv_chunk(buf)
    assert any("re-anchoring" in r.getMessage() for r in caplog.records)

    t_after_reanchor = clock.t
    # The next chunk should sleep a normal ~1ms period again, not burst.
    rx.recv_chunk(buf)
    assert abs((clock.t - t_after_reanchor) - chunk_samples / fs) < 1e-9
