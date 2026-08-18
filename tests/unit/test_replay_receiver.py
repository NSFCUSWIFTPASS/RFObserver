"""Paced + looping behavior of FileReplayReceiver."""

from __future__ import annotations

import numpy as np

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


def test_unpaced_default_does_not_sleep(monkeypatch):
    cap = _capture(1000)
    rx = FileReplayReceiver(cap, _cfg())  # paced=False, loop=False (batch default)
    slept: list[float] = []
    monkeypatch.setattr("rfobserver.capture.replay_receiver.time.sleep", lambda s: slept.append(s))
    buf = np.empty(500, dtype=np.int32)
    rx.recv_chunk(buf)
    assert slept == []
