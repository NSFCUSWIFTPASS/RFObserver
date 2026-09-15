"""Receiver.recv_chunk measures UHD overflow gaps from packet time_spec ticks.

See docs/debugging/2026-09-14_recording-overflow-accounting.md.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any

import numpy as np
import pytest

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import Receiver, ReceiverConfig

NONE, OVERFLOW = "none", "overflow"
RATE = 1000.0


class _TimeSpec:
    def __init__(self, tick: int) -> None:
        self._tick = tick

    def to_ticks(self, rate: float) -> int:
        assert rate == RATE, "gap ticks must use the stream rate"
        return self._tick

    def get_real_secs(self) -> float:  # pragma: no cover - must not be used
        raise AssertionError("gap math must use integer ticks, not float seconds")


class _Metadata:
    def __init__(self) -> None:
        self.error_code = NONE
        self.has_time_spec = False
        self.time_spec = _TimeSpec(0)

    def strerror(self) -> str:
        return "fake error"


class _Streamer:
    """Replays (n, error_code, tick) packets; tick None means no time_spec."""

    def __init__(self, packets: list[tuple[int, str, int | None]]) -> None:
        self._packets = list(packets)

    def recv(self, buf: Any, md: _Metadata, timeout: float) -> int:
        n, err, tick = self._packets.pop(0)
        n = min(n, len(buf))
        md.error_code = err
        md.has_time_spec = tick is not None
        md.time_spec = _TimeSpec(tick if tick is not None else 0)
        return n


class _StreamCMD:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.stream_now = False


@pytest.fixture
def fake_uhd(monkeypatch: pytest.MonkeyPatch) -> None:
    types = SimpleNamespace(
        RXMetadata=_Metadata,
        RXMetadataErrorCode=SimpleNamespace(none=NONE, overflow=OVERFLOW),
        StreamCMD=_StreamCMD,
        StreamMode=SimpleNamespace(start_cont="start", stop_cont="stop"),
    )
    libpyuhd = SimpleNamespace(types=SimpleNamespace(tune_request=lambda freq: freq))
    monkeypatch.setitem(sys.modules, "uhd", SimpleNamespace(types=types, libpyuhd=libpyuhd))


def _receiver(packets: list[tuple[int, str, int | None]]) -> Receiver:
    rx = Receiver(ReceiverConfig(gain_db=30, bandwidth_hz=int(RATE), duration_sec=1.0))
    rx.rx_streamer = _Streamer(packets)
    return rx


def test_contiguous_packets_report_no_gap(fake_uhd: None) -> None:
    rx = _receiver([(100, NONE, 5000), (100, NONE, 5100)])
    assert rx.recv_chunk(np.zeros(200, dtype=np.int32)) == 200
    assert rx.last_gaps == []
    assert (rx.overflow_events, rx.overflow_lost_samples) == (0, 0)


def test_overflow_gap_is_measured_at_its_buffer_offset(fake_uhd: None) -> None:
    # 100 samples at tick 5000, an overflow, then data resumes at 5130:
    # 30 samples were lost right before out_buf[100].
    rx = _receiver([(100, NONE, 5000), (0, OVERFLOW, None), (100, NONE, 5130)])
    assert rx.recv_chunk(np.zeros(200, dtype=np.int32)) == 200
    assert rx.last_gaps == [(100, 30)]
    assert (rx.overflow_events, rx.overflow_lost_samples) == (1, 30)


def test_gap_at_chunk_boundary_uses_offset_zero_and_counters_accumulate(fake_uhd: None) -> None:
    rx = _receiver([(100, NONE, 0), (100, NONE, 150), (100, NONE, 250), (100, NONE, 400)])
    rx.recv_chunk(np.zeros(100, dtype=np.int32))
    assert rx.last_gaps == []
    rx.recv_chunk(np.zeros(100, dtype=np.int32))
    assert rx.last_gaps == [(0, 50)], "lost before the first sample of this chunk"
    rx.recv_chunk(np.zeros(100, dtype=np.int32))
    assert rx.last_gaps == [], "last_gaps is replaced on every call"
    rx.recv_chunk(np.zeros(100, dtype=np.int32))
    assert rx.last_gaps == [(0, 50)]
    assert (rx.overflow_events, rx.overflow_lost_samples) == (2, 100)


def test_missing_time_spec_or_backwards_time_is_not_a_gap(fake_uhd: None) -> None:
    rx = _receiver([(100, NONE, 1000), (100, NONE, None), (100, NONE, 900), (100, NONE, 1000)])
    rx.recv_chunk(np.zeros(400, dtype=np.int32))
    assert rx.last_gaps == []
    assert rx.overflow_events == 0


def test_reset_gap_tracking_forgets_the_last_position(fake_uhd: None) -> None:
    rx = _receiver([(100, NONE, 0), (100, NONE, 10_000)])
    rx.recv_chunk(np.zeros(100, dtype=np.int32))
    rx._reset_gap_tracking()  # what a stream (re)start or stop does
    rx.recv_chunk(np.zeros(100, dtype=np.int32))
    assert rx.last_gaps == []
    assert rx.overflow_events == 0


def test_start_and_stop_streaming_reset_gap_tracking(
    fake_uhd: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    rx = _receiver([])
    monkeypatch.setattr(rx, "_reset_gap_tracking", lambda: calls.append("reset"))
    rx.usrp = SimpleNamespace(
        set_rx_freq=lambda *a: None,
        get_rx_sensor=lambda *a: SimpleNamespace(to_bool=lambda: True),
    )
    rx.rx_streamer = SimpleNamespace(issue_stream_cmd=lambda cmd: None)
    rx.start_streaming(915_000_000)
    rx.stop_streaming()
    assert calls == ["reset", "reset"]


def test_mock_receiver_reports_no_gaps() -> None:
    mock = MockReceiver(ReceiverConfig(gain_db=30, bandwidth_hz=1000, duration_sec=1.0))
    assert mock.last_gaps == []
    assert (mock.overflow_events, mock.overflow_lost_samples) == (0, 0)
