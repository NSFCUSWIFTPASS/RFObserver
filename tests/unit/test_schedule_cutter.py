"""Scheduled captures: cutting a run out of the live stream to the sample.

Rate 1000 Hz, chunks of 100 samples; stream sample i holds the value i, and
chunk c's first sample is at t0 + c * 0.1 s (unless a test says otherwise).
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pytest

from rfobserver.capture.buffer import CircularBuffer
from rfobserver.schedules.cutter import RunCutter, RunSpec

RATE = 1000.0
N = 100
T0 = 1_793_212_200.0  # s
FREQ = 915e6


def spec(start_ms: float, n: int) -> RunSpec:
    return RunSpec(
        schedule_id="s",
        run=0,
        start_ms=int(start_ms),
        n_samples=n,
        rate_hz=RATE,
        center_freq_hz=FREQ,
        label="",
    )


def chunk(c: int) -> np.ndarray:
    return np.arange(c * N, (c + 1) * N, dtype=np.int32)


def flatten(ops: list[tuple[str, Any]]) -> np.ndarray:
    parts = [op[1] if op[0] == "iq" else np.zeros(op[1], dtype=np.int32) for op in ops]
    return np.concatenate(parts) if parts else np.zeros(0, dtype=np.int32)


class Stream:
    """Feeds chunks to a cutter the way the receiver loop does: ring first."""

    def __init__(self, ring_samples: int = 1000) -> None:
        self.ring = CircularBuffer(ring_samples, dtype=np.int32)

    def feed(
        self,
        cutter: RunCutter,
        c: int,
        *,
        time: float | None = None,
        gaps: list[tuple[int, int]] | None = None,
        freq: float = FREQ,
        data: np.ndarray | None = None,
    ) -> list[tuple[str, Any]]:
        buf = chunk(c) if data is None else data
        start = self.ring.total_written
        self.ring.write(buf)
        t = T0 + c * N / RATE if time is None else time
        return cutter.feed(buf, start, t, gaps or [], freq, self.ring)


def test_start_mid_chunk_is_cut_to_the_sample() -> None:
    cut = RunCutter(spec(T0 * 1000 + 250, 50))
    st = Stream()
    ops: list[tuple[str, Any]] = []
    for c in range(4):
        ops += st.feed(cut, c)
    assert np.array_equal(flatten(ops), np.arange(250, 300))
    assert cut.done and cut.status == "recorded" and cut.written == 50
    info = cut.info()
    assert abs(info["start_error_ns"]) < 300  # epoch float resolution
    assert info["requested_start"] == "2026-10-28T18:30:00.250Z"
    assert info["gaps"] == [] and info["lost_samples"] == 0


def test_rounding_places_the_nearest_sample() -> None:
    cut = RunCutter(spec(T0 * 1000 + 250, 5))
    st = Stream()
    for c in range(3):
        st.feed(cut, c, time=T0 + c * N / RATE - 0.0004)  # samples are 0.4 ms early
    # The requested instant lies 0.4 samples after sample 250, so 250 is nearest.
    # Epoch floats resolve to about 0.24 us, so allow 0.3 us.
    assert abs(cut.info()["start_error_ns"] + 400_000) < 300


def test_start_inside_an_earlier_chunk_reads_the_ring() -> None:
    st = Stream()
    for c in range(3):
        st.feed(RunCutter(spec(0, 1)), c)  # fill the ring; this cutter never starts
    cut = RunCutter(spec(T0 * 1000 + 120, 200))
    ops = st.feed(cut, 3)
    ops += st.feed(cut, 4)
    assert np.array_equal(flatten(ops), np.arange(120, 320))
    assert cut.status == "recorded" and cut.info()["late_start_samples"] == 0


def test_start_older_than_the_ring_fills_the_head() -> None:
    st = Stream(ring_samples=150)
    for c in range(3):
        st.feed(RunCutter(spec(0, 1)), c)
    cut = RunCutter(spec(T0 * 1000 + 100, 300))  # sample 100; the ring holds 150..299
    ops = st.feed(cut, 3)  # ring now holds 250..399
    ops += st.feed(cut, 4)
    out = flatten(ops)
    assert len(out) == 300
    assert np.all(out[:150] == 0) and np.array_equal(out[150:], np.arange(250, 400))
    info = cut.info()
    assert info["late_start_samples"] == 150 and info["gaps"] == [[0, 150]]
    assert cut.status == "partial"


def test_gap_inside_the_window_is_zero_filled() -> None:
    cut = RunCutter(spec(T0 * 1000 + 50, 250))
    st = Stream()
    ops = st.feed(cut, 0)  # samples 50..99 -> file 0..49
    ops += st.feed(cut, 1, gaps=[(30, 7)])  # 7 lost before stream 130 -> zeros at file 80
    ops += st.feed(cut, 2)
    ops += st.feed(cut, 3)
    out = flatten(ops)
    assert len(out) == 250 and cut.done
    assert np.array_equal(out[:80], np.arange(50, 130))
    assert np.all(out[80:87] == 0)
    assert np.array_equal(out[87:], np.arange(130, 293))
    assert cut.info()["gaps"] == [[80, 7]] and cut.info()["lost_samples"] == 7
    assert cut.status == "partial"


def test_gap_before_the_start_in_the_first_chunk_shifts_placement() -> None:
    # 10 samples lost at offset 20 of chunk 0: stream sample 20 is 30 ms in.
    # The instant 50 ms is then stream sample 40.
    cut = RunCutter(spec(T0 * 1000 + 50, 10))
    st = Stream()
    ops = st.feed(cut, 0, gaps=[(20, 10)])
    assert np.array_equal(flatten(ops), np.arange(40, 50))


def test_start_inside_a_gap_begins_with_zeros() -> None:
    # Lost 10 samples before offset 20: instants 20..29 ms are missing.
    cut = RunCutter(spec(T0 * 1000 + 25, 10))
    st = Stream()
    ops = st.feed(cut, 0, gaps=[(20, 10)])
    out = flatten(ops)
    assert np.all(out[:5] == 0) and np.array_equal(out[5:], np.arange(20, 25))
    assert cut.info()["gaps"] == [[0, 5]]


def test_clock_jump_mid_run_keeps_stream_order() -> None:
    cut = RunCutter(spec(T0 * 1000 + 50, 200))
    st = Stream()
    ops = st.feed(cut, 0)
    ops += st.feed(cut, 1, time=T0 + 0.1 + 0.3)  # host clock stepped 0.3 s
    ops += st.feed(cut, 2, time=T0 + 0.2 + 0.3)
    assert np.array_equal(flatten(ops), np.arange(50, 250))
    assert cut.status == "recorded"


def test_retune_ends_the_run() -> None:
    cut = RunCutter(spec(T0 * 1000 + 50, 200))
    st = Stream()
    ops = st.feed(cut, 0)
    ops += st.feed(cut, 1, freq=FREQ + 1e6)
    assert cut.done and cut.status == "partial" and cut.reason == "retuned"
    assert cut.written == 50 and np.array_equal(flatten(ops), np.arange(50, 100))
    assert st.feed(cut, 2) == []


def test_end_mid_run_is_partial_and_before_start_is_missed() -> None:
    cut = RunCutter(spec(T0 * 1000 + 50, 200))
    st = Stream()
    st.feed(cut, 0)
    assert cut.end("stopped") == []
    assert cut.status == "partial" and cut.reason == "stopped" and cut.done
    early = RunCutter(spec(T0 * 1000 + 5000, 10))
    st.feed(early, 1)
    early.end("stopped")
    assert early.status == "missed" and early.reason == "stopped"


def test_not_yet_due_emits_nothing() -> None:
    cut = RunCutter(spec(T0 * 1000 + 100, 10))  # exactly chunk 1's first sample
    st = Stream()
    assert st.feed(cut, 0) == [] and not cut.started
    assert np.array_equal(flatten(st.feed(cut, 1)), np.arange(100, 110))


@pytest.mark.parametrize("n", [1, 99, 100, 101])
def test_exact_length(n: int) -> None:
    cut = RunCutter(spec(T0 * 1000, n))
    st = Stream()
    ops: list[tuple[str, Any]] = []
    for c in range(3):
        ops += st.feed(cut, c)
    assert len(flatten(ops)) == n and cut.written == n
