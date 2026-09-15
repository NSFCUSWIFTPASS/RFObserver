# Recording Overflow Accounting (Branch D) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recordings, the TIMING log and `/api/health` report exactly how many
samples UHD overflows removed, and where. Fixes issue 5: a capture said
"0 dropped" while about 20% of its samples were missing.

**Architecture:**

- `Receiver.recv_chunk()` measures each gap from the packet `time_spec` in
  integer ticks: `to_ticks(rate)` minus where the previous samples ended. It
  exposes:
  - `last_gaps`: `(offset_in_buffer, lost_samples)` pairs for the last call;
  - cumulative `overflow_events` and `overflow_lost_samples`.
- The receiver loop logs each gap at its stream position. The stream position
  is the pre-trigger ring buffer's `total_written`.
- When a recording starts, gaps inside the pre-roll are mapped to file sample
  indices. Each later chunk's gaps map at its write position. A chunk dropped
  from the recording queue becomes a gap too.
- The `.json` gains `overflow_events`, `lost_samples`, `gaps`
  (`[[file_sample_index, lost], ...]`) and `time_span_sec`. `start_time` and
  the DB capture span use the true time span. The `.sc16` stays contiguous
  (the user chose "Metadata only").
- Live: the TIMING recv line and `/api/health` `pipeline` carry the
  cumulative counters (the user chose "TIMING log + /api/health").

**Tech Stack:** Python 3.10 to 3.12, UHD 4.1 Python bindings (`uhd` is
system-provided and faked in tests), numpy, FastAPI, pytest.

**Spec:** `docs/debugging/2026-09-14_recording-overflow-accounting.md`: the
root cause, the probe evidence, and the REJECTED float-seconds approach. Issue
5 in `docs/debugging/2026-09-14_stall-safety-net-hardware-validation.md`. User
decisions (2026-09-15): "Metadata only", and "TIMING log + /api/health".

## Global Constraints

- Code must run on Python >= 3.10 (the Jetsons run 3.10.12).
- Gap arithmetic is integer ticks via `TimeSpec.to_ticks(rate)`. Never use
  `get_real_secs()`: at epoch device time it is off by up to 13 samples per
  packet (spec, REJECTED).
- The `.sc16` file content and length are unchanged. No zero-fill and no
  renaming for overflow loss. The existing `_drop<N>` rename for queue-dropped
  chunks stays as it is.
- Always prefix commands with `PYTHONPATH=`. The 3.10 test venv is
  `$V310` =
  `/tmp/claude-1000/-home-orencollaco-GitHub-RFObserver/50689e78-60fa-453e-89bd-c3c811638a7b/scratchpad/venv310`.
  Run each task's tests on `.venv` (3.11) and on `$V310`.
- No em-dashes and no emojis in anything you write or touch. If you edit a line
  that already contains one, replace it with ":" or ",".
- Stage explicit paths only. Never `git add -A` or `git add .`. Never stage
  `docs/` or `.superpowers/`.
- Commit messages carry no `Co-Authored-By:` or `Claude-Session:` trailers.
  Check with `git log -1 --format=%B` and amend them away if appended.
- Before each commit, run: ruff check, ruff format --check, mypy, and the unit
  suite on both interpreters. Integration tests that the task adds must also
  pass, with a throwaway NATS on :4222:
  `docker run -d --rm --name rfobs-test-nats -p 4222:4222 nats:2.10-alpine -js`.

## File Structure

- `src/rfobserver/capture/receiver.py`:
  - `IReceiver` gains `last_gaps`, `overflow_events` and
    `overflow_lost_samples`;
  - `Receiver` measures gaps (Task 1).
- `src/rfobserver/capture/mock_receiver.py`: no-gap defaults (Task 1).
  `FileReplayReceiver` inherits them.
- `src/rfobserver/capture/buffer.py`: `CircularBuffer.read_with_position()`
  (Task 2).
- `src/rfobserver/pipeline/streaming.py`:
  - the stream gap log and recording gap accumulation, with the metadata fields
    (Task 2);
  - `receive_loss()` and the TIMING fields (Task 3).
- `src/rfobserver/web/app.py`: health fields (Task 3).
- Tests:
  - `tests/unit/test_receiver_gaps.py` (new, Task 1);
  - `tests/unit/test_recording_gaps.py` (new, Task 2 unit);
  - `tests/integration/test_recording_gaps.py` (new, Task 2 end to end);
  - `tests/unit/test_web_routes.py` (Task 3).

---

### Task 1: Receiver measures overflow gaps

**Files:**
- Modify: `src/rfobserver/capture/receiver.py` (`IReceiver`,
  `Receiver.__init__`, `initialize`, `start_streaming`, `recv_chunk`,
  `stop_streaming`)
- Modify: `src/rfobserver/capture/mock_receiver.py` (`MockReceiver.__init__`)
- Create: `tests/unit/test_receiver_gaps.py`

**Interfaces:**
- Produces, on every receiver (Receiver, MockReceiver, FileReplayReceiver):
  - `last_gaps: list[tuple[int, int]]`: for the most recent `recv_chunk()`,
    `(offset_in_out_buf, lost_samples)`. The lost samples fell immediately
    before `out_buf[offset]`. Replaced on every call.
  - `overflow_events: int` and `overflow_lost_samples: int`: cumulative since
    the receiver object was constructed.

- [ ] **Step 1: Write the failing tests** in `tests/unit/test_receiver_gaps.py`:

```python
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
```

- [ ] **Step 2: Run to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_receiver_gaps.py -q`
Expected: FAIL with `AttributeError: 'Receiver' object has no attribute 'last_gaps'`
(and the same for MockReceiver).

- [ ] **Step 3: Implement.**

1. In `IReceiver`, under `# Streaming capture`, add:

```python
    # UHD overflow loss: gaps from the last recv_chunk() as
    # (offset_in_out_buf, lost_samples), and cumulative counters.
    last_gaps: list[tuple[int, int]]
    overflow_events: int
    overflow_lost_samples: int
```

2. In `Receiver.__init__`, add:

```python
        # Overflow gap tracking (see recv_chunk). Tick = sample at the stream rate.
        self.last_gaps: list[tuple[int, int]] = []
        self.overflow_events = 0
        self.overflow_lost_samples = 0
        self._stream_rate = float(receiver_config.bandwidth_hz)
        self._next_tick: int | None = None
```

3. In `Receiver.initialize()`, directly after `set_rx_rate(...)`:

```python
        # Hardware may coerce the requested rate; gap ticks must use the real one.
        self._stream_rate = float(self.usrp.get_rx_rate(0))
```

   Add `self._reset_gap_tracking()` at the end of `initialize()`.

4. Add the method:

```python
    def _reset_gap_tracking(self) -> None:
        """Forget where the last samples ended (a new stream is not a gap)."""
        self._next_tick = None
```

   Call it in `start_streaming()`, just before `issue_stream_cmd`, and in
   `stop_streaming()`, after `issue_stream_cmd`.

5. Replace `recv_chunk` with:

```python
    def recv_chunk(self, out_buf: np.ndarray) -> int:
        """Fill *out_buf* (int32, SC16) with samples from the running stream.

        Calls ``rx_streamer.recv()`` in a loop until the buffer is full and
        returns the number of samples received. A UHD overflow ("O") drops
        samples between packets; every packet carries a time_spec, so the loss
        before a packet is measured exactly in integer ticks and reported via
        ``last_gaps`` (offsets into *out_buf*) and the cumulative
        ``overflow_events`` / ``overflow_lost_samples``. Float seconds are not
        precise enough at epoch device time (off by up to 13 samples at
        56 MS/s). See docs/debugging/2026-09-14_recording-overflow-accounting.md.
        """
        import uhd

        assert self.rx_streamer is not None
        total = 0
        target = len(out_buf)
        rx_md = uhd.types.RXMetadata()
        gaps: list[tuple[int, int]] = []

        while total < target:
            n = self.rx_streamer.recv(out_buf[total:], rx_md, timeout=1.0)
            if rx_md.error_code == uhd.types.RXMetadataErrorCode.overflow:
                logger.warning("UHD overflow (O): lost samples")
            elif rx_md.error_code != uhd.types.RXMetadataErrorCode.none:
                logger.error("UHD recv error: %s", rx_md.strerror())
                break
            if n > 0:
                if rx_md.has_time_spec:
                    tick = int(rx_md.time_spec.to_ticks(self._stream_rate))
                    if self._next_tick is not None and tick > self._next_tick:
                        lost = tick - self._next_tick
                        gaps.append((total, lost))
                        self.overflow_events += 1
                        self.overflow_lost_samples += lost
                    self._next_tick = tick + n
                else:
                    self._next_tick = None
            total += n

        self.last_gaps = gaps
        return total
```

   A tick earlier than expected (the clock went backwards) is not counted, and
   `_next_tick` re-anchors on that packet.

6. In `MockReceiver.__init__`, add:

```python
        # Synthetic data never overflows; same attributes as Receiver.
        self.last_gaps: list[tuple[int, int]] = []
        self.overflow_events = 0
        self.overflow_lost_samples = 0
```

- [ ] **Step 4: Run the tests on both interpreters**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_receiver_gaps.py -q && PYTHONPATH= $V310/bin/pytest tests/unit/test_receiver_gaps.py -q`
Expected: all pass on both.

Then run the full check set: ruff check, ruff format --check, mypy, and the
unit suite on both interpreters. mypy may object to the Protocol attributes
against `FileReplayReceiver` or other fakes. Fix the implementations; do not
weaken the Protocol.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/capture/receiver.py src/rfobserver/capture/mock_receiver.py tests/unit/test_receiver_gaps.py
git commit -m "feat(receiver): measure UHD overflow gaps from time_spec ticks"
```

---

### Task 2: Recordings record their gaps

**Files:**
- Modify: `src/rfobserver/capture/buffer.py` (`CircularBuffer`)
- Modify: `src/rfobserver/pipeline/streaming.py`:
  - `__init__` state;
  - `_recompute_chunk_params`;
  - the receiver loop around `recv_chunk` and `_pre_trigger_buf.write`;
  - `_check_trigger_and_record`, `_write_recording_chunk`, `_begin_recording`,
    `_finalize_recording` (the saved log) and `_write_recording_metadata`.
- Create: `tests/unit/test_recording_gaps.py`,
  `tests/integration/test_recording_gaps.py`

**Interfaces:**
- Consumes: `receiver.last_gaps` (Task 1).
- Produces:
  - `CircularBuffer.read_with_position() -> tuple[np.ndarray, int]`: the same
    data as `read()`, plus `total_written` read under the same lock.
  - module-level `_preroll_gaps(stream_gaps, start, end, written) -> list[list[int]]`
    in `streaming.py`.
  - recording `.json` keys `overflow_events` (int), `lost_samples` (int),
    `gaps` (`list[[int, int]]`), `gaps_truncated` (bool) and
    `time_span_sec` (float, 3 dp).

Definitions (put them in the `_write_recording_metadata` comment):

- A **gap** is a point in the file where samples are missing:
  `[file_sample_index, lost_samples]`. The lost samples belong immediately
  before `file_sample_index`.
- Two sources:
  - UHD overflow gaps, which also count in `overflow_events`;
  - chunks dropped from the recording queue or RAM buffer, which already count
    in `dropped_chunks` and now also add a gap of that chunk's length.
- Gaps at index 0 (before the file starts) are not recorded.
- `lost_samples` is the sum over `gaps`, and
  `time_span_sec = (total_samples + lost_samples) / sample_rate_hz`.
- `gaps` is capped at `_MAX_RECORDED_GAPS = 1000` entries. Beyond that,
  `gaps_truncated = true`, while `lost_samples` and `overflow_events` keep
  counting.

- [ ] **Step 1: Write the failing unit tests** in `tests/unit/test_recording_gaps.py`:

```python
"""Mapping receive gaps into recording file positions (issue 5)."""

from __future__ import annotations

import numpy as np

from rfobserver.capture.buffer import CircularBuffer
from rfobserver.pipeline.streaming import _preroll_gaps


def test_read_with_position_pairs_data_with_total_written() -> None:
    buf = CircularBuffer(10, dtype=np.int32)
    buf.write(np.arange(4, dtype=np.int32))
    data, end = buf.read_with_position()
    assert end == 4 and list(data) == [0, 1, 2, 3]
    buf.write(np.arange(4, 12, dtype=np.int32))  # wraps: holds stream 2..11
    data, end = buf.read_with_position()
    assert end == 12 and list(data) == list(range(2, 12))


def test_preroll_gaps_map_stream_positions_to_file_indices() -> None:
    # Pre-roll holds stream samples [100, 200).
    stream_gaps = [(50, 7), (100, 9), (130, 5), (199, 3), (200, 4)]
    assert _preroll_gaps(stream_gaps, start=100, end=200, written=100) == [[30, 5], [99, 3]]


def test_preroll_gaps_respect_what_was_actually_written() -> None:
    # RAM mode may keep only the first `written` pre-roll samples.
    assert _preroll_gaps([(130, 5), (180, 2)], start=100, end=200, written=50) == [[30, 5]]
    assert _preroll_gaps([(130, 5)], start=100, end=200, written=0) == []
```

- [ ] **Step 2: Write the failing integration test** in
  `tests/integration/test_recording_gaps.py`. Copy `_settings` and the
  `_record_briefly` pattern from `tests/integration/test_recording_grids.py`,
  and drive it with a gap-injecting receiver:

```python
class _GapReceiver(MockReceiver):
    """MockReceiver that reports one overflow gap on selected chunks."""

    def __init__(self, *a, gap_on: set[int], lost: int, offset: int, **k) -> None:
        super().__init__(*a, **k)
        self._gap_on = gap_on
        self._lost = lost
        self._offset = offset
        self._calls = 0

    def recv_chunk(self, out_buf):  # type: ignore[no-untyped-def]
        n = super().recv_chunk(out_buf)
        self.last_gaps = [(self._offset, self._lost)] if self._calls in self._gap_on else []
        if self.last_gaps:
            self.overflow_events += 1
            self.overflow_lost_samples += self._lost
        self._calls += 1
        return n
```

Tests:

1. `test_gaps_inside_recording_are_in_metadata`:
   - Put gaps on chunk calls {6, 9}, with `lost=1234` and `offset=17`.
   - Wait for `_capture_count > 2`, start a manual recording, run until
     `_capture_count >= 12`, stop, then `proc.stop()`.
   - Read the single `manual/*.json`.
   - Assert:
     - `overflow_events == 2` and `lost_samples == 2468`;
     - every gap has `0 < idx < total_samples` and `lost == 1234`;
     - the gap indices are strictly increasing (distinct positions);
     - `time_span_sec == round((total_samples + 2468) / BANDWIDTH, 3)`;
     - `gaps_truncated is False` and `dropped_chunks == 0`.
2. `test_gap_before_recording_starts_is_in_preroll`:
   - Put a gap on call {1}, which is inside the TRIGGER_PRE_SEC pre-roll, with
     `TRIGGER_PRE_SEC` set to cover at least 3 chunks.
   - Start recording after `_capture_count >= 4`.
   - Assert `overflow_events == 1`, and that the gap index is `> 0` and inside
     the pre-roll: less than `pre_roll_samples = total pre-roll samples
     written`.
   - Check the pre-roll length: `TRIGGER_PRE_SEC * BANDWIDTH` capped by what
     streamed. Derive it from the ring capacity, not a magic number.
3. `test_no_gaps_keeps_old_metadata_and_zero_loss`:
   - A plain `MockReceiver` recording.
   - Assert `overflow_events == 0`, `lost_samples == 0`, `gaps == []`, and
     `time_span_sec == duration_sec`.

- [ ] **Step 3: Run to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_recording_gaps.py -q`
Expected: FAIL (ImportError for `_preroll_gaps`; no `read_with_position`).
Run the integration file with NATS up. Expected: FAIL with a KeyError on
`overflow_events`.

- [ ] **Step 4: Implement.**

1. `CircularBuffer.read_with_position()` in `buffer.py`, next to `read()`:

```python
    def read_with_position(self) -> tuple[np.ndarray, int]:
        """``read()`` plus ``total_written`` at that instant (one lock hold).

        The returned samples are stream positions
        ``[total_written - len(data), total_written)``.
        """
        with self._lock:
            if self._total_written <= self._max_samples:
                return self._buffer[: self._write_pos].copy(), self._total_written
            data = np.concatenate(
                [self._buffer[self._write_pos :], self._buffer[: self._write_pos]]
            )
            return data, self._total_written
```

2. In `streaming.py`, at module level near the other helpers:

```python
_MAX_RECORDED_GAPS = 1000
_STREAM_GAP_LOG_LEN = 4096


def _preroll_gaps(
    stream_gaps: Iterable[tuple[int, int]], start: int, end: int, written: int
) -> list[list[int]]:
    """Map stream-position gaps into a pre-roll that covers stream [start, end).

    A gap at stream position s means samples were lost right before s. Only
    gaps strictly inside the pre-roll that was actually written to the file
    (the first ``written`` samples) become file gaps ``[s - start, lost]``;
    one at ``start`` precedes the file.
    """
    return [[s - start, lost] for s, lost in stream_gaps if start < s < end and s - start < written]
```

   Add `from collections.abc import Iterable` if it is not already imported.

3. State: in `_recompute_chunk_params`, right after `self._pre_trigger_buf` is
   created, reset the gap log. A new ring restarts stream positions at 0.

```python
        self._stream_gaps: collections.deque[tuple[int, int]] = collections.deque(
            maxlen=_STREAM_GAP_LOG_LEN
        )
        self._stream_gaps_lock = threading.Lock()
```

   Recording counters, set in `_begin_recording` next to
   `self._recording_dropped = 0`:

```python
        self._recording_gaps: list[list[int]] = []
        self._recording_lost = 0
        self._recording_overflows = 0
```

   Also initialise the same three in `__init__`, next to the existing
   `self._recording_dropped: int = 0`.

4. Receiver loop: capture this chunk's gaps right after
   `n = self._receiver.recv_chunk(buf)`. Before `self._pre_trigger_buf.write(buf[:n])`,
   log them at stream positions.

```python
                        chunk_gaps = [g for g in self._receiver.last_gaps if g[0] < n]
                        if chunk_gaps:
                            chunk_start = self._pre_trigger_buf.total_written
                            with self._stream_gaps_lock:
                                for off, lost in chunk_gaps:
                                    self._stream_gaps.append((chunk_start + off, lost))
```

   Add a `total_written` read-only property to `CircularBuffer`, returning
   `self._total_written`. Only the receiver thread writes it.

   Change the call to `self._check_trigger_and_record(buf[:n], chunk_gaps)`, and
   the signature to
   `_check_trigger_and_record(self, sc16_buf, gaps: Sequence[tuple[int, int]] = ())`.
   Pass `gaps` to `self._write_recording_chunk(sc16_buf, gaps)` in the
   `"recording"` branch. The armed branch needs nothing: its chunk is already in
   the ring and is covered by the pre-roll mapping.

5. `_write_recording_chunk(self, sc16_buf, gaps: Sequence[tuple[int, int]] = ())`:
   - Compute `file_pos` first. RAM mode: `self._recording_buf_pos`. Disk mode:
     `self._recording_bytes // 4`.
   - On a successful write, call `self._add_recording_gap(file_pos + off, lost, overflow=True)`
     for each `(off, lost)` in `gaps`.
   - On a dropped chunk (either branch), call
     `self._add_recording_gap(file_pos, n + sum(l for _, l in gaps), overflow=False)`.
     Also add `len(gaps)` to `self._recording_overflows`. Its lost samples are
     inside that single gap, so do not add them twice.
   - Add the helper:

```python
    def _add_recording_gap(self, index: int, lost: int, *, overflow: bool) -> None:
        """Record samples missing from the file right before ``index`` (> 0)."""
        if index <= 0 or lost <= 0:
            return
        self._recording_lost += lost
        if overflow:
            self._recording_overflows += 1
        if len(self._recording_gaps) < _MAX_RECORDED_GAPS:
            self._recording_gaps.append([index, lost])
        else:
            self._recording_gaps_truncated = True
```

   Initialise `self._recording_gaps_truncated = False` alongside the other
   three counters (in `__init__` and in `_begin_recording`).

6. `_begin_recording`:
   - Replace `pre_data = self._pre_trigger_buf.read()` with
     `pre_data, pre_end = self._pre_trigger_buf.read_with_position()`.
   - Once the pre-roll is in the file, map its gaps:
     - RAM mode: after `self._recording_buf_pos = n` (`written = n`).
     - Disk mode: only when the pre-roll `put_nowait` succeeded
       (`written = len(pre_data)`).

```python
                with self._stream_gaps_lock:
                    logged = list(self._stream_gaps)
                for idx, lost in _preroll_gaps(logged, pre_end - len(pre_data), pre_end, written):
                    self._add_recording_gap(idx, lost, overflow=True)
```

   Factor this into one private method called from both branches, so the block
   is not duplicated.

7. `_write_recording_metadata`:
   - Compute `lost = self._recording_lost` and
     `time_span = (total_samples + lost) / sample_rate_hz if sample_rate_hz > 0 else duration`.
   - Use `time_span` in place of `signal_duration` for `start_dt` (the true wall
     start) and for the DB `stop_dt`. Keep `duration_sec` as
     `round(signal_duration, 3)`: it still describes the file's sample length.
     Keep the DB `duration_sec=` argument as `round(signal_duration, 3)` too.
   - Add to `meta`, after `"dropped_chunks"`:

```python
            "overflow_events": self._recording_overflows,
            "lost_samples": lost,
            "gaps": self._recording_gaps,
            "gaps_truncated": self._recording_gaps_truncated,
            "time_span_sec": round(time_span, 3),
```

8. The "Recording saved" log in `_finalize_recording` gains
   `%d overflow gaps (%d samples lost)`, fed from `self._recording_overflows`
   and `self._recording_lost`. Keep the existing fields.

- [ ] **Step 5: Run the tests on both interpreters**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_recording_gaps.py tests/unit/test_receiver_gaps.py -q && PYTHONPATH= $V310/bin/pytest tests/unit/test_recording_gaps.py tests/unit/test_receiver_gaps.py -q`
Expected: pass.

Run the new integration file and `tests/integration/test_recording_grids.py`
with NATS up. Expected: pass. Then run the full check set.

- [ ] **Step 6: Commit**

```bash
git add src/rfobserver/capture/buffer.py src/rfobserver/pipeline/streaming.py tests/unit/test_recording_gaps.py tests/integration/test_recording_gaps.py
git commit -m "fix(recording): record UHD overflow and dropped-chunk gaps in capture metadata"
```

---

### Task 3: Live overflow counters in TIMING and /api/health

**Files:**
- Modify: `src/rfobserver/pipeline/streaming.py`: a new `receive_loss()`, and
  the TIMING recv log.
- Modify: `src/rfobserver/web/app.py`: the `/api/health` `pipeline` block.
- Modify: `tests/unit/test_web_routes.py`, plus a small unit test for
  `receive_loss()` in `tests/unit/test_recording_gaps.py`.

**Interfaces:**
- Produces:
  - `StreamingProcessor.receive_loss() -> dict[str, int]`, with keys
    `overflow_events` and `overflow_lost_samples`. The values are read from
    the receiver with `getattr(..., 0)`.
  - `/api/health` `pipeline.overflow_events` and
    `pipeline.overflow_lost_samples`: ints, or `null` when there is no
    processor or it has no `receive_loss`, as in Standby or sweep mode.

- [ ] **Step 1: Write the failing tests.**
  - Unit: a `StreamingProcessor` built as the existing unit tests build one.
    Find the pattern in `tests/unit/test_streaming_drain_batch.py` or
    `test_streaming_beacon.py`. Give it a MockReceiver with
    `overflow_events=3` and `overflow_lost_samples=900`. Assert that
    `receive_loss()` returns `{"overflow_events": 3, "overflow_lost_samples": 900}`.
  - Web: follow the existing `/api/health` tests in `test_web_routes.py`, which
    set `app.state.supervisor`.
    - One test: a supervisor whose `processor` has
      `receive_loss() -> {"overflow_events": 2, "overflow_lost_samples": 50}`.
      Assert that both appear in `pipeline`.
    - A second test: `processor=None` gives `null` for both.

- [ ] **Step 2: Run to verify they fail.**

- [ ] **Step 3: Implement.**

```python
    def receive_loss(self) -> dict[str, int]:
        """Cumulative UHD overflow loss since this receiver was built."""
        return {
            "overflow_events": int(getattr(self._receiver, "overflow_events", 0)),
            "overflow_lost_samples": int(getattr(self._receiver, "overflow_lost_samples", 0)),
        }
```

  - TIMING recv line: append `ovf=%d lost=%d`, using `receive_loss()`, to the
    existing format string and args.
  - Health, in `web/app.py`, inside the `pipeline` dict build:

```python
            proc = sup.processor
            loss = proc.receive_loss() if proc is not None and hasattr(proc, "receive_loss") else None
            body["pipeline"]["overflow_events"] = loss["overflow_events"] if loss else None
            body["pipeline"]["overflow_lost_samples"] = loss["overflow_lost_samples"] if loss else None
```

  Keep the existing keys. Match the surrounding style: add the keys inside the
  dict literal if that reads cleaner.

- [ ] **Step 4: Run the tests on both interpreters, then the full check set.**

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/pipeline/streaming.py src/rfobserver/web/app.py tests/unit/test_web_routes.py tests/unit/test_recording_gaps.py
git commit -m "feat(health): expose cumulative UHD overflow loss in TIMING and /api/health"
```

---

### Task 4: Hardware verification (controller, not a subagent)

- [ ] Deploy to nano-super `~/rfobs-stall`. Using `start.sh` (15 W, 56 MS/s,
  watchdog on), take a manual 30 s recording:
  - Expected: the `.json` `lost_samples` is in line with the overflows.
  - Expected:
    `(total_samples + lost_samples) / 56e6` is within one chunk (36.6 ms) of
    the wall span, where wall span = the post-trigger wall duration plus
    TRIGGER_PRE_SEC. Before the fix the file said "0 dropped" with 24.1 s of
    samples in a 30.1 s wall span.
  - Expected: `/api/health` `pipeline.overflow_*` counters rise, and the
    TIMING lines show `ovf=`.
- [ ] Record the before and after in the debugging doc and in the validation
  doc's Fix status. Run the full CI set, then merge into local `main` with
  `--no-ff`.
