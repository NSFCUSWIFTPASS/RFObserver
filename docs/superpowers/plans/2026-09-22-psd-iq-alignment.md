# PSD/IQ Capture Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a capture's `.psd` companion cover exactly the same sample range as its `.sc16`, by anchoring every PSD grid row to the absolute stream sample position it was computed from.

**Architecture:** The IQ pre-roll ring is written synchronously in the receive loop; PSD grids arrive ~4 chunks later from behind `_chunk_queue` (maxsize=4) and a 3-worker pool. Today both "pre-roll" buffers are drained at the same instant and assumed to line up, so the `.psd` sits ~820 ms earlier in real time than the `.sc16`. The fix carries each chunk's `chunk_start` (already computed at `streaming.py:744`) through the worker into `_ChunkResult`, then selects and trims grid rows by absolute sample range instead of by arrival order. Because the grids covering the pre-roll have not been computed yet when the trigger fires, the position-aware pre-buffer drain correctly contributes zero rows and those rows arrive moments later as ordinary live grids; and because the grids covering the IQ tail arrive after the recording stops, finalize gains a bounded wait for them.

**Tech Stack:** Python 3.10+, numpy, threading/queue, pytest.

**Spec:** `docs/debugging/2026-09-22_trigger-psd-iq-misalignment.md` (root-cause analysis; the authority on the defect, the measured offset, and the rejected hypotheses)

## Global Constraints

- Always prefix commands with `PYTHONPATH=` (the host leaks system Python 3.10 packages into the venv).
- Python 3.10 clean (the Jetson test box runs 3.10); no 3.11+ syntax.
- Before every commit run, in order: `ruff check src/ tests/`, `ruff format --check src/ tests/`, `PYTHONPATH= .venv/bin/mypy src/rfobserver/`, `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`, `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q` (needs NATS on localhost:4222).
- Never use em-dashes in code, UI, or docs. No emojis anywhere.
- Stage explicit paths only; never `git add -A` or `git add .`.
- Never add a Claude co-author line to a commit.
- Web port for manual runs is 8888. Never `pkill -f "rfobserver run"` (it matches the invoking shell); use `fuser -k <port>/tcp`.
- The deployed field sensor (`rf-nano-002@rfnano`) is hands-off. Do not run anything there.
- `INSERT OR REPLACE` is this codebase's upsert convention; do not introduce `ON CONFLICT`.

## Decisions already made (do not re-litigate)

- **Tail timeout policy:** wait for the tail grids, capped by `RECORDING_MAX_SEC`, with a fixed fallback when that is 0 ("no limit"). The wait ends as soon as grids reach the end sample, so the normal cost is ~820 ms. A hard ceiling of 10 s sits under it because `_request_end_recording` gives a manual stop only `_end_done.wait(timeout=15)`, and `RECORDING_MAX_SEC` defaults to 30.
- **Sub-row boundary:** the recording's first sample is not slice-aligned (the pre-roll ring holds `int(TRIGGER_PRE_SEC * BANDWIDTH)` samples, e.g. 400000, which is not a multiple of 2048). Keep only rows whose start is at or after the recording start (ceiling, not overlap), and record the leftover in the sidecar as `start_sample_offset` so a reader can be exact. Do not include a straddling row.

---

### Task 1: Position-aware `GridPreBuffer`

Teach the pre-roll grid buffer where each grid came from, and let a caller drain only the rows at or after a given absolute sample position.

**Files:**
- Modify: `src/rfobserver/capture/buffer.py` (`GridPreRoll`, `GridPreBuffer.write`, `GridPreBuffer.drain`)
- Test: `tests/unit/test_grid_prebuffer.py` (create if absent; otherwise extend)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `GridPreBuffer.write(grid, freq_axis, time_res, chunk_start: int, slice_samples: int) -> None`
  - `GridPreBuffer.drain(from_sample: int | None = None) -> GridPreRoll | None`
  - `GridPreRoll` gains `start_sample: int` (absolute sample position of its first row).

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_grid_prebuffer.py
import numpy as np
import pytest

from rfobserver.capture.buffer import GridPreBuffer

SLICE = 2048
ROWS = 4
BINS = 8


def _grid(value: float) -> np.ndarray:
    return np.full((ROWS, BINS), value, dtype=np.float32)


def _axis() -> np.ndarray:
    return np.arange(BINS, dtype=np.float64)


def test_drain_from_sample_keeps_only_rows_at_or_after_position():
    buf = GridPreBuffer(10.0)
    # Three chunks, each ROWS rows of SLICE samples, starting at 0, 8192, 16384.
    for i in range(3):
        buf.write(_grid(float(i)), _axis(), 0.001024, i * ROWS * SLICE, SLICE)

    # Ask for everything from the start of the second chunk onward.
    pre = buf.drain(from_sample=ROWS * SLICE)
    assert pre is not None
    assert pre.rows == 2 * ROWS
    assert pre.start_sample == ROWS * SLICE
    # Chunk 0 must be gone; chunks 1 and 2 survive in order.
    values = [float(g[0, 0]) for g in pre.grids]
    assert values == [1.0, 2.0]


def test_drain_from_sample_trims_within_a_chunk():
    buf = GridPreBuffer(10.0)
    buf.write(_grid(7.0), _axis(), 0.001024, 0, SLICE)
    # Start midway through the chunk, not on a row boundary: the straddling row
    # is dropped, so the first kept row starts at row 2.
    pre = buf.drain(from_sample=SLICE + 1)
    assert pre is not None
    assert pre.rows == ROWS - 2
    assert pre.start_sample == 2 * SLICE


def test_drain_from_sample_past_everything_returns_none():
    buf = GridPreBuffer(10.0)
    buf.write(_grid(1.0), _axis(), 0.001024, 0, SLICE)
    # This is the production case at the current ~820 ms latency: every buffered
    # grid predates the recording, so the pre-roll contributes nothing.
    assert buf.drain(from_sample=10 * ROWS * SLICE) is None


def test_drain_without_position_returns_everything():
    buf = GridPreBuffer(10.0)
    buf.write(_grid(1.0), _axis(), 0.001024, 0, SLICE)
    pre = buf.drain()
    assert pre is not None
    assert pre.rows == ROWS
    assert pre.start_sample == 0


def test_drain_clears_the_buffer():
    buf = GridPreBuffer(10.0)
    buf.write(_grid(1.0), _axis(), 0.001024, 0, SLICE)
    assert buf.drain() is not None
    assert buf.drain() is None
    assert buf.rows == 0
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_grid_prebuffer.py -x -q`
Expected: FAIL. `GridPreBuffer.write()` takes 3 positional args, not 5; `drain()` takes no `from_sample`; `GridPreRoll` has no `start_sample`.

- [ ] **Step 3: Implement**

In `src/rfobserver/capture/buffer.py`, add `start_sample` to the `GridPreRoll` dataclass:

```python
@dataclass
class GridPreRoll:
    grids: list[np.ndarray[Any, np.dtype[Any]]]
    freq_axis: np.ndarray[Any, np.dtype[Any]]
    time_res: float
    rows: int
    grid_min: float
    grid_max: float
    start_sample: int
```

Store the stream position alongside each grid. Change the deque element type to
`tuple[np.ndarray, float, int, int]` (grid, time_res, chunk_start, slice_samples) and
update `write`:

```python
    def write(
        self,
        grid: np.ndarray[Any, np.dtype[Any]],
        freq_axis: np.ndarray[Any, np.dtype[Any]],
        time_res: float,
        chunk_start: int,
        slice_samples: int,
    ) -> None:
        """Append one chunk's grid, tagged with the absolute stream sample
        position of its first row, dropping oldest grids past ``max_seconds``.

        ``chunk_start`` is the position of the chunk's first sample in the
        receiver's sample stream and ``slice_samples`` the samples per grid
        row, so a later ``drain(from_sample=...)`` can select rows by position
        rather than by arrival order. Empty grids, non-positive ``time_res``
        and non-positive ``slice_samples`` are ignored.
        """
        if grid.size == 0 or grid.shape[0] == 0 or time_res <= 0 or slice_samples <= 0:
            return
        rows = int(grid.shape[0])
        span = rows * float(time_res)
        stored = np.ascontiguousarray(grid, dtype=np.float32).copy()
        with self._lock:
            self._grids.append((stored, float(time_res), int(chunk_start), int(slice_samples)))
            self._freq_axis = np.asarray(freq_axis).copy()
            self._rows += rows
            self._span += span
            while self._span > self._max_seconds and len(self._grids) > 1:
                g, tr, _cs, _ss = self._grids.popleft()
                self._rows -= int(g.shape[0])
                self._span -= int(g.shape[0]) * tr
```

Replace `drain` with a position-aware version:

```python
    def drain(self, from_sample: int | None = None) -> GridPreRoll | None:
        """Return the buffered pre-roll (chronological) and clear.

        With ``from_sample`` set, only rows whose first sample is at or after
        that stream position are returned; a row straddling the boundary is
        dropped rather than misplaced, so the returned ``start_sample`` is
        always a true row boundary. Returns None when nothing qualifies (the
        normal case whenever the PSD pipeline latency exceeds
        ``TRIGGER_PRE_SEC``: every buffered grid predates the recording).
        """
        with self._lock:
            if not self._grids or self._freq_axis is None:
                self._reset_locked()
                return None
            kept: list[np.ndarray[Any, np.dtype[Any]]] = []
            start_sample = -1
            for g, _tr, chunk_start, slice_samples in self._grids:
                rows = int(g.shape[0])
                first = 0
                if from_sample is not None and from_sample > chunk_start:
                    # Ceiling division: drop the straddling row.
                    first = (from_sample - chunk_start + slice_samples - 1) // slice_samples
                if first >= rows:
                    continue
                piece = g[first:] if first else g
                if start_sample < 0:
                    start_sample = chunk_start + first * slice_samples
                kept.append(piece)
            time_res = self._grids[-1][1]
            freq_axis = self._freq_axis
            self._reset_locked()
            if not kept:
                return None
            rows_kept = sum(int(g.shape[0]) for g in kept)
            return GridPreRoll(
                grids=kept,
                freq_axis=freq_axis,
                time_res=float(time_res),
                rows=int(rows_kept),
                grid_min=min(float(g.min()) for g in kept),
                grid_max=max(float(g.max()) for g in kept),
                start_sample=int(start_sample),
            )
```

Update the class docstring: it currently promises the pre-roll "has matching PSD rows for the `.psd` companion", which is the claim this plan exists to make true. Say instead that the buffer holds recent grids tagged by stream position and that the caller selects by position.

- [ ] **Step 4: Run the test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_grid_prebuffer.py -x -q`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/capture/buffer.py tests/unit/test_grid_prebuffer.py
git commit -m "feat(capture): tag pre-roll PSD grids with their stream position"
```

---

### Task 2: Carry `chunk_start` through the processing pipeline

`chunk_start` is computed in the receive loop at `streaming.py:744` and used for the recording continuity check, but it is not passed to the worker pool. Without it a grid cannot locate itself.

**Files:**
- Modify: `src/rfobserver/pipeline/streaming.py` (`_recompute_chunk_params`, `_ChunkResult`, `_process_one_chunk`, the receive-loop enqueue, the dispatch loop, `_handle_chunk_result`)

**Interfaces:**
- Consumes: `GridPreBuffer.write(..., chunk_start, slice_samples)` from Task 1.
- Produces:
  - `self._slice_samples: int` on the receiver (samples per PSD grid row).
  - `_ChunkResult.chunk_start: int`.
  - `_chunk_queue` items become the 3-tuple `(sc16_buf, recv_time, chunk_start)`.

- [ ] **Step 1: Store the slice size**

In `_recompute_chunk_params` (around `streaming.py:436-444`), `actual_slice_samples` is computed and used only locally. Keep it on the instance right after `self._chunk_duration` is set:

```python
        chunk_slices = s.STREAMING_CHUNK_SLICES
        self._chunk_samples = chunk_slices * actual_slice_samples
        self._chunk_duration = self._chunk_samples / s.BANDWIDTH
        # Samples per PSD grid row: the unit that maps a grid row index to an
        # absolute stream sample position.
        self._slice_samples = actual_slice_samples
```

Declare it in `__init__` alongside the other chunk params so mypy sees a single type:

```python
        self._slice_samples: int = 0
```

- [ ] **Step 2: Add `chunk_start` to `_ChunkResult`**

Add `"chunk_start"` to `__slots__` (after `"recv_time"`), add the parameter to `__init__`
(keyword position after `recv_time`), and assign it:

```python
    __slots__ = (
        "psd_grid",
        "iq_stats",
        "iq_moments",
        "summary_psd",
        "center_freq_hz",
        "capture_num",
        "recv_time",
        "chunk_start",
        "process_ms",
        "sc16_buf",
    )
```

```python
        recv_time: float,
        chunk_start: int,
        process_ms: float,
```

```python
        self.chunk_start = chunk_start
```

- [ ] **Step 3: Thread it from the receive loop to the worker**

In the receive loop (`streaming.py:766-779`), both enqueue paths carry the value that is
already in scope:

```python
                        if self._drop_on_overflow:
                            try:
                                self._chunk_queue.put_nowait((buf, recv_time, chunk_start))
                            except queue.Full:
                                self._dropped_chunks += 1
                                with contextlib.suppress(queue.Full):
                                    self._buf_pool.put_nowait(buf)
                        else:
                            while self._running:
                                try:
                                    self._chunk_queue.put(
                                        (buf, recv_time, chunk_start), timeout=0.1
                                    )
                                    break
                                except queue.Full:
                                    continue
```

In the dispatch loop, unpack the third element:

```python
                sc16_buf, recv_time, chunk_start = item
```

and pass `chunk_start` into the `_process_one_chunk` submission (add it to the
`executor.submit(...)` argument list in the same position as the signature below).

In `_process_one_chunk`, add the parameter and forward it:

```python
    def _process_one_chunk(
        self,
        sc16_buf: np.ndarray[Any, np.dtype[Any]],
        recv_time: float,
        chunk_start: int,
        capture_num: int,
        center_freq: int,
        grid_config: PSDGridConfig,
    ) -> _ChunkResult:
```

```python
        return _ChunkResult(
            ...
            recv_time=recv_time,
            chunk_start=chunk_start,
            ...
        )
```

- [ ] **Step 4: Feed the position into the pre-buffer**

In `_handle_chunk_result`, the not-recording branch now tags the grid:

```python
        else:
            # Not recording: keep a rolling window of recent grids tagged with
            # their stream position, so a recording that fires can take the rows
            # that actually cover its pre-roll IQ. Whenever the PSD pipeline
            # latency exceeds TRIGGER_PRE_SEC every buffered grid predates the
            # recording and the drain correctly yields nothing; the rows covering
            # the pre-roll arrive shortly after as live grids instead.
            pre_time_res = 0.0
            if len(cr.psd_grid.time_axis) > 1:
                pre_time_res = float(cr.psd_grid.time_axis[1] - cr.psd_grid.time_axis[0])
            self._grid_prebuf.write(
                cr.psd_grid.grid,
                cr.psd_grid.freq_axis,
                pre_time_res,
                cr.chunk_start,
                self._slice_samples,
            )
```

- [ ] **Step 5: Verify nothing regressed**

Run: `PYTHONPATH= .venv/bin/mypy src/rfobserver/ && PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`
Expected: PASS. Any test constructing `_ChunkResult` or pushing to `_chunk_queue` directly must be updated to the new arity; search with `grep -rn "_ChunkResult\|_chunk_queue" tests/`.

- [ ] **Step 6: Commit**

```bash
git add src/rfobserver/pipeline/streaming.py
git commit -m "feat(pipeline): carry each chunk's stream position into its PSD result"
```

---

### Task 3: Trim recorded grid rows to the capture's sample range

The pure, unit-testable core of the fix: given a chunk's grid and its position, return the rows that lie inside the recording.

**Files:**
- Modify: `src/rfobserver/capture/buffer.py` (add module-level `trim_grid_rows`)
- Test: `tests/unit/test_grid_prebuffer.py` (extend)

**Interfaces:**
- Consumes: nothing.
- Produces: `trim_grid_rows(grid, chunk_start, slice_samples, start_sample, end_sample) -> tuple[np.ndarray, int]` returning the kept rows and the absolute sample position of the first kept row (`-1` when nothing is kept).

- [ ] **Step 1: Write the failing test**

```python
# append to tests/unit/test_grid_prebuffer.py
from rfobserver.capture.buffer import trim_grid_rows


def test_trim_keeps_rows_inside_the_range():
    grid = np.arange(ROWS * BINS, dtype=np.float32).reshape(ROWS, BINS)
    kept, start = trim_grid_rows(grid, 0, SLICE, 0, ROWS * SLICE)
    assert kept.shape[0] == ROWS
    assert start == 0


def test_trim_drops_rows_before_the_start():
    grid = np.arange(ROWS * BINS, dtype=np.float32).reshape(ROWS, BINS)
    kept, start = trim_grid_rows(grid, 0, SLICE, 2 * SLICE, ROWS * SLICE)
    assert kept.shape[0] == 2
    assert start == 2 * SLICE
    assert np.array_equal(kept[0], grid[2])


def test_trim_drops_a_row_straddling_the_start():
    grid = np.zeros((ROWS, BINS), dtype=np.float32)
    kept, start = trim_grid_rows(grid, 0, SLICE, SLICE + 1, ROWS * SLICE)
    assert kept.shape[0] == ROWS - 2
    assert start == 2 * SLICE


def test_trim_drops_rows_past_the_end():
    grid = np.zeros((ROWS, BINS), dtype=np.float32)
    # End halfway through row 2: rows 0 and 1 are wholly inside, row 2 is not.
    kept, start = trim_grid_rows(grid, 0, SLICE, 0, 2 * SLICE + 5)
    assert kept.shape[0] == 2
    assert start == 0


def test_trim_returns_empty_when_the_chunk_is_wholly_outside():
    grid = np.zeros((ROWS, BINS), dtype=np.float32)
    kept, start = trim_grid_rows(grid, 0, SLICE, 100 * SLICE, 200 * SLICE)
    assert kept.shape[0] == 0
    assert start == -1


def test_trim_handles_a_chunk_starting_mid_recording():
    grid = np.zeros((ROWS, BINS), dtype=np.float32)
    kept, start = trim_grid_rows(grid, 10 * SLICE, SLICE, 0, 1000 * SLICE)
    assert kept.shape[0] == ROWS
    assert start == 10 * SLICE
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_grid_prebuffer.py -x -q`
Expected: FAIL with `ImportError: cannot import name 'trim_grid_rows'`.

- [ ] **Step 3: Implement**

Add to `src/rfobserver/capture/buffer.py` at module level:

```python
def trim_grid_rows(
    grid: np.ndarray[Any, np.dtype[Any]],
    chunk_start: int,
    slice_samples: int,
    start_sample: int,
    end_sample: int,
) -> tuple[np.ndarray[Any, np.dtype[Any]], int]:
    """Return the rows of ``grid`` lying wholly inside ``[start_sample, end_sample)``.

    Row ``k`` of a chunk beginning at ``chunk_start`` covers stream samples
    ``[chunk_start + k*slice_samples, chunk_start + (k+1)*slice_samples)``. A row
    that straddles either boundary is dropped rather than misplaced, so the
    returned position is always a true row boundary. The second element is the
    absolute sample position of the first kept row, or -1 when no row qualifies.
    """
    rows = int(grid.shape[0]) if grid.ndim == 2 else 0
    if rows == 0 or slice_samples <= 0 or end_sample <= start_sample:
        return grid[:0], -1
    first = 0
    if start_sample > chunk_start:
        first = (start_sample - chunk_start + slice_samples - 1) // slice_samples
    # Last row whose END is still within the recording.
    last = (end_sample - chunk_start) // slice_samples
    if last > rows:
        last = rows
    if first >= last:
        return grid[:0], -1
    return grid[first:last], chunk_start + first * slice_samples
```

- [ ] **Step 4: Run to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_grid_prebuffer.py -x -q`
Expected: PASS (11 tests).

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/capture/buffer.py tests/unit/test_grid_prebuffer.py
git commit -m "feat(capture): add sample-range trimming for PSD grid rows"
```

---

### Task 4: Anchor the recording and apply the trim

Wire the recording's sample bounds into `_begin_recording` / `_handle_chunk_result` so rows land where they belong.

**Files:**
- Modify: `src/rfobserver/pipeline/streaming.py` (`__init__`, `_begin_recording`, `_anchor_recording`, `_write_recording_chunk`, `_handle_chunk_result`)

**Interfaces:**
- Consumes: `trim_grid_rows` (Task 3), `GridPreBuffer.drain(from_sample)` (Task 1), `_ChunkResult.chunk_start` and `self._slice_samples` (Task 2).
- Produces: `self._recording_start_sample: int | None`, `self._recording_end_sample: int | None`, `self._grid_first_sample: int | None`, `self._grid_last_sample: int`, `self._grid_accepting: bool`.

- [ ] **Step 1: Declare the new state in `__init__`**

Next to `self._recording_next_pos: int | None = None` (line ~354):

```python
        # Absolute stream sample bounds of the current recording, and how far
        # the .psd companion has been filled within them.
        self._recording_start_sample: int | None = None
        self._recording_end_sample: int | None = None
        self._grid_first_sample: int | None = None
        self._grid_last_sample: int = 0
        self._grid_accepting: bool = False
```

- [ ] **Step 2: Set the start bound where the file start is pinned**

`_anchor_recording` already pins `_recording_next_pos`. Set the start bound in the same
place, so the two can never disagree:

```python
        self._recording_ring = ring
        self._recording_next_pos = pre_start + written if written > 0 else None
        self._recording_start_sample = pre_start if written > 0 else None
```

When no pre-roll was written the start is the first recorded chunk. In
`_write_recording_chunk`, beside the existing `self._recording_next_pos = chunk_start + n`
(line ~959), add:

```python
            if self._recording_start_sample is None:
                self._recording_start_sample = chunk_start
```

- [ ] **Step 3: Drain the pre-buffer by position**

In `_begin_recording` the drain already sits just after `pre_start` is computed (line
~1077), so no reordering is needed. Pass the position and reset the fill trackers:

```python
        pre_data, pre_end = ring.read_with_position()
        t_read = time.time()
        pre_start = pre_end - len(pre_data)
        # Take only the grid rows that actually cover this recording's IQ. When
        # the PSD pipeline latency exceeds TRIGGER_PRE_SEC (the normal case)
        # every buffered grid predates pre_start and this yields nothing; the
        # rows covering the pre-roll arrive shortly after as live grids.
        pre_roll = self._grid_prebuf.drain(from_sample=pre_start)
        self._grid_first_sample = None
        self._grid_last_sample = 0
        self._recording_end_sample = None
        if pre_roll is not None:
            self._recording_freq_axis = pre_roll.freq_axis
            self._recording_time_res = pre_roll.time_res
            self._grid_min = pre_roll.grid_min
            self._grid_max = pre_roll.grid_max
            self._grid_first_sample = pre_roll.start_sample
            self._grid_last_sample = pre_roll.start_sample + pre_roll.rows * self._slice_samples
```

Both mode branches already seed from `pre_roll`; they need no change beyond this.

Set `self._grid_accepting = True` immediately before each `self._recording_state = "recording"`
flip (lines ~1105 and ~1157), so grids are accepted for exactly as long as the file is open.

- [ ] **Step 4: Trim in `_handle_chunk_result`**

Replace the recording branch's gate and body. The gate moves from the state string to
`_grid_accepting` so the tail drain in Task 5 can keep feeding rows while finalizing:

While the recording is still running `_recording_end_sample` is None, meaning "no end
yet"; use a sentinel past every possible row so nothing is trimmed off the tail until the
stop position is known.

```python
        if self._grid_accepting and self._recording_start_sample is not None:
            end = self._recording_end_sample
            if end is None:
                # Still recording: no end bound yet, so keep every row from
                # start_sample onward. Trimming at the tail begins only once
                # _request_end_recording freezes the position.
                end = cr.chunk_start + int(cr.psd_grid.grid.shape[0]) * self._slice_samples
            grid, first_sample = trim_grid_rows(
                cr.psd_grid.grid,
                cr.chunk_start,
                self._slice_samples,
                self._recording_start_sample,
                end,
            )
            self._recording_freq_axis = cr.psd_grid.freq_axis
            if len(cr.psd_grid.time_axis) > 1:
                self._recording_time_res = float(
                    cr.psd_grid.time_axis[1] - cr.psd_grid.time_axis[0]
                )
            if grid.size:
                if self._grid_first_sample is None:
                    self._grid_first_sample = first_sample
                self._grid_last_sample = first_sample + grid.shape[0] * self._slice_samples
                self._grid_min = min(self._grid_min, float(grid.min()))
                self._grid_max = max(self._grid_max, float(grid.max()))
                if self._grid_raw_path is not None:
                    data = np.ascontiguousarray(grid, dtype=np.float32).tobytes()
                    try:
                        self._recording_queue.put_nowait(("grid", data))
                        self._grid_rows += grid.shape[0]
                    except queue.Full:
                        # Companion data — drop rather than stall the dispatch loop.
                        self._grid_dropped += grid.shape[0]
                else:
                    self._recording_grids.append(grid.copy())
        else:
            ...  # unchanged pre-buffer branch from Task 2
```

Import `trim_grid_rows` alongside the existing `CircularBuffer` / `GridPreBuffer` import.

- [ ] **Step 4a: Set the end bound when the recording stops**

In `_request_end_recording`, inside the `with self._rec_lock:` block, before flipping the
state, freeze the end of the IQ. `_recording_next_pos` is the exact position after the
last recorded sample:

```python
            self._recording_end_sample = self._recording_next_pos
            self._recording_state = "finalizing"
```

Update the stale comment two lines above it: it says the flip "stops chunk writes ... and
grid appends (`_handle_chunk_result`) — both gate on the exact 'recording' state". After
this change grid appends gate on `_grid_accepting`, and deliberately continue through
finalizing so the tail can drain. Say that.

- [ ] **Step 5: Run the suite**

Run: `PYTHONPATH= .venv/bin/mypy src/rfobserver/ && PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q && PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q`
Expected: PASS. `tests/integration/test_recording_grids.py` may now produce fewer rows than before (the stale pre-roll no longer pads the file) but its `grid_span >= 0.85 * iq_span` assertion should still hold once Task 5 lands. If it fails here, note it and continue to Task 5 rather than loosening the assertion.

- [ ] **Step 6: Commit**

```bash
git add src/rfobserver/pipeline/streaming.py
git commit -m "fix(pipeline): place recorded PSD rows by sample position, not arrival order"
```

---

### Task 5: Bounded tail drain at finalize

The grids covering the last ~820 ms of IQ arrive after the recording stops. `_finalize_recording` currently sleeps 50 ms, which is not enough by an order of magnitude.

**Files:**
- Modify: `src/rfobserver/pipeline/streaming.py` (module constant, `_finalize_recording`)

**Interfaces:**
- Consumes: `_recording_end_sample`, `_grid_last_sample`, `_grid_accepting` (Task 4).
- Produces: nothing for later tasks.

- [ ] **Step 1: Add the fallback ceiling constant**

Near the other module constants at the top of `streaming.py`:

```python
# Bounds on how long finalize waits for the in-flight PSD grids covering the tail
# of a recording. The wait ends as soon as the grids reach the IQ's last sample
# (normally about one pipeline latency, ~4 chunks). RECORDING_MAX_SEC caps it,
# with _FALLBACK used when RECORDING_MAX_SEC is 0 ("no limit") and _CEILING
# keeping the wait inside the 15 s budget that _request_end_recording's
# _end_done.wait(timeout=15) allows a manual stop -- without the ceiling a
# default RECORDING_MAX_SEC of 30 s could outlast it and let a manual stop
# return while finalize was still waiting.
_GRID_TAIL_DRAIN_FALLBACK_SEC = 5.0
_GRID_TAIL_DRAIN_CEILING_SEC = 10.0
```

- [ ] **Step 2: Replace the 50 ms sleep**

At the top of `_finalize_recording`, replace:

```python
        # Brief sleep lets any in-flight _handle_chunk_result finish its append
        time.sleep(0.05)
```

with:

```python
        self._await_tail_grids()
        self._grid_accepting = False
```

and add the method:

```python
    def _await_tail_grids(self) -> None:
        """Wait for the in-flight PSD grids covering the tail of the recording.

        The IQ is written synchronously in the receive loop but grids emerge
        about four chunks later, so at stop time the last ~4 chunks of IQ have
        no grid rows yet. Wait until the grids reach the recording's last
        sample, bounded by RECORDING_MAX_SEC where it is finite. Returns early
        the moment the grids catch up, which is the normal case.
        """
        end = self._recording_end_sample
        if end is None or self._recording_start_sample is None:
            time.sleep(0.05)
            return
        cap = float(self._settings.RECORDING_MAX_SEC or 0.0)
        if not math.isfinite(cap) or cap <= 0:
            cap = _GRID_TAIL_DRAIN_FALLBACK_SEC
        cap = min(cap, _GRID_TAIL_DRAIN_CEILING_SEC)
        deadline = time.monotonic() + cap
        while self._grid_last_sample < end and time.monotonic() < deadline:
            time.sleep(0.02)
        if self._grid_last_sample < end:
            short = (end - self._grid_last_sample) / max(1.0, float(self._settings.BANDWIDTH))
            logger.warning(
                "PSD tail did not drain within %.1fs; .psd is %.3fs short of the IQ",
                cap,
                short,
            )
```

Add `import math` if it is not already imported.

- [ ] **Step 3: Verify by hand against a short capture**

This is the case that produced the original bug report, so check it directly.

```bash
fuser -k 8899/tcp 2>/dev/null; rm -rf /tmp/psdalign; mkdir -p /tmp/psdalign
cd /home/orencollaco/GitHub/RFObserver
PYTHONPATH= RFOBS_MOCK_RECEIVER=true RFOBS_SENSOR_ACTIVE=true RFOBS_WEB_PORT=8899 \
  RFOBS_STORAGE_PATH=/tmp/psdalign RFOBS_DB_PATH=/tmp/psdalign/p.db \
  RFOBS_BANDWIDTH=2000000 RFOBS_TRIGGER_ENABLED=true RFOBS_TRIGGER_THRESHOLD_DB=-400 \
  RFOBS_TRIGGER_PRE_SEC=0.2 RFOBS_RECORDING_MAX_SEC=0.5 RFOBS_PEAKS_ROLLUP_INTERVAL_SEC=0 \
  nohup .venv/bin/rfobserver run > /tmp/psdalign/run.log 2>&1 &
```

Wait for `/api/health` to report `pipeline.active`, give the grid buffer ~30 s to fill,
then `curl -s -X POST localhost:8899/api/recording/arm` and wait for the `.psd.json` to
appear. Expected after this task: `rows * time_resolution_s` is within one row of the
`.sc16`'s `duration_sec`, where before the fix it was 0.819 s against 0.610 s.
Stop with `fuser -k 8899/tcp`.

- [ ] **Step 4: Run the full suite**

Run all five checks from Global Constraints.
Expected: PASS, including `tests/integration/test_recording_grids.py`.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/pipeline/streaming.py
git commit -m "fix(pipeline): wait for in-flight PSD grids before finalizing a capture"
```

---

### Task 6: Record the alignment in the sidecar, and honour it in the viewer

Make the invariant checkable from the files alone, and remove the viewer workaround that exists only because of this bug.

**Files:**
- Modify: `src/rfobserver/storage/psd_grid.py` (`write_meta`)
- Modify: `src/rfobserver/pipeline/streaming.py` (the `psd_grid.write_meta(...)` call at ~line 1258)
- Modify: `src/rfobserver/web/templates/captures.html` (lines ~281-289)
- Test: `tests/unit/test_psd_grid_meta.py` (create if absent)

**Interfaces:**
- Consumes: `_grid_first_sample`, `_recording_start_sample`, `_slice_samples` (Task 4).
- Produces: `.psd.json` gains `start_sample_offset` and `slice_samples`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_psd_grid_meta.py
import json

import numpy as np

from rfobserver.storage import psd_grid


def test_write_meta_records_alignment(tmp_path):
    meta_path = tmp_path / "cap.psd.json"
    psd_grid.write_meta(
        meta_path,
        rows=10,
        num_bins=4,
        time_resolution_s=0.001024,
        center_freq_hz=100_000_000,
        bandwidth_hz=2_000_000,
        freq_axis=np.arange(4, dtype=np.float64),
        grid_min=-160.0,
        grid_max=-40.0,
        cal_offset_db=None,
        start_sample_offset=1234,
        slice_samples=2048,
    )
    meta = json.loads(meta_path.read_text())
    assert meta["start_sample_offset"] == 1234
    assert meta["slice_samples"] == 2048


def test_write_meta_alignment_fields_default_to_zero(tmp_path):
    meta_path = tmp_path / "cap.psd.json"
    psd_grid.write_meta(
        meta_path,
        rows=0,
        num_bins=4,
        time_resolution_s=0.001024,
        center_freq_hz=100_000_000,
        bandwidth_hz=2_000_000,
        freq_axis=np.arange(4, dtype=np.float64),
        grid_min=0.0,
        grid_max=0.0,
        cal_offset_db=None,
    )
    meta = json.loads(meta_path.read_text())
    assert meta["start_sample_offset"] == 0
    assert meta["slice_samples"] == 0
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_psd_grid_meta.py -x -q`
Expected: FAIL with `TypeError: write_meta() got an unexpected keyword argument 'start_sample_offset'`.

- [ ] **Step 3: Implement the sidecar fields**

In `psd_grid.write_meta`, add two keyword-only parameters with defaults and write them:

```python
    start_sample_offset: int = 0,
    slice_samples: int = 0,
```

```python
        "grid_min": float(grid_min),
        "grid_max": float(grid_max),
        # Alignment to the .sc16: the first grid row starts this many samples
        # after the IQ file's first sample, and each row spans slice_samples.
        # Row k covers IQ samples
        #   [start_sample_offset + k*slice_samples,
        #    start_sample_offset + (k+1)*slice_samples).
        "start_sample_offset": int(start_sample_offset),
        "slice_samples": int(slice_samples),
```

Update the module docstring to mention that the sidecar pins the grid to the IQ.

- [ ] **Step 4: Pass the real values from the recorder**

At the `psd_grid.write_meta(...)` call in `_finalize_recording` (~line 1258):

```python
                start_sample_offset=(
                    self._grid_first_sample - self._recording_start_sample
                    if self._grid_first_sample is not None
                    and self._recording_start_sample is not None
                    else 0
                ),
                slice_samples=self._slice_samples,
```

- [ ] **Step 5: Fix the viewer's time axis**

In `captures.html` (~281-289) the comment says the grid's nominal `time_resolution_s`
does not satisfy `total_rows * tres == duration`, and divides `durationSec` by
`viewerTotalRows` instead. That was a symptom of this bug. Replace with the sidecar's own
resolution plus its offset:

```javascript
        // The .psd is pinned to the .sc16 by start_sample_offset: row k covers
        // IQ samples [start_sample_offset + k*slice_samples, ...). Use the grid's
        // own time resolution and offset rather than stretching rows across the
        // IQ duration, which silently hid a pipeline-latency misalignment.
        viewerTimeRes = first.time_resolution_s;
        viewerTimeOffset = (first.start_sample_offset && sampleRate)
            ? (first.start_sample_offset / sampleRate)
            : 0;
```

and add `viewerTimeOffset` to wherever a row index is converted to a displayed time
(`t = viewerTimeOffset + row * viewerTimeRes`). Keep a fallback to the old behaviour when
`time_resolution_s` is absent or zero, so captures recorded before this change still
render.

- [ ] **Step 6: Run to verify**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_psd_grid_meta.py -x -q` then the full
five checks.
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/rfobserver/storage/psd_grid.py src/rfobserver/pipeline/streaming.py \
        src/rfobserver/web/templates/captures.html tests/unit/test_psd_grid_meta.py
git commit -m "feat(captures): pin the .psd to the .sc16 and render it on that axis"
```

---

### Task 7: An integration test that actually checks alignment

The existing `grid_span >= 0.85 * iq_span` assertion passed throughout this bug (the span ratio was 1.25 while the content was 820 ms out of step). A length check is not an alignment check.

**Why the existing tests could never have caught it, and what the new test must do
differently.** The bug exists only when the PSD pipeline latency (about 4 chunks) exceeds
`TRIGGER_PRE_SEC`. Two of this file's settings each independently prevent that:

- `PSD_TIME_RESOLUTION_MS=0.5` with `NUM_FFT_BINS=256` gives `actual_slice_samples=896`,
  and `STREAMING_CHUNK_SLICES=10` makes a chunk 8960 samples = **4.5 ms** at 2 Msps.
  Latency is then ~18 ms.
- `TRIGGER_PRE_SEC` is not overridden, so it is the config default of **1.0 s**, not the
  0.2 s the field sensor runs.

Either way the latency sits well inside the pre-roll window, `GridPreBuffer` genuinely
does hold grids covering the pre-roll, and the old code is correct at that configuration.

**The new test must set both `STREAMING_CHUNK_SLICES=200` and `TRIGGER_PRE_SEC=0.2`.**
With 200 slices a chunk here is 179200 samples = 89.6 ms, so latency is ~358 ms, which
exceeds a 200 ms pre-roll and reproduces the defect. Setting only one of the two leaves
the test passing before and after the fix, proving nothing. Verify in Step 2.

**Files:**
- Modify: `tests/integration/test_recording_grids.py`

**Interfaces:**
- Consumes: the finished behaviour from Tasks 4-6.
- Produces: nothing.

- [ ] **Step 1: Write the failing test**

Add a test that correlates the recorded IQ against the recorded grid and asserts they
line up. Follow the existing file's fixtures for starting a recording; the new assertion
is the point:

```python
import numpy as np


@pytest.mark.asyncio
async def test_psd_rows_align_with_the_iq_they_describe(tmp_path):
    """The .psd must describe the same samples as the .sc16.

    Regression test for the ~820 ms pipeline-latency misalignment: the PSD grid
    was built from chunks that had passed through _chunk_queue and the worker
    pool, while the IQ pre-roll was written synchronously, so the two files
    covered different intervals. A span-length check cannot see this; only
    correlating the content can.

    STREAMING_CHUNK_SLICES=200 and TRIGGER_PRE_SEC=0.2 are both load-bearing:
    the defect exists only where the PSD pipeline latency (~4 chunks) exceeds
    the pre-roll window. At this file's default 10 slices a chunk is 4.5 ms
    (latency ~18 ms), and TRIGGER_PRE_SEC defaults to 1.0 s. Either default on
    its own hides the bug completely.
    """
    settings = _settings(
        tmp_path,
        STREAMING_CHUNK_SLICES=200,
        TRIGGER_PRE_SEC=0.2,
        RECORDING_RAM_BUFFER=False,
    )
    db = SensorDatabase(settings.DB_PATH)
    await db.initialize()
    try:
        # Fill the pre-trigger buffers, then record a short window, so the
        # pre-roll is a large fraction of the capture.
        out = await _record_with_preroll(settings, db, preroll_to=12, record_to=20)
    finally:
        await db.close()

    sc16 = next(out.glob("*.sc16"))
    base = sc16.with_suffix("")
    pmeta = json.loads(base.with_suffix(".psd.json").read_text())
    # Fall back to deriving these so the test still RUNS against pre-fix code
    # (where the sidecar has neither field) and fails on alignment rather than
    # on a KeyError. Step 2 depends on that.
    slice_samples = int(
        pmeta.get("slice_samples")
        or round(float(pmeta["time_resolution_s"]) * settings.BANDWIDTH)
    )
    offset = int(pmeta.get("start_sample_offset", 0))

    iq = np.fromfile(sc16, dtype=np.int32).view(np.int16)
    iq = iq.astype(np.float32).reshape(-1, 2) / 32768.0
    iq = iq[offset:]
    nrows = min(len(iq) // slice_samples, int(pmeta["rows"]))
    assert nrows > 20, "capture too short to test alignment"

    power = (iq[: nrows * slice_samples, 0] ** 2 + iq[: nrows * slice_samples, 1] ** 2)
    a = 10 * np.log10(power.reshape(nrows, slice_samples).mean(axis=1) + 1e-30)
    grid = np.fromfile(base.with_suffix(".psd"), dtype=np.float32)
    grid = grid.reshape(-1, int(pmeta["num_bins"]))[:nrows]
    b = grid.mean(axis=1)

    a = (a - a.mean()) / (a.std() + 1e-12)
    b = (b - b.mean()) / (b.std() + 1e-12)
    # Scan every lag; the best one must be zero (the files are pinned together).
    best_lag, best_corr = 0, -2.0
    for lag in range(-nrows + 5, nrows - 5):
        if lag >= 0:
            x, y = a[: nrows - lag], b[lag:nrows]
        else:
            x, y = a[-lag:nrows], b[: nrows + lag]
        if len(x) < 10 or x.std() < 1e-9 or y.std() < 1e-9:
            continue
        c = float(np.corrcoef(x, y)[0, 1])
        if c > best_corr:
            best_lag, best_corr = lag, c
    assert best_corr > 0.5, f"PSD does not describe the IQ at any lag (best {best_corr:.2f})"
    assert abs(best_lag) <= 1, f"PSD is {best_lag} rows out of step with the IQ"
```

Note for the implementer: the mock receiver's signal must have enough time structure for
correlation to mean anything. Check `best_corr > 0.5` first so a flat signal fails loudly
as "cannot test" rather than silently passing `abs(best_lag) <= 1` on noise. If the mock
output is too stationary, inject a marker: record with a burst pattern, or assert instead
that the argmax row of the IQ envelope and of the grid means agree within one row.

- [ ] **Step 2: Run against the pre-fix code to confirm it catches the bug**

Run the new test against the code as it was before Task 1. Use a worktree rather than a
stash, so the whole pre-fix source tree is consistent (stashing only some files leaves
`write_meta` callers and signatures mismatched):

```bash
BASE=$(git log --format=%H --grep="tag pre-roll PSD grids" -1)^   # commit before Task 1
git worktree add /tmp/psd-prefix "$BASE"
cp tests/integration/test_recording_grids.py /tmp/psd-prefix/tests/integration/
cd /tmp/psd-prefix && PYTHONPATH= /home/orencollaco/GitHub/RFObserver/.venv/bin/pytest \
    tests/integration/test_recording_grids.py -x -q -k align
cd /home/orencollaco/GitHub/RFObserver && git worktree remove --force /tmp/psd-prefix
```

Expected: FAIL on the pre-fix tree (`best_lag` of roughly `-(latency/time_resolution)`
rows, or no correlation at any lag), PASS on the current tree. A test that passes in both
states is not testing alignment: check that `STREAMING_CHUNK_SLICES=200` actually took
effect, and if `best_corr` is below 0.5 in both runs the mock signal is too stationary
and the test needs a marker burst instead.

- [ ] **Step 3: Strengthen the existing span assertion**

`grid_span >= 0.85 * iq_span` allowed a 1.25 ratio through. Tighten it to a two-sided
bound now that the spans are pinned:

```python
    assert 0.9 * iq_span <= grid_span <= 1.1 * iq_span, (
        f"PSD span {grid_span:.3f}s does not match IQ span {iq_span:.3f}s"
    )
```

- [ ] **Step 4: Full suite**

Run all five checks from Global Constraints.
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/test_recording_grids.py
git commit -m "test(recording): assert the .psd describes the same samples as the .sc16"
```

---

## Verification beyond the test suite

- [ ] Re-run both probes from the debugging doc (2 s cap and 0.5 s cap) and confirm the
      cross-correlation peak moves from +803 rows to 0, and that the short capture's
      `.psd` now contains the trigger instant.
- [ ] Append a dated CORRECTION section to
      `docs/debugging/2026-09-22_trigger-psd-iq-misalignment.md` recording the post-fix
      measurements, per the repo's debugging-doc convention (corrections are appended,
      never edited in place).
- [ ] Open question to close or carry forward: the exact value of the latency `L` is
      still not pinned (822 ms measured, 819 ms predicted from queue depth). The fix does
      not depend on `L` being constant, which is the point of anchoring by position, but
      the tail-drain cap does assume `L` is bounded. Note the observed worst case.
- [ ] Not reproduced on hardware. Both probes are the mock receiver on the workstation.
      Validate on nano-super before this reaches the field sensor.

## Out of scope

- Recomputing correct `.psd` files for already-archived captures. The `.sc16` is intact
  so it is possible offline, but no migration is part of this plan.
- The trigger's whole-chunk mean sensitivity (a short burst raises a 204.8 ms mean only
  slightly). Real, separate, and explicitly NOT the cause of this bug.
- `TRIGGER_DETECT_SEC` being dead config, and `TRIGGER_HYSTERESIS` being presented on the
  config page as if it governs triggering when it only governs stopping.
- IQ capture download over the UI.
