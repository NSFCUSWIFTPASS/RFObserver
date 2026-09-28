# Burst Isolation and Attribution in the Streaming Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Two separately enabled stages, burst isolation (frequency shift + decimation of each picked burst, saved as SigMF and offered to modules) and rtl_433 attribution of isolated bursts, running in the streaming pipeline, live and in replay, verified on nano-super.

**Architecture:** The rolling detector stamps each burst with its absolute stream sample range. The streaming burst thread hands completed bursts (with SNR at their peak bin) to an `IsolationStage` running on its own thread, which reads the burst's samples back out of the pre-trigger ring (grown to `ISOLATION_LOOKBACK_SEC` when enabled), channelizes them, and fans out to a SigMF archive, the module manager, and the existing asyncio `AttributionWorker`. Attribution results go to the DB live and to a replay results file during replay, and appear as labels on the live overlay. The sweep pipeline routes through the same stage.

**Tech Stack:** Python 3.10+ (Jetson runs 3.10), numpy, scipy.signal.resample_poly, asyncio, threading, aiosqlite, FastAPI/Jinja inline JS, rtl_433 (external binary), pytest + pytest-asyncio.

**Spec:** `docs/superpowers/specs/2026-09-28-burst-isolation-streaming-design.md`

## Global Constraints

- Branch `feat/rtl433-burst-attribution` (main merged in at 25ef2fd). Commit only named files by explicit path; never `git add -A` / `git add .`.
- Never add a `Co-Authored-By: Claude` (or any Claude co-author) line to commits.
- No emojis and no em-dashes anywhere (code, comments, UI text, docs). UI follows the Apple-style CSS variables in `style.css`.
- Python 3.10-clean: no `asyncio.timeout`, no `except*`, no `datetime.UTC` (use `timezone.utc`).
- Prefix python commands with `PYTHONPATH=`; ruff is global. Before each commit run: `ruff check src/ tests/`, `ruff format --check src/ tests/`, `PYTHONPATH= .venv/bin/mypy src/rfobserver/`, `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`, `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q` (10 NATS skips expected; do not start docker containers).
- Never `pkill -f "rfobserver run"`; use `fuser -k <port>/tcp`. Do not write the repo `.env`. Field sensor `rfnano` is off-limits.
- Settings (exact): `ISOLATION_ENABLED: bool = False`, `ATTRIBUTION_ENABLED: bool = False` (forces isolation on), `ISOLATION_LOOKBACK_SEC: float = 1.5`, `ISOLATION_MAX_BURST_SEC: float = 0.5`, `ISOLATION_SNR_DB: float = 13.0`, `ISOLATION_MAX_PER_SEC: int = 20`, `ISOLATION_QUEUE_MAX: int = 64`, `BURST_ARCHIVE_MAX_GB: float = 2.0`, `ATTRIBUTION_RTL433_PATH: str = ""`. Remove `ATTRIBUTION_SNR_DB`, `ATTRIBUTION_MAX_PER_CHUNK`, `ATTRIBUTION_QUEUE_MAX`.
- Constants (exact): guard band 2 ms each side; channelize tiers unchanged (1.6 Msps at or above 200 kHz burst bandwidth, else 1.0 Msps); attribution DB update retried once after 2 s when the row is missing; ring RAM guard: isolation disabled if the ring would exceed 25% of MemAvailable.
- Burst states (exact strings): `isolated`, `iq_expired`, `too_long`, `queue_full`, `error`.
- Replay never writes the DB.
- Isolation DSP never runs on the receiver, dispatch or burst threads or the event loop.

**Rulings taken while planning (deviations from the spec, for the user to confirm at plan review):**
1. Replay attribution results go to `STORAGE_PATH/bursts/replay-<capture stem>/attribution.jsonl` (JSON lines), not beside the replayed capture: replayed files often live on read-only trees (`/mnt/storage`), and the replay recording's `.detections.json` sidecar is rebuilt by re-detection with different burst_ids, so results cannot be merged into it by id. The overlay labels are unchanged.
2. The isolation queue is bounded per batch (one batch per detector evaluation); a full queue drops the incoming batch and counts each of its bursts as `queue_full`. The attribution queue keeps its drop-weakest behaviour.
3. Bursts the gate does not pick (below SNR, or beyond the per-second limit) are not "picked", so they get no state; they are counted as `gated_out`.
4. Attribution outcome counters are `decoded`, `not_decoded`, `failed` (a timeout inside `decode_cs16` already lands as `not_decoded`).
5. The startup RAM guard compares the ring against 25% of `MemAvailable` (the helper `_mem_available_bytes` the recording RAM cap already uses), not the recording cap itself, which is a different budget.

## Review Focus

1. **Recording pre-roll with a grown ring.** With isolation on, the ring holds 1.5 s, but a recording must still pre-roll only `TRIGGER_PRE_SEC`. Expect the `.sc16` pre-roll length unchanged. Test: `test_preroll_reads_only_trigger_pre_sec_from_a_grown_ring` (Task 1).
2. **Ring positions after a large single write.** `CircularBuffer.write` of more than capacity resets `_write_pos` to 0 while `total_written` is not a multiple of capacity; `read_range` must still return the right samples. Test: `test_read_range_after_an_oversized_write` (Task 1).
3. **Dropped chunks between grids.** A missing chunk means row positions are not contiguous; burst sample ranges must come from the rows' own positions. Test: `test_burst_samples_survive_a_dropped_chunk` (Task 2).
4. **A burst longer than the ring or cap.** Expect `too_long` with exactly `ISOLATION_MAX_BURST_SEC` isolated when in range, `iq_expired` when its start has left the ring, never an exception. Tests: `test_long_burst_truncated`, `test_expired_burst` (Task 3).
5. **rtl_433 missing with attribution on.** Isolation still runs and saves; health says attribution unavailable. Test: `test_attribution_without_rtl433_keeps_isolation` (Task 6).

---

## File Structure

| File | Responsibility |
|---|---|
| `src/rfobserver/config.py` | Settings above; remove the three superseded ones |
| `src/rfobserver/capture/buffer.py` | `CircularBuffer.read_range`, `read_tail_with_position` |
| `src/rfobserver/models.py` | `BurstFingerprint.start_sample`, `stop_sample` |
| `src/rfobserver/processing/rolling_burst.py` | Per-row stream positions; bursts carry sample ranges |
| `src/rfobserver/processing/isolate.py` (new) | Pure DSP: `IsolatedBurst`, `isolate_burst`, `iq_to_complex` |
| `src/rfobserver/storage/burst_archive.py` (new) | SigMF save, usage, cap, eviction for `bursts/` |
| `src/rfobserver/modules/base.py`, `modules/manager.py` | Optional `feed_burst` hook, `feed_bursts` |
| `src/rfobserver/pipeline/attribution.py` | `AttributionItem.meta`, sinks, outcome callback |
| `src/rfobserver/storage/database.py` | `update_detection_attribution` returns rowcount |
| `src/rfobserver/pipeline/isolation.py` (new) | `IsolationStage`, `IsolationBatch`, `BurstCandidate`, `RingSource`, `WholeCaptureSource`, `IsolationStats`, `build_isolation` |
| `src/rfobserver/pipeline/streaming.py` | Ring sizing, tail pre-roll, chunk_start to detector, SNR, stage wiring, labels, status |
| `src/rfobserver/pipeline/continuous.py` | Route through the stage; remove old helpers |
| `src/rfobserver/storage/governor.py`, `storage/local.py`, `pipeline/app.py` | `bursts_bytes`, burst eviction first, cap, stop saving at step 3 |
| `src/rfobserver/web/app.py`, `web/routes/config.py`, `templates/config.html`, `templates/dashboard.html` | Health `isolation` block, settings fields, overlay labels |
| `README.md` | "Burst isolation and attribution" section |

---

### Task 1: Settings, ring range reads, ring sizing, tail pre-roll

**Files:**
- Modify: `src/rfobserver/config.py` (the `# rtl_433 per-burst attribution` block near line 199)
- Modify: `src/rfobserver/capture/buffer.py` (`CircularBuffer`, after `read_with_position`)
- Modify: `src/rfobserver/pipeline/streaming.py` (`_recompute_chunk_params` ~530, `_begin_recording` pre-roll read)
- Modify: `src/rfobserver/pipeline/continuous.py` (only the three removed settings: keep it importable; Task 7 rewires it)
- Test: `tests/unit/test_buffer.py` (append), `tests/unit/test_isolation_ring.py` (new)

**Interfaces:**
- Produces: `CircularBuffer.read_range(start: int, end: int) -> np.ndarray | None`, `CircularBuffer.read_tail_with_position(n: int) -> tuple[np.ndarray, int]`, `CircularBuffer.oldest_position -> int` (property), `StreamingProcessor._isolation_wanted() -> bool`, `StreamingProcessor._isolation_disabled_reason: str | None`, `StreamingProcessor._ring_sec: float`.

- [ ] **Step 1: Settings**

Replace the attribution block in `config.py` with:

```python
    # Burst isolation: cut each picked burst out of the wideband IQ (shift its
    # peak to DC and decimate), save it as SigMF under STORAGE_PATH/bursts/ and
    # offer it to add-on modules. Off by default.
    ISOLATION_ENABLED: bool = False
    # rtl_433 attribution of isolated bursts (model / protocol onto the
    # detection). Turning it on forces isolation on. Off by default.
    ATTRIBUTION_ENABLED: bool = False
    ATTRIBUTION_RTL433_PATH: str = ""  # "" -> auto-discover via find_rtl433
    # While isolation is on, the IQ ring keeps at least this many seconds so a
    # burst's samples are still there when detection completes.
    ISOLATION_LOOKBACK_SEC: float = 1.5
    # Longer bursts are isolated only up to this length.
    ISOLATION_MAX_BURST_SEC: float = 0.5
    # Gate: dB over the noise floor at the burst's peak bin, and the most bursts
    # isolated per second (strongest first).
    ISOLATION_SNR_DB: float = 13.0
    ISOLATION_MAX_PER_SEC: int = 20
    # Bounded queues into the isolation worker and the rtl_433 worker.
    ISOLATION_QUEUE_MAX: int = 64
    # Cap on saved isolated-burst files (oldest deleted first).
    BURST_ARCHIVE_MAX_GB: float = 2.0
```

In `continuous.py`, the three removed settings are read at the attribution call site; replace `self._settings.ATTRIBUTION_SNR_DB` with `self._settings.ISOLATION_SNR_DB`, `ATTRIBUTION_MAX_PER_CHUNK` with `ISOLATION_MAX_PER_SEC`, and `StrongestQueue(maxsize=settings.ATTRIBUTION_QUEUE_MAX)` with `ISOLATION_QUEUE_MAX` (Task 7 replaces this code entirely). Grep `tests/` for the removed names and update the same way.

- [ ] **Step 2: Failing ring tests**

Append to `tests/unit/test_buffer.py`:

```python
def test_read_range_returns_exact_stream_samples():
    buf = CircularBuffer(10, dtype=np.int32)
    buf.write(np.arange(7, dtype=np.int32))
    assert list(buf.read_range(2, 5)) == [2, 3, 4]
    buf.write(np.arange(7, 15, dtype=np.int32))  # holds stream 5..14
    assert buf.oldest_position == 5
    assert list(buf.read_range(8, 13)) == [8, 9, 10, 11, 12]
    assert list(buf.read_range(5, 15)) == list(range(5, 15))


def test_read_range_outside_the_ring_is_none():
    buf = CircularBuffer(10, dtype=np.int32)
    buf.write(np.arange(15, dtype=np.int32))  # holds 5..14
    assert buf.read_range(4, 8) is None  # start already overwritten
    assert buf.read_range(10, 16) is None  # end not written yet
    assert buf.read_range(8, 8) is None  # empty


def test_read_range_after_an_oversized_write():
    buf = CircularBuffer(10, dtype=np.int32)
    buf.write(np.arange(3, dtype=np.int32))
    buf.write(np.arange(3, 26, dtype=np.int32))  # larger than capacity: holds 16..25
    assert buf.oldest_position == 16
    assert list(buf.read_range(16, 26)) == list(range(16, 26))
    buf.write(np.arange(26, 30, dtype=np.int32))  # holds 20..29
    assert list(buf.read_range(24, 30)) == list(range(24, 30))


def test_read_tail_with_position():
    buf = CircularBuffer(10, dtype=np.int32)
    buf.write(np.arange(14, dtype=np.int32))
    data, end = buf.read_tail_with_position(3)
    assert end == 14 and list(data) == [11, 12, 13]
    data, end = buf.read_tail_with_position(100)  # clamps to what is held
    assert list(data) == list(range(4, 14))
```

Create `tests/unit/test_isolation_ring.py`:

```python
"""The IQ ring grows for isolation, but recordings still pre-roll TRIGGER_PRE_SEC."""

from __future__ import annotations

import numpy as np

from tests.unit.test_recording_gaps import _proc


def test_ring_is_trigger_pre_sec_when_isolation_is_off(tmp_path):
    proc = _proc(tmp_path, TRIGGER_PRE_SEC=0.001)
    assert proc._pre_trigger_buf.capacity == 1000
    assert proc._isolation_disabled_reason is None


def test_ring_grows_to_lookback_when_isolation_is_on(tmp_path):
    proc = _proc(tmp_path, TRIGGER_PRE_SEC=0.001, ISOLATION_ENABLED=True, ISOLATION_LOOKBACK_SEC=0.01)
    assert proc._pre_trigger_buf.capacity == 10_000
    assert proc._ring_sec == 0.01


def test_attribution_alone_also_grows_the_ring(tmp_path):
    proc = _proc(tmp_path, TRIGGER_PRE_SEC=0.001, ATTRIBUTION_ENABLED=True, ISOLATION_LOOKBACK_SEC=0.01)
    assert proc._isolation_wanted()
    assert proc._pre_trigger_buf.capacity == 10_000


def test_ring_that_would_not_fit_disables_isolation(tmp_path, monkeypatch):
    monkeypatch.setattr("rfobserver.pipeline.streaming._mem_available_bytes", lambda: 100_000)
    proc = _proc(tmp_path, TRIGGER_PRE_SEC=0.001, ISOLATION_ENABLED=True, ISOLATION_LOOKBACK_SEC=0.01)
    assert proc._pre_trigger_buf.capacity == 1000
    assert "RAM" in proc._isolation_disabled_reason


def test_preroll_reads_only_trigger_pre_sec_from_a_grown_ring(tmp_path):
    proc = _proc(
        tmp_path,
        TRIGGER_PRE_SEC=0.001,
        ISOLATION_ENABLED=True,
        ISOLATION_LOOKBACK_SEC=0.01,
        RECORDING_RAM_BUFFER=True,
        RECORDING_MAX_SEC=1.0,
    )
    proc._pre_trigger_buf.write(np.arange(8000, dtype=np.int32))
    proc.start_recording()
    try:
        assert proc._recording_buf_pos == 1000  # TRIGGER_PRE_SEC at 1 Msps, not 8000
        assert list(proc._recording_buf[:3]) == [7000, 7001, 7002]
    finally:
        proc.stop_recording()
```

(`_proc` in `tests/unit/test_recording_gaps.py` builds a `StreamingProcessor` at `BANDWIDTH=1_000_000` with overrides; it is importable as shown because `tests/` is a package.)

- [ ] **Step 3: Run to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_buffer.py tests/unit/test_isolation_ring.py -q`
Expected: FAIL (`AttributeError: 'CircularBuffer' object has no attribute 'read_range'`, `_isolation_disabled_reason` missing).

- [ ] **Step 4: Implement the ring reads**

In `CircularBuffer`, after `read_with_position`:

```python
    @property
    def oldest_position(self) -> int:
        """Stream position of the oldest sample still held."""
        with self._lock:
            return self._total_written - min(self._total_written, self._max_samples)

    def _copy_range_locked(self, start: int, end: int) -> np.ndarray:
        # The newest sample (position total-1) sits at index write_pos-1. This
        # holds after an oversized write too (write_pos resets to 0 while
        # total_written is not a multiple of capacity), so never use p % cap.
        cap = self._max_samples
        i0 = (self._write_pos - (self._total_written - start)) % cap
        n = end - start
        if i0 + n <= cap:
            return self._buffer[i0 : i0 + n].copy()
        first = cap - i0
        return np.concatenate([self._buffer[i0:], self._buffer[: n - first]])

    def read_range(self, start: int, end: int) -> np.ndarray | None:
        """A copy of stream samples ``[start, end)``, or None if any of them has
        already been overwritten or not yet written (or the range is empty)."""
        with self._lock:
            oldest = self._total_written - min(self._total_written, self._max_samples)
            if start >= end or start < oldest or end > self._total_written:
                return None
            return self._copy_range_locked(start, end)

    def read_tail_with_position(self, n: int) -> tuple[np.ndarray, int]:
        """The newest ``n`` samples (fewer if not held) and ``total_written``."""
        with self._lock:
            n = min(n, self._total_written, self._max_samples)
            if n <= 0:
                return self._buffer[:0].copy(), self._total_written
            return self._copy_range_locked(self._total_written - n, self._total_written), (
                self._total_written
            )
```

- [ ] **Step 5: Implement ring sizing and the tail pre-roll**

In `StreamingProcessor`, add:

```python
    def _isolation_wanted(self) -> bool:
        s = self._settings
        return bool(s.ISOLATION_ENABLED or s.ATTRIBUTION_ENABLED)
```

In `_recompute_chunk_params`, replace `pre_trigger_samples = int(s.TRIGGER_PRE_SEC * s.BANDWIDTH)` and the ring construction with:

```python
        # The ring is the pre-trigger buffer and, when isolation is on, also the
        # lookback isolation reads bursts from after detection completes. A
        # recording still pre-rolls only TRIGGER_PRE_SEC (read_tail in
        # _begin_recording). If the grown ring would not fit, isolation is
        # disabled rather than risking OOM on the Jetson.
        self._isolation_disabled_reason: str | None = None
        ring_sec = float(s.TRIGGER_PRE_SEC)
        if self._isolation_wanted():
            want = max(ring_sec, float(s.ISOLATION_LOOKBACK_SEC))
            ring_bytes = int(want * s.BANDWIDTH) * 4
            avail = _mem_available_bytes()
            if avail is not None and ring_bytes > 0.25 * avail:
                self._isolation_disabled_reason = (
                    f"isolation ring of {ring_bytes / 1e6:.0f} MB exceeds 25% of available "
                    f"RAM ({avail / 1e6:.0f} MB); isolation disabled"
                )
                logger.error(self._isolation_disabled_reason)
            else:
                ring_sec = want
        self._ring_sec = ring_sec
        pre_trigger_samples = int(ring_sec * s.BANDWIDTH)
```

keeping the existing `with self._stream_gaps_lock:` block that builds `CircularBuffer(max(1, pre_trigger_samples), dtype=np.int32)` and the log line (log `ring_sec` instead of `s.TRIGGER_PRE_SEC`).

In `_begin_recording`, replace `pre_data, pre_end = ring.read_with_position()` with:

```python
        # The ring may be longer than TRIGGER_PRE_SEC (isolation lookback); the
        # pre-roll is only the newest TRIGGER_PRE_SEC of it.
        pre_data, pre_end = ring.read_tail_with_position(int(s.TRIGGER_PRE_SEC * s.BANDWIDTH))
```

(`s = self._settings` is already bound in that method; if `s` is bound later in the method, move the binding up.)

- [ ] **Step 6: Run the tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_buffer.py tests/unit/test_isolation_ring.py tests/unit/test_recording_gaps.py tests/unit/test_grid_prebuffer.py tests/unit/test_recording_storage.py -q`
Expected: all pass.

- [ ] **Step 7: Full checks and commit**

```bash
git add src/rfobserver/config.py src/rfobserver/capture/buffer.py src/rfobserver/pipeline/streaming.py src/rfobserver/pipeline/continuous.py tests/unit/test_buffer.py tests/unit/test_isolation_ring.py
git commit -m "feat(isolation): settings, ring range reads, lookback ring with a RAM guard"
```

(Add any test file changed for the removed settings, by path.)

---

### Task 2: Bursts carry their stream sample range

**Files:**
- Modify: `src/rfobserver/models.py` (`BurstFingerprint`)
- Modify: `src/rfobserver/processing/rolling_burst.py` (`__init__`, `feed`, `_to_fingerprint`, `reset`)
- Modify: `src/rfobserver/pipeline/streaming.py` (`_handle_chunk_result` burst-queue put; `_burst_detection_loop` unpack and `feed` call)
- Test: `tests/unit/test_rolling_positions.py` (new)

**Interfaces:**
- Consumes: nothing new.
- Produces: `BurstFingerprint.start_sample: int | None = None`, `BurstFingerprint.stop_sample: int | None = None` (stop exclusive); `RollingBurstDetector.feed(psd_grid, chunk_start: int | None = None, slice_samples: int | None = None) -> list[BurstFingerprint]`; burst queue items are `(psd_grid, center_freq_hz, capture_num, chunk_start)`.

- [ ] **Step 1: Failing tests**

Create `tests/unit/test_rolling_positions.py`:

```python
"""Completed bursts carry the absolute stream samples they came from."""

from __future__ import annotations

import numpy as np

from rfobserver.processing.burst import BurstDetectionConfig
from rfobserver.processing.rolling_burst import RollingBurstDetector
from rfobserver.processing.spectral import PSDGridResult

BINS = 64
SLICE = 100  # samples per PSD row


def _grid(rows: int, burst_rows: range | None = None) -> PSDGridResult:
    g = np.full((rows, BINS), -100.0, dtype=np.float32)
    g += np.random.default_rng(0).normal(0, 0.5, g.shape).astype(np.float32)
    if burst_rows is not None:
        g[burst_rows.start : burst_rows.stop, 30:34] = -40.0
    return PSDGridResult(
        grid=g,
        time_axis=np.arange(rows) * 1e-4,
        freq_axis=np.linspace(-5e5, 5e5, BINS),
        ffts_per_slice=1,
        total_ffts=rows,
    )


def _det() -> RollingBurstDetector:
    return RollingBurstDetector(
        window_rows=256,
        eval_interval_rows=64,
        num_bins=BINS,
        burst_config=BurstDetectionConfig(threshold_high_db=20.0),
        center_freq_hz=915e6,
        freq_axis=np.linspace(-5e5, 5e5, BINS),
        time_resolution_s=1e-4,
    )


def _run(det, grids):
    out = []
    for g, start in grids:
        out += det.feed(g, chunk_start=start, slice_samples=SLICE)
    for k in range(8):  # flush
        out += det.feed(_grid(64), chunk_start=10**7 + k * 64 * SLICE, slice_samples=SLICE)
    return out


def test_burst_samples_match_the_rows_they_span():
    det = _det()
    bursts = _run(det, [(_grid(64), 0), (_grid(64, range(10, 30)), 64 * SLICE), (_grid(64), 128 * SLICE)])
    (b,) = [b for b in bursts if b.peak_power_db > -60]
    # Rows 10..29 of the grid starting at stream sample 64*SLICE.
    assert b.start_sample == 64 * SLICE + 10 * SLICE
    assert b.stop_sample == 64 * SLICE + 30 * SLICE


def test_burst_samples_survive_a_dropped_chunk():
    det = _det()
    # The chunk at 64*SLICE was dropped: the next grid starts at 128*SLICE, not 64.
    bursts = _run(det, [(_grid(64), 0), (_grid(64, range(5, 15)), 128 * SLICE), (_grid(64), 192 * SLICE)])
    (b,) = [b for b in bursts if b.peak_power_db > -60]
    assert b.start_sample == 128 * SLICE + 5 * SLICE
    assert b.stop_sample == 128 * SLICE + 15 * SLICE


def test_without_positions_the_fields_are_none():
    det = _det()
    out = []
    for g in [_grid(64), _grid(64, range(10, 30)), _grid(64)] + [_grid(64)] * 8:
        out += det.feed(g)
    (b,) = [b for b in out if b.peak_power_db > -60]
    assert b.start_sample is None and b.stop_sample is None


def test_reset_forgets_positions():
    det = _det()
    det.feed(_grid(64), chunk_start=0, slice_samples=SLICE)
    det.reset()
    assert (det._row_pos == -1).all()
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_rolling_positions.py -q`
Expected: FAIL (`feed() got an unexpected keyword argument 'chunk_start'`).

- [ ] **Step 3: Implement**

`models.py`, in `BurstFingerprint` after `detection_timestamp`:

```python
    # Absolute stream sample range [start_sample, stop_sample) in the streaming
    # pipeline's IQ ring, so the burst's samples can be read back for
    # isolation. None where unknown (sweep pipeline, legacy paths).
    start_sample: int | None = None
    stop_sample: int | None = None
```

`rolling_burst.py`:
- In `__init__`, after `self._total_rows_written = 0`:

```python
        # Stream sample position of each absolute row (abs % len), -1 when
        # unknown. Twice the window, so a burst emitted as it scrolls out of the
        # window still has its first row's position. Recorded per row, never
        # derived from the row count, so a dropped chunk cannot shift it.
        self._row_pos = np.full(2 * window_rows, -1, dtype=np.int64)
        self._slice_samples: int | None = None
```

- `feed` signature and, before `self._total_rows_written += n_new`:

```python
    def feed(
        self,
        psd_grid: PSDGridResult,
        chunk_start: int | None = None,
        slice_samples: int | None = None,
    ) -> list[BurstFingerprint]:
        """Append rows from *psd_grid* and return any completed bursts.

        ``chunk_start`` is the stream sample of the grid's first row and
        ``slice_samples`` the samples per row; with both, completed bursts carry
        ``start_sample`` / ``stop_sample``.
        """
```

```python
        L = self._row_pos.shape[0]
        idx = (self._total_rows_written + np.arange(n_new)) % L
        if chunk_start is not None and slice_samples:
            self._row_pos[idx] = chunk_start + np.arange(n_new, dtype=np.int64) * slice_samples
            self._slice_samples = int(slice_samples)
        else:
            self._row_pos[idx] = -1
```

- In `_to_fingerprint`, before the `return`:

```python
        start_sample = stop_sample = None
        L = self._row_pos.shape[0]
        last_row = t.abs_end - 1
        if (
            self._slice_samples is not None
            and t.abs_start >= self._total_rows_written - L
            and last_row >= t.abs_start
        ):
            p0 = int(self._row_pos[t.abs_start % L])
            p1 = int(self._row_pos[last_row % L])
            if p0 >= 0 and p1 >= p0:
                start_sample, stop_sample = p0, p1 + self._slice_samples
```

and pass `start_sample=start_sample, stop_sample=stop_sample` to `BurstFingerprint(...)`.

- In `reset`: `self._row_pos[:] = -1` and `self._slice_samples = None`.

`streaming.py`:
- `_handle_chunk_result`: `self._burst_queue.put_nowait((cr.psd_grid, cr.center_freq_hz, cr.capture_num, cr.chunk_start))`.
- `_burst_detection_loop`: `psd_grid, freq_hz, capture_num, chunk_start = item` and `completed_bursts = rolling_detector.feed(psd_grid, chunk_start=chunk_start, slice_samples=self._slice_samples)`.

- [ ] **Step 4: Run the tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_rolling_positions.py tests/unit/test_rolling_burst.py tests/unit/test_streaming.py tests/unit/test_burst.py -q`
Expected: all pass. If `_run`'s flush does not emit the burst with these window sizes, raise the flush count; do not weaken the assertions.

- [ ] **Step 5: Full checks and commit**

```bash
git add src/rfobserver/models.py src/rfobserver/processing/rolling_burst.py src/rfobserver/pipeline/streaming.py tests/unit/test_rolling_positions.py
git commit -m "feat(isolation): bursts carry their absolute stream sample range"
```

---

### Task 3: Isolation DSP

**Files:**
- Create: `src/rfobserver/processing/isolate.py`
- Test: `tests/unit/test_isolate.py`

**Interfaces:**
- Consumes: `BurstFingerprint.start_sample/stop_sample` (Task 2); `channelize_to_cs16`, `select_rate_and_protocols` (existing `processing/channelize.py`).
- Produces:
  - `GUARD_SEC = 0.002`
  - `iq_to_complex(arr: np.ndarray) -> np.ndarray` (int32 packed SC16 or complex -> complex64)
  - `@dataclass IsolatedBurst: burst_id: str, cs16: bytes, rate_hz: int, passes: list[list[str]], freq_hz: float, start_sample: int | None, num_source_samples: int, truncated: bool`
  - `isolate_burst(burst, *, read_range: Callable[[int, int], np.ndarray | None] | None, read_all: Callable[[], np.ndarray | None] | None, sample_rate_hz: float, center_freq_hz: float, max_burst_sec: float) -> IsolatedBurst | str` (a `str` is the skip state: `"iq_expired"`)

- [ ] **Step 1: Failing tests**

Create `tests/unit/test_isolate.py`:

```python
"""Isolating a burst: its samples, shifted to DC and decimated to the tier rate."""

from __future__ import annotations

import numpy as np

from rfobserver.capture.buffer import CircularBuffer
from rfobserver.models import BurstFingerprint
from rfobserver.processing.isolate import GUARD_SEC, isolate_burst, iq_to_complex

FS = 8_000_000.0
CENTER = 915e6


def _pack(iq: np.ndarray) -> np.ndarray:
    v = np.empty(iq.size * 2, dtype=np.int16)
    v[0::2] = np.clip(iq.real * 20000, -32767, 32767).astype(np.int16)
    v[1::2] = np.clip(iq.imag * 20000, -32767, 32767).astype(np.int16)
    return v.view(np.int32)


def _ring_with_tone(total: int, burst: tuple[int, int], offset_hz: float) -> CircularBuffer:
    rng = np.random.default_rng(1)
    iq = (rng.normal(0, 0.01, total) + 1j * rng.normal(0, 0.01, total)).astype(np.complex64)
    n = np.arange(burst[0], burst[1])
    iq[burst[0] : burst[1]] += 0.5 * np.exp(2j * np.pi * offset_hz / FS * n)
    ring = CircularBuffer(total, dtype=np.int32)
    ring.write(_pack(iq))
    return ring


def _burst(start, stop, peak_hz, bw=250e3) -> BurstFingerprint:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    return BurstFingerprint(
        start_time=now, stop_time=now, center_freq_hz=peak_hz, peak_freq_hz=peak_hz,
        bandwidth_hz=bw, peak_power_db=-30.0, start_sample=start, stop_sample=stop,
    )


def _decode(cs16: bytes) -> np.ndarray:
    v = np.frombuffer(cs16, dtype="<i2").astype(np.float32)
    return v[0::2] + 1j * v[1::2]


def test_burst_is_centered_and_decimated_to_the_tier_rate():
    offset = 1_200_000.0
    ring = _ring_with_tone(400_000, (100_000, 180_000), offset)  # 10 ms burst
    b = _burst(100_000, 180_000, CENTER + offset)
    iso = isolate_burst(b, read_range=ring.read_range, read_all=None,
                        sample_rate_hz=FS, center_freq_hz=CENTER, max_burst_sec=0.5)
    assert not isinstance(iso, str)
    assert iso.rate_hz == 1_600_000  # 250 kHz burst -> SSN tier
    guard = int(GUARD_SEC * FS)
    assert iso.num_source_samples == 80_000 + 2 * guard
    out = _decode(iso.cs16)
    assert abs(len(out) - iso.num_source_samples * 1_600_000 / FS) <= 2
    spec = np.abs(np.fft.fftshift(np.fft.fft(out)))
    freqs = np.fft.fftshift(np.fft.fftfreq(len(out), 1 / 1_600_000))
    assert abs(freqs[np.argmax(spec)]) < 5_000  # the tone now sits at DC
    assert not iso.truncated


def test_narrow_burst_takes_the_default_tier():
    ring = _ring_with_tone(200_000, (50_000, 90_000), 0.0)
    iso = isolate_burst(_burst(50_000, 90_000, CENTER, bw=50e3), read_range=ring.read_range,
                        read_all=None, sample_rate_hz=FS, center_freq_hz=CENTER, max_burst_sec=0.5)
    assert iso.rate_hz == 1_000_000


def test_long_burst_truncated():
    ring = _ring_with_tone(400_000, (10_000, 390_000), 0.0)
    iso = isolate_burst(_burst(20_000, 380_000, CENTER), read_range=ring.read_range, read_all=None,
                        sample_rate_hz=FS, center_freq_hz=CENTER, max_burst_sec=0.01)
    assert iso.truncated
    assert iso.num_source_samples == int(0.01 * FS) + 2 * int(GUARD_SEC * FS)


def test_expired_burst():
    ring = CircularBuffer(100_000, dtype=np.int32)
    ring.write(np.zeros(300_000, dtype=np.int32))  # holds 200k..300k
    iso = isolate_burst(_burst(50_000, 60_000, CENTER), read_range=ring.read_range, read_all=None,
                        sample_rate_hz=FS, center_freq_hz=CENTER, max_burst_sec=0.5)
    assert iso == "iq_expired"


def test_burst_without_positions_uses_the_whole_capture():
    iq = np.exp(2j * np.pi * 300e3 / FS * np.arange(80_000)).astype(np.complex64)
    iso = isolate_burst(_burst(None, None, CENTER + 300e3), read_range=None, read_all=lambda: iq,
                        sample_rate_hz=FS, center_freq_hz=CENTER, max_burst_sec=0.5)
    assert iso.num_source_samples == 80_000 and iso.start_sample is None


def test_burst_without_positions_and_no_whole_capture_is_expired():
    iso = isolate_burst(_burst(None, None, CENTER), read_range=lambda a, b: None, read_all=None,
                        sample_rate_hz=FS, center_freq_hz=CENTER, max_burst_sec=0.5)
    assert iso == "iq_expired"


def test_iq_to_complex_unpacks_sc16():
    packed = _pack(np.array([0.5 + 0.25j], dtype=np.complex64))
    out = iq_to_complex(packed)
    assert out.dtype == np.complex64
    assert abs(out[0] - (10000 + 5000j) / 32768) < 1e-3
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_isolate.py -q`
Expected: FAIL (`ModuleNotFoundError: rfobserver.processing.isolate`).

- [ ] **Step 3: Implement `processing/isolate.py`**

```python
"""Isolate one detected burst from wideband IQ: read its samples (plus a small
guard band), shift its peak frequency to DC and decimate to the rtl_433 tier
rate. Pure DSP: no threads, files or subprocesses. The caller runs it on the
isolation worker thread.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np

from rfobserver.models import BurstFingerprint
from rfobserver.processing.channelize import channelize_to_cs16, select_rate_and_protocols

# Samples kept on each side of the burst so the decoder sees its edges.
GUARD_SEC = 0.002


@dataclass
class IsolatedBurst:
    burst_id: str
    cs16: bytes  # interleaved little-endian int16 I/Q at rate_hz, peak-normalized
    rate_hz: int
    passes: list[list[str]]
    freq_hz: float  # absolute frequency now at DC
    start_sample: int | None  # stream sample of cs16's first source sample
    num_source_samples: int  # wideband samples that went in
    truncated: bool  # longer than max_burst_sec; only its head was kept


def iq_to_complex(arr: np.ndarray) -> np.ndarray:
    """int32-packed SC16 (the ring's dtype) or complex -> complex64 in [-1, 1)."""
    if np.iscomplexobj(arr):
        return arr.astype(np.complex64, copy=False)
    v = np.ascontiguousarray(arr).view(np.int16).astype(np.float32) / 32768.0
    return (v[0::2] + 1j * v[1::2]).astype(np.complex64)


def isolate_burst(
    burst: BurstFingerprint,
    *,
    read_range: Callable[[int, int], np.ndarray | None] | None,
    read_all: Callable[[], np.ndarray | None] | None,
    sample_rate_hz: float,
    center_freq_hz: float,
    max_burst_sec: float,
) -> IsolatedBurst | str:
    """Return the isolated burst, or ``"iq_expired"`` if its samples are gone.

    With ``start_sample`` / ``stop_sample`` the burst's own range (plus guard)
    is read via ``read_range``; without them (sweep pipeline) the whole capture
    from ``read_all`` is channelized, as the sweep path always did.
    """
    guard = int(GUARD_SEC * sample_rate_hz)
    truncated = False
    start: int | None = None
    if burst.start_sample is not None and burst.stop_sample is not None:
        start = max(0, burst.start_sample - guard)
        stop = burst.stop_sample
        max_n = int(max_burst_sec * sample_rate_hz)
        if stop - burst.start_sample > max_n:
            stop = burst.start_sample + max_n
            truncated = True
        stop += guard
        data = read_range(start, stop) if read_range is not None else None
    else:
        data = read_all() if read_all is not None else None
    if data is None or len(data) == 0:
        return "iq_expired"
    iq = iq_to_complex(data)
    rate, passes = select_rate_and_protocols(burst.bandwidth_hz)
    offset = float(burst.peak_freq_hz) - float(center_freq_hz)
    cs16 = channelize_to_cs16(iq, float(sample_rate_hz), offset, rate)
    return IsolatedBurst(
        burst_id=burst.burst_id,
        cs16=cs16,
        rate_hz=rate,
        passes=passes,
        freq_hz=float(burst.peak_freq_hz),
        start_sample=start,
        num_source_samples=len(iq),
        truncated=truncated,
    )
```

- [ ] **Step 4: Run the tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_isolate.py tests/unit/test_channelize.py -q`
Expected: all pass.

- [ ] **Step 5: Full checks and commit**

```bash
git add src/rfobserver/processing/isolate.py tests/unit/test_isolate.py
git commit -m "feat(isolation): isolate a burst by its stream range: shift to DC and decimate"
```

---

### Task 4: Burst archive (SigMF) and the module burst hook

**Files:**
- Create: `src/rfobserver/storage/burst_archive.py`
- Modify: `src/rfobserver/modules/base.py` (`UpstreamModule.feed_burst`), `src/rfobserver/modules/manager.py` (`feed_bursts`)
- Test: `tests/unit/test_burst_archive.py`

**Interfaces:**
- Consumes: `IsolatedBurst` (Task 3); `SIGMF_VERSION` (`storage/sigmf_export.py`).
- Produces:
  - `BurstArchive(storage_path: str | Path)` with `.root` (`<storage>/bursts`), `save(iso: IsolatedBurst, meta: dict[str, Any], subdir: str | None = None) -> Path` (the `.sigmf-data` path), `usage_bytes() -> int`, `enforce_cap(max_bytes: int) -> int`, `evict_until_free(target_free_bytes: int, free_bytes: Callable[[], int] | None = None) -> int`
  - `UpstreamModule.feed_burst(self, iq: np.ndarray, sample_rate: int, meta: dict[str, Any]) -> None` (no-op default)
  - `ModuleManager.feed_bursts(iq: np.ndarray, sample_rate: int, meta: dict[str, Any]) -> None`

- [ ] **Step 1: Failing tests**

Create `tests/unit/test_burst_archive.py`:

```python
"""Isolated bursts saved as SigMF, capped and evicted oldest first."""

from __future__ import annotations

import json
import os

import numpy as np

from rfobserver.modules.base import UpstreamModule
from rfobserver.modules.manager import ModuleManager
from rfobserver.processing.isolate import IsolatedBurst
from rfobserver.storage.burst_archive import BurstArchive


def _iso(bid: str, n: int = 100) -> IsolatedBurst:
    cs16 = np.arange(2 * n, dtype="<i2").tobytes()
    return IsolatedBurst(bid, cs16, 1_600_000, [["-R", "383"]], 919.4e6, 1234, 5000, False)


def test_save_writes_a_loadable_sigmf_pair(tmp_path):
    a = BurstArchive(tmp_path)
    data = a.save(_iso("b1"), {"rfobs:snr_db": 40.0, "core:datetime": "2026-09-28T00:00:00Z"})
    assert data.name == "b1.sigmf-data" and data.parent.parent == a.root
    meta = json.loads(data.with_suffix(".sigmf-meta").read_text())
    g = meta["global"]
    assert g["core:datatype"] == "ci16_le" and g["core:sample_rate"] == 1_600_000
    assert g["rfobs:burst_id"] == "b1" and g["rfobs:snr_db"] == 40.0
    assert meta["captures"][0]["core:frequency"] == 919.4e6
    assert data.read_bytes() == _iso("b1").cs16
    import sigmf  # the official library, as used for the capture export tests

    rec = sigmf.sigmffile.fromfile(str(data.with_suffix(".sigmf-meta")))
    assert rec.get_global_field("core:sample_rate") == 1_600_000


def test_replay_bursts_go_to_their_own_subdir(tmp_path):
    a = BurstArchive(tmp_path)
    p = a.save(_iso("b2"), {}, subdir="replay-feb4")
    assert p.parent == a.root / "replay-feb4"


def test_cap_deletes_oldest_pairs_first(tmp_path):
    a = BurstArchive(tmp_path)
    paths = []
    for i in range(4):
        p = a.save(_iso(f"b{i}", n=1000), {})
        os.utime(p, (1000 + i, 1000 + i))
        os.utime(p.with_suffix(".sigmf-meta"), (1000 + i, 1000 + i))
        paths.append(p)
    one = paths[0].stat().st_size + paths[0].with_suffix(".sigmf-meta").stat().st_size
    freed = a.enforce_cap(2 * one + 10)
    assert freed >= 2 * one - 10
    assert not paths[0].exists() and not paths[1].exists()
    assert not paths[0].with_suffix(".sigmf-meta").exists()
    assert paths[2].exists() and paths[3].exists()


def test_evict_until_free_stops_at_the_target(tmp_path):
    a = BurstArchive(tmp_path)
    for i in range(3):
        p = a.save(_iso(f"b{i}", n=1000), {})
        os.utime(p, (1000 + i, 1000 + i))
    state = {"free": 0}

    def free():
        return state["free"]

    orig = a._delete_pair

    def counting(p):
        n = orig(p)
        state["free"] += n
        return n

    a._delete_pair = counting
    a.evict_until_free(1, free_bytes=free)
    assert len(list(a.root.rglob("*.sigmf-data"))) == 2


def test_usage_counts_both_files(tmp_path):
    a = BurstArchive(tmp_path)
    p = a.save(_iso("b1"), {})
    assert a.usage_bytes() == p.stat().st_size + p.with_suffix(".sigmf-meta").stat().st_size


class _Rec(UpstreamModule):
    kind = "rec"

    def __init__(self):
        super().__init__({})
        self.got = []

    @classmethod
    def parameters(cls):
        return []

    def configure(self, params):
        pass

    def feed(self, sc16_buf, center_freq_hz, sample_rate):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def status(self):
        return {}

    def feed_burst(self, iq, sample_rate, meta):
        self.got.append((len(iq), sample_rate, meta["burst_id"]))


def test_modules_receive_isolated_bursts_and_default_is_a_noop():
    mm = ModuleManager()
    rec = _Rec()
    mm._modules["r"] = rec
    mm.feed_bursts(np.zeros(10, dtype=np.complex64), 1_600_000, {"burst_id": "b1"})
    assert rec.got == [(10, 1_600_000, "b1")]
    # A module without feed_burst (like fm_demod) is unaffected.
    UpstreamModule.feed_burst(rec, np.zeros(1, dtype=np.complex64), 1, {})
```

Before writing `_Rec`, read `modules/base.py` for the exact abstract method set and constructor, and match it (keep the test's intent: a module that overrides `feed_burst` receives bursts).

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_burst_archive.py -q`
Expected: FAIL (module missing).

- [ ] **Step 3: Implement**

`storage/burst_archive.py`:

```python
"""Isolated bursts on disk: one SigMF pair per burst under <storage>/bursts/.

Live bursts go into a folder per UTC day; replay bursts into
``replay-<capture stem>/`` so they never mix with the sensor's own. Bounded by
BURST_ARCHIVE_MAX_GB (oldest first), and evicted before automatic captures when
the storage governor needs space.
"""

from __future__ import annotations

import json
import logging
import shutil
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rfobserver.storage.sigmf_export import SIGMF_VERSION

if TYPE_CHECKING:
    from rfobserver.processing.isolate import IsolatedBurst

logger = logging.getLogger(__name__)


class BurstArchive:
    def __init__(self, storage_path: str | Path) -> None:
        self.root = Path(storage_path) / "bursts"
        self.root.mkdir(parents=True, exist_ok=True)

    def save(
        self, iso: IsolatedBurst, meta: dict[str, Any], subdir: str | None = None
    ) -> Path:
        folder = self.root / (subdir or datetime.now(timezone.utc).strftime("%Y%m%d"))
        folder.mkdir(parents=True, exist_ok=True)
        data = folder / f"{iso.burst_id}.sigmf-data"
        glob: dict[str, Any] = {
            "core:datatype": "ci16_le",
            "core:sample_rate": iso.rate_hz,
            "core:version": SIGMF_VERSION,
            "core:description": "RFObserver isolated burst (peak-normalized)",
            "rfobs:burst_id": iso.burst_id,
            "rfobs:truncated": iso.truncated,
            "rfobs:source_samples": iso.num_source_samples,
        }
        capture: dict[str, Any] = {"core:sample_start": 0, "core:frequency": iso.freq_hz}
        for k, v in meta.items():
            (capture if k == "core:datetime" else glob)[k] = v
        data.write_bytes(iso.cs16)
        data.with_suffix(".sigmf-meta").write_text(
            json.dumps({"global": glob, "captures": [capture], "annotations": []}, indent=2)
        )
        return data

    def _pairs(self) -> list[Path]:
        def mtime(p: Path) -> float:
            try:
                return p.stat().st_mtime
            except OSError:
                return 0.0

        return sorted(self.root.rglob("*.sigmf-data"), key=mtime)

    @staticmethod
    def _size(p: Path) -> int:
        total = 0
        for f in (p, p.with_suffix(".sigmf-meta")):
            try:
                total += f.stat().st_size
            except OSError:
                pass
        return total

    def _delete_pair(self, p: Path) -> int:
        freed = self._size(p)
        for f in (p, p.with_suffix(".sigmf-meta")):
            try:
                f.unlink(missing_ok=True)
            except OSError:
                logger.warning("Could not delete burst file %s", f)
        return freed

    def usage_bytes(self) -> int:
        return sum(self._size(p) for p in self._pairs())

    def enforce_cap(self, max_bytes: int) -> int:
        pairs = self._pairs()
        usage = sum(self._size(p) for p in pairs)
        freed = 0
        while usage > max_bytes and pairs:
            n = self._delete_pair(pairs.pop(0))
            usage -= n
            freed += n
        return freed

    def evict_until_free(
        self, target_free_bytes: int, free_bytes: Callable[[], int] | None = None
    ) -> int:
        free = free_bytes or (lambda: shutil.disk_usage(self.root).free)
        pairs = self._pairs()
        freed = 0
        while pairs and free() < target_free_bytes:
            freed += self._delete_pair(pairs.pop(0))
        if freed:
            logger.warning("Storage floor: evicted %.1f MB of isolated bursts", freed / 1e6)
        return freed
```

`modules/base.py`, in `UpstreamModule` (not abstract):

```python
    def feed_burst(self, iq: np.ndarray, sample_rate: int, meta: dict[str, Any]) -> None:
        """Receive one isolated burst (complex64 at ``sample_rate``, its peak at
        DC). Called on the isolation worker thread; keep it quick. Optional:
        modules that only use the wideband stream ignore it."""
        return None
```

`modules/manager.py`:

```python
    def feed_bursts(self, iq: np.ndarray, sample_rate: int, meta: dict[str, Any]) -> None:
        """Offer an isolated burst to every module. Non-blocking."""
        for module in list(self._modules.values()):
            try:
                module.feed_burst(iq, sample_rate, meta)
            except Exception:
                logger.exception("Module %s feed_burst error", module.module_id)
```

- [ ] **Step 4: Run the tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_burst_archive.py -q` (and any existing module tests: `grep -l modules tests/unit/*.py`).
Expected: all pass. The sigmf library is installed in the venv (the capture-download tests use it); if `import sigmf` fails, check `.venv` and report rather than dropping the assertion.

- [ ] **Step 5: Full checks and commit**

```bash
git add src/rfobserver/storage/burst_archive.py src/rfobserver/modules/base.py src/rfobserver/modules/manager.py tests/unit/test_burst_archive.py
git commit -m "feat(isolation): SigMF burst archive and an optional module burst hook"
```

---

### Task 5: Attribution sinks and outcomes

**Files:**
- Modify: `src/rfobserver/pipeline/attribution.py`
- Modify: `src/rfobserver/storage/database.py` (`update_detection_attribution`)
- Test: `tests/unit/test_attribution_sinks.py` (new); adjust `tests/unit/test_attribution_queue.py` only if `AttributionItem` construction changes break it (it gains a defaulted field, so it should not)

**Interfaces:**
- Produces:
  - `AttributionItem.meta: dict[str, Any] = field(default_factory=dict)` (freq_hz, start_time_ms, stop_time_ms, freq_low_hz, freq_high_hz, snr_db)
  - `AttributionResult` dataclass: `item: AttributionItem, model: str | None, protocol_id: int | None, attribution: str, outcome: str` (`"decoded"` / `"not_decoded"` / `"failed"`)
  - `Sink = Callable[[AttributionResult], Awaitable[None]]`
  - `db_sink(database, retry_delay_sec: float = 2.0) -> Sink`
  - `ReplayFileSink(path: Path)` with `async __call__(result)` appending one JSON line
  - `AttributionWorker(database, rtl_path, queue=None, sinks: list[Sink] | None = None, on_outcome: Callable[[str], None] | None = None)`; with `sinks=None` it uses `[db_sink(database)]` (old behaviour)
  - `SensorDatabase.update_detection_attribution(...) -> int` (rows updated)

- [ ] **Step 1: Failing tests**

Create `tests/unit/test_attribution_sinks.py`:

```python
"""Attribution results go to sinks: the DB (with one retry) live, a file in replay."""

from __future__ import annotations

import asyncio
import json

import pytest

from rfobserver.pipeline import attribution as attr
from rfobserver.pipeline.attribution import (
    AttributionItem,
    AttributionResult,
    AttributionWorker,
    ReplayFileSink,
    StrongestQueue,
    db_sink,
)


def _item(bid="b1", power=-30.0):
    return AttributionItem(bid, b"\0\0" * 10, 1_600_000, [["-R", "383"]], power,
                           meta={"freq_hz": 919.4e6, "start_time_ms": 1.0, "stop_time_ms": 2.0})


class _DB:
    def __init__(self, rows_first=0):
        self.calls = []
        self._first = rows_first

    async def update_detection_attribution(self, **kw):
        self.calls.append(kw)
        return self._first if len(self.calls) == 1 else 1


async def test_db_sink_retries_once_when_the_row_is_not_there_yet():
    db = _DB(rows_first=0)
    sink = db_sink(db, retry_delay_sec=0.01)
    await sink(AttributionResult(_item(), "SilverSpring-Mesh", 383, "{}", "decoded"))
    assert len(db.calls) == 2 and db.calls[0]["burst_id"] == "b1"


async def test_db_sink_no_retry_when_updated():
    db = _DB(rows_first=1)
    await db_sink(db, retry_delay_sec=0.01)(AttributionResult(_item(), None, None, "{}", "not_decoded"))
    assert len(db.calls) == 1


async def test_replay_file_sink_appends_json_lines(tmp_path):
    p = tmp_path / "bursts" / "replay-x" / "attribution.jsonl"
    sink = ReplayFileSink(p)
    await sink(AttributionResult(_item("b1"), "SilverSpring-Mesh", 383, '{"decoded": true}', "decoded"))
    await sink(AttributionResult(_item("b2"), None, None, "{}", "not_decoded"))
    rows = [json.loads(l) for l in p.read_text().splitlines()]
    assert [r["burst_id"] for r in rows] == ["b1", "b2"]
    assert rows[0]["model"] == "SilverSpring-Mesh" and rows[0]["protocol_id"] == 383
    assert rows[0]["freq_hz"] == 919.4e6 and rows[1]["outcome"] == "not_decoded"


@pytest.mark.parametrize(
    "frames,outcome", [([{"model": "SilverSpring-Mesh"}], "decoded"), ([], "not_decoded")]
)
async def test_worker_sends_results_to_every_sink_and_counts_outcomes(monkeypatch, frames, outcome):
    monkeypatch.setattr(attr, "decode_cs16", lambda *a, **k: frames)
    got, outcomes = [], []

    async def sink(r):
        got.append(r)

    q = StrongestQueue(4)
    w = AttributionWorker(None, "/bin/true", queue=q, sinks=[sink, sink], on_outcome=outcomes.append)
    q.put_nowait(_item())
    task = asyncio.create_task(w.run())
    for _ in range(50):
        await asyncio.sleep(0.01)
        if len(got) == 2:
            break
    task.cancel()
    assert len(got) == 2 and got[0].outcome == outcome and outcomes == [outcome]
    if outcome == "decoded":
        assert got[0].protocol_id == 383


async def test_worker_counts_a_decoder_crash_as_failed(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("x")

    monkeypatch.setattr(attr, "decode_cs16", boom)
    outcomes = []
    q = StrongestQueue(4)
    w = AttributionWorker(None, "/bin/true", queue=q, sinks=[], on_outcome=outcomes.append)
    q.put_nowait(_item())
    task = asyncio.create_task(w.run())
    await asyncio.sleep(0.1)
    task.cancel()
    assert outcomes == ["failed"]


async def test_update_detection_attribution_returns_rowcount(tmp_path):
    from rfobserver.storage.database import SensorDatabase

    db = SensorDatabase(str(tmp_path / "a.db"))
    await db.connect()
    try:
        n = await db.update_detection_attribution(burst_id="nope", model=None, protocol_id=None, attribution="{}")
        assert n == 0
    finally:
        await db.close()
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_attribution_sinks.py -q`
Expected: FAIL (`ImportError: cannot import name 'AttributionResult'`).

- [ ] **Step 3: Implement**

`database.py`: `update_detection_attribution` keeps its signature and `@_guarded_write`, captures the cursor and ends with `return int(cursor.rowcount)`; annotate `-> int`.

`attribution.py`:
- imports: add `from collections.abc import Awaitable, Callable`, `from dataclasses import dataclass, field`, `from pathlib import Path`.
- `AttributionItem` gains `meta: dict[str, Any] = field(default_factory=dict)`.
- Add after `AttributionItem`:

```python
@dataclass
class AttributionResult:
    item: AttributionItem
    model: str | None
    protocol_id: int | None
    attribution: str  # JSON: decoded frames, or the attempted-not-decoded marker
    outcome: str  # "decoded" | "not_decoded" | "failed"


Sink = Callable[[AttributionResult], Awaitable[None]]


def db_sink(database: Any, retry_delay_sec: float = 2.0) -> Sink:
    """Merge a result onto its detections row. The row is normally inserted
    within the same drain as the burst was detected; if it is not there yet,
    retry once after ``retry_delay_sec``."""

    async def sink(r: AttributionResult) -> None:
        kw = {
            "burst_id": r.item.burst_id,
            "model": r.model,
            "protocol_id": r.protocol_id,
            "attribution": r.attribution,
        }
        if await database.update_detection_attribution(**kw) == 0:
            await asyncio.sleep(retry_delay_sec)
            if await database.update_detection_attribution(**kw) == 0:
                logger.debug("attribution: no detection row for burst %s", r.item.burst_id)

    return sink


class ReplayFileSink:
    """Replay results never touch the DB: one JSON line per result."""

    def __init__(self, path: Path) -> None:
        self.path = path

    async def __call__(self, r: AttributionResult) -> None:
        row = {
            "burst_id": r.item.burst_id,
            **r.item.meta,
            "model": r.model,
            "protocol_id": r.protocol_id,
            "outcome": r.outcome,
            "attribution": json.loads(r.attribution) if r.attribution else None,
        }

        def append() -> None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "a") as fh:
                fh.write(json.dumps(row) + "\n")

        await asyncio.to_thread(append)
```

- `AttributionWorker.__init__(self, database, rtl_path, queue=None, sinks=None, on_outcome=None)`: store `self._sinks = sinks if sinks is not None else [db_sink(database)]` and `self._on_outcome = on_outcome`.
- `run()` body per item:

```python
            item = await self.queue.get()
            now_iso = datetime.now(timezone.utc).isoformat()
            try:
                frames = await asyncio.to_thread(
                    decode_cs16, self._rtl, item.cs16, item.target_rate_hz, item.passes
                )
            except Exception:
                logger.exception("rtl_433 decode failed for burst %s", item.burst_id)
                self._count("failed")
                continue
            if frames:
                model = frames[0].get("model")
                result = AttributionResult(
                    item, model, _protocol_id_for(model),
                    json.dumps({"decoded": True, "at": now_iso, "frames": frames}), "decoded",
                )
            else:
                result = AttributionResult(
                    item, None, None,
                    json.dumps({"attempted": True, "decoded": False, "at": now_iso}), "not_decoded",
                )
            self._count(result.outcome)
            for sink in self._sinks:
                try:
                    await sink(result)
                except Exception:
                    logger.exception("attribution sink failed for burst %s", item.burst_id)
```

with

```python
    def _count(self, outcome: str) -> None:
        if self._on_outcome is not None:
            self._on_outcome(outcome)
```

- [ ] **Step 4: Run the tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_attribution_sinks.py tests/unit/test_attribution_queue.py tests/unit/test_attribution_decode.py tests/unit/test_database.py -q`
Expected: all pass (decode tests skip where rtl_433 is absent).

- [ ] **Step 5: Full checks and commit**

```bash
git add src/rfobserver/pipeline/attribution.py src/rfobserver/storage/database.py tests/unit/test_attribution_sinks.py
git commit -m "feat(attribution): result sinks (DB with one retry, replay file) and outcome counts"
```

---

### Task 6: The isolation stage

**Files:**
- Create: `src/rfobserver/pipeline/isolation.py`
- Test: `tests/unit/test_isolation_stage.py`

**Interfaces:**
- Consumes: `isolate_burst`, `iq_to_complex`, `IsolatedBurst` (Task 3); `BurstArchive` (Task 4); `AttributionItem`, `AttributionWorker`, `StrongestQueue`, `ReplayFileSink`, `db_sink`, `find_rtl433` (Task 5 and existing); `CircularBuffer.read_range` (Task 1).
- Produces:
  - `STATES = ("isolated", "iq_expired", "too_long", "queue_full", "error")`
  - `@dataclass BurstCandidate: burst: BurstFingerprint, snr_db: float`
  - `class RingSource(ring: CircularBuffer)`: `read_range`, `read_all() -> None`
  - `class WholeCaptureSource(iq_bytes: bytes)`: `read_range(a, b) -> None`, `read_all() -> np.ndarray` (converts lazily, on the stage thread)
  - `@dataclass IsolationBatch: candidates: list[BurstCandidate], center_freq_hz: float, sample_rate_hz: float, source: RingSource | WholeCaptureSource`
  - `class IsolationStats`: `count(name: str, n: int = 1)`, `snapshot() -> dict[str, int]`
  - `class IsolationStage(settings, *, archive: BurstArchive | None, module_feed: Callable[[np.ndarray, int, dict], None] | None, attribution_handoff: Callable[[AttributionItem], None] | None, refuse_saving: Callable[[], bool] = lambda: False, archive_subdir: str | None = None, clock: Callable[[], float] = time.monotonic)`: `submit(batch) -> bool`, `process_batch(batch) -> list[tuple[str, str]]` (burst_id, state), `start()`, `stop()`, `stats`
  - `build_isolation(settings, *, database, storage_path, loop, module_feed, refuse_saving, replay_source: str | None, on_label) -> tuple[IsolationStage | None, AttributionWorker | None, str | None]` (the third is the rtl_433 status: path, or a reason it is unavailable)

- [ ] **Step 1: Failing tests**

Create `tests/unit/test_isolation_stage.py`:

```python
"""The isolation stage: gate, states, fan-out, and the attribution switch."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

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
    WholeCaptureSource,
    build_isolation,
)
from rfobserver.storage.burst_archive import BurstArchive

FS = 2_000_000.0


def _settings(**kw):
    base = dict(ISOLATION_ENABLED=True, ISOLATION_SNR_DB=13.0, ISOLATION_MAX_PER_SEC=3,
                ISOLATION_MAX_BURST_SEC=0.5, ISOLATION_QUEUE_MAX=2, _env_file=None)
    base.update(kw)
    return AppSettings(**base)


def _ring(n=400_000):
    r = CircularBuffer(n, dtype=np.int32)
    r.write(np.random.default_rng(0).integers(-2000, 2000, n, dtype=np.int32))
    return r


def _cand(i, snr, start=10_000, stop=20_000):
    now = datetime.now(timezone.utc)
    b = BurstFingerprint(burst_id=f"b{i}", start_time=now, stop_time=now, center_freq_hz=915e6,
                         peak_freq_hz=915.1e6, bandwidth_hz=250e3, peak_power_db=-40 + snr,
                         start_sample=start, stop_sample=stop)
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
    st = IsolationStage(_settings(**kw), archive=archive,
                        module_feed=lambda iq, rate, meta: fed.append(meta["burst_id"]),
                        attribution_handoff=lambda item: handed.append(item.burst_id),
                        clock=clock or _Clock())
    return st, saved, fed, handed


def test_gate_takes_strongest_first_and_respects_the_per_second_limit(tmp_path):
    st, saved, fed, handed = _stage(tmp_path)
    cands = [_cand(i, snr) for i, snr in enumerate([20, 5, 40, 30, 25])]
    out = st.process_batch(IsolationBatch(cands, 915e6, FS, RingSource(_ring())))
    assert [bid for bid, _ in out] == ["b2", "b3", "b4"]  # 40, 30, 25 dB; b0 over the limit, b1 below SNR
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
    out = dict(st.process_batch(IsolationBatch([_cand(0, 40), _cand(1, 30)], 915e6, FS, RingSource(_ring()))))
    assert out == {"b0": "error", "b1": "isolated"}


def test_full_queue_counts_queue_full(tmp_path):
    st, *_ = _stage(tmp_path)  # queue max 2 batches, not started: nothing drains
    b = lambda i: IsolationBatch([_cand(i, 30)], 915e6, FS, RingSource(_ring(1000)))
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
        s, database=None, storage_path=str(tmp_path), loop=asyncio.get_running_loop(),
        module_feed=None, refuse_saving=lambda: False, replay_source=None, on_label=None)
    assert stage is not None and worker is None
    assert "not found" in rtl


async def test_nothing_is_built_when_both_switches_are_off(tmp_path):
    s = _settings(ISOLATION_ENABLED=False, ATTRIBUTION_ENABLED=False)
    assert build_isolation(s, database=None, storage_path=str(tmp_path),
                           loop=asyncio.get_running_loop(), module_feed=None,
                           refuse_saving=lambda: False, replay_source=None,
                           on_label=None) == (None, None, None)


async def test_replay_routes_results_to_a_file_not_the_db(tmp_path, monkeypatch):
    monkeypatch.setattr("rfobserver.pipeline.isolation.find_rtl433", lambda override=None: "/bin/true")
    s = _settings(ATTRIBUTION_ENABLED=True)
    stage, worker, rtl = build_isolation(
        s, database=object(), storage_path=str(tmp_path), loop=asyncio.get_running_loop(),
        module_feed=None, refuse_saving=lambda: False, replay_source="feb4_19-39-48.dat",
        on_label=None)
    from rfobserver.pipeline.attribution import ReplayFileSink

    assert stage._archive_subdir == "replay-feb4_19-39-48"
    assert any(isinstance(k, ReplayFileSink) for k in worker._sinks)
    assert all(not getattr(k, "__name__", "") == "sink" for k in worker._sinks)  # no db_sink
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_isolation_stage.py -q`
Expected: FAIL (module missing).

- [ ] **Step 3: Implement `pipeline/isolation.py`**

```python
"""The burst isolation stage.

Completed bursts arrive in batches (one per detector evaluation) from the
burst thread. On its own worker thread the stage applies the gate (SNR over
the noise floor at the burst's peak bin, strongest first, at most
ISOLATION_MAX_PER_SEC), isolates each picked burst (processing/isolate.py) and
fans it out: SigMF archive, add-on modules, and (when on) the rtl_433
attribution worker. Every picked burst ends in exactly one state.
Design: docs/superpowers/specs/2026-09-28-burst-isolation-streaming-design.md
"""

from __future__ import annotations

import asyncio
import logging
import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np

from rfobserver.pipeline.attribution import (
    AttributionItem,
    AttributionResult,
    AttributionWorker,
    ReplayFileSink,
    StrongestQueue,
    db_sink,
    find_rtl433,
)
from rfobserver.processing.isolate import isolate_burst, iq_to_complex
from rfobserver.storage.burst_archive import BurstArchive

if TYPE_CHECKING:
    from rfobserver.capture.buffer import CircularBuffer
    from rfobserver.config import AppSettings
    from rfobserver.models import BurstFingerprint

logger = logging.getLogger(__name__)

STATES = ("isolated", "iq_expired", "too_long", "queue_full", "error")
_STOP = object()


@dataclass
class BurstCandidate:
    burst: BurstFingerprint
    snr_db: float


class RingSource:
    def __init__(self, ring: CircularBuffer) -> None:
        self._ring = ring

    def read_range(self, start: int, end: int) -> np.ndarray | None:
        return self._ring.read_range(start, end)

    def read_all(self) -> np.ndarray | None:
        return None


class WholeCaptureSource:
    """The sweep pipeline's per-capture IQ; converted on the stage thread."""

    def __init__(self, iq_bytes: bytes) -> None:
        self._bytes = iq_bytes
        self._iq: np.ndarray | None = None

    def read_range(self, start: int, end: int) -> np.ndarray | None:
        return None

    def read_all(self) -> np.ndarray | None:
        if self._iq is None:
            self._iq = iq_to_complex(np.frombuffer(self._bytes, dtype=np.int32))
        return self._iq


@dataclass
class IsolationBatch:
    candidates: list[BurstCandidate]
    center_freq_hz: float
    sample_rate_hz: float
    source: RingSource | WholeCaptureSource


class IsolationStats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[str, int] = {}

    def count(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._counts[name] = self._counts.get(name, 0) + n

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)


class IsolationStage:
    def __init__(
        self,
        settings: AppSettings,
        *,
        archive: BurstArchive | None,
        module_feed: Callable[[np.ndarray, int, dict[str, Any]], None] | None,
        attribution_handoff: Callable[[AttributionItem], None] | None,
        refuse_saving: Callable[[], bool] = lambda: False,
        archive_subdir: str | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._s = settings
        self._archive = archive
        self._module_feed = module_feed
        self._handoff = attribution_handoff
        self._refuse_saving = refuse_saving
        self._archive_subdir = archive_subdir
        self._clock = clock
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=max(1, settings.ISOLATION_QUEUE_MAX))
        self._thread: threading.Thread | None = None
        self._window_start = -1e18
        self._window_count = 0
        self.stats = IsolationStats()

    # -- producer side (burst thread / sweep loop): never blocks --

    def submit(self, batch: IsolationBatch) -> bool:
        try:
            self._queue.put_nowait(batch)
            return True
        except queue.Full:
            self.stats.count("queue_full", len(batch.candidates))
            return False

    # -- worker --

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="isolation", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._thread is None:
            return
        try:
            self._queue.put(_STOP, timeout=1.0)
        except queue.Full:
            pass
        self._thread.join(timeout=5.0)
        self._thread = None

    def _loop(self) -> None:
        while True:
            batch = self._queue.get()
            if batch is _STOP:
                return
            try:
                self.process_batch(batch)
            except Exception:
                logger.exception("Isolation batch failed")

    def _gate(self, cands: list[BurstCandidate]) -> list[BurstCandidate]:
        s = self._s
        passed = sorted(
            (c for c in cands if c.snr_db >= s.ISOLATION_SNR_DB),
            key=lambda c: c.snr_db,
            reverse=True,
        )
        now = self._clock()
        if now - self._window_start >= 1.0:
            self._window_start, self._window_count = now, 0
        room = max(0, int(s.ISOLATION_MAX_PER_SEC) - self._window_count)
        picked = passed[:room]
        self._window_count += len(picked)
        gated_out = len(cands) - len(picked)
        if gated_out:
            self.stats.count("gated_out", gated_out)
        return picked

    def process_batch(self, batch: IsolationBatch) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        for cand in self._gate(batch.candidates):
            state = self._one(cand, batch)
            self.stats.count(state)
            out.append((cand.burst.burst_id, state))
        return out

    def _one(self, cand: BurstCandidate, batch: IsolationBatch) -> str:
        b = cand.burst
        try:
            iso = isolate_burst(
                b,
                read_range=batch.source.read_range,
                read_all=batch.source.read_all,
                sample_rate_hz=batch.sample_rate_hz,
                center_freq_hz=batch.center_freq_hz,
                max_burst_sec=float(self._s.ISOLATION_MAX_BURST_SEC),
            )
        except Exception:
            logger.exception("Isolation failed for burst %s", b.burst_id)
            return "error"
        if isinstance(iso, str):
            return iso
        meta: dict[str, Any] = {
            "burst_id": b.burst_id,
            "freq_hz": iso.freq_hz,
            "freq_low_hz": b.center_freq_hz - b.bandwidth_hz / 2,
            "freq_high_hz": b.center_freq_hz + b.bandwidth_hz / 2,
            "start_time_ms": b.start_time.timestamp() * 1000.0,
            "stop_time_ms": b.stop_time.timestamp() * 1000.0,
            "snr_db": round(cand.snr_db, 1),
        }
        try:
            if self._archive is not None and not self._refuse_saving():
                self._archive.save(
                    iso,
                    {
                        "rfobs:snr_db": meta["snr_db"],
                        "rfobs:peak_power_db": b.peak_power_db,
                        "rfobs:bandwidth_hz": b.bandwidth_hz,
                        "core:datetime": b.start_time.isoformat().replace("+00:00", "Z"),
                    },
                    subdir=self._archive_subdir,
                )
            if self._module_feed is not None:
                v = np.frombuffer(iso.cs16, dtype="<i2").astype(np.float32) / 32768.0
                self._module_feed((v[0::2] + 1j * v[1::2]).astype(np.complex64), iso.rate_hz, meta)
            if self._handoff is not None:
                self._handoff(
                    AttributionItem(
                        burst_id=b.burst_id,
                        cs16=iso.cs16,
                        target_rate_hz=iso.rate_hz,
                        passes=iso.passes,
                        power_db=b.peak_power_db,
                        meta=meta,
                    )
                )
        except Exception:
            logger.exception("Isolation fan-out failed for burst %s", b.burst_id)
            return "error"
        return "too_long" if iso.truncated else "isolated"


def build_isolation(
    settings: AppSettings,
    *,
    database: Any,
    storage_path: str,
    loop: asyncio.AbstractEventLoop,
    module_feed: Callable[[np.ndarray, int, dict[str, Any]], None] | None,
    refuse_saving: Callable[[], bool],
    replay_source: str | None,
    on_label: Callable[[AttributionResult], None] | None,
) -> tuple[IsolationStage | None, AttributionWorker | None, str | None]:
    """Build the stage (and the rtl_433 worker when attribution is on).

    Attribution forces isolation on. In replay, burst files go to
    ``bursts/replay-<stem>/`` and results to its ``attribution.jsonl``, never
    the DB. Returns (stage, worker, rtl_status); all None when both are off.
    """
    if not (settings.ISOLATION_ENABLED or settings.ATTRIBUTION_ENABLED):
        return None, None, None
    archive = BurstArchive(storage_path)
    subdir = f"replay-{Path(replay_source).stem}" if replay_source else None
    worker: AttributionWorker | None = None
    rtl_status: str | None = None
    handoff: Callable[[AttributionItem], None] | None = None
    if settings.ATTRIBUTION_ENABLED:
        rtl = find_rtl433(settings.ATTRIBUTION_RTL433_PATH or None)
        if rtl is None:
            rtl_status = "rtl_433 not found; attribution unavailable"
            logger.warning("ATTRIBUTION_ENABLED but %s", rtl_status)
        else:
            rtl_status = rtl
            sinks: list[Any] = []
            if subdir is not None:
                sinks.append(ReplayFileSink(archive.root / subdir / "attribution.jsonl"))
            else:
                sinks.append(db_sink(database))
            if on_label is not None:
                label = on_label

                async def label_sink(r: AttributionResult) -> None:
                    label(r)

                sinks.append(label_sink)
            q = StrongestQueue(maxsize=max(1, settings.ISOLATION_QUEUE_MAX))
            stats_holder: dict[str, IsolationStage] = {}
            worker = AttributionWorker(
                database,
                rtl,
                queue=q,
                sinks=sinks,
                on_outcome=lambda o: stats_holder["stage"].stats.count(f"attr_{o}"),
            )

            def handoff(item: AttributionItem) -> None:
                # StrongestQueue is asyncio-only: hand over on the loop thread.
                loop.call_soon_threadsafe(q.put_nowait, item)

    stage = IsolationStage(
        settings,
        archive=archive,
        module_feed=module_feed,
        attribution_handoff=handoff,
        refuse_saving=refuse_saving,
        archive_subdir=subdir,
    )
    if worker is not None:
        stats_holder["stage"] = stage
    return stage, worker, rtl_status
```

(`handoff` is rebound inside the `else` branch by the nested `def`; mypy may need `handoff = _handoff` with the function named `_handoff`. Keep the behaviour.)

- [ ] **Step 4: Run the tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_isolation_stage.py -q`
Expected: all pass. The last test's "no db_sink" assertion checks by the inner function name; if you name the DB closure differently, assert on sink types instead, keeping the intent (no DB sink in replay).

- [ ] **Step 5: Full checks and commit**

```bash
git add src/rfobserver/pipeline/isolation.py tests/unit/test_isolation_stage.py
git commit -m "feat(isolation): the isolation stage: gate, states, archive/module/attribution fan-out"
```

---

### Task 7: Wire the stage into both pipelines; end-to-end test

**Files:**
- Modify: `src/rfobserver/pipeline/streaming.py` (`__init__`, `run()` start/stop, `_burst_detection_loop`, `_result_consumer_loop` broadcast payload, new `isolation_status()`)
- Modify: `src/rfobserver/pipeline/continuous.py` (replace `select_bursts_for_attribution`, `_build_attribution_items` and the worker setup with the stage)
- Modify: `src/rfobserver/pipeline/app.py` (`build_processor` passes `replay_source` for replays)
- Delete: `tests/unit/test_attribution_producer.py` (its gate is now tested by `test_isolation_stage.py`)
- Test: `tests/unit/test_isolation_wiring.py` (new), `tests/integration/test_isolation_attribution_e2e.py` (new)

**Interfaces:**
- Consumes: `build_isolation`, `IsolationBatch`, `BurstCandidate`, `RingSource`, `WholeCaptureSource` (Task 6); `RollingBurstDetector.last_detection.noise_floor_per_bin` (existing).
- Produces:
  - `StreamingProcessor(..., replay_source: str | None = None)` and `isolation_status() -> dict[str, Any]` returning `{"enabled": bool, "attribution": bool, "rtl433": str | None, "ring_sec": float, "disabled_reason": str | None, "counts": dict[str, int]}`
  - `ContinuousProcessor.isolation_status()` with the same shape (`ring_sec` 0.0)
  - psd broadcast payload key `"attributions"`: list of `{"id", "freq_low_hz", "freq_high_hz", "start_time_ms", "stop_time_ms", "model", "protocol_id"}` (newest 50)
  - `peak_bin_snr(burst, noise_per_bin, freq_axis, center_freq_hz, fallback_noise_db) -> float` in `streaming.py`

- [ ] **Step 1: Failing tests**

Create `tests/unit/test_isolation_wiring.py`:

```python
"""The streaming pipeline hands completed bursts to the isolation stage."""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np

from rfobserver.models import BurstFingerprint
from rfobserver.pipeline.streaming import peak_bin_snr
from tests.unit.test_recording_gaps import _proc


def _b(peak):
    now = datetime.now(timezone.utc)
    return BurstFingerprint(start_time=now, stop_time=now, center_freq_hz=peak, peak_freq_hz=peak,
                            bandwidth_hz=1e5, peak_power_db=-40.0)


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
```

Create `tests/integration/test_isolation_attribution_e2e.py`:

```python
"""End to end: real SSN bursts inside a wideband stream, through the streaming
pipeline in replay mode, isolated and decoded by rtl_433 as SilverSpring-Mesh.

The three fixtures are narrowband cs16 captures (1.6 Msps) that rtl_433 decodes
as protocol 383. They are upsampled to 26 Msps and placed at offsets inside
noise, written as a SigMF capture, and replayed with isolation and attribution
on. Skips where rtl_433 or the fixtures are absent (CI).
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
from scipy import signal as sig

from rfobserver.pipeline.attribution import find_rtl433
from rfobserver.pipeline.replay import run_replay

FIX = Path.home() / "ssn_bursts"
FIXTURES = [
    ("burst_feb4_919MHz_75dB.cs16", 919.4e6),
    ("burst_feb5_917MHz_56dB.cs16", 917.9e6),
    ("burst_feb5_913MHz_47dB.cs16", 913.4e6),
]
FS_IN = 1_600_000
FS = 26_000_000
CENTER = 915e6

pytestmark = pytest.mark.skipif(
    find_rtl433() is None or not all((FIX / f).exists() for f, _ in FIXTURES),
    reason="rtl_433 or the SSN fixtures are not on this host",
)


def _load(name: str) -> np.ndarray:
    v = np.fromfile(FIX / name, dtype="<i2").astype(np.float32) / 32768.0
    return (v[0::2] + 1j * v[1::2]).astype(np.complex64)


def _write_ci16_sigmf(base: Path, iq: np.ndarray) -> None:
    meta = {
        "global": {"core:datatype": "ci16_le", "core:sample_rate": FS, "core:version": "1.0.0"},
        "captures": [{"core:sample_start": 0, "core:frequency": CENTER}],
        "annotations": [],
    }
    base.with_suffix(".sigmf-meta").write_text(json.dumps(meta))
    out = np.empty(iq.size * 2, dtype="<i2")
    peak = float(np.max(np.abs(iq))) or 1.0
    out[0::2] = (iq.real / peak * 20000).astype("<i2")
    out[1::2] = (iq.imag / peak * 20000).astype("<i2")
    out.tofile(base.with_suffix(".sigmf-data"))


@pytest.mark.asyncio
async def test_ssn_bursts_decode_through_the_streaming_pipeline(tmp_path):
    rng = np.random.default_rng(3)
    gap = int(0.3 * FS)
    parts = [np.zeros(gap, dtype=np.complex64)]
    for name, freq in FIXTURES:
        b = sig.resample_poly(_load(name), FS // 200_000, FS_IN // 200_000).astype(np.complex64)
        n = np.arange(b.size)
        parts.append(b * np.exp(2j * np.pi * (freq - CENTER) / FS * n).astype(np.complex64))
        parts.append(np.zeros(gap, dtype=np.complex64))
    iq = np.concatenate(parts)
    iq += (rng.normal(0, 1e-3, iq.size) + 1j * rng.normal(0, 1e-3, iq.size)).astype(np.complex64)
    base = tmp_path / "ssn_wide"
    _write_ci16_sigmf(base, iq)

    result = await run_replay(
        base.with_suffix(".sigmf-data"),
        threshold_db=30.0,
        overrides={"ATTRIBUTION_ENABLED": True, "ISOLATION_LOOKBACK_SEC": 3.0},
        attribution_wait_sec=60.0,
    )
    models = {round(d["center_freq_hz"] / 1e5): d.get("model") for d in result["detections"]}
    decoded = [d for d in result["detections"] if d.get("protocol_id") == 383]
    assert len(decoded) >= 2, f"expected SSN decodes, got models {models}"
```

This test needs two small additions to `run_replay` (`pipeline/replay.py`), made in Step 3: an `overrides: dict[str, Any] | None = None` applied to the replay `AppSettings`, and `attribution_wait_sec: float = 0.0`, which after the drive-to-end waits (polling every 0.5 s) until the processor's isolation status shows no queued attribution work or the time runs out, before reading detections. `run_replay` runs the processor in non-replay mode against its own temporary DB (it is the offline analysis harness, not the UI replay), so attribution lands on DB rows there, which is what this test reads.

- [ ] **Step 2: Run to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_isolation_wiring.py tests/integration/test_isolation_attribution_e2e.py -q`
Expected: FAIL (`ImportError: cannot import name 'peak_bin_snr'`; the e2e fails on `overrides`, or skips where rtl_433 is absent).

- [ ] **Step 3: Implement**

`streaming.py`:
- Module-level helper:

```python
def peak_bin_snr(
    burst: BurstFingerprint,
    noise_per_bin: Any,
    freq_axis: np.ndarray[Any, np.dtype[Any]],
    center_freq_hz: float,
    fallback_noise_db: float,
) -> float:
    """dB of the burst's peak over the noise floor at its peak bin (the
    detector's per-bin floor; the scalar floor when that is unavailable)."""
    if noise_per_bin is None or len(freq_axis) == 0:
        return float(burst.peak_power_db - fallback_noise_db)
    idx = int(np.argmin(np.abs(np.asarray(freq_axis) - (burst.peak_freq_hz - center_freq_hz))))
    return float(burst.peak_power_db - float(np.asarray(noise_per_bin)[idx]))
```

- `__init__`: new keyword `replay_source: str | None = None` (stored). Add `self._isolation = None`, `self._attrib_worker = None`, `self._attrib_task = None`, `self._rtl_status = None`, `self._labels: collections.deque[dict[str, Any]] = collections.deque(maxlen=50)`.
- `run()`: after `self._loop` is set and before threads start:

```python
        if self._isolation_wanted() and self._isolation_disabled_reason is None:
            from rfobserver.pipeline.isolation import build_isolation

            mm = self._module_manager
            self._isolation, self._attrib_worker, self._rtl_status = build_isolation(
                self._settings,
                database=self._db,
                storage_path=str(self._storage.storage_path),
                loop=self._loop,
                module_feed=(lambda iq, rate, meta: mm.feed_bursts(iq, rate, meta))
                if mm is not None
                else None,
                refuse_saving=lambda: self._governor is not None
                and self._governor.state.refuse_recording,
                replay_source=self._replay_source if self._replay_mode else None,
                on_label=self._add_label,
            )
            if self._isolation is not None:
                self._isolation.start()
            if self._attrib_worker is not None:
                self._attrib_task = asyncio.create_task(self._attrib_worker.run())
```

  and in the `finally` after the burst thread join: `if self._isolation is not None: self._isolation.stop()`; `if self._attrib_worker is not None: self._attrib_worker.stop()`; cancel and await `self._attrib_task` with `contextlib.suppress(asyncio.CancelledError)`.
- `_add_label(self, r)`: when `r.outcome == "decoded"`, append `{"id": r.item.burst_id, "freq_low_hz": r.item.meta.get("freq_low_hz"), "freq_high_hz": r.item.meta.get("freq_high_hz"), "start_time_ms": r.item.meta.get("start_time_ms"), "stop_time_ms": r.item.meta.get("stop_time_ms"), "model": r.model, "protocol_id": r.protocol_id}` to `self._labels`.
- `_burst_detection_loop`, right after `completed_bursts = rolling_detector.feed(...)`:

```python
                if completed_bursts and self._isolation is not None:
                    from rfobserver.pipeline.isolation import (
                        BurstCandidate,
                        IsolationBatch,
                        RingSource,
                    )

                    det = rolling_detector.last_detection
                    noise = det.noise_floor_per_bin if det is not None else None
                    fallback = det.noise_floor_db if det is not None else -200.0
                    center = float(rolling_detector._center_freq_hz)
                    self._isolation.submit(
                        IsolationBatch(
                            [
                                BurstCandidate(
                                    b,
                                    peak_bin_snr(b, noise, psd_grid.freq_axis, center, fallback),
                                )
                                for b in completed_bursts
                            ],
                            center,
                            float(s.BANDWIDTH),
                            RingSource(self._pre_trigger_buf),
                        )
                    )
```

- Broadcast payload (`"type": "psd"` dict): add `"attributions": list(self._labels),`.
- Counter log: in the existing once-per-second stats block of `_result_consumer_loop`, every 60 s, if `self._isolation` is not None and its snapshot differs from the last logged one, `logger.info("Isolation: %s", snapshot)`.
- `isolation_status()`:

```python
    def isolation_status(self) -> dict[str, Any]:
        on = self._isolation is not None
        return {
            "enabled": on,
            "attribution": self._attrib_worker is not None,
            "rtl433": self._rtl_status,
            "ring_sec": self._ring_sec,
            "disabled_reason": self._isolation_disabled_reason,
            "counts": self._isolation.stats.snapshot() if on else {},
        }
```

`app.py` `build_processor`: pass `replay_source=getattr(receiver, "source_name", None) if replay_mode else None` to `StreamingProcessor`.

`continuous.py`:
- Delete `select_bursts_for_attribution` and `_build_attribution_items` and the `__init__` attribution block; add `self._isolation = None`, `self._attrib_worker = None`, `self._attrib_task = None`, `self._rtl_status = None`.
- In `run()`, where the worker task was started, build with `build_isolation(... replay_source=None, on_label=None, module_feed=mm.feed_bursts if mm else None, refuse_saving=lambda: False)`, start the stage and worker task; stop both where the worker was stopped.
- Replace the attribution block in `_store_and_broadcast` with:

```python
        if self._isolation is not None and pr.bursts:
            from rfobserver.pipeline.isolation import (
                BurstCandidate,
                IsolationBatch,
                WholeCaptureSource,
            )

            self._isolation.submit(
                IsolationBatch(
                    [BurstCandidate(b, b.peak_power_db - pr.noise_floor_db) for b in pr.bursts],
                    float(pr.center_freq_hz),
                    float(self._settings.BANDWIDTH),
                    WholeCaptureSource(pr.iq_bytes),
                )
            )
```

- Add `isolation_status()` returning the same shape with `ring_sec: 0.0` and `disabled_reason: None`.

`replay.py` `run_replay`: add parameters `overrides: dict[str, Any] | None = None` (applied with `object.__setattr__(settings, k, v)` after construction, like `threshold_db`) and `attribution_wait_sec: float = 0.0`. After `_drive_to_end`, if `attribution_wait_sec > 0`, poll `processor.isolation_status()["counts"]` every 0.5 s until `attr_decoded + attr_not_decoded + attr_failed >= isolated + too_long` or the wait expires. The drive-to-end stops the processor, which stops the attribution task; restructure so the wait happens before the stop: run the wait inside `_drive_to_end`'s stopper, after the drain, when `attribution_wait_sec > 0`.

- [ ] **Step 4: Run the tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_isolation_wiring.py tests/unit/test_isolation_stage.py tests/unit/test_streaming.py tests/unit/test_pipeline_wiring.py tests/unit/test_streaming_replay_mode.py -q` then `PYTHONPATH= .venv/bin/pytest tests/integration/test_isolation_attribution_e2e.py tests/integration/test_replay.py -q -s`
Expected: all pass; the e2e passes on this workstation (rtl_433 and `~/ssn_bursts` exist via symlink). If fewer than two fixtures decode, investigate the placement (frequency, level, resample) and report the numbers; do not lower the threshold of the assertion without saying why.

- [ ] **Step 5: Full checks and commit**

```bash
git rm tests/unit/test_attribution_producer.py
git add src/rfobserver/pipeline/streaming.py src/rfobserver/pipeline/continuous.py src/rfobserver/pipeline/app.py src/rfobserver/pipeline/replay.py tests/unit/test_isolation_wiring.py tests/integration/test_isolation_attribution_e2e.py
git commit -m "feat(isolation): wire the stage into streaming and sweep pipelines; SSN end-to-end test"
```

---

### Task 8: Storage, health, config page, overlay labels, README

**Files:**
- Modify: `src/rfobserver/storage/governor.py` (`StorageSample.bursts_bytes`, `to_health` `bursts_gb`)
- Modify: `src/rfobserver/storage/local.py` (`sample()` fills `bursts_bytes`; bursts count as evictable)
- Modify: `src/rfobserver/pipeline/app.py` (`_storage_tick`: burst eviction before auto captures; burst cap each tick)
- Modify: `src/rfobserver/web/app.py` (health `isolation` block)
- Modify: `src/rfobserver/web/routes/config.py`, `src/rfobserver/web/templates/config.html` (settings fields)
- Modify: `src/rfobserver/web/templates/dashboard.html` (labels on the overlay)
- Modify: `README.md`
- Test: `tests/unit/test_isolation_storage.py` (new), `tests/unit/test_storage_health.py` (append)

**Interfaces:**
- Consumes: `BurstArchive` (Task 4), `isolation_status()` (Task 7), the governor/storage loop from the storage-budgeting work.
- Produces: `StorageSample.bursts_bytes: int = 0`; health `storage.bursts_gb`; health `isolation` (the `isolation_status()` dict) when a processor exists.

- [ ] **Step 1: Failing tests**

Create `tests/unit/test_isolation_storage.py`:

```python
"""Isolated bursts under the storage governor."""

from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace

import numpy as np

from rfobserver.config import AppSettings
from rfobserver.pipeline.app import _storage_tick
from rfobserver.processing.isolate import IsolatedBurst
from rfobserver.storage.burst_archive import BurstArchive
from rfobserver.storage.governor import GB, StorageGovernor
from rfobserver.storage.local import LocalStorage


def _iso(bid):
    return IsolatedBurst(bid, np.zeros(2000, dtype="<i2").tobytes(), 1_000_000, [[]], 915e6, 0, 1, False)


def test_sample_counts_bursts_and_makes_them_evictable(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    a = BurstArchive(tmp_path)
    p = a.save(_iso("b1"), {})
    s = ls.sample(db_path=tmp_path / "db", active_names=(), db_file_bytes=0, db_reusable_bytes=0)
    assert s.bursts_bytes == a.usage_bytes() > 0
    assert s.evictable_auto is True  # no auto captures, but bursts can go


class _DB:
    async def file_stats(self):
        return 0, 0

    async def set_config(self, k, v):
        pass


async def test_storage_tick_evicts_bursts_before_captures(tmp_path, monkeypatch):
    s = AppSettings(_env_file=None, STORAGE_PATH=str(tmp_path), DISK_MIN_FREE_GB=10**6)
    ls = LocalStorage(str(tmp_path), max_gb=100)
    a = BurstArchive(tmp_path)
    for i in range(3):
        p = a.save(_iso(f"b{i}"), {})
        os.utime(p, (1000 + i, 1000 + i))
    order = []
    real_b = BurstArchive.evict_until_free
    real_c = LocalStorage.evict_until_free
    monkeypatch.setattr(BurstArchive, "evict_until_free", lambda self, *a, **k: order.append("bursts") or real_b(self, *a, **k))
    monkeypatch.setattr(LocalStorage, "evict_until_free", lambda self, *a, **k: order.append("captures") or real_c(self, *a, **k))
    sup = SimpleNamespace(processor=None)
    await _storage_tick(s, StorageGovernor(), _DB(), ls, sup, asyncio.Event())
    assert order[:2] == ["bursts", "captures"]


async def test_storage_tick_enforces_the_burst_cap(tmp_path):
    s = AppSettings(_env_file=None, STORAGE_PATH=str(tmp_path), BURST_ARCHIVE_MAX_GB=1e-9)
    ls = LocalStorage(str(tmp_path), max_gb=100)
    a = BurstArchive(tmp_path)
    a.save(_iso("b1"), {})
    await _storage_tick(s, StorageGovernor(), _DB(), ls, SimpleNamespace(processor=None), asyncio.Event())
    assert a.usage_bytes() == 0
```

Append to `tests/unit/test_storage_health.py`:

```python
def test_health_reports_isolation_status():
    from types import SimpleNamespace

    app = create_app(AppSettings(_env_file=None))
    status = {"enabled": True, "attribution": False, "rtl433": None, "ring_sec": 1.5,
              "disabled_reason": None, "counts": {"isolated": 3}}
    proc = SimpleNamespace(isolation_status=lambda: status)
    app.state.supervisor = SimpleNamespace(processor=proc, active=True, gave_up=False,
                                           consecutive_crashes=0)
    body = TestClient(app).get("/api/health").json()
    assert body["isolation"] == status


def test_config_page_has_the_isolation_fields():
    html = _client(None).get("/config").text
    for name in ("isolation_enabled", "attribution_enabled", "isolation_snr_db",
                 "isolation_max_per_sec", "isolation_lookback_sec", "burst_archive_max_gb"):
        assert f'name="{name}"' in html


def test_dashboard_draws_attribution_labels():
    assert "attributions" in _client(None).get("/live/").text
```

(`_client`, `create_app`, `AppSettings`, `TestClient` are already imported in that file; if the health test needs more supervisor attributes than shown, add them to the SimpleNamespace.)

- [ ] **Step 2: Run to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_isolation_storage.py tests/unit/test_storage_health.py -q`
Expected: FAIL.

- [ ] **Step 3: Implement**

- `governor.py`: `StorageSample` gains `bursts_bytes: int = 0` (last field, defaulted); `to_health` adds `"bursts_gb": _gb(s.bursts_bytes) if s else None`.
- `local.py` `sample()`: compute `bursts_bytes = BurstArchive(self.storage_path).usage_bytes()` (import inside the method to avoid a cycle) and set `evictable_auto=any(...) or bursts_bytes > 0`, `bursts_bytes=bursts_bytes`.
- `app.py` `_storage_tick`: after the tick, `archive = BurstArchive(local_storage.storage_path)`; `await asyncio.to_thread(archive.enforce_cap, int(settings.BURST_ARCHIVE_MAX_GB * GB))`; and when `actions.evict_to_free_bytes is not None`, first `await asyncio.to_thread(archive.evict_until_free, actions.evict_to_free_bytes)`, then the existing capture eviction (which returns immediately if the target is already met).
- `web/app.py` health: when `sup.processor` has `isolation_status`, `body["isolation"] = proc.isolation_status()`.
- `routes/config.py` field_map: `"isolation_enabled": ("ISOLATION_ENABLED", _to_bool)`, `"attribution_enabled": ("ATTRIBUTION_ENABLED", _to_bool)`, `"isolation_snr_db": ("ISOLATION_SNR_DB", float)`, `"isolation_max_per_sec": ("ISOLATION_MAX_PER_SEC", int)`, `"isolation_lookback_sec": ("ISOLATION_LOOKBACK_SEC", float)`, `"isolation_max_burst_sec": ("ISOLATION_MAX_BURST_SEC", float)`, `"burst_archive_max_gb": ("BURST_ARCHIVE_MAX_GB", float)`. Check how the page handles checkbox booleans (the `boolIds` list in `config.html`) and register the two toggles there.
- `config.html`: a "Burst Isolation" card after the Storage card, in the existing card/form-row markup, with the two toggles and five number inputs, each with a one-line help text:
  - Isolation: "Cut each strong burst out of the IQ (shift to DC, decimate), save it as SigMF under bursts/ and offer it to modules."
  - Attribution: "Decode isolated bursts with rtl_433 and label the detection (turns isolation on)."
  - SNR: "Bursts must be this many dB over the noise floor at their peak."
  - Max per second: "At most this many bursts isolated per second, strongest first."
  - Lookback: "IQ kept for isolation; costs about 104 MB per second at 26 Msps, 224 MB at 56 Msps."
  - Max burst: "Longer bursts are isolated only up to this length."
  - Archive max: "Saved burst files are capped at this size, oldest deleted first."
  Show the current `isolation` health block (counts, rtl433 status, disabled reason) under the card via the existing `/api/health` fetch pattern the storage bar uses, with `textContent` only.
- `dashboard.html`: keep a `const attrLabels = new Map()` fed from `data.attributions` in the psd handler (`attrLabels.set(a.id, a)`; drop entries older than the oldest visible row like `seenBursts`); in `drawBurstOverlay`, after the rectangles, for each label compute `x` from `freq_low_hz/freq_high_hz` and `y` from `rowForTime(start/stop_time_ms)` exactly as the rectangle code does, and draw `a.model` with `burstCtx.fillText` in `rgba(255, 214, 10, 0.95)` (system yellow), 11px system font, at the top-left of that box. Clear `attrLabels` in `clearWaterfall`.
- `README.md`: a "Burst isolation and attribution" section after "Storage": what the two switches do, that attribution needs rtl_433 built from master (protocol 383) at `~/rtl_433_build/build/src/rtl_433` or `ATTRIBUTION_RTL433_PATH`, where files go (`bursts/YYYYMMDD/`, `bursts/replay-<name>/` with `attribution.jsonl`), the RAM cost of the lookback, and that replays never write the DB. No em-dashes.

- [ ] **Step 4: Run the tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_isolation_storage.py tests/unit/test_storage_health.py tests/unit/test_storage_loop.py tests/unit/test_storage_local_budget.py tests/unit/test_web_routes.py -q`
Expected: all pass.

- [ ] **Step 5: Headless check (workstation, mock receiver)**

Run the mock pipeline from a scratch dir with `RFOBS_ISOLATION_ENABLED=true` (and the scratch `STORAGE_PATH`/`DB_PATH`), open `/config` and `/live/` with puppeteer (`launchOptions {"headless": true}`), and confirm the card, the health counts and no console errors. Stop with `fuser -k 8888/tcp`.

- [ ] **Step 6: Full checks and commit**

```bash
git add src/rfobserver/storage/governor.py src/rfobserver/storage/local.py src/rfobserver/pipeline/app.py src/rfobserver/web/app.py src/rfobserver/web/routes/config.py src/rfobserver/web/templates/config.html src/rfobserver/web/templates/dashboard.html README.md tests/unit/test_isolation_storage.py tests/unit/test_storage_health.py
git commit -m "feat(isolation): storage governor, health, config card, overlay labels, README"
```

---

### Task 9: Acceptance on nano-super

**Files:**
- Create: `docs/debugging/2026-09-28_burst-isolation-acceptance.md`

nano-super: `ssh ocollaco@192.168.97.153`, passwordless sudo, Python 3.10, rtl_433 at `~/rtl_433_build/build/src/rtl_433`, fixtures in `~/ssn_bursts`, real B200mini. The controller ships the branch to `~/rfobs-attrib` (git worktree via a bundle; nothing pushed); run with `PYTHONPATH=$HOME/rfobs-attrib/src ~/GitHub/RFObserver/.venv/bin/rfobserver run` from inside it and confirm `rfobserver.__file__`. Hard rules: do not touch `~/GitHub/RFObserver`'s branch or `stash@{0}`, or the pre-existing `~/rfobs-*` dirs and `~/rfobs-replay-data`; scratch only under `~/rfobs-attr-val/`; delete everything you create; leave no process running; set MAXN (`sudo nvpmodel -m 2`) only if it is not already the mode, and put back the mode you found. Never touch `rfnano`.

- [ ] **Step 1: Unit and e2e on the Jetson.** Run the unit suite and `tests/integration/test_isolation_attribution_e2e.py` on nano-super (Python 3.10, aarch64). Record the pass counts and the decode results.
- [ ] **Step 2: Replay acceptance.** Download `feb4_19-39-48` (`gdown 1dCnRoLKx3AZs9x4qakzzDgoJIj_reraq`) and `feb5_05-29-58` (`gdown 1p9Ivfn--vPV2jC0omjtZE9p1boyJU0Da`) into `~/rfobs-attr-val/`; check each file's size and datatype against `/mnt/storage/ssn-batch/run_batch.sh` / `batch_ssn.py` on the workstation (read how that script loads them: sample rate, center, datatype). Start the server with `RFOBS_ATTRIBUTION_ENABLED=true`, start a replay of each via `POST /api/replay/start` (read `web/routes/api.py` for the body fields). Pass: `bursts/replay-<name>/attribution.jsonl` contains `SilverSpring-Mesh` / `383` at 919.399 MHz (feb4) and at 917.000 / 904.688 / 913.4 MHz where they decode (feb5); weaker bursts are `not_decoded`; SigMF files exist and open; the DB `detections` table gains no rows from the replay.
- [ ] **Step 3: Live acceptance.** B200mini at 915 MHz, 26 Msps, both switches on, 10 minutes. Record the pipeline latency and `excess_ms` against a 10-minute baseline with both off; dropped chunks and overflows; the `isolation` counts from `/api/health` at 1-minute intervals (they must add up: picked = isolated + iq_expired + too_long + queue_full + error); any on-air decodes; process RSS versus the ring estimate. Screenshot the dashboard overlay with a label if any decode happens.
- [ ] **Step 4: Record.** Write `docs/debugging/2026-09-28_burst-isolation-acceptance.md` in the usual order: question, answer, procedure, raw evidence, measured and REJECTED, traps, open items. Anything that does not behave as designed goes under Findings with verbatim evidence; do not fix product code in this task.
- [ ] **Step 5: Commit (workstation)**

```bash
git add docs/debugging/2026-09-28_burst-isolation-acceptance.md
git commit -m "docs(isolation): acceptance on nano-super: replayed SSN decodes and live load"
```
