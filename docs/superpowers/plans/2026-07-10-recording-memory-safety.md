# Recording memory-safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop recordings from exhausting RAM (grids accumulated in RAM regardless of the RAM-buffer flag) and from freezing the WebUI on stop, and add an OS-level memory backstop.

**Architecture:** Grid persistence follows `RECORDING_RAM_BUFFER`: disk mode streams grid rows to a raw companion file during recording; RAM mode keeps them in RAM but auto-stops at a duration derived from available RAM. New grid format is a memmap-friendly raw `.psd` + `.psd.json` sidecar (reader falls back to legacy `.npz`). Recording stop runs off the asyncio event loop. `MemoryMax` is added to the systemd unit.

**Tech Stack:** Python 3.11+, numpy, FastAPI, asyncio, pytest/pytest-asyncio, systemd.

## Global Constraints

- Run Python via the venv with cleared PYTHONPATH: `PYTHONPATH= .venv/bin/<tool>`. ruff is global.
- No emojis anywhere. UI Apple-style.
- Before every commit run all of: `ruff check src/ tests/`, `ruff format --check src/ tests/`, `PYTHONPATH= .venv/bin/mypy src/rfobserver/`, `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`, `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q` (integration needs NATS: `docker run -d --rm --name rfobs-nats-test -p 4222:4222 nats:latest`).
- No "Co-Authored-By: Claude" trailer in commits.
- PSD grid is `float32`, shape `(n_slices, num_fft_bins)`, C-contiguous (`spectral.py:126`).

---

### Task 1: `RECORDING_MEM_FRACTION` setting + RAM-derived duration helper

**Files:**
- Modify: `src/rfobserver/config.py` (near `RECORDING_MAX_SEC`, line ~123)
- Modify: `src/rfobserver/pipeline/streaming.py` (module-level helpers near top, after imports)
- Test: `tests/unit/test_recording_cap.py` (create)

**Interfaces:**
- Produces: `AppSettings.RECORDING_MEM_FRACTION: float = 0.5`
- Produces (streaming.py module level):
  - `def _mem_available_bytes() -> int | None` — reads `/proc/meminfo` `MemAvailable` (kB→bytes); `None` if unreadable.
  - `def _effective_max_recording_sec(settings: AppSettings, mem_available_bytes: int | None) -> float` — returns the auto-stop duration (may be `math.inf`).

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_recording_cap.py
import math

from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import _effective_max_recording_sec


def _settings(**kw) -> AppSettings:
    base = dict(
        BANDWIDTH=26_000_000, NUM_FFT_BINS=1024, PSD_TIME_RESOLUTION_MS=0.2,
        RECORDING_MAX_SEC=0.0, RECORDING_RAM_BUFFER=False, RECORDING_MEM_FRACTION=0.5,
        _env_file=None,
    )
    base.update(kw)
    return AppSettings(**base)


def test_disk_mode_is_not_ram_capped() -> None:
    # Disk mode streams grids to disk -> no RAM cap; unlimited when MAX_SEC=0.
    assert _effective_max_recording_sec(_settings(), 4_000_000_000) == math.inf


def test_disk_mode_respects_configured_max() -> None:
    assert _effective_max_recording_sec(_settings(RECORDING_MAX_SEC=30.0), 4_000_000_000) == 30.0


def test_ram_mode_caps_by_available_ram() -> None:
    # RAM mode holds IQ + grids: iq=26e6*4=104MB/s, grid=(1000/0.2)*1024*4=20.48MB/s
    # budget = 0.5 * 2.0GB = 1.0GB -> ~1.0e9 / 124.48e6 ~= 8.03s
    s = _settings(RECORDING_RAM_BUFFER=True)
    cap = _effective_max_recording_sec(s, 2_000_000_000)
    assert 7.0 < cap < 9.0


def test_ram_mode_takes_min_with_configured() -> None:
    s = _settings(RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=3.0)
    assert _effective_max_recording_sec(s, 2_000_000_000) == 3.0


def test_unreadable_mem_falls_back() -> None:
    # RAM mode with no mem info -> conservative: configured max, or 30s if unlimited.
    assert _effective_max_recording_sec(_settings(RECORDING_RAM_BUFFER=True), None) == 30.0
    assert _effective_max_recording_sec(
        _settings(RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=10.0), None) == 10.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_recording_cap.py -q`
Expected: FAIL (import error / attr missing).

- [ ] **Step 3: Add the setting**

In `src/rfobserver/config.py` after `RECORDING_MAX_SEC` (line ~123):

```python
    RECORDING_MEM_FRACTION: float = 0.5  # RAM-mode: max fraction of available RAM a recording may use
```

- [ ] **Step 4: Add the helpers**

In `src/rfobserver/pipeline/streaming.py`, ensure `import math` is present (add if missing), and add module-level (after `_signal_stop`):

```python
def _mem_available_bytes() -> int | None:
    """Available RAM in bytes from /proc/meminfo, or None if unreadable."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def _effective_max_recording_sec(settings: Any, mem_available_bytes: int | None) -> float:
    """Auto-stop duration for a recording.

    Disk mode streams IQ+grids to disk, so RAM is not the limit — only the
    configured RECORDING_MAX_SEC (or unlimited). RAM mode holds IQ+grids in RAM,
    so cap the duration to a fraction of available RAM. Returns math.inf for
    "no limit". Falls back to 30 s (or the configured max) if RAM is unknown.
    """
    configured = settings.RECORDING_MAX_SEC if settings.RECORDING_MAX_SEC > 0 else math.inf
    if not settings.RECORDING_RAM_BUFFER:
        return configured
    if mem_available_bytes is None:
        return min(configured, 30.0)
    grid_bps = (1000.0 / settings.PSD_TIME_RESOLUTION_MS) * settings.NUM_FFT_BINS * 4
    iq_bps = settings.BANDWIDTH * 4
    ram_bps = grid_bps + iq_bps
    ram_max = (mem_available_bytes * settings.RECORDING_MEM_FRACTION) / ram_bps
    return min(configured, ram_max)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_recording_cap.py -q`
Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
git add src/rfobserver/config.py src/rfobserver/pipeline/streaming.py tests/unit/test_recording_cap.py
git commit -m "Add RECORDING_MEM_FRACTION + RAM-derived recording duration helper"
```

---

### Task 2: PSD grid on-disk format module (`storage/psd_grid.py`)

**Files:**
- Create: `src/rfobserver/storage/psd_grid.py`
- Test: `tests/unit/test_psd_grid.py` (create)

**Interfaces:**
- Produces:
  - `def grid_paths(sc16_path: Path) -> tuple[Path, Path]` → `(raw_path=<base>.psd, meta_path=<base>.psd.json)` where base strips a trailing `.sc16`.
  - `def write_meta(meta_path: Path, *, rows: int, num_bins: int, time_resolution_s: float, center_freq_hz: int, bandwidth_hz: int, freq_axis: np.ndarray, grid_min: float, grid_max: float, cal_offset_db: float | None) -> None`
  - `def load_grid(sc16_path: Path) -> tuple[np.memmap, dict] | None` — returns a read-only memmap `(rows, num_bins)` float32 and the parsed meta, or `None` if the new-format companion is absent/invalid.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_psd_grid.py
import numpy as np

from rfobserver.storage import psd_grid


def test_write_and_load_roundtrip(tmp_path) -> None:
    sc16 = tmp_path / "cap.sc16"
    raw, meta = psd_grid.grid_paths(sc16)
    assert raw.name == "cap.psd" and meta.name == "cap.psd.json"

    grid = np.arange(6 * 4, dtype=np.float32).reshape(6, 4)
    raw.write_bytes(grid.tobytes())
    psd_grid.write_meta(
        meta, rows=6, num_bins=4, time_resolution_s=0.0005,
        center_freq_hz=915_000_000, bandwidth_hz=26_000_000,
        freq_axis=np.arange(4, dtype=np.float64), grid_min=0.0, grid_max=23.0,
        cal_offset_db=None,
    )

    loaded = psd_grid.load_grid(sc16)
    assert loaded is not None
    mm, m = loaded
    assert mm.shape == (6, 4) and mm.dtype == np.float32
    np.testing.assert_array_equal(mm[:], grid)
    assert m["num_bins"] == 4 and m["rows"] == 6
    assert m["grid_max"] == 23.0 and m["center_freq_hz"] == 915_000_000
    assert "cal_offset_db" not in m


def test_load_missing_returns_none(tmp_path) -> None:
    assert psd_grid.load_grid(tmp_path / "nope.sc16") is None


def test_cal_offset_included_when_set(tmp_path) -> None:
    sc16 = tmp_path / "c.sc16"
    raw, meta = psd_grid.grid_paths(sc16)
    raw.write_bytes(np.zeros((2, 2), np.float32).tobytes())
    psd_grid.write_meta(
        meta, rows=2, num_bins=2, time_resolution_s=1.0, center_freq_hz=1,
        bandwidth_hz=1, freq_axis=np.zeros(2), grid_min=0.0, grid_max=0.0,
        cal_offset_db=-12.5,
    )
    _, m = psd_grid.load_grid(sc16)
    assert m["cal_offset_db"] == -12.5
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_psd_grid.py -q`
Expected: FAIL (module missing).

- [ ] **Step 3: Implement the module**

```python
# src/rfobserver/storage/psd_grid.py
"""On-disk PSD grid companion for recordings.

Grid is stored as raw C-contiguous float32 ``(rows, num_bins)`` in ``<base>.psd``
with a JSON sidecar ``<base>.psd.json``. Raw + memmap keeps both the writer
(streaming append) and reader (windowed slice) off the RAM heap — unlike the old
compressed ``.npz`` which materialized the whole grid.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np


def grid_paths(sc16_path: Path) -> tuple[Path, Path]:
    """Return (raw .psd, meta .psd.json) paths for a recording's .sc16 path."""
    base = sc16_path
    if base.suffix == ".sc16":
        base = base.with_suffix("")
    return base.with_suffix(".psd"), base.with_suffix(".psd.json")


def write_meta(
    meta_path: Path,
    *,
    rows: int,
    num_bins: int,
    time_resolution_s: float,
    center_freq_hz: int,
    bandwidth_hz: int,
    freq_axis: np.ndarray,
    grid_min: float,
    grid_max: float,
    cal_offset_db: float | None,
) -> None:
    """Write the JSON sidecar describing the raw .psd grid."""
    meta: dict[str, Any] = {
        "rows": int(rows),
        "num_bins": int(num_bins),
        "time_resolution_s": float(time_resolution_s),
        "center_freq_hz": int(center_freq_hz),
        "bandwidth_hz": int(bandwidth_hz),
        "freq_axis": [float(x) for x in np.asarray(freq_axis).tolist()],
        "grid_min": float(grid_min),
        "grid_max": float(grid_max),
    }
    if cal_offset_db is not None:
        meta["cal_offset_db"] = float(cal_offset_db)
    meta_path.write_text(json.dumps(meta))


def load_grid(sc16_path: Path) -> tuple[np.memmap, dict] | None:
    """Memmap the raw grid + parse meta, or None if the companion is absent/invalid."""
    raw_path, meta_path = grid_paths(sc16_path)
    if not raw_path.exists() or not meta_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text())
        rows = int(meta["rows"])
        num_bins = int(meta["num_bins"])
    except (OSError, ValueError, KeyError):
        return None
    if rows == 0:
        # Empty grid: return a real (0, num_bins) array (memmap can't be zero-length).
        return np.zeros((0, num_bins), dtype=np.float32), meta  # type: ignore[return-value]
    mm = np.memmap(raw_path, dtype=np.float32, mode="r", shape=(rows, num_bins))
    return mm, meta
```

- [ ] **Step 4: Run to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_psd_grid.py -q`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/storage/psd_grid.py tests/unit/test_psd_grid.py
git commit -m "Add raw+sidecar PSD grid on-disk format (memmap-friendly)"
```

---

### Task 3: Stream grids by flag + wire RAM cap (`streaming.py`)

**Files:**
- Modify: `src/rfobserver/pipeline/streaming.py` (`__init__` ~203-225, `_check_trigger_and_record` ~514-535, `_begin_recording` ~576-633, `_end_recording` ~635-702, `_handle_chunk_result` grid-append ~912-918)
- Test: `tests/integration/test_recording_grids.py` (create)

**Interfaces:**
- Consumes: `psd_grid.grid_paths`, `psd_grid.write_meta`, `_effective_max_recording_sec`, `_mem_available_bytes`.
- Produces: recordings write `<base>.psd` + `<base>.psd.json` (both modes); no `.npz`. New attrs: `self._grid_file`, `self._grid_rows`, `self._grid_min`, `self._grid_max`, `self._effective_max_sec`.

- [ ] **Step 1: Write the failing test**

```python
# tests/integration/test_recording_grids.py
from __future__ import annotations

import asyncio
from pathlib import Path

import numpy as np
import pytest

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import StreamingProcessor
from rfobserver.storage import psd_grid
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.local import LocalStorage


def _settings(tmp_path: Path, **kw) -> AppSettings:
    storage = tmp_path / "st"
    storage.mkdir()
    base = dict(
        FREQUENCY_START=915_000_000, FREQUENCY_END=915_000_000, BANDWIDTH=2_000_000,
        DURATION_SEC=0.5, GAIN=30, NUM_FFT_BINS=256, PSD_TIME_RESOLUTION_MS=0.5,
        STREAMING_CHUNK_SLICES=10, MOCK_RECEIVER=True, STORAGE_PATH=str(storage),
        DB_PATH=str(tmp_path / "t.db"), ARCHIVE_MAX_GB=0.01, _env_file=None,
    )
    base.update(kw)
    return AppSettings(**base)


async def _record_briefly(settings: AppSettings, db: SensorDatabase) -> Path:
    receiver = MockReceiver(
        ReceiverConfig(gain_db=settings.GAIN, bandwidth_hz=settings.BANDWIDTH,
                       duration_sec=settings.DURATION_SEC)
    )
    receiver.initialize()
    storage = LocalStorage(storage_path=settings.STORAGE_PATH, max_gb=settings.ARCHIVE_MAX_GB)
    proc = StreamingProcessor(receiver=receiver, database=db, local_storage=storage,
                              settings=settings)

    async def driver() -> None:
        for _ in range(200):  # wait for streaming to start
            if proc._capture_count > 2:
                break
            await asyncio.sleep(0.02)
        proc.start_recording()
        while proc._capture_count < 12:  # let some chunks accumulate grids
            await asyncio.sleep(0.02)
        proc.stop_recording()
        await asyncio.sleep(0.1)
        proc.stop()

    await asyncio.wait_for(asyncio.gather(proc.run(), driver()), timeout=30.0)
    return Path(settings.STORAGE_PATH)


@pytest.mark.asyncio
async def test_disk_mode_writes_psd_not_npz(tmp_path: Path) -> None:
    settings = _settings(tmp_path, RECORDING_RAM_BUFFER=False)
    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        storage_dir = await _record_briefly(settings, db)
    finally:
        await db.close()
    sc16 = next(storage_dir.glob("*.sc16"))
    raw, meta = psd_grid.grid_paths(sc16)
    assert raw.exists() and meta.exists(), "disk-mode recording must write .psd + .psd.json"
    assert not list(storage_dir.glob("*.npz")), "no legacy .npz for new recordings"
    loaded = psd_grid.load_grid(sc16)
    assert loaded is not None
    mm, m = loaded
    assert mm.shape[0] == m["rows"] > 0 and mm.shape[1] == 256
```

- [ ] **Step 2: Run to verify it fails**

Run (needs NATS running): `PYTHONPATH= .venv/bin/pytest tests/integration/test_recording_grids.py -q`
Expected: FAIL (no `.psd`; `.npz` written instead).

- [ ] **Step 3: Add recording-grid attrs to `__init__`**

After line 225 (`self._recording_cal_offset`), add:

```python
        # New raw-grid streaming state (replaces the in-RAM _recording_grids list
        # in disk mode; RAM mode still uses the list but bounded by _effective_max_sec).
        self._grid_file: Any = None
        self._grid_rows: int = 0
        self._grid_min: float = float("inf")
        self._grid_max: float = float("-inf")
        self._effective_max_sec: float = float("inf")
```

- [ ] **Step 4: Replace the grid-append site in `_handle_chunk_result`**

Replace lines ~912-918 (the `# Accumulate PSD grids ...` block) with:

```python
        # Persist PSD grids during recording. Disk mode streams rows straight to
        # the raw .psd file (bounded RAM); RAM mode keeps them in the list, which
        # is bounded by the RAM-derived _effective_max_sec auto-stop.
        if self._recording_state == "recording":
            grid = cr.psd_grid.grid
            self._recording_freq_axis = cr.psd_grid.freq_axis
            if len(cr.psd_grid.time_axis) > 1:
                self._recording_time_res = float(
                    cr.psd_grid.time_axis[1] - cr.psd_grid.time_axis[0]
                )
            if grid.size:
                self._grid_min = min(self._grid_min, float(grid.min()))
                self._grid_max = max(self._grid_max, float(grid.max()))
            if self._grid_file is not None:
                self._grid_file.write(np.ascontiguousarray(grid, dtype=np.float32).tobytes())
                self._grid_rows += grid.shape[0]
            else:
                self._recording_grids.append(grid.copy())
```

- [ ] **Step 5: Wire the cap + open the grid file in `_begin_recording`**

At the top of `_begin_recording` (after `self._recording_state = "recording"`, resetting state), add the effective-cap computation and grid init. Replace the grid-state resets (`self._recording_grids = []` etc.) with:

```python
        self._recording_grids = []
        self._recording_freq_axis = None
        self._recording_time_res = 0.0
        self._recording_cal_offset = self._settings.CAL_OFFSET_DB
        self._grid_rows = 0
        self._grid_min = float("inf")
        self._grid_max = float("-inf")
        self._effective_max_sec = _effective_max_recording_sec(
            self._settings, _mem_available_bytes()
        )
        if self._effective_max_sec != float("inf"):
            logger.info("Recording auto-stop cap: %.1fs", self._effective_max_sec)
        # Disk mode: stream grid rows to <base>.psd; RAM mode: accumulate in list.
        from rfobserver.storage import psd_grid

        if not self._settings.RECORDING_RAM_BUFFER:
            raw_path, _ = psd_grid.grid_paths(self._storage.storage_path / self._recording_file)
            self._grid_file = open(raw_path, "wb")
        else:
            self._grid_file = None
```

Note: `self._recording_file` is set earlier in `_begin_recording` (line ~579); ensure this block is placed after that assignment.

- [ ] **Step 6: Use the cap in the auto-stop check (`_check_trigger_and_record`)**

Replace lines ~522-524:

```python
            # Auto-stop on max duration
            max_sec = self._settings.RECORDING_MAX_SEC
            if max_sec > 0 and (time.monotonic() - self._recording_start) >= max_sec:
                self.stop_recording()
                return
```

with:

```python
            # Auto-stop on the effective max duration (RAM-derived cap in RAM mode).
            if (time.monotonic() - self._recording_start) >= self._effective_max_sec:
                self.stop_recording()
                return
```

- [ ] **Step 7: Rewrite the grid-finalize in `_end_recording`**

Replace the `# Save PSD grid data as .npz companion` block (lines ~673-691, from `grids = self._recording_grids` through the `np.savez_compressed`/log) with:

```python
        # Finalize the PSD grid companion (<base>.psd + .psd.json). Disk mode has
        # been streaming rows; RAM mode flushes its list here (row-by-row, no
        # np.concatenate). Either way, no whole-grid RAM copy.
        from rfobserver.storage import psd_grid

        raw_path, meta_path = psd_grid.grid_paths(self._storage.storage_path / base_name)
        if self._grid_file is not None:
            self._grid_file.close()
            self._grid_file = None
        elif self._recording_grids:
            with open(raw_path, "wb") as fh:
                for g in self._recording_grids:
                    fh.write(np.ascontiguousarray(g, dtype=np.float32).tobytes())
                    self._grid_rows += g.shape[0]
            self._recording_grids = []
        if self._grid_rows > 0 and self._recording_freq_axis is not None:
            num_bins = int(self._recording_freq_axis.shape[0])
            psd_grid.write_meta(
                meta_path,
                rows=self._grid_rows,
                num_bins=num_bins,
                time_resolution_s=self._recording_time_res,
                center_freq_hz=self._settings.FREQUENCY_START,
                bandwidth_hz=self._settings.BANDWIDTH,
                freq_axis=self._recording_freq_axis,
                grid_min=(0.0 if self._grid_min == float("inf") else self._grid_min),
                grid_max=(0.0 if self._grid_max == float("-inf") else self._grid_max),
                cal_offset_db=self._recording_cal_offset,
            )
            logger.info("PSD data saved: %s (%d rows)", meta_path.name, self._grid_rows)
```

Confirm `np` is imported (it is) and remove any now-unused `.npz` references in this method. The base-name rename-on-drops logic above this block stays.

- [ ] **Step 8: Run the test to verify it passes**

Run (NATS up): `PYTHONPATH= .venv/bin/pytest tests/integration/test_recording_grids.py -q`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/rfobserver/pipeline/streaming.py tests/integration/test_recording_grids.py
git commit -m "Stream recording PSD grids by RAM/disk flag; RAM-derived duration cap"
```

---

### Task 4: Off-loop recording stop (`api.py`)

**Files:**
- Modify: `src/rfobserver/web/routes/api.py` (`recording_stop`, ~line 222)
- Test: `tests/unit/test_web_routes.py` (add)

**Interfaces:**
- Consumes: `StreamingProcessor.stop_recording` (sync, does blocking file I/O).
- Produces: `POST /api/recording/stop` runs the blocking stop via `asyncio.to_thread` so the event loop is not blocked.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/unit/test_web_routes.py
import asyncio as _asyncio


def test_recording_stop_runs_off_event_loop(monkeypatch, settings):
    """stop_recording (blocking) must be dispatched via asyncio.to_thread."""
    from rfobserver.web.app import create_app
    from fastapi.testclient import TestClient

    calls = {"to_thread": 0}
    real_to_thread = _asyncio.to_thread

    async def spy(fn, *a, **k):
        calls["to_thread"] += 1
        return await real_to_thread(fn, *a, **k)

    monkeypatch.setattr(_asyncio, "to_thread", spy)

    class Proc:
        def stop_recording(self):
            pass
        def recording_status(self):
            return {"state": "idle", "file": None, "bytes": 0, "duration_sec": 0}

    app = create_app(settings)
    app.state.processor = Proc()
    r = TestClient(app).post("/api/recording/stop")
    assert r.status_code == 200
    assert calls["to_thread"] == 1
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_web_routes.py -q -k recording_stop_runs_off`
Expected: FAIL (`to_thread` not called).

- [ ] **Step 3: Implement**

In `src/rfobserver/web/routes/api.py`, add `import asyncio` at top if absent, and change `recording_stop`:

```python
@router.post("/recording/stop")
async def recording_stop(request: Request) -> dict[str, Any]:
    """Stop recording or disarm trigger."""
    proc = _get_processor(request)
    if proc is not None and hasattr(proc, "stop_recording"):
        # Finalizing a recording does blocking file I/O; keep it off the event
        # loop so the WebSocket/heartbeat stay responsive.
        await asyncio.to_thread(proc.stop_recording)
        return _rec_status(proc)
    return _idle_status()
```

- [ ] **Step 4: Run to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_web_routes.py -q -k recording_stop_runs_off`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/web/routes/api.py tests/unit/test_web_routes.py
git commit -m "Run recording stop off the event loop so the WebUI stays responsive"
```

---

### Task 5: Reader reads new `.psd` format + legacy `.npz` (`captures.py`)

**Files:**
- Modify: `src/rfobserver/web/routes/captures.py` (`get_psd_grid` ~100-155; `has_psd` checks ~48, ~77)
- Test: `tests/unit/test_web_routes.py` (add)

**Interfaces:**
- Consumes: `psd_grid.load_grid`.
- Produces: `GET /api/captures/{...}/psd` serves a window from either a new `.psd` capture (memmap) or a legacy `.npz`; `has_psd` true if either companion exists.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/unit/test_web_routes.py
def test_psd_grid_reads_new_format(tmp_path, settings):
    import numpy as np
    from fastapi.testclient import TestClient
    from rfobserver.web.app import create_app
    from rfobserver.storage import psd_grid

    settings.STORAGE_PATH = str(tmp_path)
    sc16 = tmp_path / "cap.sc16"
    sc16.write_bytes(b"\x00" * 16)
    raw, meta = psd_grid.grid_paths(sc16)
    grid = np.linspace(-120, -40, 8 * 4, dtype=np.float32).reshape(8, 4)
    raw.write_bytes(grid.tobytes())
    psd_grid.write_meta(meta, rows=8, num_bins=4, time_resolution_s=0.001,
                        center_freq_hz=915_000_000, bandwidth_hz=26_000_000,
                        freq_axis=np.arange(4, dtype=np.float64),
                        grid_min=float(grid.min()), grid_max=float(grid.max()),
                        cal_offset_db=None)

    app = create_app(settings)
    client = TestClient(app)
    r = client.get("/api/captures/cap.sc16/psd?start=0&count=4")
    assert r.status_code == 200
    d = r.json()
    assert d["total_rows"] == 8 and d["num_bins"] == 4
    assert len(d["grid"]) == 4  # windowed
    assert d["grid_min"] == float(grid.min())
```

(Adjust the exact `/api/captures/.../psd` path to match the existing route signature in `captures.py`.)

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_web_routes.py -q -k psd_grid_reads_new`
Expected: FAIL (reader only handles `.npz`).

- [ ] **Step 3: Implement — new-format-first, legacy fallback**

In `captures.py get_psd_grid`, replace the load block (lines ~110-124 up to `total_rows, num_bins = grid.shape`) with:

```python
    base = filename.replace(".sc16", "").replace(".npz", "")
    from rfobserver.storage import psd_grid as _psd

    sc16_path = _validate_filename(base + ".sc16", storage)
    loaded = _psd.load_grid(sc16_path)
    if loaded is not None:
        grid, meta = loaded
        freq_axis = np.asarray(meta["freq_axis"])
        time_res = float(meta["time_resolution_s"])
        center_freq = int(meta["center_freq_hz"])
        bandwidth = int(meta["bandwidth_hz"])
        total_rows = int(meta["rows"])
        num_bins = int(meta["num_bins"])
        grid_min = float(meta["grid_min"])
        grid_max = float(meta["grid_max"])
        cal_offset_db = float(meta["cal_offset_db"]) if "cal_offset_db" in meta else None
    else:
        npz_path = _validate_filename(base + ".npz", storage)
        if not npz_path.exists():
            raise HTTPException(status_code=404, detail="No PSD data for this capture")
        data = np.load(npz_path)
        grid = data["grid"]
        freq_axis = data["freq_axis"]
        time_res = float(data["time_resolution_s"])
        center_freq = int(data["center_freq_hz"])
        bandwidth = int(data["bandwidth_hz"])
        total_rows, num_bins = grid.shape
        grid_min = float(grid.min()) if total_rows else -120.0
        grid_max = float(grid.max()) if total_rows else -40.0
        cal_offset_db = float(data["cal_offset_db"]) if "cal_offset_db" in data.files else None
```

Then in the rest of the function: remove the now-duplicated `total_rows, num_bins = grid.shape`, `grid_min`/`grid_max` scan, and `cal_offset_db` re-read (they're set above). Keep the row-slice + bin-downsample logic. The memmap slice `grid[start:end]` only materializes the window. Match the existing `HTTPException`/`_validate_filename` names already imported in the file.

- [ ] **Step 4: Update `has_psd` checks (lines ~48, ~77)**

Replace `sc16.with_suffix(".npz").exists()` (and the `sc16_path` variant) with a helper:

```python
def _has_psd(sc16_path: Path) -> bool:
    from rfobserver.storage import psd_grid as _psd
    raw, meta = _psd.grid_paths(sc16_path)
    return (raw.exists() and meta.exists()) or sc16_path.with_suffix(".npz").exists()
```

and call `_has_psd(sc16)` / `_has_psd(sc16_path)` at the two sites.

- [ ] **Step 5: Run to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_web_routes.py -q -k "psd_grid_reads_new or psd"`
Expected: PASS. Add/keep a legacy `.npz` test too (build a small `.npz` with the old fields and assert it still serves).

- [ ] **Step 6: Commit**

```bash
git add src/rfobserver/web/routes/captures.py tests/unit/test_web_routes.py
git commit -m "Captures viewer reads new .psd memmap format + legacy .npz"
```

---

### Task 6: Memory backstop in the systemd unit

**Files:**
- Modify: `deploy/rfobserver.service`

**Interfaces:** none (deploy config).

- [ ] **Step 1: Add the limits**

In `deploy/rfobserver.service`, under `[Service]` (after `WorkingDirectory`), add:

```ini
# Memory backstop: if a bug blows past this, OOM-kill just this service (then
# Restart=on-failure brings it back) instead of freezing the whole board.
# Tune per box (sized for a ~7-8 GB sensor).
MemoryHigh=3G
MemoryMax=4G
```

- [ ] **Step 2: Validate + commit**

Run: `bash -n deploy/install.sh` (unchanged, sanity) and eyeball the unit.

```bash
git add deploy/rfobserver.service
git commit -m "deploy: add MemoryHigh/MemoryMax backstop to rfobserver.service"
```

---

## Final verification

Run the full suite (NATS up) and confirm green:
```bash
docker run -d --rm --name rfobs-nats-test -p 4222:4222 nats:latest
ruff check src/ tests/ && ruff format --check src/ tests/
PYTHONPATH= .venv/bin/mypy src/rfobserver/
PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q
PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q
docker stop rfobs-nats-test
```

Then a manual reproduction check with the mock receiver: a long recording in disk mode keeps RSS flat (grids on disk); a RAM-mode recording auto-stops at the RAM-derived cap; stopping a recording does not stall `/api/*`.

## Self-review notes

- Spec coverage: flag-gated grids (T3), disk streaming (T3), RAM-derived cap (T1+T3), off-loop stop (T4), reader new+legacy (T5), MemoryMax (T6), new setting (T1). All covered.
- Type consistency: `grid_paths`/`write_meta`/`load_grid` signatures match across T2/T3/T5; `_effective_max_recording_sec`/`_mem_available_bytes` match across T1/T3.
- `_recording_grids` remains only for RAM mode; disk mode uses `_grid_file`.
