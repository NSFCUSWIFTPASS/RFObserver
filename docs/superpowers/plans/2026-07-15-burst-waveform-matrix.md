# Burst-Waveform Detection Matrix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a parametrized integration-test matrix that drives the real `StreamingProcessor` with generated wideband bursts across a grid of pulse lengths, occupied bandwidths, and frequency offsets at the two field sample rates, asserting detection presence, duration, center frequency, and bandwidth within per-combo derived tolerances.

**Architecture:** New band-limited-noise burst generator and a per-combo grid-sizing helper live in `tests/integration/_synth.py`. A new test file drives the full pipeline (receiver → dispatch → rolling burst detection → SQLite) exactly like the existing `test_burst_detection.py`, but with wideband bursts and measurement assertions. The field `BURST_WINDOW_ROWS` default is raised so the deployed config can measure the ~400 ms modes. The heaviest combos are gated behind a `--runslow` pytest option so default CI stays fast.

**Tech Stack:** Python 3.11, numpy, scipy, pytest + pytest-asyncio (`asyncio_mode="auto"`), the existing `rfobserver` streaming pipeline and `SensorDatabase`.

## Global Constraints

- **Always clear leaked system packages:** prefix every Python command with `PYTHONPATH=` (e.g. `PYTHONPATH= .venv/bin/pytest ...`). ruff is global (`ruff ...`, no prefix).
- **mypy scope is `src/` only** (`PYTHONPATH= .venv/bin/mypy src/rfobserver/`); test files are not type-checked but MUST pass `ruff check src/ tests/` and `ruff format --check src/ tests/`.
- **Integration tests require NATS** on `localhost:4222`.
- **No emojis anywhere.** No `Co-Authored-By: Claude` in commits.
- **Field sample rates only:** `Fs ∈ {28_000_000, 56_000_000}`.
- **Occupied BW list:** `{50_000, 150_000, 500_000, 2_000_000, 20_000_000}` Hz.
- **Duration list:** `{1.3, 2.7, 10.24, 83.2, 393.1}` ms.
- **Full pipeline, lossless:** build processors with `drop_on_overflow=False` so replay is deterministic.
- Run the full check suite from `CLAUDE.md` before the final commit.

## File Structure

- `tests/conftest.py` — **modify**: add the `--runslow` option + `slow`-marker skip hook (global so it applies to all test dirs).
- `pyproject.toml` — **modify**: register the `slow` marker under `[tool.pytest.ini_options]`.
- `src/rfobserver/config.py` — **modify**: raise `BURST_WINDOW_ROWS` / `BURST_EVAL_INTERVAL_ROWS` defaults with a cost comment.
- `tests/integration/_synth.py` — **modify**: add `GridParams`, `derive_grid_params(...)`, and `make_iq_with_wideband_burst(...)`.
- `tests/integration/test_synth_helpers.py` — **create**: fast unit-style tests for the two new helpers (no NATS, no pipeline).
- `tests/integration/test_burst_waveform_matrix.py` — **create**: shared harness + the parametrized matrix, the offset sweep, and the field-default long-burst validation test.

---

### Task 1: `slow` marker + `--runslow` gate

**Files:**
- Modify: `tests/conftest.py`
- Modify: `pyproject.toml:131-134` (the `[tool.pytest.ini_options]` block)

**Interfaces:**
- Produces: a `@pytest.mark.slow` marker that is **skipped by default** and runs only with `--runslow`. All later `slow`-marked tests rely on this.

- [ ] **Step 1: Register the marker in `pyproject.toml`**

Under `[tool.pytest.ini_options]` (currently `asyncio_mode`, `testpaths`), add:

```toml
markers = [
    "slow: heavy end-to-end combos; skipped unless --runslow is passed",
]
```

- [ ] **Step 2: Add the gate to `tests/conftest.py`**

Append to `tests/conftest.py`:

```python
import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--runslow",
        action="store_true",
        default=False,
        help="run tests marked @pytest.mark.slow",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if config.getoption("--runslow"):
        return
    skip_slow = pytest.mark.skip(reason="need --runslow to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)
```

- [ ] **Step 3: Add a temporary proof test**

Create `tests/integration/test_slow_gate_tmp.py`:

```python
import pytest


@pytest.mark.slow
def test_slow_marker_is_gated() -> None:
    assert True
```

- [ ] **Step 4: Verify it is skipped by default, runs with the flag**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_slow_gate_tmp.py -q`
Expected: `1 skipped`.

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_slow_gate_tmp.py -q --runslow`
Expected: `1 passed`.

- [ ] **Step 5: Delete the temporary proof test**

Run: `rm tests/integration/test_slow_gate_tmp.py`

- [ ] **Step 6: Lint + commit**

Run: `ruff check tests/ && ruff format --check tests/`
Expected: pass.

```bash
git add tests/conftest.py pyproject.toml
git commit -m "test: add --runslow gate for heavy end-to-end combos"
```

---

### Task 2: Raise field `BURST_WINDOW_ROWS` default

**Files:**
- Modify: `src/rfobserver/config.py:118-120`
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Produces: `AppSettings().BURST_WINDOW_ROWS == 2048`, `AppSettings().BURST_EVAL_INTERVAL_ROWS == 1024`.

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_config.py`:

```python
def test_burst_window_covers_long_bursts() -> None:
    """Field default window must span ~400 ms bursts at 0.2 ms resolution."""
    s = AppSettings(_env_file=None)
    # 393.1 ms / 0.2 ms = ~1966 rows; window must exceed that.
    assert s.BURST_WINDOW_ROWS >= 2000
    assert s.BURST_EVAL_INTERVAL_ROWS == s.BURST_WINDOW_ROWS // 2
```

(If `AppSettings` is not already imported in this file, add `from rfobserver.config import AppSettings`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_config.py::test_burst_window_covers_long_bursts -v`
Expected: FAIL (`BURST_WINDOW_ROWS` is 500).

- [ ] **Step 3: Raise the defaults**

In `src/rfobserver/config.py`, replace lines 119-120:

```python
    BURST_WINDOW_ROWS: int = 500  # rolling burst detection window (rows)
    BURST_EVAL_INTERVAL_ROWS: int = 250  # how often to run burst detection (rows)
```

with:

```python
    # Rolling burst-detection window. At PSD_TIME_RESOLUTION_MS=0.2, 2048 rows =
    # ~410 ms, so bursts up to ~400 ms are measured without the circular window
    # truncating their start. Cost vs the old 500-row window: the window buffer
    # is 2048 * NUM_FFT_BINS * 4 B (~8.4 MB at 1024 bins) and each eval runs
    # connected-component labeling over ~2.1 M cells; at the ~205 ms eval cadence
    # this is an acceptable duty cycle on the field Jetson.
    BURST_WINDOW_ROWS: int = 2048
    BURST_EVAL_INTERVAL_ROWS: int = 1024  # ~half the window (matches prior ratio)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_config.py::test_burst_window_covers_long_bursts -v`
Expected: PASS.

- [ ] **Step 5: Run the full unit suite to catch fallout**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`
Expected: all pass (no test hard-codes the old 500/250 defaults; if one does, update it to the new values).

- [ ] **Step 6: Commit**

```bash
git add src/rfobserver/config.py tests/unit/test_config.py
git commit -m "config: raise BURST_WINDOW_ROWS default to 2048 to measure ~400 ms bursts"
```

---

### Task 3: `derive_grid_params` helper

**Files:**
- Modify: `tests/integration/_synth.py`
- Test: `tests/integration/test_synth_helpers.py` (create)

**Interfaces:**
- Produces:
  ```python
  @dataclass(frozen=True)
  class GridParams:
      num_bins: int
      time_resolution_ms: float
      window_rows: int
      eval_interval_rows: int
      chunk_slices: int

  def derive_grid_params(fs_hz: int, occupied_bw_hz: float, duration_ms: float) -> GridParams
  ```
  Later tasks call `derive_grid_params(fs, b, d)` to build per-combo `AppSettings`.

- [ ] **Step 1: Write the failing test**

Create `tests/integration/test_synth_helpers.py`:

```python
"""Fast tests for the synthetic-waveform helpers (no NATS, no pipeline)."""

from __future__ import annotations

import math

import numpy as np

from ._synth import GridParams, derive_grid_params


def test_grid_params_window_holds_long_burst() -> None:
    """A 393.1 ms burst must fit inside the derived window with margin."""
    p = derive_grid_params(fs_hz=56_000_000, occupied_bw_hz=2_000_000, duration_ms=393.1)
    burst_rows = 393.1 / p.time_resolution_ms
    assert p.window_rows > burst_rows, "window must span the whole burst"
    assert p.eval_interval_rows <= p.window_rows
    assert p.chunk_slices >= 1


def test_grid_params_fft_resolves_narrow_burst() -> None:
    """A 50 kHz burst in a 28 MHz span must occupy at least a couple of bins."""
    p = derive_grid_params(fs_hz=28_000_000, occupied_bw_hz=50_000, duration_ms=10.24)
    bin_spacing = 28_000_000 / p.num_bins
    occupied_bins = 50_000 / bin_spacing
    assert occupied_bins >= 2.0
    assert 256 <= p.num_bins <= 8192
    assert (p.num_bins & (p.num_bins - 1)) == 0, "num_bins must be a power of two"


def test_grid_params_slice_has_enough_samples_for_fft() -> None:
    """time_resolution must give at least num_bins samples per slice."""
    for fs in (28_000_000, 56_000_000):
        for b in (50_000, 150_000, 500_000, 2_000_000, 20_000_000):
            for d in (1.3, 2.7, 10.24, 83.2, 393.1):
                p = derive_grid_params(fs_hz=fs, occupied_bw_hz=b, duration_ms=d)
                slice_samples = fs * p.time_resolution_ms / 1000.0
                assert slice_samples >= p.num_bins, (fs, b, d, slice_samples, p.num_bins)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_synth_helpers.py -q`
Expected: FAIL with `ImportError: cannot import name 'GridParams'`.

- [ ] **Step 3: Implement `GridParams` + `derive_grid_params` in `_synth.py`**

Add near the top of `tests/integration/_synth.py` (after the existing imports; add `import math` if absent):

```python
@dataclass(frozen=True)
class GridParams:
    """Per-combo PSD-grid / rolling-detector settings.

    Sample rate is pinned to a field value, so the grid must be sized so the
    burst is resolvable: enough FFT bins across the occupied bandwidth, enough
    time slices across the duration, and a rolling window that holds the whole
    burst.
    """

    num_bins: int
    time_resolution_ms: float
    window_rows: int
    eval_interval_rows: int
    chunk_slices: int


def _clamp_pow2(x: float, lo: int, hi: int) -> int:
    """Nearest power of two to *x*, clamped to [lo, hi] (both powers of two)."""
    if x <= lo:
        return lo
    if x >= hi:
        return hi
    exp = round(math.log2(x))
    return int(max(lo, min(hi, 2**exp)))


def derive_grid_params(
    fs_hz: int, occupied_bw_hz: float, duration_ms: float
) -> GridParams:
    """Size the PSD grid + rolling window for one (Fs, BW, duration) combo.

    - num_bins: aim for ~64 bins across the occupied BW, clamped to [256, 8192]
      and snapped to a power of two. Narrow-in-wide corners land near the
      frequency-resolution floor by physics.
    - time_resolution_ms: aim for ~40 slices across the burst, but floored so a
      slice holds >= num_bins samples (an FFT needs that many).
    - window_rows: hold the whole burst plus 50% margin, floored sensibly.
    """
    target_occupied_bins = 64
    ideal_n = target_occupied_bins * fs_hz / occupied_bw_hz
    num_bins = _clamp_pow2(ideal_n, lo=256, hi=8192)

    min_time_res_ms = num_bins / fs_hz * 1000.0
    target_slices = 40
    time_res_ms = max(duration_ms / target_slices, min_time_res_ms)

    burst_rows = duration_ms / time_res_ms
    window_rows = max(int(math.ceil(burst_rows * 1.5)) + 40, 100)
    eval_interval_rows = max(window_rows // 2, 20)
    chunk_slices = max(min(window_rows // 2, 200), 10)

    return GridParams(
        num_bins=num_bins,
        time_resolution_ms=time_res_ms,
        window_rows=window_rows,
        eval_interval_rows=eval_interval_rows,
        chunk_slices=chunk_slices,
    )
```

- [ ] **Step 4: Run the two passing helper tests (generator test still errors — expected)**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_synth_helpers.py -q`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/_synth.py tests/integration/test_synth_helpers.py
git commit -m "test: add derive_grid_params for per-combo PSD-grid sizing"
```

---

### Task 4: `make_iq_with_wideband_burst` generator

**Files:**
- Modify: `tests/integration/_synth.py`
- Test: `tests/integration/test_synth_helpers.py`

**Interfaces:**
- Produces:
  ```python
  def make_iq_with_wideband_burst(
      duration_sec: float,
      sample_rate_hz: int,
      *,
      burst_start_sec: float,
      burst_duration_sec: float,
      burst_bw_hz: float,
      burst_offset_hz: float,
      burst_amplitude: float,
      noise_stddev: float = 0.01,
      seed: int = 42,
  ) -> np.ndarray   # 1-D complex64
  ```
  Returns complex64 IQ; pack it with the existing `iq_to_sc16_int32`.

- [ ] **Step 1: Write the failing test**

Add the generator import to the top of `tests/integration/test_synth_helpers.py` (change the `._synth` import line to include it):

```python
from ._synth import GridParams, derive_grid_params, make_iq_with_wideband_burst
```

Then add to `tests/integration/test_synth_helpers.py`:

```python
def test_wideband_burst_occupies_expected_band() -> None:
    """The generated burst's PSD shows raised power across ~[offset +/- bw/2]."""
    from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid

    fs = 28_000_000
    bw = 2_000_000
    offset = 3_000_000
    iq = make_iq_with_wideband_burst(
        duration_sec=0.02,
        sample_rate_hz=fs,
        burst_start_sec=0.005,
        burst_duration_sec=0.010,
        burst_bw_hz=bw,
        burst_offset_hz=offset,
        burst_amplitude=0.5,
    )
    assert iq.dtype == np.complex64
    assert iq.shape == (int(0.02 * fs),)

    grid_res = compute_psd_grid(iq, fs, PSDGridConfig(num_bins=1024, time_resolution_ms=0.2))
    # Average PSD over the burst time slices (middle of the buffer).
    n = grid_res.grid.shape[0]
    burst_psd = grid_res.grid[n // 3 : 2 * n // 3].mean(axis=0)
    freqs = grid_res.freq_axis

    in_band = (freqs >= offset - bw / 2) & (freqs <= offset + bw / 2)
    # A guard band well away from the burst, used as the noise reference.
    out_band = (freqs >= -fs / 2 + 1_000_000) & (freqs <= -fs / 2 + 3_000_000)
    assert burst_psd[in_band].mean() - burst_psd[out_band].mean() > 15.0, (
        "occupied band must sit >15 dB above the out-of-band noise"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_synth_helpers.py::test_wideband_burst_occupies_expected_band -q`
Expected: FAIL with `ImportError`/`AttributeError` (function missing).

- [ ] **Step 3: Implement the generator in `_synth.py`**

Add to `tests/integration/_synth.py` (after `make_iq_with_bursts`):

```python
def make_iq_with_wideband_burst(
    duration_sec: float,
    sample_rate_hz: int,
    *,
    burst_start_sec: float,
    burst_duration_sec: float,
    burst_bw_hz: float,
    burst_offset_hz: float,
    burst_amplitude: float,
    noise_stddev: float = 0.01,
    seed: int = 42,
) -> np.ndarray:
    """complex64 IQ with a single band-limited-noise burst.

    The burst is white complex noise band-limited (in the frequency domain) to
    ``[burst_offset_hz - bw/2, burst_offset_hz + bw/2]``, normalized to unit RMS
    then scaled to ``burst_amplitude``, and shaped by a 5% raised-cosine time
    envelope. It presents a flat, fully-occupied ``bw`` in every time slice, so
    the PSD grid shows a clean duration x bandwidth rectangle.
    """
    n_samples = int(duration_sec * sample_rate_hz)
    rng = np.random.default_rng(seed)

    iq = (rng.standard_normal(n_samples) + 1j * rng.standard_normal(n_samples)).astype(
        np.complex64
    )
    iq *= np.float32(noise_stddev)

    i0 = max(0, int(burst_start_sec * sample_rate_hz))
    i1 = min(n_samples, int((burst_start_sec + burst_duration_sec) * sample_rate_hz))
    seg = i1 - i0
    if seg <= 1:
        return iq

    burst = (rng.standard_normal(seg) + 1j * rng.standard_normal(seg)).astype(np.complex64)
    spec = np.fft.fft(burst)
    freqs = np.fft.fftfreq(seg, d=1.0 / sample_rate_hz)
    band = (freqs >= burst_offset_hz - burst_bw_hz / 2) & (
        freqs <= burst_offset_hz + burst_bw_hz / 2
    )
    spec[~band] = 0.0
    burst = np.fft.ifft(spec).astype(np.complex64)

    rms = float(np.sqrt(np.mean(np.abs(burst) ** 2)))
    if rms > 0.0:
        burst = (burst / np.float32(rms)).astype(np.complex64)
    burst *= np.float32(burst_amplitude)

    env = np.ones(seg, dtype=np.float32)
    ramp = max(1, seg // 20)
    env[:ramp] = (0.5 * (1 - np.cos(np.pi * np.arange(ramp) / ramp))).astype(np.float32)
    env[-ramp:] = env[:ramp][::-1]

    iq[i0:i1] += (burst * env).astype(np.complex64)
    return iq
```

- [ ] **Step 4: Run the generator test**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_synth_helpers.py::test_wideband_burst_occupies_expected_band -q`
Expected: PASS. If the in-band/out-of-band gap is < 15 dB, raise `burst_amplitude` in the test (calibration); if the band edges bleed, that is expected and irrelevant to this task.

- [ ] **Step 5: Run the whole helper file + lint**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_synth_helpers.py -q`
Expected: all pass.
Run: `ruff check tests/ && ruff format --check tests/`
Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add tests/integration/_synth.py tests/integration/test_synth_helpers.py
git commit -m "test: add band-limited-noise wideband burst generator"
```

---

### Task 5: Matrix harness + one calibrated smoke combo

**Files:**
- Create: `tests/integration/test_burst_waveform_matrix.py`

**Interfaces:**
- Consumes: `GridParams`, `derive_grid_params`, `make_iq_with_wideband_burst`, `iq_to_sc16_int32`, `SyntheticBurstReceiver` (from `._synth`); `StreamingProcessor`, `AppSettings`, `SensorDatabase`, `LocalStorage`, `ReceiverConfig`.
- Produces (module-level helpers reused by Tasks 6-8):
  ```python
  SDR_CENTER_HZ = 915_000_000
  def build_matrix_settings(tmp_path, fs, params, threshold_high_db) -> AppSettings
  def run_combo(tmp_path, *, fs, occupied_bw_hz, duration_ms, offset_hz,
                amplitude, threshold_high_db, slow_gate=False) -> list[dict]
  def select_burst(detections, *, center_hz, occupied_bw_hz) -> dict | None
  def max_offset_hz(fs, occupied_bw_hz) -> float
  ```

- [ ] **Step 1: Write the harness + one smoke test**

Create `tests/integration/test_burst_waveform_matrix.py`:

```python
"""Wideband-burst detection matrix against the full StreamingProcessor.

Each combo generates a band-limited-noise burst of a given occupied bandwidth,
duration, and frequency offset, plays it through the real streaming pipeline at
a field sample rate, and asserts the detector reports the burst's presence,
duration, center frequency, and bandwidth within a tolerance derived from that
combo's own grid resolution (see derive_grid_params).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import StreamingProcessor
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.local import LocalStorage

from ._synth import (
    GridParams,
    SyntheticBurstReceiver,
    derive_grid_params,
    iq_to_sc16_int32,
    make_iq_with_wideband_burst,
)

if TYPE_CHECKING:
    from pathlib import Path

SDR_CENTER_HZ = 915_000_000
PRE_MARGIN_SEC = 0.02  # quiet lead-in before the burst


def max_offset_hz(fs: int, occupied_bw_hz: float) -> float:
    """Largest |offset| keeping the occupied band clear of the +/-Fs/2 edge."""
    return 0.45 * fs - occupied_bw_hz / 2.0


def build_matrix_settings(
    tmp_path: Path, fs: int, params: GridParams, threshold_high_db: float
) -> AppSettings:
    storage = tmp_path / "storage"
    storage.mkdir()
    settings = AppSettings(
        FREQUENCY_START=SDR_CENTER_HZ,
        FREQUENCY_END=SDR_CENTER_HZ,
        BANDWIDTH=fs,
        DURATION_SEC=0.5,
        GAIN=35,
        NUM_FFT_BINS=params.num_bins,
        PSD_TIME_RESOLUTION_MS=params.time_resolution_ms,
        STREAMING_CHUNK_SLICES=params.chunk_slices,
        BURST_WINDOW_ROWS=params.window_rows,
        BURST_EVAL_INTERVAL_ROWS=params.eval_interval_rows,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage),
        DB_PATH=str(tmp_path / "test.db"),
        ARCHIVE_MAX_GB=0.01,
        _env_file=None,
    )
    object.__setattr__(settings, "BURST_THRESHOLD_HIGH_DB", threshold_high_db)
    return settings


def select_burst(
    detections: list[dict], *, center_hz: float, occupied_bw_hz: float
) -> dict | None:
    """Pick the detection best matching the planted burst (freq overlap, then power)."""
    tol = max(occupied_bw_hz, 100_000.0)
    candidates = [d for d in detections if abs(d["center_freq_hz"] - center_hz) <= tol]
    if not candidates:
        return None
    return max(candidates, key=lambda d: d["peak_power_db"])


async def _run_until_exhausted_then_drain(
    processor: StreamingProcessor,
    receiver: SyntheticBurstReceiver,
    drain_chunks: int,
) -> None:
    async def stopper() -> None:
        while not receiver.exhausted:
            await asyncio.sleep(0.02)
        end_target = processor._capture_count + drain_chunks
        while processor._capture_count < end_target:
            await asyncio.sleep(0.02)
        processor.stop()

    await asyncio.wait_for(asyncio.gather(processor.run(), stopper()), timeout=120.0)


async def run_combo(
    tmp_path: Path,
    *,
    fs: int,
    occupied_bw_hz: float,
    duration_ms: float,
    offset_hz: float,
    amplitude: float,
    threshold_high_db: float,
) -> list[dict]:
    """Generate, stream, and return all detections for one combo."""
    params = derive_grid_params(fs, occupied_bw_hz, duration_ms)
    duration_sec = duration_ms / 1000.0
    buffer_sec = PRE_MARGIN_SEC + duration_sec + 0.02

    iq = make_iq_with_wideband_burst(
        duration_sec=buffer_sec,
        sample_rate_hz=fs,
        burst_start_sec=PRE_MARGIN_SEC,
        burst_duration_sec=duration_sec,
        burst_bw_hz=occupied_bw_hz,
        burst_offset_hz=offset_hz,
        burst_amplitude=amplitude,
    )
    sc16 = iq_to_sc16_int32(iq)

    settings = build_matrix_settings(tmp_path, fs, params, threshold_high_db)

    # Drain enough rows past the burst to flush it out of the trailing margin.
    drain_rows = params.window_rows + params.eval_interval_rows + 60
    drain_chunks = drain_rows // params.chunk_slices + 3

    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        receiver = SyntheticBurstReceiver(
            receiver_config=ReceiverConfig(
                gain_db=settings.GAIN,
                bandwidth_hz=settings.BANDWIDTH,
                duration_sec=settings.DURATION_SEC,
            ),
            iq_int32=sc16,
            pacing_factor=50.0,  # feed fast; lossless mode keeps it correct
        )
        receiver.initialize()
        storage = LocalStorage(
            storage_path=settings.STORAGE_PATH, max_gb=settings.ARCHIVE_MAX_GB
        )
        processor = StreamingProcessor(
            receiver=receiver,
            database=db,
            local_storage=storage,
            settings=settings,
            drop_on_overflow=False,
        )
        await _run_until_exhausted_then_drain(processor, receiver, drain_chunks)
        return await db.query_detections(limit=5000)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_smoke_wideband_combo(tmp_path: Path) -> None:
    """One representative combo: 28 MHz, 500 kHz occupied, 10.24 ms, offset."""
    fs, bw, dur = 28_000_000, 500_000, 10.24
    offset = 0.3 * max_offset_hz(fs, bw)
    detections = await run_combo(
        tmp_path,
        fs=fs,
        occupied_bw_hz=bw,
        duration_ms=dur,
        offset_hz=offset,
        amplitude=0.5,
        threshold_high_db=12.0,
    )
    burst = select_burst(
        detections, center_hz=SDR_CENTER_HZ + offset, occupied_bw_hz=bw
    )
    assert burst is not None, (
        f"no detection near planted burst; got {len(detections)} detections"
    )
```

- [ ] **Step 2: Run the smoke test and calibrate**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_burst_waveform_matrix.py::test_smoke_wideband_combo -q`
Expected: PASS.
If it fails to detect, raise `amplitude` (e.g. 0.5 -> 1.0) and/or lower `threshold_high_db` (e.g. 12 -> 10). If many spurious detections appear far from the burst, raise `threshold_high_db`. Record the working `(amplitude, threshold_high_db)` — they become the matrix defaults in Task 6.

- [ ] **Step 3: Lint + commit**

Run: `ruff check tests/ && ruff format --check tests/`
Expected: pass.

```bash
git add tests/integration/test_burst_waveform_matrix.py
git commit -m "test: wideband burst matrix harness + calibrated smoke combo"
```

---

### Task 6: Full parametrized matrix with measurement assertions

**Files:**
- Modify: `tests/integration/test_burst_waveform_matrix.py`

**Interfaces:**
- Consumes: `run_combo`, `select_burst`, `max_offset_hz`, `derive_grid_params`, `SDR_CENTER_HZ`.
- Produces: `assert_burst_measured(...)` reused by Tasks 7-8.

- [ ] **Step 1: Add the derived-tolerance assertion helper + the parametrized matrix**

Append to `tests/integration/test_burst_waveform_matrix.py`. Use the calibrated `AMPLITUDE` / `THRESHOLD_HIGH_DB` from Task 5:

```python
AMPLITUDE = 0.5  # from Task 5 calibration; adjust to the recorded value
THRESHOLD_HIGH_DB = 12.0  # from Task 5 calibration

SAMPLE_RATES = [28_000_000, 56_000_000]
OCCUPIED_BWS = [50_000, 150_000, 500_000, 2_000_000, 20_000_000]
DURATIONS_MS = [1.3, 2.7, 10.24, 83.2, 393.1]

# Combos marked slow: the ~400 ms bursts and the 83.2 ms x 56 MHz cases.
def _is_slow(fs: int, duration_ms: float) -> bool:
    return duration_ms >= 393.0 or (duration_ms >= 83.0 and fs >= 56_000_000)


def _combo_offset(fs: int, bw: float, index: int) -> float:
    """A deterministic, non-zero, sign-alternating offset that fits the band."""
    fracs = [0.3, -0.35, 0.5, -0.25, 0.4]
    return fracs[index % len(fracs)] * max_offset_hz(fs, bw)


def assert_burst_measured(
    burst: dict,
    *,
    fs: int,
    occupied_bw_hz: float,
    duration_ms: float,
    offset_hz: float,
) -> None:
    """Assert duration / center / bandwidth within per-combo derived tolerances."""
    params = derive_grid_params(fs, occupied_bw_hz, duration_ms)
    bin_spacing = fs / params.num_bins

    # Duration: slice quantization + the detector's end_row+1 bias.
    dur_tol_ms = max(3.0 * params.time_resolution_ms, 0.08 * duration_ms)
    assert abs(burst["duration_ms"] - duration_ms) <= dur_tol_ms, (
        f"duration {burst['duration_ms']:.3f} vs {duration_ms} "
        f"(tol {dur_tol_ms:.3f} ms)"
    )

    # Center frequency.
    ctr_tol = 2.0 * bin_spacing
    expected_center = SDR_CENTER_HZ + offset_hz
    assert abs(burst["center_freq_hz"] - expected_center) <= ctr_tol, (
        f"center {burst['center_freq_hz']:.0f} vs {expected_center:.0f} "
        f"(tol {ctr_tol:.0f} Hz)"
    )

    # Bandwidth: bin quantization + envelope broadening (generous at low TBP).
    bw_tol = max(4.0 * bin_spacing, 0.4 * occupied_bw_hz)
    assert abs(burst["bandwidth_hz"] - occupied_bw_hz) <= bw_tol, (
        f"bandwidth {burst['bandwidth_hz']:.0f} vs {occupied_bw_hz} "
        f"(tol {bw_tol:.0f} Hz)"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fs", "occupied_bw_hz", "duration_ms"),
    [
        pytest.param(
            fs,
            bw,
            dur,
            id=f"fs{fs // 1_000_000}M-bw{int(bw)}-dur{dur}ms",
            marks=pytest.mark.slow if _is_slow(fs, dur) else [],
        )
        for fs in SAMPLE_RATES
        for bw in OCCUPIED_BWS
        for dur in DURATIONS_MS
    ],
)
async def test_burst_matrix(
    tmp_path: Path, fs: int, occupied_bw_hz: float, duration_ms: float
) -> None:
    index = OCCUPIED_BWS.index(occupied_bw_hz) + DURATIONS_MS.index(duration_ms)
    offset = _combo_offset(fs, occupied_bw_hz, index)

    detections = await run_combo(
        tmp_path,
        fs=fs,
        occupied_bw_hz=occupied_bw_hz,
        duration_ms=duration_ms,
        offset_hz=offset,
        amplitude=AMPLITUDE,
        threshold_high_db=THRESHOLD_HIGH_DB,
    )
    burst = select_burst(
        detections, center_hz=SDR_CENTER_HZ + offset, occupied_bw_hz=occupied_bw_hz
    )
    assert burst is not None, (
        f"no detection near planted burst (fs={fs}, bw={occupied_bw_hz}, "
        f"dur={duration_ms}); got {len(detections)} detections"
    )
    assert_burst_measured(
        burst,
        fs=fs,
        occupied_bw_hz=occupied_bw_hz,
        duration_ms=duration_ms,
        offset_hz=offset,
    )
```

- [ ] **Step 2: Run the non-slow matrix and calibrate tolerances**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_burst_waveform_matrix.py -q -k test_burst_matrix`
Expected: all non-slow combos pass (slow ones show as skipped).
Calibration: if a specific corner fails a measurement assertion, widen only that dimension's tolerance formula (e.g. bump the `0.08 * duration_ms` floor, or the `0.4 * occupied_bw_hz` floor) — do NOT loosen to hide a real detector bug. If detection itself fails at a corner, adjust `AMPLITUDE` / `THRESHOLD_HIGH_DB`. Re-run until green.

- [ ] **Step 3: Run the slow matrix combos**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_burst_waveform_matrix.py -q -k test_burst_matrix --runslow`
Expected: all combos (including slow) pass. These take longer (large buffers); the 120 s per-combo timeout in the harness covers them.

- [ ] **Step 4: Lint + commit**

Run: `ruff check tests/ && ruff format --check tests/`
Expected: pass.

```bash
git add tests/integration/test_burst_waveform_matrix.py
git commit -m "test: full wideband burst matrix with derived-tolerance assertions"
```

---

### Task 7: Frequency-offset sweep

**Files:**
- Modify: `tests/integration/test_burst_waveform_matrix.py`

**Interfaces:**
- Consumes: `run_combo`, `select_burst`, `assert_burst_measured`, `max_offset_hz`, `SDR_CENTER_HZ`, `AMPLITUDE`, `THRESHOLD_HIGH_DB`.

- [ ] **Step 1: Add the offset-sweep test**

Append to `tests/integration/test_burst_waveform_matrix.py`:

```python
@pytest.mark.asyncio
@pytest.mark.parametrize("offset_frac", [-0.4, 0.0, 0.4])
async def test_offset_sweep_center_arithmetic(
    tmp_path: Path, offset_frac: float
) -> None:
    """One combo swept across offsets proves center = SDR_center + offset."""
    fs, bw, dur = 28_000_000, 500_000, 10.24
    offset = offset_frac * max_offset_hz(fs, bw)

    detections = await run_combo(
        tmp_path,
        fs=fs,
        occupied_bw_hz=bw,
        duration_ms=dur,
        offset_hz=offset,
        amplitude=AMPLITUDE,
        threshold_high_db=THRESHOLD_HIGH_DB,
    )
    burst = select_burst(detections, center_hz=SDR_CENTER_HZ + offset, occupied_bw_hz=bw)
    assert burst is not None, f"no detection at offset {offset:.0f} Hz"
    assert_burst_measured(
        burst, fs=fs, occupied_bw_hz=bw, duration_ms=dur, offset_hz=offset
    )
```

- [ ] **Step 2: Run the sweep**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_burst_waveform_matrix.py::test_offset_sweep_center_arithmetic -q`
Expected: 3 passed.

- [ ] **Step 3: Lint + commit**

Run: `ruff check tests/ && ruff format --check tests/`

```bash
git add tests/integration/test_burst_waveform_matrix.py
git commit -m "test: frequency-offset sweep for center-freq arithmetic"
```

---

### Task 8: Field-default long-burst validation (slow)

**Files:**
- Modify: `tests/integration/test_burst_waveform_matrix.py`

**Interfaces:**
- Consumes: `run_combo` is NOT used here (it derives per-combo params); this test uses the **field defaults** directly. It builds settings inline with `AppSettings` field defaults for `NUM_FFT_BINS`, `PSD_TIME_RESOLUTION_MS`, `BURST_WINDOW_ROWS`, `BURST_EVAL_INTERVAL_ROWS` and asserts a long burst is measured to full duration.

- [ ] **Step 1: Add the field-default test**

Append to `tests/integration/test_burst_waveform_matrix.py`:

```python
@pytest.mark.slow
@pytest.mark.asyncio
@pytest.mark.parametrize("duration_ms", [83.2, 393.1])
async def test_field_default_window_measures_long_burst(
    tmp_path: Path, duration_ms: float
) -> None:
    """With the raised field-default window, long bursts are not truncated.

    Uses the field NUM_FFT_BINS / PSD_TIME_RESOLUTION_MS / BURST_WINDOW_ROWS
    defaults (not per-combo tuning) to prove the deployed config measures the
    ~83 ms and ~393 ms modes to full duration.
    """
    fs, bw = 56_000_000, 2_000_000
    offset = 0.3 * max_offset_hz(fs, bw)
    defaults = AppSettings(_env_file=None)

    duration_sec = duration_ms / 1000.0
    buffer_sec = PRE_MARGIN_SEC + duration_sec + 0.02
    iq = make_iq_with_wideband_burst(
        duration_sec=buffer_sec,
        sample_rate_hz=fs,
        burst_start_sec=PRE_MARGIN_SEC,
        burst_duration_sec=duration_sec,
        burst_bw_hz=bw,
        burst_offset_hz=offset,
        burst_amplitude=AMPLITUDE,
    )
    sc16 = iq_to_sc16_int32(iq)

    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    settings = AppSettings(
        FREQUENCY_START=SDR_CENTER_HZ,
        FREQUENCY_END=SDR_CENTER_HZ,
        BANDWIDTH=fs,
        DURATION_SEC=0.5,
        GAIN=35,
        NUM_FFT_BINS=defaults.NUM_FFT_BINS,
        PSD_TIME_RESOLUTION_MS=defaults.PSD_TIME_RESOLUTION_MS,
        STREAMING_CHUNK_SLICES=defaults.STREAMING_CHUNK_SLICES,
        BURST_WINDOW_ROWS=defaults.BURST_WINDOW_ROWS,
        BURST_EVAL_INTERVAL_ROWS=defaults.BURST_EVAL_INTERVAL_ROWS,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage_dir),
        DB_PATH=str(tmp_path / "test.db"),
        ARCHIVE_MAX_GB=0.01,
        _env_file=None,
    )
    object.__setattr__(settings, "BURST_THRESHOLD_HIGH_DB", THRESHOLD_HIGH_DB)

    drain_rows = settings.BURST_WINDOW_ROWS + settings.BURST_EVAL_INTERVAL_ROWS + 60
    drain_chunks = drain_rows // settings.STREAMING_CHUNK_SLICES + 3

    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        receiver = SyntheticBurstReceiver(
            receiver_config=ReceiverConfig(
                gain_db=settings.GAIN,
                bandwidth_hz=settings.BANDWIDTH,
                duration_sec=settings.DURATION_SEC,
            ),
            iq_int32=sc16,
            pacing_factor=50.0,
        )
        receiver.initialize()
        storage = LocalStorage(
            storage_path=settings.STORAGE_PATH, max_gb=settings.ARCHIVE_MAX_GB
        )
        processor = StreamingProcessor(
            receiver=receiver,
            database=db,
            local_storage=storage,
            settings=settings,
            drop_on_overflow=False,
        )
        await _run_until_exhausted_then_drain(processor, receiver, drain_chunks)
        detections = await db.query_detections(limit=5000)
    finally:
        await db.close()

    burst = select_burst(detections, center_hz=SDR_CENTER_HZ + offset, occupied_bw_hz=bw)
    assert burst is not None, f"no detection for {duration_ms} ms field-default burst"
    # The key assertion: duration is NOT truncated to the old ~100 ms window.
    assert abs(burst["duration_ms"] - duration_ms) <= 0.15 * duration_ms, (
        f"field-default window truncated the burst: measured "
        f"{burst['duration_ms']:.1f} ms vs {duration_ms} ms"
    )
```

- [ ] **Step 2: Run the field-default test (slow)**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_burst_waveform_matrix.py::test_field_default_window_measures_long_burst -q --runslow`
Expected: 2 passed. If the 393.1 ms case measures ~100 ms, the window default (Task 2) did not take effect — recheck `config.py`.

- [ ] **Step 3: Lint + commit**

Run: `ruff check tests/ && ruff format --check tests/`

```bash
git add tests/integration/test_burst_waveform_matrix.py
git commit -m "test: field-default window measures 83/393 ms bursts (slow)"
```

---

### Task 9: Full-suite verification

**Files:** none (verification only).

- [ ] **Step 1: Lint + format**

Run: `ruff check src/ tests/ && ruff format --check src/ tests/`
Expected: pass.

- [ ] **Step 2: Type check**

Run: `PYTHONPATH= .venv/bin/mypy src/rfobserver/`
Expected: pass (no `src/` changes beyond the config default, which is type-clean).

- [ ] **Step 3: Unit tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`
Expected: all pass.

- [ ] **Step 4: Integration tests (default = slow skipped)**

Ensure NATS is running on `localhost:4222`, then:
Run: `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q`
Expected: all pass; slow combos reported as skipped.

- [ ] **Step 5: Integration tests including slow**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/ -q --runslow`
Expected: all pass (this is the long run).

- [ ] **Step 6: Final confirmation commit (if any calibration constants changed)**

If Steps 1-5 required tweaks, commit them:

```bash
git add -A
git commit -m "test: finalize wideband burst matrix calibration"
```

---

## Self-Review Notes

- **Spec coverage:** matrix (Task 6), 28/56 MHz only (constants), occupied-BW bursts (Task 4), per-combo sizing (Task 3), derived tolerances for detected/duration/center/bandwidth (Task 6 `assert_burst_measured`), offset per combo + sweep (Tasks 6/7), field-window finding + raised default + its test (Tasks 2/8), slow gating (Task 1), runtime mitigations (tight buffers + `pacing_factor=50`). All covered.
- **Calibration constants** (`AMPLITUDE`, `THRESHOLD_HIGH_DB`, tolerance floors) are given as starting values with an explicit, bounded calibration procedure — matching how the existing burst tests were calibrated. This is intended, not a placeholder.
- **Type consistency:** `GridParams` fields (`num_bins`, `time_resolution_ms`, `window_rows`, `eval_interval_rows`, `chunk_slices`) are used identically in `build_matrix_settings` and `derive_grid_params`. `run_combo`, `select_burst`, `assert_burst_measured`, `max_offset_hz` signatures match across Tasks 5-8.
