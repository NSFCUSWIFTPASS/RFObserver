# Burst Detector Narrowband Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make RFObserver's shared burst detector match the validated waterfall_plot.py simple-thresholding so a replayed FHSS capture reproduces the reference hop list, without regressing the synthetic burst matrix.

**Architecture:** Three additive/contained changes to the existing dual-threshold CCL detector: (1) per-bin noise floor becomes a configurable percentile defaulting to median; (2) a new additive `peak_freq_hz` field reports the peak-power bin (existing `center_freq_hz` midpoint semantics unchanged); (3) both flow through the rolling tracker, DB, and API. A skip-if-absent integration test asserts reproduction of the committed reference CSV.

**Tech Stack:** Python 3.10+, numpy, scipy.ndimage, aiosqlite, pydantic. Env: prefix every Python command with `PYTHONPATH=` (host leaks system 3.10 into the venv). ruff is global (no venv prefix).

## Global Constraints

- No emojis anywhere; no em-dashes in code/docs/UI. No "Co-Authored-By: Claude" in commits.
- Every Python command prefixed with `PYTHONPATH=`; use `.venv/bin/...`.
- All existing checks must stay green (CLAUDE.md): `ruff check src/ tests/`, `ruff format --check src/ tests/`, `PYTHONPATH= .venv/bin/mypy src/rfobserver/`, `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`, `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q` (needs NATS on :4222).
- `center_freq_hz` semantics (geometric midpoint of the occupied band) MUST NOT change; `peak_freq_hz` is purely additive.
- Reference capture: `~/Documents/iq_capture_hcro-rpi-002_2025-12-12T19-33-52.92Z_915MHz_26.0Msps_20.0s_35dB_ssm_fhss_OVF.dat` (raw ci16_le, 26 MS/s, center 915e6). Reference CSV committed at `~/GitHub/gr-modules/iq-processing/waterfall_plot_iq_capture_hcro-rpi-002_2025-12-12T19-33-52.92Z_915MHz_26.0Msps_20.0s_35dB_ssm_fhss_OVF.csv`.

---

### Task 1: Configurable noise-floor percentile (default median)

**Files:**
- Modify: `src/rfobserver/processing/spectral.py:160-163` (`compute_noise_floor`)
- Modify: `src/rfobserver/processing/burst.py:20-32` (`BurstDetectionConfig`), `:70` (call site)
- Modify: `src/rfobserver/config.py` (after line 69, the `BURST_*` block)
- Modify: `src/rfobserver/pipeline/continuous.py:348` and `src/rfobserver/pipeline/streaming.py:1153` (both `BurstDetectionConfig(...)` build sites)
- Test: `tests/unit/test_burst.py`

**Interfaces:**
- Produces: `compute_noise_floor(grid, percentile: float = 10.0)`; `BurstDetectionConfig.noise_floor_percentile: float = 50.0`; `AppSettings.BURST_NOISE_FLOOR_PERCENTILE: float = 50.0`.

- [ ] **Step 1: Write the failing test** in `tests/unit/test_burst.py`:

```python
import numpy as np
from rfobserver.processing.spectral import compute_noise_floor

def test_compute_noise_floor_percentile_param():
    # 100 rows: 90 at 0 dB, 10 at 100 dB -> p10=0, median=0, p95=100
    grid = np.zeros((100, 4), dtype=np.float32)
    grid[90:, :] = 100.0
    assert np.allclose(compute_noise_floor(grid), 0.0)              # default p10
    assert np.allclose(compute_noise_floor(grid, 50.0), 0.0)        # median
    assert np.allclose(compute_noise_floor(grid, 95.0), 100.0)      # high pct
```

- [ ] **Step 2: Run it, expect fail** — `PYTHONPATH= .venv/bin/pytest tests/unit/test_burst.py::test_compute_noise_floor_percentile_param -x -q` → TypeError (unexpected arg).

- [ ] **Step 3: Implement.** In `spectral.py`:

```python
def compute_noise_floor(grid: np.ndarray, percentile: float = 10.0) -> np.ndarray:
    """Estimate per-bin noise floor as the given percentile across time slices."""
    result: np.ndarray = np.percentile(grid, percentile, axis=0).astype(np.float32)
    return result
```

In `burst.py` `BurstDetectionConfig` add field `noise_floor_percentile: float = 50.0`, and change line 70 to `noise_floor = compute_noise_floor(grid, config.noise_floor_percentile)`.

In `config.py` after the `BURST_MERGE_TIME_MS` line add:
```python
    BURST_NOISE_FLOOR_PERCENTILE: float = 50.0
```

In `continuous.py:348` and `streaming.py:1153` add `noise_floor_percentile=settings.BURST_NOISE_FLOOR_PERCENTILE,` to the `BurstDetectionConfig(...)` kwargs (match the existing indentation/style at each site).

- [ ] **Step 4: Run** the test → PASS. Also `PYTHONPATH= .venv/bin/pytest tests/unit/test_config.py -x -q` (config field exposed).

- [ ] **Step 5: Commit** — `git add -A && git commit -m "burst: configurable noise-floor percentile, default median"`.

---

### Task 2: Add `peak_freq_hz` to the fingerprint + compute it in detection

**Files:**
- Modify: `src/rfobserver/models.py:103-113` (`BurstFingerprint`)
- Modify: `src/rfobserver/processing/burst.py:116-173` (`_extract_fingerprints`)
- Test: `tests/unit/test_burst.py`

**Interfaces:**
- Consumes: `BurstDetectionConfig.noise_floor_percentile` (Task 1).
- Produces: `BurstFingerprint.peak_freq_hz: float` = absolute Hz of the peak-power bin; `center_freq_hz` unchanged (midpoint).

- [ ] **Step 1: Write the failing test** in `tests/unit/test_burst.py`:

```python
from datetime import datetime, timezone
from rfobserver.processing.burst import detect_bursts, BurstDetectionConfig
from rfobserver.processing.spectral import PSDGridResult

def test_peak_freq_hz_reports_peak_bin_not_midpoint():
    # 20 rows x 8 bins; occupied band bins 2..6, but the PEAK is at bin 3 (asymmetric).
    grid = np.full((20, 8), -100.0, dtype=np.float32)
    grid[5:15, 2:7] = -50.0   # occupied plateau -> midpoint at bin 4
    grid[5:15, 3] = -10.0     # strong peak at bin 3
    freq_axis = (np.arange(8) - 4) * 1_000_000.0   # 1 MHz bins, centered
    psd = PSDGridResult(grid=grid, time_axis=np.arange(20) * 0.001,
                        freq_axis=freq_axis, ffts_per_slice=1, total_ffts=20)
    cfg = BurstDetectionConfig(threshold_high_db=20.0, min_duration_sec=0.0)
    res = detect_bursts(psd, cfg, center_freq_hz=915e6,
                        capture_time=datetime.now(timezone.utc))
    assert len(res.bursts) == 1
    b = res.bursts[0]
    assert abs(b.peak_freq_hz - (915e6 + freq_axis[3])) < 1.0     # peak = bin 3
    assert abs(b.center_freq_hz - (915e6 + freq_axis[4])) < 1.0   # midpoint = bin 4 (unchanged)
```

- [ ] **Step 2: Run, expect fail** — `AttributeError: peak_freq_hz` (or pydantic missing field).

- [ ] **Step 3: Implement.** In `models.py` `BurstFingerprint` add after `center_freq_hz`:
```python
    peak_freq_hz: float = 0.0  # absolute Hz of the peak-power bin (center_freq_hz is the band midpoint)
```
In `burst.py` `_extract_fingerprints`, the peak index is already computed (`peak_idx` over `region_powers = grid[rows, cols]`). Map it to a frequency and pass it through:
```python
        peak_idx = int(np.argmax(region_powers))
        peak_power = float(region_powers[peak_idx])
        peak_freq_hz = center_freq_hz + float(freq_axis[cols[peak_idx]])
        ...
        bursts.append(
            BurstFingerprint(
                ...
                center_freq_hz=burst_center_freq,
                peak_freq_hz=peak_freq_hz,
                ...
            )
        )
```

- [ ] **Step 4: Run** the test → PASS. Then `PYTHONPATH= .venv/bin/pytest tests/unit/test_burst.py -x -q`.

- [ ] **Step 5: Commit** — `git commit -am "burst: add peak_freq_hz (peak-power bin) alongside midpoint center"`.

---

### Task 3: Carry `peak_freq_hz` through the rolling tracker

**Files:**
- Modify: `src/rfobserver/processing/rolling_burst.py:36-46` (`_TrackedBurst`), `:170-212` (`_absorb`), `:238-253` (`_to_fingerprint`)
- Test: `tests/unit/test_rolling_burst.py`

**Interfaces:**
- Consumes: `BurstFingerprint.peak_freq_hz` (Task 2).
- Produces: emitted fingerprints carry the `peak_freq_hz` of the highest-power constituent detection.

- [ ] **Step 1: Write the failing test** in `tests/unit/test_rolling_burst.py` — feed the detector PSD windows containing one asymmetric burst (peak off-center) and assert the emitted fingerprint's `peak_freq_hz` equals the peak bin (reuse the grid-building helper already in that test module; if none, build a small `PSDGridResult` inline like Task 2). Assert `emitted.peak_freq_hz` is nearer the peak bin than the midpoint.

- [ ] **Step 2: Run, expect fail** (emitted `peak_freq_hz == 0.0`).

- [ ] **Step 3: Implement.** Add `peak_freq_hz: float` to `_TrackedBurst`. In `_evaluate` (around line 156-166) pass `burst.peak_freq_hz` into `_absorb` (add a parameter). In `_absorb`, when merging (line 196 area), adopt the stronger constituent's peak BEFORE updating power:
```python
                if burst.peak_power_db > t.peak_power_db:
                    t.peak_freq_hz = burst.peak_freq_hz
                t.peak_power_db = max(t.peak_power_db, burst.peak_power_db)
```
Set `peak_freq_hz=burst.peak_freq_hz` in the new-track `_TrackedBurst(...)` (line 201-212). In `_to_fingerprint` (line 245) add `peak_freq_hz=t.peak_freq_hz,`.

Update the `_absorb` signature and its call site to thread `peak_freq_hz` (or pass the whole `burst`, which `_absorb` already receives as `burst` — prefer reading `burst.peak_freq_hz` directly inside `_absorb`, avoiding a new parameter).

- [ ] **Step 4: Run** — `PYTHONPATH= .venv/bin/pytest tests/unit/test_rolling_burst.py -x -q` → PASS.

- [ ] **Step 5: Commit** — `git commit -am "rolling_burst: carry peak_freq_hz of strongest constituent"`.

---

### Task 4: Persist and expose `peak_freq_hz` (DB + API)

**Files:**
- Modify: `src/rfobserver/storage/database.py` (schema dict ~line 80, `insert_detection` ~152-190, `query_detections` SELECT)
- Modify: `src/rfobserver/web/routes/api.py` (`detections_json` ~594, `detections_fragment` ~537 if it lists per-row fields)
- Modify: the pipeline call site(s) that call `insert_detection(...)` with a `BurstFingerprint` (grep `insert_detection(`)
- Test: `tests/unit/test_database.py`

**Interfaces:**
- Consumes: `BurstFingerprint.peak_freq_hz`.
- Produces: `detections.peak_freq_hz` column; `insert_detection(..., peak_freq_hz: float = 0.0)`; `query_detections` rows include `peak_freq_hz`; `/api/detections.json` includes `peak_freq_hz`.

- [ ] **Step 1: Write the failing test** in `tests/unit/test_database.py`: connect an in-memory/temp DB, `insert_detection(..., peak_freq_hz=917.3e6)`, `query_detections()`, assert the returned row has `peak_freq_hz == 917.3e6`. (Mirror an existing insert/query test in that file for setup.)

- [ ] **Step 2: Run, expect fail** (unexpected kwarg / missing key).

- [ ] **Step 3: Implement.**
  - Add `peak_freq_hz REAL` to the `detections` CREATE TABLE and to the additive-migration schema dict near line 80 (`"peak_freq_hz": "REAL"`) so existing DBs get an `ALTER TABLE ... ADD COLUMN` tolerant of NULL.
  - Add `peak_freq_hz: float = 0.0` param to `insert_detection`, include it in the column list + VALUES + params tuple.
  - Add `peak_freq_hz` to the `query_detections` SELECT (it uses `row_factory = aiosqlite.Row`, so dict rows pick it up; confirm the SELECT is explicit-columns and add it there).
  - At the `insert_detection(...)` call site(s), pass `peak_freq_hz=burst.peak_freq_hz`.
  - In `api.py` `detections_json`, add `"peak_freq_hz": r.get("peak_freq_hz")` to the serialized dict; in `detections_fragment` optionally show peak MHz next to center (keep `colspan` correct if a column is added — otherwise leave the table as-is).

- [ ] **Step 4: Run** — `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py tests/unit/test_web_routes.py -x -q` → PASS.

- [ ] **Step 5: Commit** — `git commit -am "storage/api: persist and expose peak_freq_hz"`.

---

### Task 5: Reference-reproduction integration test + regression verification

**Files:**
- Create: `tests/integration/test_replay_reference.py`
- Verify (no change expected): `tests/integration/test_burst_waveform_matrix.py`

**Interfaces:**
- Consumes: `rfobserver.pipeline.replay.run_replay` (raw mode: `sample_rate_hz`, `center_freq_hz`, `datatype`, `max_seconds`, `threshold_db`), and `peak_freq_hz` in returned detections.

- [ ] **Step 1: Write the test.** Skip if the capture file is absent so CI without it still passes:

```python
import os
import pytest
from rfobserver.pipeline.replay import run_replay

CAP = os.path.expanduser(
    "~/Documents/iq_capture_hcro-rpi-002_2025-12-12T19-33-52.92Z_915MHz_26.0Msps_20.0s_35dB_ssm_fhss_OVF.dat"
)
# Reference hop peak centers (MHz), first 2 s, from the validated waterfall_plot.py CSV.
REF_MHZ = [917.260, 923.912, 906.189, 922.109, 913.400, 909.490, 919.697, 926.299, 902.889]

@pytest.mark.skipif(not os.path.exists(CAP), reason="SSM FHSS capture not present")
@pytest.mark.asyncio
async def test_replay_reproduces_reference_hops():
    res = await run_replay(
        CAP, sample_rate_hz=26_000_000, center_freq_hz=915_000_000,
        datatype="ci16_le", threshold_db=40.0, max_seconds=2.0,
    )
    dets = res["detections"]
    assert not any(d["bandwidth_hz"] >= 20_000_000 for d in dets), "full-span collapse"
    assert abs(len(dets) - len(REF_MHZ)) <= 2, f"{len(dets)} detections"
    for ref in REF_MHZ:
        peaks = [d.get("peak_freq_hz", d["center_freq_hz"]) / 1e6 for d in dets]
        assert any(abs(p - ref) < 0.06 for p in peaks), f"no hop near {ref} MHz in {sorted(round(p,3) for p in peaks)}"
    # durations ~82 ms
    near82 = [d for d in dets if abs(d["duration_ms"] - 82.0) < 12.0]
    assert len(near82) >= len(REF_MHZ) - 2
```

- [ ] **Step 2: Run it** — `PYTHONPATH= .venv/bin/pytest tests/integration/test_replay_reference.py -x -q`. It exercises the new median floor + peak_freq_hz end-to-end. If it fails on count/bandwidth, tune only the test's `threshold_db`/`max_seconds` and (if needed) pass `run_replay` a merge-time/window that mirrors the reference `--burst-merge-time 0.030`; do NOT weaken the assertions below the spec tolerances. If a genuine detector gap remains (e.g. residual bridged-wide bursts), STOP and report — do not loosen the full-span assertion.

- [ ] **Step 3: Regression check.** Run the synthetic matrix and the whole suite:
```
PYTHONPATH= .venv/bin/pytest tests/integration/test_burst_waveform_matrix.py -x -q
PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q
```
If the median-floor default regresses any matrix case, do NOT loosen the matrix; instead reconcile `BURST_NOISE_FLOOR_PERCENTILE` (document the chosen value in the spec + config docstring) so both the matrix and the reference test pass, then re-run both.

- [ ] **Step 4: Full CI** (per CLAUDE.md), all green:
```
ruff check src/ tests/ && ruff format --check src/ tests/
PYTHONPATH= .venv/bin/mypy src/rfobserver/
PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q
PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q
```

- [ ] **Step 5: Commit** — `git commit -am "test: replay reproduces validated FHSS reference hops"`.

## Self-Review (author)

- Spec coverage: Task 1 = median floor; Task 2/3 = peak_freq_hz through detection + tracker; Task 4 = DB/API; Task 5 = acceptance + regression. All spec sections covered.
- Placeholders: none; each code step shows the code.
- Type consistency: `peak_freq_hz: float` used identically in model, detector, tracker, DB, API, and test.
