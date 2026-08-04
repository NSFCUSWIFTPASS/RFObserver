# Burst detector: recover narrowband bursts on a broadband pedestal

## Problem

Replaying a real 26 MS/s FHSS over-the-air capture
(`iq_capture_...915MHz_26.0Msps_20.0s_35dB_ssm_fhss_OVF.dat`) through RFObserver's
pipeline exposed a detector deficiency. The capture contains discrete narrowband
hops (~82 ms each, ~230-355 kHz wide) that step across 902-928 MHz throughout the
whole 20 s. Ground truth: a validated waterfall_plot.py CSV (9 hops in the first 2 s;
the transmission continues past 2 s).

RFObserver's `detect_bursts` collapsed these to ~2 detections spanning the full
26 MHz band, centered at the band midpoint (915.0 MHz), and missed the rest. Root
cause, established empirically:

- Each strong hop (~55-60 dB peak) sits on a **broadband pedestal** that lifts
  ~18-25% of FFT bins ~15 dB above the true noise floor.
- With `threshold_high_db = 10`, that pedestal crosses the low threshold, so
  8-connectivity CCL bridges the scattered pedestal bins into one band-spanning
  connected component. `bandwidth = f_max - f_min` (bounding box) then reports
  ~26 MHz and the geometric-midpoint center lands at 915.0 MHz.
- Once a full-span burst exists, the rolling tracker's match tolerance scales with
  track width (`rolling_burst.py:186`), so it absorbs every later hop → under-detection.

The validated reference (`gr-modules/iq-processing/waterfall_plot.py`) reproduces the
9-hop CSV exactly with: **median** per-bin floor, **peak-frequency** center,
`--burst-threshold 40`, `--burst-merge-time 0.030`. Confirmed byte-for-byte.

Minimal-hypothesis test in RFObserver: replay with `--threshold-db 40` alone already
fixed durations (all ~82 ms) and de-collapsed the centers (spread 906-927 MHz, no
26 MHz full-span), but left residual bridged-wide detections (3-11 MHz) with
midpoint-biased centers. The two remaining deltas versus the reference are the
**median floor** and **peak-frequency center**.

## Goal

Make RFObserver's shared burst detector match the validated reference's simple
thresholding so a replayed capture reproduces the reference hop list, without
regressing the existing synthetic burst matrix.

## Scope (decided)

Fix the **shared field detector** (`processing/burst.py` + `processing/spectral.py`),
not a replay-only path. Replay then validates the real field path. Threshold stays
configurable (the operator/replay sets it per signal strength; the field default is
unchanged). Guarded by the 54-combo synthetic burst matrix plus a new
replay-vs-reference acceptance test.

## Changes

### 1. Per-bin noise floor: configurable percentile, default median

`processing/spectral.py::compute_noise_floor(grid)` hardcodes the 10th percentile.

- Add a `percentile: float = 10.0` parameter (default 10 preserves the only other
  conceptual user, `tone_check.py`, which keeps its own `_NOISE_PERCENTILE`).
- `BurstDetectionConfig` gains `noise_floor_percentile: float = 50.0` (median).
- `config.py` gains `BURST_NOISE_FLOOR_PERCENTILE: float = 50.0`, wired into the
  `BurstDetectionConfig` built by the pipeline (same place the other `BURST_*`
  knobs are wired).
- `detect_bursts` calls `compute_noise_floor(grid, config.noise_floor_percentile)`.

Rationale: p10 sits below the true noise and lets low-level noise fragment
detections; the median sits at true noise for an FHSS grid (no bin occupied >50% of
a window) and is what the reference uses. Empirically p10 produced hundreds of
fragments where median produced clean per-hop components.

### 2. Add a peak-frequency field (do NOT change `center_freq_hz` semantics)

`_extract_fingerprints` sets `center_freq_hz = center + (f_min + f_max)/2` (midpoint)
and the rolling tracker reconstructs `f_lo/f_hi = center ± bandwidth/2`
(`rolling_burst.py:164-165`); viz and span queries also assume
`center ± bandwidth/2 == occupied range`. For a narrowband tone midpoint ≈ peak, but
for any asymmetric/partially-bridged component the midpoint is off (this is the
915.0 MHz artifact). The reference reports the peak bin as the burst frequency.

Design decision (per review): **keep `center_freq_hz` = geometric midpoint,
unchanged everywhere**, and **add a new `peak_freq_hz` field** carrying the
peak-power bin frequency. This is purely additive — the `center ± bandwidth/2`
invariant, the rolling tracker, viz, and span queries are untouched.

- Add `peak_freq_hz: float` to `BurstFingerprint` (models.py); document it as the
  peak-power frequency (vs `center_freq_hz` = occupied-band midpoint).
- `_extract_fingerprints`: compute the peak column within the component and set
  `peak_freq_hz = center + freq_axis[peak_col]`. `center_freq_hz`, `bandwidth_hz`
  unchanged.
- `rolling_burst.py`: unchanged (still derives `f_lo/f_hi` from `center ±
  bandwidth/2`). When the tracker merges detections into a `_TrackedBurst`, carry
  the peak of the highest-power constituent through to the emitted fingerprint's
  `peak_freq_hz`.
- DB (`storage/database.py`) and API: persist/expose `peak_freq_hz` additively
  (new column tolerant of old NULL rows; new JSON key). No change to existing
  `center_freq_hz` columns/keys.

The acceptance test (below) compares the reference hop centers against
`peak_freq_hz`. With the median floor + adequate threshold making bursts narrow,
`center_freq_hz` (midpoint) will usually also land near the hop; `peak_freq_hz` is
the robust one to assert.

### 3. Merge time

The field `BURST_MERGE_TIME_MS` default is 3 ms; the reference used 30 ms to stitch a
hop that briefly dips. Do **not** change the field default (regression risk); instead
ensure the value is threaded through and let the acceptance test set it (replay/config)
to reproduce the reference. No code change if it is already configurable end-to-end —
verify it is.

## Acceptance criteria

1. **Reference reproduction (new integration test).** Replaying the SSM capture,
   first 2 s, `threshold_db=40`, `noise_floor_percentile=50`, `merge_time≈30 ms`,
   yields detections matching the committed reference CSV within tolerance:
   - count within +/-2 of 9;
   - for each reference hop, a detection whose peak center is within ~2 FFT bins
     (~50 kHz at 2048-bin/26 MS/s) of the reference center;
   - durations within ~10% of ~82 ms;
   - bandwidths within ~2x of the reference (~230-355 kHz), and **no full-span
     (>= 20 MHz) detection**.
   (The capture is large/local; gate this test behind a marker/skip-if-absent so CI
   without the file still passes. Store the expected hop table inline in the test.)
2. **No field regression.** The existing 54-combo synthetic burst matrix and all unit
   tests still pass with the new defaults. If the median default regresses any matrix
   case, keep the code path but reconcile the default (documented) so both pass.
3. Full CI green per CLAUDE.md (ruff check + format, mypy, unit, integration).

## Files

- `src/rfobserver/processing/spectral.py` — `compute_noise_floor(grid, percentile=10.0)`
- `src/rfobserver/processing/burst.py` — config field; pass percentile; compute
  `peak_freq_hz`
- `src/rfobserver/processing/rolling_burst.py` — carry the peak of the
  highest-power constituent into the emitted `peak_freq_hz` (tracker logic
  otherwise unchanged)
- `src/rfobserver/models.py` — add `BurstFingerprint.peak_freq_hz`
- `src/rfobserver/config.py` — `BURST_NOISE_FLOOR_PERCENTILE = 50.0`
- `src/rfobserver/storage/database.py` + web routes — persist/expose `peak_freq_hz`
  (additive; `center_freq_hz` unchanged)
- `tests/unit/test_burst.py` (+ matrix) — floor percentile, peak center, edges
- `tests/integration/test_replay_reference.py` — reference-reproduction test

## Out of scope

- Automatic per-signal threshold adaptation (the field default threshold is
  unchanged; strong-on-pedestal signals still require an operator-set threshold).
- Changing the field PSD time resolution or the rolling window sizing.
