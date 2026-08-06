# Restore realtime per-chunk stats on the sensor (subsample moments, keep max full-res)

## Problem (live regression)

The interval-statistics parity change (merged in `feature/stats-interval-averaging`) made
`moments_from_iq` full-resolution: per chunk it does `abs`+square, `sum`, `dot`, and a
`np.histogram` over ALL ~2M samples (4000 log-spaced bins). On the HCRO Jetson at 56 MS/s
this costs **100-300 ms/chunk** (was ~5 ms when subsampled), so:

- `total` per chunk = 150-350 ms for 39.4 ms of IQ -> pipeline runs **4-9x slower than realtime**;
- repeated **`UHD overflow - lost samples`**;
- CPU saturated -> the asyncio event loop that runs the heartbeat + WS broadcast is starved
  -> `/ws/live` opens but delivers **nothing (no heartbeat, no PSD)** -> dashboard shows no
  live data.

Confirmed on `rf-nano-002@10.1.42.31` (running latest `3e70980`): WORKER logs show
`stats=258/296/148 ms`, recurring UHD overflow warnings, SSH sluggish; local mock on a fast
box is fine. The dominant cost is the full-array `np.histogram` (searchsorted per element).

## Goal

Per-chunk stats back to ~5-10 ms so the sensor processes in realtime (no overflow) and the
event loop stays responsive (live data returns), while interval statistics still match
rf-processor within sampling noise and `max` stays exact.

## Design

Change ONLY `moments_from_iq` in `processing/iq_utils.py`:

- Compute **`max_pow` over the FULL array**, cheaply and exactly, without `sqrt`:
  `max_pow = float(np.max(data.real.astype(np.float64) ** 2 + data.imag.astype(np.float64) ** 2))`
  (one O(n) reduction; keeps the true peak that rf-processor reports).
- **Subsample the rest** (the expensive sums + histogram) to ~262K samples, exactly as the
  original pre-parity code did for speed:
  `step = max(1, n_full // (1 << 18)); sub = data[::step]`
  then compute `n`, `s_abs`, `s_pow`, `s_pow2`, `hist` on `sub` (float64).
  `n` is the SUBSAMPLE count so `mean = s_pow / n` etc. stay correct.

Everything else is unchanged: `IQMoments.add` folds subsampled sums/hists across the interval
(a 262K-sample subsample per chunk, folded over a whole DURATION, is a very large effective
sample -> mean/std/kurtosis match rf-processor to well within sampling noise; median from the
folded histogram is within a bin; `max` is exact). `finalize_moments`, the pipeline wiring,
config, and the heartbeat/WS code are untouched.

Why this over "keep full-res": the real sensor proves full-res per-chunk is too costly for the
Jetson; subsampling restores realtime. This is the "subsample moments, full-res max" option
from the original design, now chosen with real-sensor evidence.

## Testing

- Update `tests/unit/test_iq_stats_parity.py`:
  - Keep an EXACT formula test by constructing `IQMoments` directly from full-array sums (no
    subsampling) and asserting `finalize_moments` matches a verbatim rf-processor copy to
    < 1e-6 (average/std/kurtosis) - proves the math is still correct.
  - Relax the `moments_from_iq`-folded test to sampling-noise tolerances: average within
    0.2 dB, std within 5%, kurtosis within 5%, median within 0.1 dB, and **max EXACT** (the
    point of full-res max). Keep the fold-order-independence test.
- New micro-perf assertion (fast, no hardware): `moments_from_iq` on a 2,000,000-sample
  complex64 chunk returns in well under the old full-res path; assert it processes at least,
  say, 5 such chunks in < 1 s on CI (loose, just guards against a full-array histogram
  regression). Mark/skip if timing is too environment-sensitive; primary proof is the redeploy
  measurement.
- Full CI per CLAUDE.md (ruff, mypy, unit, integration).

## Verification on hardware (post-merge)

Redeploy to `rf-nano-002@10.1.42.31` (git pull + `pip install --user --force-reinstall --no-deps .`
+ `systemctl restart rfobserver` - match how it is installed there: user site-packages) and
confirm: WORKER `stats=` drops to ~5-10 ms, `total` < 39 ms (realtime), UHD overflow warnings
stop, and `/ws/live` delivers heartbeat + PSD (dashboard live data returns).

## Files

- `src/rfobserver/processing/iq_utils.py` - `moments_from_iq` (subsample sums/hist, full-res max).
- `tests/unit/test_iq_stats_parity.py` - exact formula test + relaxed subsample-accuracy test.

## Out of scope

- PSD (`compute_psd_grid`) cost (40-77 ms/chunk) - within the realtime budget once stats is
  cheap; revisit only if the sensor still can't keep up after this fix.
- Any change to `finalize_moments`, the interval accumulation, or the envelope.
