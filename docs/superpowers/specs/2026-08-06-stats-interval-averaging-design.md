# Interval statistics: match rf-processor's whole-capture computation

## Problem

RFObserver's per-interval IQ statistics (sent to ZMS + RFS NATS) do not match the
reference `rf-processor` (`reference_software/rf-processor/src/rf_processor/iq_utils.py`,
invoked over a whole capture in `processing.py:39-49`). Validated against identical
IQ, three independent causes:

1. **`std` is a different quantity.** rf-processor: `np.std(np.abs(data))` (std of the
   magnitude). RFObserver (`processing/iq_utils.py:64`): `sqrt(mean|z|^2 - mean(Re)^2 -
   mean(Im)^2)` = `sqrt(Var(Re)+Var(Im))`. For noise-like IQ this is ~2x larger, always.
2. **`max` is subsampled.** All stats run on `data[::step]` (~262K samples,
   `iq_utils.py:50-51`); `max` then under-reports the peak (~0.8 dB in a 4M test, worse
   at the real ~28M-sample interval).
3. **Statistics are not interval-averaged.** The envelope uses `statistics=result.iq_stats`
   (`pipeline/streaming.py:1356`) which is the **last ~37 ms chunk**, while rf-processor
   computes over the **whole DURATION capture**. Only the PSD `powers` array is averaged.
   So average/median/kurtosis match only for a stationary signal and are noisier.

Reference formulas (rf-processor, to reproduce exactly), with `p = |z|^2`:
- `average = 10*log10(mean(p)/50)`
- `max     = 10*log10(max(p)/50)`
- `median  = 10*log10(median(p)/50)`
- `std     = std(|z|)`
- `kurtosis (spectral) = (M * S2/S1^2 - 1) * (M+1)/(M-1)`, `S1=sum(p)`, `S2=sum(p^2)`, `M=len`

## Goal

Per-interval statistics sent to ZMS/NATS reproduce rf-processor's whole-capture result
(within numerical noise; median within one histogram bin), computed incrementally over
the streaming chunks so no full interval of IQ is retained.

## Decisions (from review)

- **Full-resolution** moment accumulation (every sample of every chunk); a few ms extra
  CPU per chunk is acceptable. No subsampling.
- **Median via a streaming log-spaced `|z|^2` histogram** (uniform-in-dB bins). Exact
  online median needs Omega(n) memory (retaining every sample, ~112 MB/interval at
  56 MS/s x 0.5 s), which breaks the streaming design; the histogram matches
  rf-processor to within one bin (~0.02-0.05 dB) at a few KB of state.

## Key insight

Every statistic except median folds exactly from additive moments:
`{count N, S_abs=sum|z|, S_pow=sum p, S_pow2=sum p^2, max_pow=max p}` plus a histogram of
`p` for the median. Summing these across an interval's chunks and finalizing is
identical to computing over the concatenated samples. So:
- `average = 10*log10(S_pow/N/50)`
- `std     = sqrt(max(0, S_pow/N - (S_abs/N)^2))`  (= std(|z|), exact)
- `kurtosis = (N*S_pow2/S_pow^2 - 1)*(N+1)/(N-1)`
- `max     = 10*log10(max_pow/50)`  (full-resolution running max)
- `median  = 10*log10(hist_percentile_50(p)/50)`

## Design

### New: `IQMoments` + moment-based stats (`processing/iq_utils.py`)

- `HIST_EDGES`: fixed log-spaced bin edges over `p=|z|^2` spanning the representable
  range (e.g. 1e-12 .. 1e2, ~4000 bins → ~0.02 dB/bin near the noise floor). Module
  constant, computed once.
- `@dataclass IQMoments`: `n: int`, `s_abs: float`, `s_pow: float`, `s_pow2: float`,
  `max_pow: float`, `hist: np.ndarray` (int64 counts, len == len(HIST_EDGES)-1).
  - `def add(self, other: IQMoments)` (or `__iadd__`): field-wise sum, `max` for
    `max_pow`, `+` for `hist`. Used to fold chunks across an interval.
- `def moments_from_iq(data: np.ndarray) -> IQMoments`: full-resolution, one pass:
  `mag = np.abs(data)` (float64 to avoid float32 sum error over 10s of M samples);
  `p = mag*mag`; `n=len`, `s_abs=mag.sum()`, `s_pow=p.sum()`,
  `s_pow2=np.dot(p,p)`, `max_pow=p.max()`, `hist=np.histogram(p, bins=HIST_EDGES)[0]`.
- `def finalize_moments(m: IQMoments) -> IQStatistics`: applies the formulas above;
  median from the histogram (cumsum → first bin crossing N/2 → bin center in `p`, then
  to dB). Guard N==0 / empty.
- **Replace** `calculate_iq_statistics(data)` body with
  `finalize_moments(moments_from_iq(data))` so the single-array path (and any caller /
  test) now uses the corrected formulas (fixes `std` and `max` everywhere, including the
  live per-chunk dashboard value). Keep the signature and `IQStatistics` return type.

Numerical note: accumulate in **float64**. `s_pow2 = sum(p^2)` over ~28M samples with
p~1e-3 needs float64 or the kurtosis denominator loses precision; rf-processor uses
numpy defaults (float64) so matching dtype matters.

### Wire into the pipeline (`pipeline/streaming.py`)

- Worker (where `complex_chunk` exists, ~line 963): compute `moments_from_iq(complex_chunk)`
  once; derive the per-chunk `IQStatistics` for the live UI via `finalize_moments`
  (replaces the current `calculate_iq_statistics` subsampled call), and stash the
  `IQMoments` on the `_StreamResult` (new field) so the consumer can fold it.
- Consumer accumulation loop (~1177-1230): keep an `IQMoments` accumulator alongside
  `accum_powers`; fold each chunk's moments in; reset both at interval start and on the
  shape-change clear. At the DURATION flush (and the pending-flush timeout path at
  1187-1191), `finalize_moments(accumulator)` and pass the resulting `IQStatistics` into
  `_publish_processed` / `_build_envelope`.
- `_build_envelope`: take the interval `IQStatistics` as a parameter (or read it off the
  accumulator) instead of `result.iq_stats`; set `statistics=<interval stats>`. Nothing
  else in the envelope changes.
- The live per-chunk broadcast (`avg_power_db`/`kurtosis` at 1251-1253 / 1298-1300) may
  keep using the last chunk's stats (UI liveness) — only the ZMS/NATS envelope must be
  interval-accurate. Note this explicitly so a reviewer does not "fix" it.

## Acceptance criteria

1. **Parity (unit, `tests/unit/test_iq_stats_parity.py`):** for several synthetic IQ
   arrays (Gaussian noise; noise+CW; a bursty mix), splitting into N chunks and folding
   `moments_from_iq` per chunk then `finalize_moments` yields `average`, `std`,
   `kurtosis`, `max` equal to a **verbatim copy of rf-processor's `calculate_iq_statistics`**
   over the whole array within tight tolerance (`< 1e-6` rel for average/std/kurtosis;
   `max` exact); `median` within one histogram bin (~0.05 dB).
2. **Formula unit tests:** `finalize_moments` on a single-chunk `moments_from_iq` matches
   the reference for each field; `IQMoments.add` is associative (fold order independent).
3. **std/max fixed:** a direct test that `std` now equals `np.std(np.abs(data))` and
   `max` equals the full-resolution `10log10(max(|z|^2)/50)` (no subsample).
4. **Pipeline:** existing streaming/ZMS/NATS tests still pass; the envelope's
   `statistics` reflects the interval accumulator (a test that feeds two chunks with
   different power and asserts the envelope's `average` equals the combined-moment value,
   not the last chunk's).
5. Full CI per CLAUDE.md (ruff, mypy, unit, integration).

## Files

- `src/rfobserver/processing/iq_utils.py` — `IQMoments`, `HIST_EDGES`, `moments_from_iq`,
  `finalize_moments`; `calculate_iq_statistics` delegates to them.
- `src/rfobserver/pipeline/streaming.py` — worker stashes `IQMoments`; consumer
  accumulates + finalizes at flush; `_build_envelope` uses interval stats.
- `src/rfobserver/models.py` — only if `_StreamResult`/envelope need a type; `IQStatistics`
  unchanged.
- `tests/unit/test_iq_stats_parity.py` (new), plus targeted asserts in existing streaming tests.

## Out of scope

- **PSD averaging** (`np.mean(accum_powers)` mean-of-dB vs rf-processor's Welch
  linear-average-then-dB, and Welch's first-1408-samples-only window). Separate issue;
  flagged, not fixed here.
- Changing `IQStatistics` fields, the DB schema, or the live per-chunk UI cadence.
