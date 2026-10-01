# IQ stats parity with rf-processor on real captures

## 1. The question

Date: 2026-10-01. Asked by the user: "We tested the stats calculations against the
rf-processor originals to produce similar results correct?" and then "yes, run it on the
real captures on nano-super".

Before this, parity was only tested on synthetic IQ (`tests/unit/test_iq_stats_parity.py`:
Gaussian noise and noise plus a tone, 1M samples). Build: main at 82ffa15;
`src/rfobserver/processing/iq_utils.py` at 37390df on both this host and nano-super.
Host: nano-super (Orin Nano, JetPack 6.2, numpy 1.26.4, 15 W profile).

## 2. The answer

The stored, ZMS and rfdb stats (one per 0.5 s window) match rf-processor closely on real
captures. Max is exact (within 2e-6 dB). Average is within 0.053 dB, std within 0.91%
and kurtosis within 2.2%. Median is the exception: it was off by up to **0.254 dB**.
This is a bug. Samples with exactly zero power (I = Q = 0, 0.13% to 0.31% of real samples)
fall below the first histogram edge (1e-12), `np.histogram` drops them silently, and the
histogram median shifts up. Clipping power into the edge range before binning brings the
worst window median error down to 0.039 dB. The per-chunk stats (live dashboard only) are
much looser, as expected from a single 64K-sample subsample: kurtosis is off by up to 20%.

## 3. Procedure

1. Reference: `calculate_iq_statistics` extracted unmodified, via `ast`, from
   `reference_software/rf-processor/src/rf_processor/iq_utils.py`. This guards against the
   test's hand copy drifting. `IQStatistics` was stubbed as a namespace.
2. Ours: the production streaming path. `moments_from_iq` runs per chunk of 1,024,000
   samples, which is the 26 Msps default: 200 slices x 5120 samples, with NUM_FFT_BINS
   2048, PSD_TIME_RESOLUTION_MS 0.2 and 50% overlap. Chunks are folded with `.add` over 13
   chunks, about 0.5 s (DURATION_SEC). This mirrors `streaming.py` around line 3005.
   `finalize_moments` gives the window stats, which are what the DB, ZMS and rfdb receive
   (`interval_stats`).
3. Both sides get identical complex64 input. cf32 is read as is. sc16 is converted with
   /32768, the same scaling as rf-processor's `convert_bytes_to_complex_numpy` and our
   `convert_sc16_to_complex`.
4. Each 0.5 s window is compared in full. Every 4th chunk is also compared on its own,
   which isolates the subsampling effect (single-chunk finalize = live dashboard path).
5. For the median outlier, the full-resolution histogram (no subsampling) was compared with
   the subsampled one. This separates histogram error from subsample error.
6. The histogram bins around the chosen and the true median were dumped, and samples below
   `HIST_EDGES[0]` were counted.
7. The candidate fix (`np.clip(p, HIST_EDGES[0], HIST_EDGES[-1])` before
   `np.histogram`) was rerun on every window and chunk.

Captures (all 915 MHz, 26 Msps, real over-the-air):

| Name | File | Format | Samples |
|---|---|---|---|
| feb4 | `/mnt/storage/ssn-wide/feb4_19-39-48_915MHz_26Msps_cf32.dat` | cf32 | 156 M |
| feb5 | `/mnt/storage/ssn-wide/feb5_05-29-58_915MHz_26Msps_cf32.dat` | cf32 | 156 M |
| ssm | nano-super `~/rfobs-replay-data/iq_capture_hcro-rpi-002_..._ssm_fhss_OVF.dat` | sc16 | 520 M |
| replay | nano-super `~/rfobs-replay-data/REPLAY-nano-super-20260818T231810.sc16` | sc16 | 624 M |

## 4. Evidence

Max |diff|, with p50 in parentheses. Average, max and median are in dB; std and kurtosis
are relative %. "ref kurt" is the range of rf-processor kurtosis across the rows, which
shows how bursty the signal is.

```
== windows (0.5 s, the stored/ZMS stats)
  feb4_cf32      n= 11 ref kurt [53.28,68.99]  average=0.05271 (0.0243)  max=1.008e-06 (4.91e-07)  median=0.2536 (0.0214)  std=0.906 (0.379)  kurtosis=2.172 (0.677)
  feb5_cf32      n= 11 ref kurt [8.12,121.32]  average=0.03005 (0.00559)  max=1.746e-06 (6.14e-07)  median=0.03869 (0.00854)  std=0.4603 (0.0674)  kurtosis=1.394 (0.175)
  ssm_fhss_sc16  n= 39 ref kurt [1.84,2.62]  average=0.002121 (0.000563)  max=1.105e-06 (2.61e-07)  median=0.05029 (0.0223)  std=0.02087 (0.00547)  kurtosis=0.05904 (0.016)
  replay_sc16    n= 46 ref kurt [1.86,2.62]  average=0.001313 (0.00052)  max=9.775e-07 (3.66e-07)  median=0.07533 (0.016)  std=0.01359 (0.00495)  kurtosis=0.04807 (0.0138)
== chunks (1,024,000 samples, live dashboard only)
  feb4_cf32      n= 44 ref kurt [50.00,75.04]  average=0.2072 (0.067)  max=1.432e-06 (5.3e-07)  median=0.2886 (0.0146)  std=3.499 (1.11)  kurtosis=10.82 (3.05)
  feb5_cf32      n= 44 ref kurt [2.52,50.46]  average=0.2254 (0.0665)  max=2.024e-06 (6.04e-07)  median=0.2829 (0.0174)  std=3.434 (0.977)  kurtosis=7.196 (2.9)
  ssm_fhss_sc16  n=156 ref kurt [0.05,57.13]  average=0.23 (0.00638)  max=2.094e-06 (4.95e-07)  median=0.413 (0.0146)  std=4.264 (0.196)  kurtosis=20.3 (0.664)
  replay_sc16    n=184 ref kurt [0.05,1267.80]  average=0.2217 (0.00496)  max=1.824e-06 (4.67e-07)  median=0.5536 (0.0126)  std=3.939 (0.176)  kurtosis=12.55 (0.525)
```

feb4 median, per window: the full-resolution histogram is as wrong as the subsampled one,
so subsampling is not the cause:

```
4 exact=-64.545 fullhist_err=+0.022 subsample_err=+0.022
5 exact=-64.496 fullhist_err=+0.254 subsample_err=+0.254
6 exact=-64.668 fullhist_err=+0.041 subsample_err=+0.006
```

feb4 window 5, the bins around the median:

```
n 13312000 tot 13269151 chosen 2078 true bin 2071
counts around: [76801, 91669, 61729, 24775, 1866, 4, 0, 0, 0, 439, 16557, 54960, 67583]
cum at j,i: 6603082 6627857 6630166 6646723 half 6634576.0
below edge0: 42849 exact zeros: 42849
```

The histogram total is short by exactly the 42,849 zero-power samples. Without them, the
half-count crosses on the far side of an empty gap between quantization levels (bins 2076
to 2078 hold 0, 0, 439). This gives a 7-bin (0.25 dB) jump. With the zeros counted
(half = 6,656,000), the crossing lands in the true bin.

Zero-power samples and the effect of the clip fix:

```
feb4:   zero samples=482587 (0.309%) runs=480653 longest=10 | window median err max now=0.254 fixed=0.015 dB | chunk median err max now=0.289 fixed=0.254 dB
feb5:   zero samples=414720 (0.266%) runs=413141 longest=10 | window median err max now=0.039 fixed=0.017 dB | chunk median err max now=0.283 fixed=0.248 dB
ssm:    zero samples=689260 (0.133%) runs=687416 longest=10 | window median err max now=0.050 fixed=0.039 dB | chunk median err max now=0.413 fixed=0.197 dB
replay: zero samples=813026 (0.130%) runs=810843 longest=10 | window median err max now=0.075 fixed=0.038 dB | chunk median err max now=0.554 fixed=0.221 dB
```

The zeros are isolated (runs of at most 10, nearly all of length 1). They are I = Q = 0
quantization at a low signal level, not zero-filled dropouts. Field data will contain them.

## 5. Measured and REJECTED (do not retry)

- **"The median error is subsample noise."** Rejected: the full-resolution histogram gives
  the same +0.254 dB on feb4 window 5.
- **"Zeros are capture dropouts, so this only affects bad files."** Rejected: 480k zero
  samples in 480k runs, longest 10. This is ordinary quantization, present in all four
  captures, both cf32 and sc16.

## 6. Measurement traps

- The synthetic unit-test signals are float Gaussian noise. They contain no exact zeros and
  no quantization gaps, which is why `test_folded_moments_match_refproc` (0.1 dB median
  tolerance) never saw this.
- With quantized data, the median is not continuous. Removing a small fraction of the
  samples can move the crossing across an empty gap between quantization levels. The
  error size depends on where the gap falls (feb4 w5: 0.25 dB; feb5: 0.04 dB). It is not
  proportional to the zero fraction.
- `max` differs by about 1e-6 dB only because rf-processor computes `np.abs` in float32.
  That difference is not ours.

## 7. Open, not yet answered

- The per-chunk (live dashboard) stats keep about 0.2 dB median error and up to 20%
  kurtosis error after the fix. This comes from the 64K subsample on bursty signals, by
  design (c4e046b). Whether the dashboard needs better accuracy has not been decided.
- Window kurtosis error reaches 2.2% on the bursty SSN captures (ref kurtosis 50 to 120).
  This was judged acceptable but is not pinned by a test on real data.

## 8. Fix (2026-10-01)

`power_histogram()` in `processing/iq_utils.py` clips power into
`[HIST_EDGES[0], HIST_EDGES[-1]]` before `np.histogram`, so zero-power (and over-range)
samples land in the end bins. `moments_from_iq` uses it, and so does the parity test's
full-resolution helper. `test_median_counts_zero_power_samples` (30% exact zeros) failed at
3.17 dB before the fix and passes after it. The real-capture numbers in section 4 ("fixed")
were measured with the same clip expression on nano-super.
