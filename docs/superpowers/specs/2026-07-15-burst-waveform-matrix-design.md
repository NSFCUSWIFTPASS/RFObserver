# Burst-waveform detection matrix (full pipeline)

Date: 2026-07-15

## Context and goal

The existing burst-detection integration tests
(`tests/integration/test_burst_detection.py`) plant **pure tones** (near-zero
occupied bandwidth) and check only that a detection lands near the planted
frequency. They do not verify that the detector measures a burst's **duration**
or **occupied bandwidth**, and they do not cover the range of pulse lengths and
signal bandwidths seen in the field.

This work adds a parametrized matrix that drives the real `StreamingProcessor`
with **generated wideband bursts** at a grid of pulse lengths and occupied
bandwidths, at the field sample rates, and asserts that the detector reports the
burst's presence, duration, center frequency, and bandwidth — each within a
tolerance derived from that combo's own grid resolution.

## Locked decisions (from brainstorming)

- **Test layer:** the full `StreamingProcessor` pipeline (same harness as the
  existing burst tests), not a lighter direct-`detect_bursts` path.
- **Sample rates:** only the two field values, **28 MHz** and **56 MHz**. These
  set the pipeline `BANDWIDTH`.
- **BW list = occupied signal bandwidth** of the burst (not sample rate): the
  burst is a wideband signal filling `B` of spectrum; the SDR span (28/56 MHz)
  sits above it.
- **Grid params tuned per combo**, with **assertion tolerances derived** from
  each combo's bin-spacing / time-resolution — no fixed global percentage.
- **Frequency dimension** = the burst is offset within the wideband IQ. One
  scaled non-zero offset per combo, plus a small dedicated offset sweep.
- **Heaviest long-burst combos gated behind a `slow` marker** so default CI stays
  fast.
- **Raise the field `BURST_WINDOW_ROWS` default** so the deployed config can
  measure the long modes, with the RAM/compute cost documented and its own test.

## The matrix

50 combos, each run end-to-end through `StreamingProcessor`:

- `Fs` ∈ {28_000_000, 56_000_000}
- `B` (occupied) ∈ {50_000, 150_000, 500_000, 2_000_000, 20_000_000}
- `D` ∈ {1.3, 2.7, 10.24, 83.2, 393.1} ms

Note the 20 MHz occupied burst fits inside both spans (71% of 28 MHz, 36% of
56 MHz). Every combo places its burst at a single non-zero offset (see
"Frequency offset").

A separate **offset sweep** runs one representative combo (e.g. `Fs`=28 MHz,
`B`=500 kHz, `D`=10.24 ms) at offsets `{-f, 0, +f}` to explicitly verify the
`center_freq_hz = SDR_center + offset` arithmetic (3-4 extra tests).

## Waveform generation

New helper in `tests/integration/_synth.py` (alongside the existing tone
generator), producing a **band-limited complex-Gaussian noise** burst:

1. Generate white complex Gaussian noise for the burst's sample span
   (`D * Fs` samples).
2. Band-limit to `B` in the frequency domain: FFT, zero all bins outside the
   occupied band, IFFT. The occupied band is placed at the target **offset**
   (band spans `[offset - B/2, offset + B/2]`).
3. Apply the existing 5% raised-cosine time envelope (reused from
   `make_iq_with_bursts`) so burst edges do not splatter.
4. Scale to a strong per-bin SNR (~30 dB+ headroom above the background noise
   floor) so detection is robust and the D×B rectangle in the PSD grid has
   clean edges. Because a wideband burst spreads power across many bins, total
   burst power is scaled up relative to the tone case to keep **per-bin** power
   well above the floor; the exact scale is calibrated during implementation.
5. Add the burst onto a background complex-noise buffer sized to
   `pre_margin + D + post_drain` (kept tight to bound cost, not the 2 s the
   existing tests use).

Band-limited noise is chosen (over a chirp or comb) because it presents a flat,
fully-occupied `B` in **every** time slice, giving a clean rectangle for both
duration and bandwidth measurement.

Known effect, absorbed by tolerance: the time envelope broadens the spectrum by
roughly `1/ramp_time`. At the shortest/narrowest corner (1.3 ms × 50 kHz) this
is a meaningful fraction of `B`, so the bandwidth tolerance there is generous
(see below). This is a genuine time-bandwidth-product limit (`D·B ≈ 65`), not a
detector error.

## Per-combo grid sizing

A helper `derive_grid_params(Fs, B, D)` returns the settings for each combo so
that the pinned sample rate still yields a resolvable burst:

- **`NUM_FFT_BINS`**: nearest power of two giving a healthy occupied-bin count
  (target on the order of tens of bins), clamped to roughly `[256, 8192]`.
  Narrow-in-wide corners (50 kHz in 28/56 MHz) land near the frequency
  resolution floor by physics — that is expected and reflected in the tolerance.
- **`PSD_TIME_RESOLUTION_MS`**: fine enough to give a useful slice count over
  `D`, floored by `NUM_FFT_BINS / Fs` (an FFT needs ≥ `NUM_FFT_BINS` samples per
  slice) and capped so long bursts don't explode the window row count.
- **`BURST_WINDOW_ROWS`**: sized to **hold the whole burst** plus margin
  (`ceil(D / time_res)` rows + margin). This is what lets the 393.1 ms burst be
  measured without the circular window truncating its start. Floored at a small
  minimum and at `2 × BURST_EVAL_INTERVAL_ROWS`.
- **`BURST_EVAL_INTERVAL_ROWS`**: ~half the window (matching the default ratio).
- **`STREAMING_CHUNK_SLICES`**: a moderate rows-per-recv-chunk value.

Exact formulas/constants are finalized during implementation (TDD), calibrated
so every combo passes with margin.

## Assertions and derived tolerances

For each combo, after running the pipeline to exhaustion + drain and querying
the detections DB:

- **Detected**: at least one detection overlaps the planted time/frequency
  region.
- **Duration**: `|measured_duration_ms − D|` within `~3 × time_res_ms` (plus a
  small percentage floor). Accounts for slice quantization and the detector's
  `t_end = time_axis[end_row + 1]` bias.
- **Center frequency**: `|measured_center − (SDR_center + offset)|` within
  `~2 × bin_spacing`.
- **Bandwidth**: `|measured_bw − B|` within `~(a few × bin_spacing)` plus an
  envelope-broadening allowance — generous at the low-TBP corners, tight at the
  high-TBP ones.

Tolerance **constants are calibrated empirically during implementation**, the
same approach the existing burst tests document (amplitudes/thresholds were
tuned against the live streaming pipeline, which has a tighter margin than a
single-grid `compute_psd_grid` call).

## Frequency offset handling

Per combo, the max usable offset is `max_off = 0.45 * Fs - B/2` (keeps the
occupied band clear of the ±Fs/2 edge, avoiding wraparound/aliasing). The combo's
offset is a deterministic non-zero fraction of `max_off` (varied across combos
so both signs and a range of magnitudes are exercised). For the widest burst
(20 MHz in 28 MHz) `max_off` is small (~2.6 MHz) but still non-zero.

The dedicated offset sweep uses `{-0.4, 0, +0.4} * max_off` at one mid combo.

## Field default change: `BURST_WINDOW_ROWS`

The current default `BURST_WINDOW_ROWS = 500` at `PSD_TIME_RESOLUTION_MS = 0.2 ms`
is a **100 ms** rolling window, so the field config cannot correctly measure
bursts longer than ~90 ms (it truncates the duration and mis-times the start).
This work raises the default to cover the ~400 ms modes:

- New default: **`BURST_WINDOW_ROWS = 2048`** (≈ 410 ms at 0.2 ms), with
  `BURST_EVAL_INTERVAL_ROWS` raised proportionally (≈ 1024).
- **Documented cost:** the rolling window buffer grows to `2048 × NUM_FFT_BINS ×
  4 B` ≈ 8.4 MB at `NUM_FFT_BINS=1024`, and each detection eval runs
  connected-component labeling over ~2.1 M cells. At the ~205 ms eval cadence
  this is an acceptable duty cycle on the field Jetson but is a real CPU increase
  over the old 500-row window; the comment in `config.py` states this tradeoff.
- **Own test:** a full-pipeline test at the field defaults (`Fs`=56 MHz,
  `NUM_FFT_BINS`=1024, `time_res`=0.2 ms, new window) confirming an ~83 ms and an
  ~393 ms burst are measured to full duration (not truncated to ~100 ms). This
  test is `slow`-gated (it uses a large buffer).

The exact window value is open to adjustment during spec review if the field CPU
budget argues for a different tradeoff.

## Test structure and files

- **`tests/integration/_synth.py`** — add `make_wideband_burst_iq(...)` (or a
  `bandwidth_occupied_hz` parameter on the burst dataclass) and
  `derive_grid_params(Fs, B, D)`. Reuse the SC16 packing and the raised-cosine
  envelope.
- **`tests/integration/test_burst_waveform_matrix.py`** (new) — the parametrized
  matrix and the offset sweep. Reuses `SyntheticBurstReceiver`, the
  `_make_processor(..., drop_on_overflow=False)` lossless harness, and the
  run-until-exhausted-then-drain helper (generalized/shared from the existing
  test if convenient). Combo IDs encode `Fs/B/D` for readable failures.
- **`src/rfobserver/config.py`** — raise `BURST_WINDOW_ROWS` /
  `BURST_EVAL_INTERVAL_ROWS` defaults with an explanatory comment.
- **`pyproject.toml` / pytest config** — register a `slow` marker if not already
  present.

## Runtime and `slow` gating

50 full-pipeline combos, with the 83.2/393.1 ms × 56 MHz cases needing
~30 M-sample buffers, make this the heaviest test file (~a couple of minutes if
all run). Mitigations:

- Buffers are the burst + tight margins only.
- Feed at a high `pacing_factor` (lossless mode, so no drops — correctness is
  pacing-independent).
- The heaviest combos (393.1 ms, and 83.2 ms × 56 MHz) are marked `slow` and run
  only when the `slow` marker is enabled; the rest run on every CI invocation.

## Risks and calibration notes

- **Per-bin SNR of wideband bursts**: spreading power across many bins lowers
  per-bin SNR versus a tone. Burst amplitude is calibrated up so per-bin power
  clears the detection threshold with margin at every `B`.
- **Ragged noise-burst edges**: statistical fluctuation can fragment the
  connected component. The dual-threshold hysteresis + merge step and a strong
  SNR keep the rectangle intact; the derived tolerances absorb residual edge
  raggedness.
- **Low-TBP corners** (short × narrow): duration and bandwidth cannot both be
  tight; tolerances there are intentionally loose, matching the physics.
- **Calibration approach**: mirror the existing burst tests — tune
  amplitude/threshold/tolerance against the live streaming pipeline, not just a
  single `compute_psd_grid`.

## Verification

Full CI per `CLAUDE.md`, all `PYTHONPATH=`-prefixed:

```bash
ruff check src/ tests/
ruff format --check src/ tests/
PYTHONPATH= .venv/bin/mypy src/rfobserver/
PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q
PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q          # default: slow combos skipped
PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q -m slow  # heaviest combos
```

Integration tests require NATS on `localhost:4222`.
```
