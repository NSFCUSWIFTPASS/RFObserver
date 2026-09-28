# Triggered capture shows no burst: the .psd companion is ~820 ms out of step with the .sc16

Date: 2026-09-22
Repo state: `e9e88ca` (main)
Hardware: local workstation, mock receiver (`RFOBS_MOCK_RECEIVER=true`), 2 Msps

## The question

Verbatim, from the user:

> "even though we are triggering on power threshold, I don't see the captured signal
> burst with a 0.2s pre buffer. That means we're not correctly time co-related when
> capturing, either we're late or too early. How is the time-correlation between a power
> threshold detections timeframe being correlated to the pre buffer and the capture?"

and, after a first (wrong) answer from me:

> "I was looking at the live display, the chunk avg power went beyond the threshold and
> recording started and stopped, but then I didn't see anything in the capture. I did see
> a high power burst in the high res waterfall. Are you sure that time-correlation you
> provided is accurate?"

The user was looking at the **capture's waterfall**, which is rendered from the `.psd`
companion file, not from the `.sc16` IQ.

## The answer

The IQ capture is correctly time-correlated. **The `.psd` companion is not.**

The `.psd` grid rows are produced by the PSD processing pipeline, which sits behind
`_chunk_queue` (`maxsize=4`) and a 3-worker `ThreadPoolExecutor`. The IQ pre-roll ring is
written **synchronously in the receive loop**. Both are drained at the same instant in
`_begin_recording`, but they are anchored about four chunks apart:

| buffer | written from | contents at trigger time T |
|---|---|---|
| `_pre_trigger_buf` (IQ) | receive loop, synchronous (`streaming.py:751`) | IQ for `[T-0.2, T]` |
| `_grid_prebuf` (PSD) | `_handle_chunk_result`, after the queue + worker pool | grids for `[T-L-0.2, T-L]` |

with `L` ~= 4 chunks = 4 x 204.8 ms = **819 ms** at 2 Msps.

So the `.psd` starts roughly 820 ms **earlier in real time** than the `.sc16`, while
`.psd.json` carries no start timestamp at all and the Captures page assumes PSD row 0
lines up with `.sc16` `start_time`. Consequences:

1. Everything in the rendered waterfall is displaced by ~820 ms.
2. The last ~820 ms of the IQ has no PSD coverage at all (the grids for it arrive after
   the recording has already stopped).
3. **If the recording is shorter than ~820 ms post-trigger, the `.psd` never reaches the
   trigger instant.** The burst that fired the trigger is in the `.sc16` but completely
   absent from the `.psd`. This is the user's symptom.

The code comment at `streaming.py:1073-1076` asserts the opposite -- "so the .psd
companion starts with rows covering the same pre-roll span as the IQ prepended below" --
and it is only true if grid computation were instantaneous.

## The procedure

Each probe and what it isolates.

1. **Read the receive-loop ordering** (`streaming.py:744-758`). Isolates "are we writing
   the pre-roll before or after the trigger check?" Result: write-before-check, correct.
   Not the bug.
2. **Force a trigger with a known, clean capture.** Local instance on port 8899,
   `TRIGGER_THRESHOLD_DB=-400` so arming fires immediately, `TRIGGER_PRE_SEC=0.2`,
   `RECORDING_MAX_SEC=2`. Isolates the file-writing path from any question about whether
   detection is correct: detection is forced, so anything wrong downstream is downstream.
3. **Compare the declared spans of the two files.** Isolates "do these two files even
   claim to cover the same interval?" Result: they do not (1.634 s vs 2.048 s).
4. **Cross-correlate the IQ power envelope against the PSD row means.** The reusable
   measurement. Slice the `.sc16` into rows of `time_resolution_s * sample_rate` samples,
   take mean power in dB per row; take the per-row mean over bins of the `.psd`;
   normalise both; scan every lag. Isolates the *actual* offset independent of what the
   metadata claims. This is the probe that produced the number.
5. **Segment-wise alignment as the control on step 4.** Split the PSD into 200-row blocks
   and align each independently. Isolates "is the whole file uniformly shifted, or is the
   prepended pre-roll from a different time than the body?" Three consecutive blocks
   agreeing on the same shift is what promotes a mediocre global correlation into a
   finding.
6. **Prediction-first short capture.** Restart with `RECORDING_MAX_SEC=0.5` and predict,
   before running, that the PSD will not contain the trigger instant. Isolates the
   mechanism from the particular file: if the offset is a fixed pipeline latency, then
   shortening the recording below that latency must drop the burst entirely.
7. **Read `_begin_recording`** (`streaming.py:1069-1149`). Confirms the mechanism in
   source rather than by inference from timings.

## The evidence

### Capture 1: `RECORDING_MAX_SEC=2`

`MOCK0001-Oren-Dell-Ubuntu-20260922T170719`

```
.sc16.json : duration_sec 1.634, total_samples 3267200, pre_trigger_sec 0.2,
             overflow_events 0, lost_samples 0, dropped_chunks 0, gaps []
.psd.json  : rows 2000, num_bins 2048, time_resolution_s 0.001024  -> 2.048 s
             (no start_time field; keys are bandwidth_hz, center_freq_hz, freq_axis,
              grid_max, grid_min, num_bins, rows, time_resolution_s)
```

A clean capture: nothing was dropped. Global cross-correlation over the full lag range:

```
global correlation, FULL lag range (PSD row 0 corresponds to IQ row -lag):
   lag   +803  (   +822.3 ms)   corr 0.5112
   lag   +799  (   +818.2 ms)   corr 0.5099
   lag   +802  (   +821.2 ms)   corr 0.5093
   lag   +800  (   +819.2 ms)   corr 0.5091
   lag   +805  (   +824.3 ms)   corr 0.5079

   lag      0 (what the viewer assumes)  corr -0.0924
```

Measured offset **+822 ms**; predicted from queue depth **819 ms**. Two independent
routes to the same number.

Segment-wise control (200-row blocks), showing the shift is uniform across the file
rather than confined to the prepended pre-roll:

```
 PSD rows      best-match IQ row   shift(rows)   corr
 1200-1400        395            -805      0.500
 1400-1600        595            -805      0.478
 1600-1800        795            -805      0.416
```

Resulting coverage:

```
PSD covers  trigger-1.022 s .. trigger+1.026 s
IQ  covers  trigger-0.200 s .. trigger+1.434 s
```

### Capture 2: `RECORDING_MAX_SEC=0.5` (prediction-first)

Prediction stated before the run: the PSD will cover roughly `trigger-1.02 s` to
`trigger-0.1 s` and will not contain the trigger instant.

`MOCK0001-Oren-Dell-Ubuntu-20260922T171429`

```
IQ  span 0.610 s  (595 rows)   pre_trigger 0.2 s
PSD span 0.819 s  (800 rows)

best lag over ALL lags: corr 0.085     [lag 0 = +0.009]
```

The maximum correlation over every possible alignment is 0.085. That is not a lag
measurement, it is the **absence of any overlap**: for a capture this short the `.psd`
and the `.sc16` cover disjoint intervals. Prediction confirmed.

## Measured and REJECTED -- do not retry

- **"The whole-chunk mean dilutes short bursts, so the trigger never fired."** I built a
  table showing a 1 ms burst at +20 dB raises a 204.8 ms chunk mean by only +5.6 dB, and
  presented it as the explanation. **Wrong, and rejected by direct observation:** the user
  watched the chunk average cross the threshold on the live display and watched recording
  start and stop. Detection worked. The dilution arithmetic is real but it is a separate
  sensitivity concern, not this bug. Do not re-open it as the cause of a missing burst.
- **"The `.psd` simply omits the pre-roll (shift = one `TRIGGER_PRE_SEC`)."** My first
  correlation returned exactly -195 rows = -199.7 ms, which matches `TRIGGER_PRE_SEC`
  to three digits and is extremely convincing. It is an artifact -- see traps below.
- **Backpressure / dropped grid rows.** `_grid_dropped` exists and grid rows can be
  dropped at `streaming.py:1147-1149`. Checked: this capture had zero overflows, zero
  dropped chunks, no gaps. Not the cause. A lossless capture still shows the full offset.
- **"The capture predates the `GridPreBuffer` fix (`8511d6b`, 2026-09-02)."** No. These
  captures were produced minutes ago at HEAD `e9e88ca`. The pre-roll mechanism is present
  and running; it is the mechanism itself that is mis-anchored.

## Measurement traps hit

- **I scanned too narrow a lag range and got a beautifully wrong answer.** The first
  cross-correlation scanned lags -500..+500 only. The true peak is at +803, outside it.
  Inside that window the best available fit was -195 rows, which happens to equal
  `TRIGGER_PRE_SEC` exactly -- so the artifact looked like a clean confirmation of a
  plausible hypothesis ("the pre-roll is missing from the PSD"). I reported it before
  widening the range. **Always scan the full overlap range before interpreting an argmax,
  especially when the answer lands on a round number you were already expecting.** What
  caught it was the segment-wise control in step 5, which pointed at -805 and could not
  be reconciled with -195.
- **An argmax with no correlation is not a measurement.** On the short capture the script
  dutifully printed "best lag -302 rows (-309 ms)" with corr 0.085 and derived a coverage
  window from it. That window is meaningless. Check the correlation value before using
  the lag; the finding there is "no overlap at any lag", not a number.
- **The existing test cannot catch this, for two independent reasons.** First,
  `tests/integration/test_recording_grids.py` asserts `grid_span >= 0.85 * iq_span` -- a
  span *length* check. Here the span ratio is 1.25, so it passes with the alignment
  completely wrong. A length check is not an alignment check.

  Second, and more insidious: **the test's own settings make the bug physically absent.**
  The defect exists only where the PSD pipeline latency (about 4 chunks) exceeds
  `TRIGGER_PRE_SEC`. Two settings each independently prevent that:

  - `PSD_TIME_RESOLUTION_MS=0.5` with `NUM_FFT_BINS=256` gives
    `actual_slice_samples=896`, and `STREAMING_CHUNK_SLICES=10` makes a chunk 8960
    samples = **4.5 ms** at 2 Msps, so latency is ~18 ms.
  - The pre-roll tests pass `TRIGGER_PRE_SEC=1.0` explicitly, five times the 0.2 s the
    field sensor runs.

  At that configuration `GridPreBuffer` really does hold grids covering the pre-roll and
  the old code is correct. Production hits the defect because chunks are 204.8 ms
  (`STREAMING_CHUNK_SLICES=200`, slice clamped to `NUM_FFT_BINS=2048`), giving ~820 ms
  latency against a 200 ms pre-roll.

  **Consequence for anyone writing the regression test: it must set BOTH
  `STREAMING_CHUNK_SLICES=200` and `TRIGGER_PRE_SEC=0.2`.** Setting one alone still
  passes before and after any fix. Scaling a test's timing parameters down for speed can
  scale the bug out of existence, and a green suite then means only that the test is
  configured outside the regime where the defect lives.
- **An editable install makes "run the test against the old code" silently useless.**
  `.venv` holds `pip install -e .` pointing at `/home/orencollaco/GitHub/RFObserver`, so
  `git worktree add /tmp/psd-prefix <old-commit>` and running pytest from inside that
  worktree still imports `rfobserver` from the MAIN checkout. The new regression test
  duly "passed" against what looked like pre-fix code, which would have shipped a test
  that never fails. What exposed it: the pre-fix run printed
  `start_sample_offset=384`, a sidecar field that does not exist before the fix.
  **Verify the source under test, do not assume it:**

  ```
  PYTHONPATH=/tmp/psd-prefix/src .venv/bin/python -c "import rfobserver; print(rfobserver.__file__)"
  ```

  With `PYTHONPATH` pointed at the worktree's `src`, the same test fails with
  `PSD is 1154 rows out of step with the IQ` (517 ms) and passes on the fixed tree at
  lag 0. A regression test is only evidence once it has been seen to fail.
- **`.psd.json` having no timestamp makes the defect invisible to inspection.** Nothing in
  either sidecar disagrees with anything else, because the PSD sidecar makes no claim
  about when it starts. The misalignment is only reachable by correlating content.

## Secondary defect found on the way (separate from the root cause)

The Captures page does not use `.psd.json`'s `time_resolution_s`. It derives the per-row
time as `.sc16.json duration_sec / total_rows` (`captures.html:281-289`). For capture 1
that is `1.634 / 2000 = 0.817 ms` against a true `1.024 ms` -- the time axis is
compressed by 20% on top of the ~820 ms offset. Worth fixing with the root cause, but it
is not what hid the burst.

## Open, not yet answered

- **The exact value of `L` is not pinned.** 822 ms measured, 819 ms predicted from
  `maxsize=4` at 2 Msps. Whether `L` is exactly the queue depth, or queue depth plus
  worker occupancy, is not established, and it will vary with load, bandwidth, and worker
  count. Any fix must carry the real per-capture offset rather than assume a constant.
- **Not reproduced on hardware.** Both probes are the mock receiver on the workstation.
  The offset is a software pipeline property so it should hold on the Jetson and on the
  deployed sensor, but that is an inference, not a measurement. The field sensor is
  hands-off.
- **No fix proposed yet.** Root cause only. The obvious candidates (stamp the `.psd` with
  a real start time and have the viewer honour it; or anchor the grid pre-buffer by
  sample position instead of arrival order; or hold the recording open until in-flight
  grids drain) have not been evaluated against each other.
- **Whether any already-archived capture is recoverable.** The PSD rows for the missing
  tail were never computed into the file, and `GridPreBuffer` is in-memory only. Existing
  `.psd` files are probably not correctable in place, but the `.sc16` is intact and a
  correct PSD can be recomputed from it offline. Not attempted.

## Post-fix verification (2026-09-22, commit b281ef2 + the regression test)

Appended, not edited in: the sections above record the state at diagnosis.

Fix: anchor grid rows by absolute stream sample position instead of arrival order
(`GridPreBuffer` tags each grid with its chunk position, `drain(from_sample=...)` selects
by it, `trim_grid_rows()` keeps only rows inside the recorded range), plus a bounded wait
at finalize for the in-flight grids covering the tail, plus `start_sample_offset` /
`slice_samples` in `.psd.json` so the pinning is checkable from the files.

**The short capture that produced the original report** (`RECORDING_MAX_SEC=0.5`,
`TRIGGER_PRE_SEC=0.2`, 2 Msps), before and after:

```
                       BEFORE                      AFTER
IQ  span               0.610 s                     0.610 s
PSD span               0.819 s                     0.609 s   (595 rows)
best correlation       0.085 at ANY lag            0.440 at lag 0
                       (= no overlap at all)
start_sample_offset    field did not exist         640 samples (0.320 ms, sub-row)
PSD covers             disjoint from the IQ        trigger-0.200s .. trigger+0.410s
IQ  covers             trigger-0.200s .. +0.410s   trigger-0.200s .. trigger+0.410s
contains trigger?      NO                          YES
```

**The regression test**, `test_psd_rows_align_with_the_iq_they_describe`, at
`STREAMING_CHUNK_SLICES=200` and `TRIGGER_PRE_SEC=0.2`:

```
pre-fix (e9e88ca):  IQ 2046 rows, PSD 2000 rows
                    best lag +1154 rows (517 ms), corr 0.945; lag 0 corr 0.047
                    FAILS: "PSD is 1154 rows out of step with the IQ"

post-fix (b281ef2): IQ 2046 rows, PSD 2046 rows
                    best lag 0, corr 0.940
                    PASSES
```

Full suite green: 573 unit, 106 integration + 10 skipped.

### Still open after the fix

- **Not validated on hardware.** Mock receiver on the workstation only. Validate on
  nano-super before this reaches the field sensor.
- **The ~820 ms latency itself is untouched**, and is now tracked separately in
  `2026-09-22_psd-pipeline-latency.md`: it is not compute-bound (a chunk doing 10.8 ms of
  work took 726 ms to surface), and the fix deliberately does not depend on its value.
- **Existing archived captures are not corrected.** Their `.psd` files remain misaligned
  and carry no `start_sample_offset`, so the viewer falls back to the old stretched axis
  for them. The `.sc16` is intact, so a correct grid can be recomputed offline; no
  migration was written.
- **`best_corr` discrimination is soft.** On the mock signal lag 0 scores 0.940 while
  lag +/-4 scores ~0.917, so the test's `abs(best_lag) <= 1` leans on the argmax rather
  than a sharp peak. It separates 1154 from 0 decisively, which is what it exists for,
  but it would not reliably catch a 2-3 row regression.

## CORRECTION and Jetson validation (2026-09-22)

Appended per convention. The "Post-fix verification" section above reported the
workstation result as if the fix were complete. **It was not.** Validating on nano-super
(6 cores -> 3 workers, Python 3.10, aarch64) found two further defects that the
workstation could not surface, both introduced by the fix itself.

### Defect 1: the tail wait was bounded by the wrong quantity

The first implementation capped `_await_tail_grids` at `RECORDING_MAX_SEC`. On
nano-super with `RECORDING_MAX_SEC=0.5`:

```
WARNING PSD tail did not drain within 0.5s; .psd is 0.205s short of the IQ
nano-super latency over the run: min 879.2 ms  median 918.8 ms  max 1183.9 ms  (n=11)
```

How long in-flight grids take to arrive is a property of the pipeline, not of how long
the recording ran. Scaling the wait to the recording length breaks precisely the
short-capture case the whole fix exists for, and the shorter the capture the worse the
mismatch. The bound is now a fixed floor (3 s) under the existing ceiling (10 s), with
`RECORDING_MAX_SEC` able to raise the floor but never pass the ceiling.

**The workstation could not have caught this**: at 21 workers its latency is ~820 ms
but the probe there used `RECORDING_MAX_SEC=2`, so the 2 s cap happened to exceed it.
The defect needed both a slow box and a short cap.

### Defect 2: the sidecar row count could exceed the file

The second nano-super capture produced:

```
sidecar rows : 595
num_bins     : 2048
file bytes   : 3235840   ->  file rows 395.0
load_grid    : ValueError: mmap length is greater than file size
```

595 - 395 = 200 rows, exactly one chunk's grid. `load_grid` memmaps at the sidecar's
declared shape, so this capture would have failed the Captures page outright -- a worse
symptom than the misalignment being fixed.

Two causes:

1. `_grid_last_sample` advanced *before* the rows were queued, so `_await_tail_grids`
   could declare the tail complete for rows that were never stored. It now advances only
   after a successful put.
2. `rows` came from `_grid_rows`, a counter incremented at put time. A put landing after
   the writer's `None` sentinel is counted but never written. The count is now read from
   the file size after the writer has closed it, and a partial trailing row is truncated.
   A disagreement is logged rather than shipped.

**Trap for next time: a counter of what was enqueued is not a count of what was
written.** Any sidecar field describing a file's shape should be derived from the file
after it is closed. The new assertion in `_assert_psd_covers_iq` checks exactly this
(`st_size == rows * num_bins * 4`) and is what would have caught it.

### Final nano-super result

```
IQ  span 0.6100 s   pre_trigger 0.2 s   dropped 0
PSD span 0.6093 s   rows 595
start_sample_offset 640   slice_samples 2048

best lag +0 rows, corr 0.535 over all 595 rows
PSD covers trigger-0.200s .. trigger+0.410s
IQ  covers trigger-0.200s .. trigger+0.410s
.psd contains the trigger instant?  YES
sidecar rows 595 | file rows 595 | load_grid -> (595, 2048)
no tail-drain warning, no row-count warning
```

### A measurement trap hit during this validation

**An argmax over a tiny overlap window is not a measurement.** My first check of the
nano-super capture reported `best lag -570 rows (-583.7 ms), corr 0.714` and I briefly
took it as a real misalignment. At lag -570 out of 595 total rows only **25 rows**
overlap, and a 25-point correlation reaches 0.7 by chance. The same failure in a
different disguise as the too-narrow lag range recorded earlier in this file. The check
now requires an overlap of at least `max(50, n//2)` rows, after which the same capture
reported lag +2, and lag 0 once the row-count defect was fixed.

The regression test originally carried a weaker floor (`len(x) < 10`), safe only by
accident. It now requires `max(50, nrows // 4)`. A quarter of the capture was chosen
deliberately: a half would have excluded the pre-fix peak itself (1154 rows of 2046),
turning a precise failure into the vaguer "does not describe the IQ at any lag". Verified
to still fail pre-fix with `PSD is 1154 rows out of step with the IQ` and pass post-fix.

## CORRECTION (2026-09-22): the "819 ms predicted from queue depth" match was coincidence

**WITHDRAWN:** "Measured offset +822 ms; predicted from queue depth 819 ms. Two
independent routes to the same number." Stage timing (see
`2026-09-22_psd-pipeline-latency.md`, CORRECTION) shows `_chunk_queue` is not standing
full (queue stage median 4-7 ms). The ~820 ms offset was acquisition (~235 ms mock) plus
GIL-inflated processing plus collection delay; that it landed on 4 x 204.8 ms was
numerology. The alignment fix is unaffected, since it anchors by sample position and does
not depend on the latency's value or cause. Also correct the table in "The answer": the
grid pre-buffer lags by the pipeline latency `L`, not by "about four chunks".
