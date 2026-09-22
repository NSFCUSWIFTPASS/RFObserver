# PSD pipeline latency is ~820 ms, and almost none of it is computation

Date: 2026-09-22
Repo state: `e9e88ca` (main)
Hardware: local workstation, 24 cores, mock receiver, 2 Msps, chunk = 409600 samples (204.8 ms)

Status: **measured, root cause identified in source, fix NOT attempted.** Split off from
`2026-09-22_trigger-psd-iq-misalignment.md`, where this latency is the mechanism behind
the `.psd` / `.sc16` misalignment. The alignment fix is deliberately independent of the
value of this latency, so that work does not wait on this.

## The question

While root-causing the capture misalignment, the PSD pipeline latency was measured at
~820 ms and the user asked: "why is there a 820ms of latency? Isn't that high?"

## The answer

It is high, and it is not compute-bound. On this box the worker pool runs at roughly 7%
utilisation while chunks still take 500-1100 ms to surface. The delay comes from two
properties of the dispatch loop (`src/rfobserver/pipeline/streaming.py:1510-1521`):

1. **Completed results are only collected when a NEW chunk arrives.** The loop drains
   finished futures at the top of an iteration, then blocks on
   `self._chunk_queue.get(timeout=0.5)`. Chunks arrive one per 204.8 ms, so the loop
   turns over once per chunk period and a result that finished mid-period waits for the
   next arrival to be noticed. Floor: about two chunk periods, ~410 ms, no matter how
   fast processing is.
2. **In-order draining head-of-line blocks.** `while pending_futures and
   pending_futures[0].done()` pops only from the front, so one slow chunk holds up every
   finished chunk queued behind it. Observed per-chunk processing varies 84-503 ms, so
   this is not rare.

The in-order requirement itself is legitimate: the waterfall and the recording need
chunks in sequence. But nothing requires waiting for a *new arrival* to notice a
*completed* one, which is the part that costs the floor.

## The evidence

`latency` is measured from `recv_time` (after the full chunk is in hand) to
`_handle_chunk_result`, so it excludes the 204.8 ms acquisition and contains only queue
wait plus processing.

```
PROC chunk#1250: process= 10.8ms latency= 725.9ms (IQ=204.8ms)
PROC chunk#1350: process=238.9ms latency= 509.0ms (IQ=204.8ms)
PROC chunk#1400: process=486.2ms latency= 748.2ms (IQ=204.8ms)
PROC chunk#1450: process=663.2ms latency=1105.0ms (IQ=204.8ms)
PROC chunk#1500: process=540.4ms latency=1027.4ms (IQ=204.8ms)
PROC chunk#1550: process=495.1ms latency= 805.4ms (IQ=204.8ms)
PROC chunk#1600: process=243.5ms latency= 521.1ms (IQ=204.8ms)
```

**Chunk #1250 is the decisive row: 10.8 ms of work, 726 ms to surface.** 715 ms of pure
waiting on an idle pool. That single line rules out "the pipeline is busy" as the
explanation and points at the collection schedule instead.

Per-chunk cost breakdown, showing both the magnitude and the variance that feeds the
head-of-line blocking:

```
WORKER chunk# 100: convert=11.6ms psd= 46.3ms stats= 25.9ms total= 84.1ms
WORKER chunk# 150: convert=34.3ms psd=322.8ms stats=125.1ms total=502.8ms
WORKER chunk# 200: convert=16.8ms psd=198.8ms stats= 18.9ms total=240.3ms
WORKER chunk# 250: convert=16.1ms psd=296.3ms stats=129.2ms total=441.9ms
WORKER chunk# 300: convert= 0.8ms psd=282.8ms stats=164.0ms total=458.3ms
```

Pool sizing at startup:

```
StreamingProcessor: chunk=409600 samples (204.8 ms), 21 PSD workers (fft_workers=1 each),
pre-trigger=0.20s (400000 samples)
```

`_num_proc_workers = max(1, total_cores - 3)` (`streaming.py:313`) gives 21 here, and
`max_inflight = self._num_proc_workers * 2` (`streaming.py:1497`) gives 42. Capacity is
21 chunks per ~300 ms against a demand of one per 204.8 ms: roughly 7% utilisation.

Receive side is real-time and lossless throughout, so the delay is not upstream:

```
TIMING recv#1600: recv=232.4ms dropped=0 (IQ=204.8ms) handoff_dropped=0/0 ovf=0 lost=0
```

## Measurement traps

- **`process_ms` and `latency_ms` measure different things and the gap IS the finding.**
  Reading only `process` (84-503 ms) suggests a compute problem and leads to tuning FFT
  sizes or worker counts. Reading only `latency` (500-1100 ms) suggests the same. The
  pair, per chunk, is what shows the pool is idle and the schedule is the cost.
- **The module docstring is stale and will mislead sizing work.** `streaming.py:9` says
  the pool has "3 workers"; it is actually `cores - 3`, which is 21 on this box. A
  conclusion drawn from the docstring rather than from the startup log will be wrong by
  7x here.
- **This profile is machine-specific.** 24 cores gives 21 workers and an idle pool. The
  Jetson (6 cores) gets 3 workers, and with slower per-chunk ARM processing the pool may
  be genuinely near saturation, making the latency worse and compute-bound for a
  different reason. Do not carry this box's conclusion to the Jetson without re-measuring.

## Open, not yet answered

- **The ~410 ms floor is inferred from the loop structure, not measured directly.** The
  direct evidence is only that latency is decoupled from process time (chunk #1250). A
  clean measurement would instrument the time from future completion to
  `_handle_chunk_result`.
- **Not measured on the Jetson or on real SDR hardware.** Both probes are the mock
  receiver on the workstation. The mock generates signal on the same box, which inflates
  and destabilises the per-chunk processing numbers above.
- **No fix evaluated.** The apparent candidate is collecting results without waiting for
  a new chunk arrival (a collector that blocks on the head future, preserving order),
  which should bring latency close to processing time. Not designed, not costed, and its
  interaction with `_LoopHandoff` and the burst-detection path is unexamined.
- **Why `psd=` is 200-350 ms at all is unexplained.** A chunk is 200 slices of a 2048
  point FFT, which should be far cheaper than that. Possibly contention from 21 threads
  plus the mock generator on one box, possibly something costlier inside
  `compute_psd_grid`. Worth a separate look before any worker-count tuning.

## CORRECTION (2026-09-22, same day): root cause is GIL starvation by the burst tracker

Appended, not edited in. Three claims above are **withdrawn**:

1. **WITHDRAWN: "latency ... excludes the 204.8 ms acquisition."** `recv_time` is taken
   at `streaming.py:772`, *before* `recv_chunk()`, so the logged `latency` includes the
   chunk's own acquisition (~205 ms real, ~235 ms mock).
2. **WITHDRAWN: "almost none of it is computation ... the pool runs at roughly 7%
   utilisation."** The workers did use little CPU, but because they were starved of the
   GIL, not because they were idle. Processing is 34-60% of latency.
3. **WITHDRAWN: the ~410 ms "collection floor" and head-of-line blocking as the main
   causes.** Collection-on-arrival is real but is ~110-170 ms median; head-of-line
   blocking is a side effect of GIL-inflated variance and vanishes when the GIL is freed.

### The answer

The ~11 ms (workstation) / ~30 ms (Jetson) worker job is inflated ~20x because the
`burst` thread holds the GIL ~80% of wall time, in pure-Python code:

- `detect_bursts` finds ~4,000-7,000 "bursts" per 4096-row window **on noise alone**.
  Each PSD row is a single unaveraged FFT, so every cell is an exponentially distributed
  noise sample and the fixed 10 dB-over-median threshold is crossed by ~0.1% of ~8.4M
  cells per window.
- `RollingBurstDetector._absorb` (`processing/rolling_burst.py:171`) matches each detected
  burst against every tracked burst with a linear Python scan: O(bursts x tracked) per
  evaluation, GIL held throughout.
- Every PSD worker returns into Python after each numpy call and must reacquire the GIL,
  waiting up to `sys.getswitchinterval()` (5 ms) each time behind the burst thread.

### Procedure

1. **Stage decomposition on a live `StreamingProcessor`**, no source changes: wrap
   `recv_chunk`, the `_chunk_queue.get`, `_process_one_chunk` and `_handle_chunk_result`
   and timestamp each chunk (`scratchpad latency_probe.py`, reproduced below as numbers).
   Isolates which stage owns the time.
2. **Isolated benchmark** of each worker step on one quiet thread. Isolates inherent cost
   from contention.
3. **BLAS/OMP threads pinned to 1** as the control for thread-pool oversubscription.
4. **`sys.setswitchinterval` 5 ms -> 0.5 ms** as the GIL-convoy test: if the GIL is the
   contention, shrinking the switch interval must collapse processing time.
5. **py-spy `--gil --threads`** (binary extracted from its wheel, no install) to name the
   thread and function holding the GIL.
6. **Repeat 5 on real hardware** (nano-super, B200mini, 915 MHz) plus per-thread CPU from
   `/proc/<pid>/task/*/stat`, to rule out a mock artifact.
7. **Pure Gaussian noise through `compute_psd_grid` + `detect_bursts`** to decide whether
   the burst count is RF activity or detector false alarms.

### Evidence

Stage decomposition, workstation, mock, 21 workers (ms):

```
             default (5 ms switch)            switch interval 0.5 ms
stage     median    p90    max             median    p90    max
acq        236.6  302.0  432.9              210.2  221.9  418.5
queue        4.1   20.5   28.3                1.3    1.9   10.9
exec         0.5   10.5   27.4                0.7    0.8    1.4
process    214.2  503.0  580.6               60.2   67.0  158.1
hol          0.0  143.3  213.7                0.0    0.0    0.0
poll       166.7  238.3  413.3              150.9  196.3  418.6
total      638.7  898.7 1082.7              422.6  442.0  648.2
BLAS pinned to 1 thread (5 ms switch): process median 451.5 -- no improvement
```

Isolated worker cost (one thread, no pipeline):

```
             workstation   nano-super
convert          0.5 ms       1.3 ms
psd_grid         4.9 ms      18.5 ms
moments          5.8 ms       9.5 ms
summary          0.1 ms       0.4 ms
total          ~11 ms       ~30 ms      live: 214-650 ms
```

GIL holders (py-spy `--gil`, 200 Hz):

```
workstation (mock, 75 s):   burst 97.4%   _absorb 87.9%  find_objects 5.2%
nano-super  (B200mini, 30s): burst 94.3%   _absorb 85.5%  find_objects 3.8%
nano-super: 5234 GIL samples of 6000 possible -> burst thread holds the GIL ~82% of wall
```

Per-thread CPU on nano-super with the real B200mini, 20 s (2000 ticks = one core):

```
1888  burst      (py-spy: active+gil)
 189  psd_0
 183  psd_2
 161  psd_1
  69  recv       UHD recv() copy/convert + ring write + trigger check
  42  (native)   not a Python thread; consistent with UHD's libusb event loop
  32  MainThread
   9  dispatch
```

nano-super live pipeline at the edge of real time: process 520-650 ms per 204.8 ms chunk
on 3 workers; counters showed `dropped=23` chunks and one UHD overflow losing 1,018,910
samples (~0.5 s) during a 100 s run.

Detector on pure complex Gaussian noise, one 4096 x 2048 window:

```
7411 bursts   duration median 1.02 ms (one row)   bandwidth median 1953 Hz (~2 bins)
detect_bursts itself: 500 ms per evaluation
```

Real B200mini at 915 MHz: 4,300-4,834 bursts per evaluation, and **10,653 rows written
to `detections` in ~84 s of streaming** (~127/s, ~11M/day), essentially all noise.

### Measured and REJECTED -- do not retry

- **Queue depth (`_chunk_queue` maxsize=4) as the latency mechanism.** Queue stage median
  4-7 ms: the queue is not standing full.
- **BLAS/OpenMP thread oversubscription.** Pinning pools to 1 thread did not reduce
  processing time (451 ms median).
- **"It is a mock artifact."** Reproduced on the real B200mini, same thread, same function.
- **Adding or removing PSD workers.** Workers are GIL-starved, not CPU-bound; each chunk
  runs on one thread, so more workers cannot shorten a chunk's latency.

### Measurement traps

- **`recv_time` is taken before the receive.** Any latency read from the `PROC` log line
  includes one full chunk of acquisition.
- **Low worker CPU looked like idleness.** A GIL-starved thread and an idle thread both
  show low CPU; only `py-spy --gil` or the switch-interval test tells them apart.
- **`pgrep -f "bin/rfobserver run"` matched the `timeout` wrapper and would match the
  invoking ssh shell.** Have the process write its own PID (`sh -c 'echo $$ > pid; exec
  ...'`) and confirm `readlink /proc/$PID/exe` is python.

### Open, not yet answered

- Fix not designed. Candidate levers, not yet weighed: stop the noise false alarms at the
  source (average FFTs per row, or a threshold scaled to the single-cell noise
  distribution, or a minimum burst size); make `_absorb` sub-quadratic; move burst
  detection to a process so it cannot hold the pipeline's GIL; collect results without
  waiting for a new chunk.
- The field sensor's burst settings and band are unknown from here, so its detection rate
  is not measured.
- Identity of native thread 42-tick thread asserted from the design (libusb event loop),
  not confirmed by stack.

## Threshold test: BURST_THRESHOLD_HIGH_DB 10 -> 30 (2026-09-22)

The user reported the deployed sensor sees nowhere near thousands of bursts and asked to
test with a 30 dB threshold. Setting only, no code change (`RFOBS_BURST_THRESHOLD_HIGH_DB=30`).

Pure Gaussian noise, one 4096 x 2048 window:

```
10 dB: 7411 bursts   detect_bursts 508 ms
30 dB:    0 bursts   detect_bursts 168 ms
```

nano-super, real B200mini, 915 MHz, 2 Msps, same profiling as above:

```
                         10 dB                    30 dB
bursts per evaluation    4,300-4,834              1-4
PROC process             520-650 ms               56-61 ms
PROC latency             1,027-1,247 ms           409.4-410.8 ms
dropped chunks           23                       0
UHD overflow / lost      1 / 1,018,910 samples    0 / 0
detections written       10,653 in ~84 s          18 in ~92 s
burst thread CPU         94% of a core, +gil      20% of a core, idle at dump
GIL busy (wall)          ~82%, 94% burst          5%, 4% of that burst
```

**Conclusion: the latency blow-up, the drops and the overflow on the test box were all
the 10 dB default threshold turning single-cell noise into thousands of bursts.** At 30 dB
the GIL is effectively uncontended.

### What remains: 410 ms, and it is now deterministic

410 ms is 2 x 204.8 ms. Processing (~60 ms) finishes well inside one chunk period, and
the dispatch loop only collects finished results when the *next* chunk arrives, so every
chunk surfaces exactly one chunk period after its own acquisition ends:

```
204.8 ms acquisition + ~60 ms processing + ~145 ms waiting for the next arrival = ~410 ms
```

Collecting results as soon as they finish would bring this to ~265 ms. Acquisition is
fixed by the chunk size.

Live worker cost is still ~2x the isolated ~30 ms (60 vs 30 ms): residual contention,
not investigated.

### Open

- The 10 dB default in `config.py` is what a fresh install or the test box runs; the
  field sensor's actual setting has not been read from here.
- A genuinely busy band at 30 dB could still yield many real bursts, and `_absorb` is
  still O(bursts x tracked). The quadratic is latent, not removed.

## Fixes applied and validated (branch perf/burst-latency, 2026-09-22)

Three changes, each with its own test:

1. `BURST_THRESHOLD_HIGH_DB` default 10 -> 30 dB (`3983ee5`). Test: the default
   configuration must not turn pure noise into bursts (1,886 at 10 dB on 1024 rows).
2. `_absorb` candidates from a frequency-bucket index (`9d62d76`). Same decisions as the
   linear scan (randomized equivalence test, confirmed to fail when re-indexing on
   widening is skipped). 7,000 bursts: 1,760 ms -> 80 ms.
3. Dispatch loop polls every 5 ms while work is in flight (`c13f7c8`). Test: a finished
   chunk is handled without waiting for the next arrival (500 ms pre-fix).

nano-super, real B200mini, 915 MHz, 2 Msps:

```
                     before (10 dB)     30 dB, old loop    new default (30 dB)   new code at 10 dB
bursts / eval        4,300-4,834        1-4                1-3                   5,700-10,422
process              520-650 ms         56-61 ms           56-63 ms              34-670 ms
latency              1,027-1,247 ms     409-411 ms         266-274 ms            212-1,413 ms
dropped chunks       23                 0                  0                     49
GIL busy (wall)      ~82%               5%                 5%                    82% (burst 94%)
```

Latency at the new default is 204.8 ms acquisition + ~60 ms processing, as predicted.

**The index does not rescue a 10 dB noise storm on its own.** Workstation profile of the
burst thread at 10 dB after the index (70 s, mock): GIL share down from ~80% of wall to
~29%, now spread over the per-burst Python everywhere:

```
inclusive: _evaluate 76%   detect_bursts/_extract_fingerprints 47%   _absorb 26%
leaves:    find_objects 36%   _burst_detection_loop 24%   _candidates 11%   _absorb 9%
```

At thousands of bursts per evaluation the cost is the number of bursts, not any one
algorithm. On the Jetson's slower cores that still saturates the GIL. Not producing noise
bursts is the fix; the index is defence for a genuinely busy band.

### Measurement trap

- **An instantaneous worker stub hides the collection delay.** The first version of the
  dispatch test used a stub that returned immediately; the worker finished before the
  loop re-entered `get()`, and the test passed against the unfixed code. It needs work
  that outlasts the loop's turnaround (50 ms), after which it fails at exactly 500 ms.

### Still open

- Bursts per evaluation at 10 dB differed between runs (4,300-4,834 vs 5,700-10,422) on
  the same band; not investigated (RF environment, or evaluations previously skipped
  while the tracker fell behind).
- If a real busy band must run at a low threshold, the remaining lever is moving burst
  detection into its own process, or capping bursts per evaluation.
