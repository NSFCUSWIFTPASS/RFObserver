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
