# Would a Rust/C++ rewrite of the hot path speed RFObserver up?

**Date:** 2026-09-30
**Hardware:** dev workstation, 12th Gen Intel Core i9-12900K (x86_64). NOT the
realtime target (nano-super / Orin Nano aarch64). Numbers here establish the
*shape* of the cost, not the absolute headroom on the sensor.
**Build:** `feat/rtl433-burst-attribution`, Python 3.11 venv, numpy 1.26.4,
scipy pocketfft.
**Symptom / question:** the maintainer is weighing a Rust (or C++-in-the-hot-path)
rewrite. Where is the time actually going, and would a native rewrite recover it?

## Answer (up front)

A native rewrite recovers almost nothing from "removing Python." The hot path
spends its time in numpy/scipy array + FFT kernels that are already native and
**memory-bandwidth bound** (measured ~6-8 GB/s implied, i.e. near single-thread
DRAM bandwidth). Interpreter dispatch is negligible: the expensive frames make
1-3 Python calls each. The only real native win is **kernel fusion** (fewer
passes over memory, no temporaries) and **threading**, and both are achievable in
numpy/numexpr too. The isolation path the rewrite talk centred on (the
channelizer and the power trigger) is NOT a bottleneck. Recommendation: do not
rewrite; if anything, fuse the three memory-bound stages, and note the FPGA
PSD/stats offload plan already targets the two biggest ones.

## Procedure (probes, in order, and what each isolates)

All at the real sensor rate SR = 26 Msps, 0.5 s window (13M complex samples)
unless noted. Scripts: `docs/debugging/2026-09-30_psd-cuda-proto/` (`profile_hot2.py`, `profile_nano.py`; the first-pass `profile_hot.py` with the flawed bucketing was not kept).

1. **Existing `bench_processing.py`** — wall time per stage. Isolates which stage
   dominates end-to-end. (Ran at its default 56 Msps.)
2. **cProfile self-time bucketed native vs python (first pass)** — WRONG, see
   trap below. Reported 58-99% "python glue"; this was an artifact.
3. **cProfile with python-CALLS-per-invocation + bytes-touched (second pass)** —
   the corrective probe. Python-call count is a direct proxy for interpreter
   dispatch; bytes-touched / wall time gives the implied bandwidth. Together they
   separate "interpreter is slow" from "memory is the wall."
4. **Trigger path replicated exactly** (`_check_power_above_threshold`,
   streaming.py:1587) on a 0.1 s chunk — isolates the per-chunk trigger cost.
5. **`channelize_to_cs16` on a 10 ms burst** — isolates the isolation/attribution
   DSP that the rewrite discussion was aimed at.

## Evidence (i9-12900K, 26 Msps)

| Stage | ms/call | python-fn calls/invocation | implied GB/s | verdict |
|---|---|---|---|---|
| IQ trigger check (0.1 s chunk) | 0.03 | 6 | 355 (subsampled, trivial) | not a target |
| convert_bytes_to_complex (0.5 s) | 25.8 | **3** | 6.1 | memory-bound; fuse |
| calculate_iq_statistics (0.5 s) | 38.3 | 43 | 8.1 | memory-bound (full-res max); fuse |
| compute_psd_grid (0.5 s) | 67.3 | 1211 | 6.2 | biggest cost; fuse window+FFT+power+avg |
| detect_bursts (grid) | 9.7 | 68 | 0.3 (np.partition) | native already; leave |
| channelize_to_cs16 (10 ms burst) | 1.5 | 48 | 5.5 (FFT-bound) | not a target |

Per-window total (trigger+convert+stats+psd+detect) ~141 ms for 0.5 s of data on
x86 => large realtime headroom here. channelize is 1.5 ms/burst; at the 20
burst/s cap that is ~30 ms/s, ~3% of a core.

Key per-frame detail from the corrected pass:
- `convert`: 25.7 ms all inside ONE python frame doing inline numpy ops over
  156 MB. 3 python calls total. Not interpreter.
- `stats`: 31.6 ms inside `moments_from_iq`; the subsampled sums are cheap, the
  cost is the FULL-resolution max `re*re+im*im` then reduce (~3 passes / 312 MB,
  temporaries).
- `psd_grid`: 36.8 ms in-frame (|.|^2, log10, mean, nan_to_num over 416 MB) plus
  reshape x103 (8.4 ms) + copy x51 (8.0 ms); the FFT itself is only 7.7 ms.
- `detect_bursts`: 8.2 ms is a single `np.partition` (percentile noise floor) —
  native, not beatable by a rewrite.

## Measured and REJECTED (do not retry)

- **"~60-99% of the time is Python glue, so a rewrite wins big."** REJECTED. That
  number came from bucketing cProfile `tottime` by frame kind. It is an artifact
  of the attribution trap below. The corrected call-count probe shows the hot
  frames make 1-3 Python calls; the time is native array kernels.
- **Port the channelizer / trigger to C++/Rust.** REJECTED as a perf move.
  channelize is 1.5 ms/burst and FFT-bound; trigger is 0.03 ms. No headroom to
  recover.
- **Rewrite `detect_bursts` natively.** REJECTED. 85% of it is `np.partition`,
  already optimal native code.

## Measurement traps hit

1. **cProfile `tottime` attribution (the big one).** Inline numpy operators
   (`arr*arr`, `arr.real = ...`, `arr[::k]`, `arr *= s`) and fancy indexing do
   NOT register as separate profiled calls — their time is charged to the calling
   Python frame's self-time. Bucketing self-time by "is this frame a .py file"
   therefore labels native array work as "python glue." It made the first pass
   read 83% python in the stats stage when the true interpreter cost is ~one
   frame setup. Fix: count python CALLS per invocation (dispatch proxy) and
   compute implied bandwidth (bytes/time); do not trust self-time buckets.
2. **Wrong sample rate.** `bench_processing.py` uses 56 Msps; the sensor is
   26 Msps (per the detections CSV `sample_rate_hz`). Absolute ms differ ~2x;
   re-ran everything at 26 Msps.
3. **Wrong hardware.** These are i9 numbers. The realtime wall is on nano-super
   (aarch64, much lower DRAM bandwidth). The *ratios* (memory-bound, interpreter
   negligible) carry over; the *headroom* does not. Confirm fusion wins there
   before acting.

## nano-super (Orin Nano, aarch64, 15W profile) — the real target

Ran the same corrected probe on nano-super at its default 15W profile (6 cores,
numpy 1.26.4 / scipy 1.13.1; its checkout lacks the channelizer, so that stage is
absent). PSD used `num_workers=-1` (all 6 cores) as the pipeline does.

| Stage | nano-super 15W | i9-12900K | slowdown | implied GB/s (nano) |
|---|---|---|---|---|
| IQ trigger check (0.1 s) | 0.12 ms | 0.03 | 4x | 90 (trivial) |
| convert_bytes_to_complex (0.5 s) | 70.9 ms | 25.8 | 2.8x | 2.2 |
| calculate_iq_statistics (0.5 s) | 70.0 ms | 38.3 | 1.8x | 4.5 |
| compute_psd_grid (0.5 s) | **340.8 ms** | 67.3 | 5.1x | 1.2 |
| detect_bursts (grid) | 28.0 ms | 9.7 | 2.9x | 0.1 (partition) |

**Per-window total (trigger+convert+stats+psd+detect): ~510 ms to process 500 ms
of data.** At 15W the raw per-window compute is at/just over the realtime budget —
no headroom, unlike the i9 (141/500).

Findings:
- Interpreter overhead is negligible on aarch64 too (convert = 3 Python calls,
  71 ms). Confirms the language is not the lever.
- nano-super is severely **memory-bandwidth starved**: 1.2-4.5 GB/s vs the i9's
  6-8. That is why PSD is 5x slower, not 2-3x like the others.
- PSD dominates (340 of 510 ms). Of it, ~68 ms is the (already 6-core) FFT and
  ~270 ms is SINGLE-THREADED, memory-bound element-wise work (|.|^2, log10, mean,
  nan_to_num) plus reshape (38 ms) + copy (23 ms). FFT workers do not touch that
  270 ms. THIS is the target.

Revised recommendation:
- The one lever that matters on the real hardware is `compute_psd_grid`, and
  specifically its single-threaded element-wise + copy churn (~270 ms).
- Cheapest test, no language change: `numexpr` (multi-threads element-wise
  expressions across the 6 cores) and/or restructuring to avoid the reshape/copy
  intermediates. Could reclaim a large fraction of the 270 ms.
- A fused C++/Rust + NEON kernel is a legitimate option for that same stage, but
  the win is fusion + threading of ONE stage, not a language migration, and not
  the isolation path.
- The FPGA PSD/stats offload is strongly justified by this data: PSD is THE
  bottleneck on the sensor, and offloading it removes 340 of the 510 ms.

Caveats on these numbers:
- 15W profile (memory: nano-super is not MAXN-capable). A higher-power/clock
  profile would raise bandwidth and shrink these; measure before assuming.
- This is raw per-0.5s-window compute. Actual duty cycle depends on the streaming
  cadence (detect_bursts on a full grid is already noted as multi-second work and
  is throttled — streaming.py:2251), so "over realtime" is about the per-window
  compute headroom, not a proven end-to-end overrun.

## Prototype 1: fused numexpr compute_psd_grid (nano-super, 15W)

Built `compute_psd_grid_fused`: fuses windowed-copy+Hann into one numexpr pass,
computes |z|^2 directly as `re**2+im**2` (no sqrt, replacing np.abs+np.square),
multi-threads the element-wise ops via numexpr across all 6 cores, uses a float32
accumulator, and fuses log10+scale+floor into one `where(g>0,10*log10(g),-200)`.

**Equivalence: exact.** max|dB diff| = 0.0000 vs the current implementation.

**Speedup: only ~1.1x** (352 -> 303-323 ms). Section breakdown of the fused loop:

| section | ms |
|---|---|
| windowed copy + Hann (numexpr) | 52.5 |
| FFT (scipy, workers=-1) | 124.4 |
| power \|z\|^2 (numexpr) | 68.1 |
| mean over ffts/slice | 47.2 |
| dB (numexpr) | 4.1 |
| total | 303 |

numexpr DOES thread (standalone |z|^2: 38.8 ms @1 thread -> 12.6 ms @6, 3.1x). The
limit is structural: this grid computes ~99k FFTs over the 0.5 s window (0.2 ms
time res, 50% overlap), so the **FFT (124 ms, already 6-core) is now the single
largest cost** and the threaded element-wise ops cannot move the total much. The
CPU is near its practical floor for this workload. numexpr fusion is a real but
minor win; not worth a dependency on its own.

## Prototype 2 RESULT: native CUDA compute_psd_grid — 7.4x

CUDA 12.6 toolkit installed on nano-super (cuda-nvcc-12-6 + cuda-cudart-dev-12-6
+ libcufft-dev-12-6; lean set, no CuPy). Wrote `psd_cuda.cu`: cuFFT batched
plan (created once, reused) + three kernels — windowing+Hann, fused |z|^2+mean,
and dB+fftshift — behind an init/run/free C API, called from Python via ctypes.
Built with `nvcc -O3 -shared -Xcompiler -fPIC -gencode arch=compute_87,code=sm_87`
(Orin = sm_87). Scripts in `docs/debugging/2026-09-30_psd-cuda-proto/`: `psd_cuda.cu`,
`bench_psd_cuda.py`.

**Equivalence: exact.** max|dB diff| = 0.0000, mean 3e-6 vs the current CPU code.

**Timing (nano-super, 15W, includes host<->device transfers):**

| impl | mean | min | vs CPU |
|---|---|---|---|
| CPU (current) | 343 ms | 336 ms | 1.0x |
| numexpr fused (proto 1) | ~310 ms | 303 ms | 1.1x |
| **native CUDA (proto 2)** | **46.4 ms** | **36.6 ms** | **7.4x** |

The GPU removes PSD from the per-window bottleneck: 340 -> 46 ms takes the
per-0.5s-window total from ~510 ms (over budget) to ~215 ms. This is the only
prototype that changes the realtime picture on the sensor.

Caveats:
- 15W profile; a higher-power profile would widen the gap further.
- 46 ms includes the 104 MB input copy over the shared bus; pinned/managed
  (zero-copy) memory on the Orin's unified DRAM could cut it further.
- Verified on synthetic data (noise + tone); confirm against a real capture
  before trusting in production.
- Deployment cost: needs nvcc at build (or a prebuilt per-arch .so) and
  cudart+cufft at runtime; MUST fall back to the CPU path when CUDA is absent
  (the config toggle + runtime detection below).

## Prototype 3: CUDA v2 — memory mode + kernel restructure (19.0 ms, 18x)

Sources for all prototypes are preserved in `docs/debugging/2026-09-30_psd-cuda-proto/`
(the session scratchpad and nano-super's `/tmp/psdcuda/` are both temporary).
Build on nano-super:
`nvcc -O3 -shared -Xcompiler -fPIC -gencode arch=compute_87,code=sm_87 psd_cuda2.cu -lcufft -L/usr/local/cuda-12.6/targets/aarch64-linux/lib -o libpsdcuda2.so`,
run with `LD_LIBRARY_PATH=/usr/local/cuda-12.6/targets/aarch64-linux/lib`.

Step 1 was per-stage cudaEvent timing of prototype 2 (43.6 ms event-timed):
H2D copy 19.2 ms (44%), extract+Hann 9.9, cuFFT 9.1, power+mean 4.6, dB 0.2,
D2H 0.7. The biggest cost was the pageable input copy, not compute.

v2 (`psd_cuda2.cu`) makes the input memory selectable and adds a second kernel
set: block-per-window extract (thread = bin, no per-element div/mod) and a
single block-per-slice kernel fusing |z|^2 + mean + window_norm + dB + fftshift.
The input is 26 Msps SC16 with a tone, converted by the real `convert_sc16_to_complex`.

| input memory | v1 kernels | v2 kernels | exact? |
|---|---|---|---|
| pageable + memcpy (proto 2) | 48.6 ms | 47.5 ms | yes (0.0000) |
| pinned + memcpy | 28.2 | 26.4 | yes |
| zero-copy mapped pinned | 19.8 | **19.0** | yes |
| managed (unified) | 18.8 | 17.8 | yes |

(wall time per call from Python, including handing the caller a copied grid;
CPU `compute_psd_grid` in the same run: 342 ms.)

CPU producer cost of writing into each memory type
(`convert_sc16_to_complex(sc16, out=buf)`, 13M samples): numpy 73.2 ms,
pinned mapped 73.1 ms, managed 73.2 ms. **No penalty**: on the Orin, pinned and
managed memory are CPU-cached, so the pipeline can convert straight into
GPU-visible memory and the zero-copy saving is real, not moved elsewhere.

Device attributes (cudaDeviceGetAttribute on nano-super):
`ConcurrentManagedAccess = 0`, `PageableMemoryAccess = 0`,
`CanUseHostPointerForRegisteredMem = 0`, `DirectManagedMemAccessFromHost = 0`.

**Choice: zero-copy mapped pinned + v2 kernels, 19.0 ms (18x the CPU).** Managed
is 1.2 ms faster, but see the trap below: it is unsafe for the streaming pipeline.

Per-0.5 s-window total at 15W becomes trigger 0.1 + convert 70.9 + stats 70.0 +
PSD 19.0 + detect 28.0 = **~188 ms (was ~510 ms)**. PSD is no longer the
bottleneck; CPU convert (71 ms) and IQ stats (70 ms) now are.

### Measured and REJECTED (prototype 3), do not retry

- **"The kernels run at half the bandwidth because of per-element integer
  division, so restructuring them is the next big win."** Mostly wrong.
  Block-per-window extract plus fusing power+mean+dB saved about 1 ms (5%) in
  every memory mode (managed 18.8 -> 17.8, pageable 48.6 -> 47.5). Memory mode
  was the entire win (48.6 -> 18.8 with the SAME v1 kernels).
- **Managed memory for the streaming pipeline.** Rejected despite being fastest
  (17.8 ms); see trap 2.

### Measurement traps (prototype 3)

1. **Stage times move with the input mode even for stages that only touch
   device memory.** cuFFT took 9.4-9.8 ms with pageable input but 6.3-6.5 ms with
   pinned/zero-copy/managed, and power+mean 4.9-5.1 vs 3.2-3.4 ms, although
   both always operate on `d_win` in device memory. Unverified hypothesis: GPU
   DVFS. The GPU idles through the 19 ms CPU-driven pageable copy, the devfreq
   governor drops its clock, and the following kernels run slower. Consequence:
   per-stage GPU timings are clock-dependent; do not compare stage times across
   runs with different idle patterns without pinning clocks (`jetson_clocks`),
   which was NOT done here.
2. **Managed memory looked fastest and would crash the real pipeline.** With
   `ConcurrentManagedAccess = 0`, the CPU may not access ANY managed allocation
   while any kernel is running, unless the buffer is attached to a stream
   (`cudaStreamAttachMemAsync`). The benchmark is single-threaded and
   sequential, so it never violated this; the streaming pipeline writes the next
   chunk while the GPU processes the current one and would fault. Zero-copy
   mapped pinned memory has no such rule (normal double-buffering still applies).

## (superseded framing) Prototype 2 pre-install note

The Orin's iGPU shares LPDDR5 with the CPU but reaches it far more efficiently
(~102 GB/s vs the 1-4 GB/s effective we measure for strided numpy), and cuFFT
would crush the 99k batched FFTs. Estimated GPU total is plausibly 5-10x faster,
which is the only path that meaningfully changes the 340 ms. BUT:

- nano-super (L4T R36.5.0 / JetPack 6.2) has **no CUDA toolkit/runtime installed**:
  no `/usr/local/cuda`, no `libcufft.so`, no `nvcc`, no TensorRT.
- Both approaches need it: CuPy (fast to prototype, large dependency) and a
  hand-written `.cu` + cuFFT via ctypes (smaller runtime, more work) each require
  cudart + cuFFT at runtime and nvcc to build.
- So the GPU prototype cannot be measured until CUDA is installed on the box.

## Open, not yet answered

Resolved since first written (kept for the record):
- ~~Actual headroom on nano-super at 26 Msps~~: measured, ~510 ms per 0.5 s
  window at 15W (over budget), ~188 ms with the zero-copy CUDA PSD.
- ~~How much a fused PSD kernel saves~~: numexpr CPU fusion 1.1x; native CUDA
  7.4x (pageable), 18x (zero-copy mapped). See prototypes 1-3.

Still open:
- The DVFS hypothesis for trap 1 above: re-time with clocks pinned
  (`jetson_clocks`, then restore) to separate clock effects from memory effects.
- CUDA on the field sensor (`rfnano`): nano-super had NO CUDA toolkit until it
  was installed for this work (2026-09-30). Whether rfnano has cudart + cuFFT is
  unknown; the GPU path must fall back to CPU cleanly when they are absent.
- Equivalence on a real capture. All checks here used synthetic noise + tone.
- Moving convert (71 ms) and IQ stats (70 ms) to the GPU too. The producer can
  write raw SC16 into pinned memory and a kernel can convert it (or the extract
  kernel can read SC16 directly). This is now the largest remaining per-window
  cost, not PSD.
- cuFFT load/store callbacks to fuse extract into the FFT load and power+mean
  into the FFT store (removes both 202 MB passes over `d_win`). Not tried; needs
  static cuFFT and device linking. PSD-only win, now of limited system value.
- Interaction with the FPGA PSD/stats offload: if PSD + IQ stats move to the
  B205mini FPGA, both the GPU PSD path and a GPU stats path become redundant.

## CORRECTION (2026-09-30, later): production geometry, equivalence, zero-copy

Three earlier claims were wrong or too broad. They are left in place above and
corrected here.

1. **Wrong benchmark geometry.** Prototypes 1-3 used 0.5 s windows (13M samples)
   with 256 bins and multi-threaded CPU FFTs. Production is different:
   `RFOBS_BANDWIDTH=26000000`, `NUM_FFT_BINS=2048` (the default), 0.2 ms rows and
   `STREAMING_CHUNK_SLICES=200` give **1,024,000-sample chunks (39.4 ms of
   signal), 800 FFTs of 2048 points each, ~25 chunks/s**. Each of the 3 streaming
   workers runs its FFT single-threaded (`num_workers=1`). The ~510 ms and
   ~188 ms "per-window total" figures above are for the wrong geometry and are
   WITHDRAWN as a description of the deployed pipeline.
2. **"Exact (max diff 0.0000 dB)" was too broad.** It held for the averaged
   geometries benchmarked. Both paths FFT in float32 (scipy keeps complex64 input
   single precision; cuFFT C2C is float32) with different algorithms. With one
   FFT per row and a strong tone, a row spans >100 dB and bins far below its
   peak sit in float32 rounding noise: measured 0.022 dB at a bin 104.7 dB below
   its row peak, but at most 0.00065 dB within 60 dB of the peak. Production
   geometry (4 FFTs averaged per row): at most 0.0007 dB anywhere. The GPU tests
   assert < 0.002 dB within 60 dB of each row's peak and < 0.1 dB anywhere.
3. **Zero-copy is a small win at production size, not 2.5x.** Per 1M-sample
   chunk, the GPU PSD took 4.0-4.2 ms in place vs 4.5-5.7 ms with the copy; the
   2.5x was an artifact of the 13M-sample windows.

### Production-geometry measurements (nano-super, 15W)

Micro-benchmark, per chunk (`bench_chunk.py`): CPU `compute_psd_grid`
(1 thread) 46.4 ms, GPU copy path 5.6 ms, GPU zero-copy 3.0 ms, convert 4.9 ms,
`moments_from_iq` 12.2 ms.

End to end: `rfobserver run` with the mock receiver at 26 Msps (real-time
paced), the worker's own `WORKER chunk#N` log line, last lines after warm-up:

| backend | convert | psd | stats | total per chunk |
|---|---|---|---|---|
| cpu | 6.1-8.4 ms | 41-69 ms | 12.8-17.7 ms | 61-95 ms |
| cuda (zero-copy) | 7.7-7.9 ms | 4.2-4.4 ms | 22.6-23.1 ms | ~36 ms |
| cuda, pinned pool off (copy path) | 7.0-7.4 ms | 5.3-5.7 ms | 19.9-23.4 ms | 33.5-37.3 ms |

### Measured and REJECTED: why stats got slower with the GPU (do not retry)

`stats` (moments + finalize) rose from 13-18 ms to 20-23 ms whenever PSD ran on
the GPU, with less total CPU load. Rejected, with the numbers that killed each:

- **CPU reads of pinned memory are slow.** Isolated `moments_from_iq`: pinned
  16.1 ms vs numpy 16.2 ms (`bench_pinned_reads.py`).
- **CPU clocks drop when the CPU is less busy (schedutil).** Mean clock over
  10 s of each run: cpu backend 1145 MHz, cuda backend 1154 MHz.
- **The GPU touching the buffer just before the CPU reads it (I/O-coherence
  side effects).** In the pipeline's own order (convert, PSD, moments) on one
  buffer: moments 15.3 ms pinned vs 15.7 ms numpy (`bench_sequence.py`).
- **Anything specific to pinned memory.** With the pinned pool disabled
  (`MAX_PINNED = 0`, every chunk takes the copy path in plain numpy memory),
  stats is still 19.9-23.4 ms.

Open: what does slow stats. Candidates not yet tested: DRAM bandwidth contention
from the other workers' GPU kernels, or thread/GIL scheduling with 3 workers.
Discriminating test: the sequence benchmark run on 3 threads at once, CPU vs
GPU backend.

### Measurement traps (this round)

- Benchmarking a geometry nobody runs (above). Read `_recompute_chunk_params`
  and the deployed env before choosing a benchmark size.
- `rsync` of the checkout to nano-super copied the local `.env` (ZMS endpoint
  and monitor id, no token) into the scratch copy. Removed; exclude `.env`.

### What was integrated (branch `feat/psd-cuda-backend`)

- `src/rfobserver/processing/cuda/psd_cuda.cu`: production kernel (prototype v2
  generalized: block-strided loops so any bin count works, 2048 being the
  default and above the 1024 threads-per-block limit; copy or zero-copy input).
- `src/rfobserver/processing/psd_cuda.py`: ctypes loader, per-geometry engines
  (locked, bounded), pinned buffer pool, CPU fallback signalling.
- `PSDGridConfig.backend` / `PSD_BACKEND` setting / Config page select;
  streaming converts into a pinned buffer when the backend is cuda.
- `deploy/build_psd_cuda.sh` (also called by `deploy/install.sh`, non-fatal);
  the `.so` is a hatch wheel artifact.
- Tests: `tests/unit/test_psd_backend.py` (runs anywhere),
  `tests/unit/test_psd_cuda_gpu.py` (skips without a GPU; 23 pass on nano-super).
