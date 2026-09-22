# Peak finder rollup: backfill cost on a field-size database

Date: 2026-09-21. Task 7 of `.superpowers/sdd/2026-09-21-dashboard-peak-finder/`.
See `docs/superpowers/specs/2026-09-21-dashboard-peak-finder-design.md`.

## The question

How long does `_rollup_backfill` (in `src/rfobserver/pipeline/app.py`) take to
walk a field-size `avg_windows` table backwards and fill `avg_minutes`, and
does any single `_rollup_span` call block the event loop long enough to
matter? This was the one number in the design spec that was estimated, not
measured. Scale: the field sensor runs at ~2 averaged windows/sec
(`DURATION_SEC=0.5`), `NUM_FFT_BINS=2048` so each `psd_powers` blob is 8192
bytes, and 30-day retention gives ~5.2M rows / ~30 GB at full depth. Measured
here: 7 days / 1,209,600 rows / 10.65 GB, on the development workstation
(12th Gen Intel Core i9-12900K, 31 GiB RAM, ext4 on NVMe, Ubuntu 22.04,
kernel 6.8.0-138-generic), Python 3.11 venv, `aiosqlite`, WAL journal mode
(`_configure_writer`'s defaults) -- not on the Jetson field box.

## The answer

A full 7-day backfill took **6.68 s wall clock** (1,209,600 rows scanned,
10,081 `avg_minutes` rows produced, peak RSS ~65 MB), i.e. **0.95 s per day
of history**, extrapolating to **~29 s for a full 30-day / ~5.2M-row
backfill**. A single `_rollup_span` call over one hour of real data (~7,200
rows, the unit the backfill and the live rollup both step by) took
**12-41 ms** across seven samples at different points in history. Both
figures clear the spec's bar (single span in the tens of milliseconds, full
backfill in minutes not hours) by roughly two orders of magnitude, so
**`_ROLLUP_SPAN` (1 hour) and `_ROLLUP_BUDGET_SEC` (5 s) are unchanged** --
`src/rfobserver/pipeline/app.py` was not touched. The workstation numbers are
not a safe stand-in for the Jetson field box; see "What was NOT determined"
below for how much confidence to place in the extrapolation.

## Procedure

1. **Build a field-scale database** at the real row layout: the
   `avg_windows` `CREATE TABLE` and both `CREATE INDEX` statements were
   copied verbatim from `SCHEMA` in `src/rfobserver/storage/database.py`
   (plus `avg_minutes` and `config`, since the backfill writes both), so the
   btree and index layout matches production. `psd_powers` was filled with a
   real 8192-byte blob (`os.urandom(8192)`, matching `NUM_FFT_BINS=2048`
   float32 bins) per row, not shrunk -- the blob is ~98% of a row's storage
   and is what spreads rows across pages, which is the thing being measured.
   Built with `sqlite3` directly (`journal_mode=OFF`, `synchronous=OFF`, no
   durability needed for a throwaway file), timestamps spanning
   `now - 7 days` to `now` so the rollup's forward-anchor step lands right at
   the newest row, matching how a live pipeline would find it.
   Script: `seed_backfill_db.py` (scratch dir, not in the repo).

2. **Anchor the rollup** the same way a first pipeline start does: call
   `_rollup_forward(db, now)` once. With no `rollup_newest` key yet this
   only sets the config anchor to the current minute and returns -- no scan
   (confirmed: 0.4 ms).

3. **Time one `_rollup_span` call** on the single newest hour of data,
   isolated from the backfill loop, to get the number that decides whether a
   step can block the event loop.

4. **Time the full backfill**: call `_rollup_backfill(db, now)` in a loop,
   recording each pass's wall time and the `rollup_oldest` config value,
   until two consecutive passes report the same `rollup_oldest` (the
   brief's stopping rule). Recorded total wall clock, pass count, resulting
   `avg_minutes` row count, and peak RSS (`resource.getrusage().ru_maxrss`).
   Script: `bench_backfill.py`.

5. **Sample `_rollup_span` at 6 more points** spread across the 7-day
   history (4h, 24h, 48h, 96h, 144h, 166h from the oldest row), run after the
   full backfill so these hit the upsert-overwrite path (the steady-state
   case once history is already folded), to check the single-hour timing
   was not a lucky outlier. Script: `bench_span_samples.py`.

6. **Deleted the synthetic database** (`rm backfill_bench.db*`) and
   confirmed free disk space returned to its prior level.

All scripts ran through the real production code paths
(`rfobserver.storage.database.SensorDatabase`,
`rfobserver.pipeline.app._rollup_forward/_rollup_backfill/_rollup_span`),
not a reimplementation, so the timings reflect what actually runs in the
pipeline.

## Evidence

Seed build (`seed_backfill_db.py`):

```
inserted=100000/1209600 elapsed=0.7s size=0.88GB
inserted=200000/1209600 elapsed=1.5s size=1.76GB
inserted=300000/1209600 elapsed=2.3s size=2.64GB
inserted=400000/1209600 elapsed=3.1s size=3.52GB
inserted=500000/1209600 elapsed=4.0s size=4.40GB
inserted=600000/1209600 elapsed=4.9s size=5.28GB
inserted=700000/1209600 elapsed=5.8s size=6.16GB
inserted=800000/1209600 elapsed=7.2s size=7.05GB
inserted=900000/1209600 elapsed=7.9s size=7.93GB
inserted=1200000/1209600 elapsed=11.2s size=10.57GB
inserted=1209600/1209600 elapsed=11.3s size=10.65GB
DONE inserted=1209600 elapsed=11.3s size=10.65GB
```

Full backfill (`bench_backfill.py`):

```
rss before connect: 55.8 MB
rss after connect: 56.4 MB
oldest_avg_window_time=2026-09-15T03:53:27.979160+00:00 now=2026-09-22T03:54:03.215988+00:00
_rollup_forward (anchor-only, first run) took 0.0004s
SINGLE _rollup_span(1 hour): elapsed=0.0224s written=60 rows since=2026-09-22T02:54:00+00:00 until=2026-09-22T03:54:00+00:00
pass=1 pass_elapsed=5.087s rollup_oldest=2026-09-15T20:54 rss=63.1MB
pass=2 pass_elapsed=1.596s rollup_oldest=2026-09-15T03:53 rss=63.1MB
pass=3 pass_elapsed=0.000s rollup_oldest=2026-09-15T03:53 rss=63.1MB
total backfill wall clock: 6.68s over 3 passes
avg_minutes row count: 10081
peak RSS: 63.1 MB
seconds per day of history (7 days): 0.955 s/day
extrapolated 30-day backfill: 28.6 s = 0.48 min
```

`/usr/bin/time -v` on the same run: `Maximum resident set size: 64816` KB,
elapsed 0:06.95, user 2.87s + sys 1.58s CPU.

Note pass 1 runs to the `_ROLLUP_BUDGET_SEC=5` budget and pass 2 finishes the
rest in 1.6 s; pass 3 is the confirmation pass with nothing left to do
(0.000 s), which is the loop's stopping condition.

Additional `_rollup_span` samples at 6 points across the 7-day history
(`bench_span_samples.py`, run after the backfill so `avg_minutes` rows
already exist -- this is the upsert-overwrite cost, the steady-state case):

```
offset=   4h since=2026-09-15T07:53:27.979160+00:00 elapsed=0.0413s written=61
offset=  24h since=2026-09-16T03:53:27.979160+00:00 elapsed=0.0127s written=61
offset=  48h since=2026-09-17T03:53:27.979160+00:00 elapsed=0.0120s written=61
offset=  96h since=2026-09-19T03:53:27.979160+00:00 elapsed=0.0121s written=61
offset= 144h since=2026-09-21T03:53:27.979160+00:00 elapsed=0.0122s written=61
offset= 166h since=2026-09-22T01:53:27.979160+00:00 elapsed=0.0120s written=61
```

Range across all 7 single-hour samples (the isolated one plus these six):
12.0-41.3 ms. All comfortably inside "tens of milliseconds is fine".

Disk before build: 297 GB free. After build: 287 GB free (10 GB consumed, as
predicted). After `rm backfill_bench.db*`: back to 297 GB free, confirmed
with `df -h`.

## Measured and REJECTED (do not retry)

Nothing was rejected. The measured numbers cleared the spec's bar by a wide
enough margin (roughly 2 orders of magnitude on both the per-span and the
full-backfill figures) that there was no case for lowering `_ROLLUP_SPAN` or
raising `_ROLLUP_BUDGET_SEC`, so no alternative constants were tried. Do not
read the comfortable margin here as license to skip re-measuring after a
future schema or hardware change -- see "What was NOT determined".

## Measurement traps hit

- **Warm page cache, not disk I/O.** The 10.65 GB file was written seconds
  before it was read back for the benchmark, and the workstation has 31 GiB
  RAM. `free -h` immediately after the backfill run showed `buff/cache`
  holding ~12 GiB with only 344 MiB `free` -- the whole database was very
  likely resident in the page cache for the entire benchmark. That means
  these numbers measure SQLite/Python overhead over a range scan, not real
  storage I/O. I do not have passwordless sudo on this workstation
  (`sudo -n true` failed), so `/proc/sys/vm/drop_caches` was not available
  to force a cold read, and deliberately evicting 10+ GB of page cache on a
  shared workstation by brute force (reading unrelated large files until
  eviction) was avoided as unnecessarily disruptive to other processes on
  the box. This is flagged rather than papered over with a fabricated cold
  number -- see "What was NOT determined".
- **UTC vs local time is not a bug.** `bench_backfill.py`'s `now` prints as
  `2026-09-22T03:54:03Z` while the seed script's on-disk file timestamp
  reads `Sep 21 21:53` local time. Both are correct: the workstation's local
  zone is UTC-6, so the two agree. Flagged because it looked like a stale
  read or a clock skew at first glance and was worth ruling out explicitly
  before trusting the anchor-alignment logic in step 2 above.
- **`_rollup_span`'s single-hour timing must be read alongside the loop's
  deadline check.** `_rollup_backfill` checks `time.monotonic() < deadline`
  *before* starting a span, not after, so one pass can run slightly past
  `_ROLLUP_BUDGET_SEC` (pass 1 above: 5.087 s against a 5.0 s budget). This
  is expected and bounded by one span's worth of overrun, not a bug; worth
  stating explicitly since a naive read of "5 second budget" might expect a
  hard cutoff.

## What was NOT determined

- **Cold-disk / cold-cache cost.** All numbers above are warm-cache. If the
  field box's storage is meaningfully slower than page-cache reads under
  memory pressure (more likely at the full 30-day / ~30 GB depth, which
  will not fit in the Jetson's RAM the way 10.65 GB fits in this
  workstation's 31 GiB), the real number could be higher than reported here.
  Not quantified.
- **Jetson field-box numbers were not measured, only extrapolated.**
  `nano-super` (ARM Cortex-A78AE via JetPack 6.2, far less RAM than this
  workstation, NVMe rootfs but a slower storage/CPU/memory subsystem
  overall, per `CLAUDE.md`) was not used for this task -- the brief scoped
  the measurement to a synthetic database on the workstation. Given the
  margins here (single span ~12-41 ms vs. a "tens of ms is fine, seconds is
  not" bar; full backfill ~29 s extrapolated at 30 days vs. a "minutes not
  hours" bar), a slowdown of even 10-20x would still land inside both
  bounds, so I am **reasonably confident but not certain** the current
  constants hold up on the Jetson too. This is an extrapolation with a
  comfortable margin, not a verified number -- if the Jetson benchmark is
  ever run, it should replace this paragraph rather than be assumed.
  Recommend timing it there before or shortly after first field deployment
  of this feature, since it is a one-time cost only paid once, on upgrade.
  If done, use the same `seed_backfill_db.py` / `bench_backfill.py` /
  `bench_span_samples.py` scripts (referenced in "Procedure" above; the
  seed script takes minutes to build a comparable 7-day fixture even at the
  Jetson's slower disk).
  These scripts were scratch files, not checked into the repo -- they were
  deleted along with the database. Recreate from the "Procedure" section
  above and the copy of the `avg_windows` schema in
  `src/rfobserver/storage/database.py` if a Jetson run is done later.
- **Behaviour under concurrent pipeline writes.** The benchmark ran the
  rollup alone against a static database; it did not measure the backfill
  running concurrently with an active mock or real pipeline writing new
  `avg_windows` rows and dashboard reads happening on the read-only
  connection at the same time. WAL mode is designed for exactly this, and
  the per-span time is small enough that this is unlikely to matter, but it
  was not directly observed.
- **30-day / full-depth database was not built.** Per the brief, only the
  7-day / 10.65 GB fixture was built and the 30-day figure is a linear
  extrapolation (`total_elapsed / 7 * 30`). The per-row/per-hour cost should
  scale linearly since every hour has the same row count and blob size by
  construction, but this was not independently confirmed at the full 30-day
  size.

---

## CORRECTION 2026-09-22: measured on the Jetson, and the open items above are now closed

The numbers above were taken on the x86 workstation (31 GiB RAM, warm page
cache). The run below was taken on **nano-super** (Jetson Orin Nano, aarch64,
6 cores, **7 GB RAM**, NVMe, Python 3.10.12), against a **10.6 GB / 1,209,600
row** fixture with real 8192-byte blobs, with the page cache dropped
immediately beforehand. 7 GB of RAM against a 10.6 GB database means the reads
were genuinely served from disk, which the workstation run could not claim.

The "open, not yet answered" items above about Jetson timing, concurrent
pipeline writes and the per-statement distribution are all closed by this run.
The 30-day figure remains a linear extrapolation.

### Cold backfill, two independent runs

```
Full cold backfill: 7 _rollup_backfill() calls, 29.6s wall
  avg_minutes rows produced: 10080
  history span backfilled: 7.00 days -> 4.22 s/day
  chunk statement timings across full backfill:
    n=1352 min=0.2ms median=15.8ms p95=18.6ms max=27.0ms mean=16.1ms

Full cold backfill: 7 _rollup_backfill() calls, 29.2s wall
  history span backfilled: 7.00 days -> 4.17 s/day
  chunk statement timings across full backfill:
    n=1352 min=0.1ms median=15.7ms p95=18.6ms max=28.3ms mean=15.9ms

Single _rollup_span(15 min): 0.041s, 16 avg_minutes rows written
```

So a 30-day field backfill extrapolates to about **two minutes**, and the
per-statement worst case is **27 ms**.

### Why this is the number that matters

A chunk read is one statement on the writer connection, and aiosqlite
serialises a connection through one worker thread, so it blocks the pipeline's
inline `insert_avg_window`. That insert is fed by an 8-slot queue that drops at
the producer rather than waiting. At roughly 25 chunks per second the queue
fills in about 300 ms, so a single statement blocking longer than that starts
silently discarding recorded spectrum. The measured maximum of 27 ms sits about
**11x under that line**, and the p95 of 18.6 ms about 16x under it.

### Contention observed directly

With the pipeline running against the same database and the backfill genuinely
walking seven days of unfolded history, across 54 TIMING lines:

```
TIMING recv#2700: recv=63.1ms dropped=0 (IQ=36.6ms) handoff_dropped=0/0 ovf=0 lost=0
```

Zero drops, and the write rate under active backfill (1.66 rows/s) matched the
steady-state rate (1.70 rows/s), so the rollup was not quietly starving the
pipeline either.

### The reason it is fast, which is worth knowing before anyone reorders columns

`iter_rollup_windows` selects only `start_time, sdr_center_freq_hz,
sample_rate_hz, gain_db, pwr_max, pwr_median, pwr_avg`, and every one of those
columns precedes `psd_powers` in the `avg_windows` row layout. SQLite parses a
record left to right and stops once it has the columns asked for, so it never
faults in the 8 KB blob's overflow pages. Measured disk I/O for the full 7-day
cold backfill was about **708 MB, not 10 GB**.

This is a property of the column ORDER in `CREATE TABLE avg_windows`, not
something the query asks for. If `psd_powers` is ever moved earlier in the
table definition, this backfill gets dramatically slower and the safety margin
above evaporates. Treat the column order as load-bearing.

### Suites on the target hardware

- Unit: 549 passed, 33.5 s.
- Integration: 104 passed, 12 skipped, 0 failed, 14 min 2 s. The skips are
  `@pytest.mark.slow` burst-matrix cases gated behind `--runslow`. `nats-server`
  is not installed on that box and its absence caused zero failures.

### Measurement traps hit during this run

- **A backfill that has already finished proves nothing.** The first contention
  observation was taken after the benchmark runs had already walked
  `rollup_oldest` to the floor, so the rollup loop had only the trivial forward
  pass to do. It had to be re-queued by resetting `rollup_oldest` back to the
  newest minute before the zero-drop result meant anything. Check the watermark
  is not already at the floor before trusting a contention result.
- **`pkill -f "rfobserver run"` over ssh kills the calling shell**, exactly as
  this project's CLAUDE.md warns, and so does `pgrep -f rfobserver` when the
  pattern appears in the command's own text. Match on the venv path instead, or
  use `fuser -k 8888/tcp`.

### Still not determined

- The field sensor's real 30 GB / 30-day database and a real SDR were not
  tested; the 30-day figure is extrapolated by row count from the 7-day fixture.
- nano-super runs at the 15 W profile and is not MAXN-capable, so it is if
  anything a pessimistic proxy for CPU, and a fair one for I/O.

### CORRECTION to the trap above, same day

The trap entry above states that the contention observation had to be re-queued
before it meant anything. That is wrong about the run actually reported here.
The operator who ran it has transcript evidence that the watermarks and
`avg_minutes` were empty immediately before the pipeline was launched
(`config rows left: []`, `avg_minutes rows: 0`), and that `rollup_oldest`
reached the floor for the first time only about 140 s into the run. So the
observed window was genuinely during an active backfill.

What happened is that a second observer checked the watermark later, after the
backfill had completed, saw it at the floor, and wrongly inferred the whole run
had been steady-state. The general advice in the trap still stands: check the
watermark is not already at the floor before trusting a contention result. The
specific claim that this run was invalid does not.

Either reading gives the same verdict, since neither shows any queue pressure.
