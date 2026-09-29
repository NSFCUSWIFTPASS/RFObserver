# Averaged PSD waterfall shows dark vertical stripes that are not data gaps

## 1. The question

2026-09-28. The user wrote: "the blank spots in the Average PSD render, I don't think
these are actual gaps in data (which is fine), it happens in 15 minutes duration
(haven't tested all). Also probably related to screen size/resizing the tab window."

Screenshot (Dashboard, Averaged PSD Waterfall, 2026-09-20 about 19:57 local): the header
reads "143 windows, no averaging (raw rows)", the axis runs about 19:56:40 to 20:01:05,
and dark stripes sit between nearly every row, about 40 to 50% of the width. The build
and host of the screenshot were not stated. From the timestamps it is the deployed sensor,
not the workstation.

## 2. The answer

Every averaged window is stored with `duration_sec = DURATION_SEC` (the nominal length).
Windows are actually written further apart than that, and the waterfall paints each raw
row only across `[start_epoch, start_epoch + duration_sec)`. The uncovered remainder of
each period stays the dark "gap" colour. The data is not missing: the chunks from that
remainder are averaged into the next window. A second, related defect is that
`start_time` is taken when the window is persisted, which is its end, not its start. So
rows are also drawn about one window later than the data they hold.

## 3. The procedure

1. **Read the render path.** `averaged.js`, `renderWaterfall` and `colSpan`.
   - The canvas is filled with the dark base.
   - Each bucket with `count > 0` is stamped over the pixel columns
     `floor(x(start))` to `ceil(x(start + duration_sec))`.
   - In raw mode (window count <= `MAX_ROWS` = 600), each row is a real window with
     its own start and duration.

   This isolates the renderer: a stripe means a window's duration is shorter than
   the spacing to the next window.
2. **Read the writer.** `streaming.py`, `_result_consumer_loop` and `_persist_avg_window`.
   - A window closes when the first result arrives with
     `time.monotonic() - accum_start >= DURATION_SEC`.
   - `accum_start` is reset only after the awaited work: the tone check, the UI
     broadcast, and `_publish_processed`, which awaits the DB insert inline.
   - The row is written with `start_time=datetime.now(timezone.utc)` (the persist
     time) and `duration_sec=s.DURATION_SEC`.

   This isolates the stored metadata from the real period.
3. **Measure real spacing against the stored duration.** Workstation, `main` at 22bd52f,
   mock receiver, `RFOBS_SENSOR_ACTIVE=true`, default `DURATION_SEC=0.5`. After about 60
   windows, `GET /api/averaged`. This measures the writer with no renderer involved.
4. **Explain why it shows at 15 minutes and depends on screen size.**
   - Raw mode needs 600 windows or fewer in the range. At 0.557 s spacing, 15 minutes
     is about 1600 windows, so the workstation is aggregated: buckets tile the width
     and there are no stripes. The deployed sensor's screenshot shows about 1.85 s
     spacing, which puts 15 minutes at about 485 windows: raw mode, so stripes.
   - Width: `colSpan` rounds the start down and the end up. When a gap is under one
     pixel it disappears; widen the window, or zoom, and the gaps become whole pixels.

## 4. Evidence

Step 3, raw output:
```
n 62 duration_sec {0.5}
spacing s: min 0.514 p50 0.557 p90 0.593 max 0.605
covered fraction (dur/spacing): 0.90
```
The chunk is 36.6 ms (`StreamingProcessor: chunk=2048000 samples (36.6 ms), 21 PSD workers`).
So a window closes on the first chunk past 0.5 s (up to +37 ms), plus the awaited work.

Screenshot arithmetic (inferred, not measured on that host): 143 windows over about
265 s is about 1.85 s apart. The stripes cover about 45% of each period, so the stored
`duration_sec` is about 1.0 s there (the deploy example sets `RFOBS_DURATION_SEC=1.0`).

`start_time` is set in `_persist_avg_window`
(`start_time=datetime.now(timezone.utc)`), after the window has closed.

## 5. Measured and REJECTED (do not retry)

- "Real data gaps": rejected. The windows are contiguous in time, about 0.56 s apart
  with 0.5 s stored duration locally. Nothing between them was dropped by this mechanism.
- "A canvas scaling or resize bug in `putImageData`": rejected as the cause. Width only
  changes how visible the sub-window gaps are (floor/ceil rounding). The stripes come
  from the stored durations.

## 6. Measurement traps

- `/api/averaged` does not exist on the .177 box (version 0.9.0b0 returns 404), so the
  deployed-sensor numbers above come from the screenshot, not from its DB.
- On the workstation a 15-minute range is aggregated, not raw, so the symptom does not
  show there at 15 minutes. Use a shorter range (under about 5 minutes) to see raw rows
  locally.

## 7. Corrections

None yet.

## 8. Open, not yet answered

- Why the deployed sensor's spacing is about 1.85 s against 1.0 s: the time spent in
  the awaited work per window (DB insert behind the writer thread, tone check, broadcast)
  on the Jetson. Not measured.
- Whether any results are dropped while that work runs (the result queue bound), which
  would make some of the time truly missing. Not checked.
- `detections_for_window` joins on `start_time` and `duration_sec`, so a window's
  "Detections in Range" is likely offset by one window too. Not verified.

## 9. Fix (2026-09-28)

Branch `fix/avg-window-times`, commits 5693346 (source) and ffc5733 (render fallback).

### What changed

- **Source** (`streaming.py`). A small `_AvgWindowClock` tracks each window's
  real span. It starts at the first result's arrival, or at the previous window's
  close in steady state, so consecutive windows tile. The window closes before the
  awaited tone check, broadcast and DB insert, so results arriving during that work
  belong to the next window. The idle-flush path (queue empty for 0.5 s) ends the
  window at its last result's arrival, and the next window starts on the next
  result. `_publish_processed` and `_persist_avg_window` now take
  `start_time` and `duration_sec` as required keyword arguments and read no
  clocks. Both monotonic (duration) and wall (start) clocks are read at each
  boundary, so a wall-clock step does not accumulate into later windows.
- **Render fallback** (`averaged.js`, `rowEndSec` used by `colSpan`). Raw mode
  only: a row's painted span runs to the next row's start when the gap after its
  own `start + duration_sec` is under 2x its `duration_sec`. Longer gaps stay
  dark. Aggregated mode is unchanged. `rowForPixelX` already picks the latest row
  starting at or before the click, which is the row painted there, so it needed
  no change.
- **Side effect on cadence.** Because the next window now starts at the previous
  close rather than after the awaited work, the window cadence tightens: the
  awaited work no longer adds to the spacing (p50 0.559 s to 0.532 s locally).
  Samples are not lost either way; before, the data from that time went into the
  next window but its span was not recorded.

### Tests

`tests/unit/test_streaming_avg_window.py`, written RED first (all three failed on
22bd52f):
- `test_stored_windows_have_real_start_and_duration_and_tile`: drives
  `_result_consumer_loop` with a fake monotonic and wall clock (patched on the
  streaming module only, so the event loop keeps its real clock), results every
  0.1 s and a DB insert that takes 0.25 s. It asserts that the first window
  starts at the first result, lasts 0.5 s, and that every
  `next.start == prev.start + prev.duration_sec`.
- `test_flushed_window_ends_at_its_last_result`: the idle-flush path.
- `test_detection_joins_the_window_whose_real_span_contains_it`: the same loop
  against a real `SensorDatabase`. A burst in the middle of window k's real span
  is returned by `detections_for_window` for window k only. This closes the
  third item of section 8 (the join is right once the stored span is real).

No JS unit harness exists (tests/ui is puppeteer against a live instance), so the
render fallback was checked manually as below.

### Before and after (workstation, mock receiver, DURATION_SEC=0.5)

Measured with `GET /api/averaged?limit=2000` after about 140 windows.

Before, 22bd52f (worktree of main):
```
n 140 duration_sec: min 0.500 p50 0.500 max 0.500 distinct 1
spacing s: min 0.511 p50 0.559 p90 0.588 max 0.621
next.start - (start+dur) s: min 10.64ms p50 59.25ms max 120.93ms
covered fraction sum(dur)/span: 0.896
```
After, ffc5733:
```
n 144 duration_sec: min 0.501 p50 0.532 max 0.592 distinct 73
spacing s: min 0.501 p50 0.532 p90 0.572 max 0.592
next.start - (start+dur) s: min -0.00ms p50 0.00ms max 0.00ms
covered fraction sum(dur)/span: 1.000
```

Waterfall (headless Chrome, 20 s absolute range, raw mode, canvas 656 px wide;
dark columns counted on the middle pixel row between the first and last data
column):

| Case | Code | Data | Dark columns |
|---|---|---|---|
| before | 22bd52f | rows written by 22bd52f | 44 of 650 (6.8%) |
| fallback | ffc5733 | the same old rows (copied DB) | 0 of 644 |
| after | ffc5733 | rows written by ffc5733 | 0 of 650 (also 0 at 60 s) |

Screenshots (scratch, not committed):
- `/tmp/claude-1000/-home-orencollaco-GitHub-RFObserver/50689e78-60fa-453e-89bd-c3c811638a7b/scratchpad/avgfix/before_waterfall_20s.png`
- `/tmp/claude-1000/-home-orencollaco-GitHub-RFObserver/50689e78-60fa-453e-89bd-c3c811638a7b/scratchpad/avgfix/fallback_oldrows_20s.png`
- `/tmp/claude-1000/-home-orencollaco-GitHub-RFObserver/50689e78-60fa-453e-89bd-c3c811638a7b/scratchpad/avgfix/after_waterfall_20s.png`
- `/tmp/claude-1000/-home-orencollaco-GitHub-RFObserver/50689e78-60fa-453e-89bd-c3c811638a7b/scratchpad/avgfix/after_waterfall_60s.png`

### Measurement traps hit

- A 60 s range on a 656 px canvas hides the local stripes: a 0.06 s gap is about
  0.65 px, and `colSpan`'s floor/ceil rounding covers it (1 dark column at 60 s
  before the fix). Use about 20 s locally to make them whole pixels.
- `rfobserver web` (web only) returns 503 on `/api/averaged/*`, so rendering an
  existing DB needs `rfobserver run` pointed at a copy of it.

### Open, not yet answered

- Why the Jetson sensor spaced windows about 1.85 s apart against 1.0 s. The
  fix makes the stored spans honest there too, but the awaited work per window on
  the Jetson was not measured.
- Whether results are dropped while that work runs (the result queue holds 8), in
  which case part of a window's span holds no samples. Not checked.
- The ZMS/NATS envelope has the same shape: `_build_envelope` sets
  `MetadataRecord.timestamp=datetime.now()` at publish (the window's end) and
  `length=DURATION_SEC`. For an IQ capture file that field means the capture
  start. It is left unchanged, since changing it changes the envelope contract.
- The fallback cannot tell a short real outage (under 2x duration) from the old
  spacing bug, and will paint over it on rows written before the fix.

### Review follow-up (2026-09-28)

A review of 5693346/ffc5733 found one regression and four smaller issues, fixed in
one follow-up commit.

1. **CORRECTION: a window could span an outage (regression from 5693346).** After a
   steady-state close the next window starts at the close instant. If results then
   stopped (reconfigure, receiver restart, stall), the idle-flush branch did nothing
   because nothing was pending, so the clock was never reset, and the first window
   after the outage claimed the whole outage. Reviewer's reproduction: results at
   0.1 to 0.6 s, then from 10.0 s, stored `(0.1, 0.5), (0.6, 9.4), (10.0, 0.5)`,
   where main stored `(10.0, 0.5)`. The claim in "What changed" that windows tile is
   withdrawn for this case. Fix: the idle branch resets the clock when nothing is
   pending. Also, a window opened by a close restarts at its first result when the
   consumer waited longer than `max(0.2 s, 4 chunks)` for it, which covers stalls
   under the 0.5 s idle timeout. The wait is measured from when the consumer was
   ready for the next result, not from the previous arrival: results queue up while
   the awaited per-window work runs (0.85 s per window was inferred on the Jetson),
   and measuring from the previous arrival would restart nearly every window there.
   Now stored: `(0.1, 0.5), (10.0, 0.5)`.
2. **The minute rollup could skip the last window of a minute.** Windows are
   inserted about one window after their start, and `_rollup_forward` folded up to
   `now.replace(second=0)`. It now folds up to `now - lag`, with
   `lag = max(10 s, 4 * DURATION_SEC)` (`_rollup_lag`).
3. **Exactly 600 raw rows.** The client tested `bucketCount < MAX_ROWS`, and the
   server returns raw when the window count is `<= max_rows`. The binary header
   has no mode flag, and the row count alone is ambiguous: aggregation also yields
   `max_rows` or `max_rows + 1` buckets. `parseWaterfall` now sums the per-row
   window `count`s, which equal the range's window count in both modes, and sets
   `isRaw = sum <= MAX_ROWS`. The status line, the label and `rowEndSec` use it.
4. **A window could span a retune.** A change of `center_freq_hz` now ends the
   pending window at its last result (flushed and stored), and the new tuning
   starts its own window. This deviates from the review's "reset the
   accumulation": in sweep mode a dwell is `int(DURATION_SEC / chunk)` chunks,
   which is shorter than `DURATION_SEC`, so dropping the partial window would drop
   every window.
5. **Leftovers.** `_last_wall` was removed. The tone-check row timestamp and the
   averaged broadcast's `chunk_time_ms` are now the window's start, the same time
   as its stored row, not the time of the awaited work.

Tests added (each failed on ffc5733): `test_window_after_an_outage_starts_when_results_resume`,
`test_short_stall_after_a_close_does_not_stretch_the_next_window`,
`test_retune_ends_the_window_at_the_old_tunings_last_result`,
`test_tone_check_is_stamped_with_the_window_start`,
`test_forward_lag_keeps_a_late_inserted_window_in_its_minute` and
`test_rollup_lag_covers_several_windows`. Item 3 has no JS unit harness and was not
re-screenshotted.

Open follow-ups (not done here):
- Window timing is by result arrival at the consumer, not stream time. Arrival
  jitter (worker completion order, event-loop stalls) goes into the spans. The
  reviewer recommends carrying a `recv_time` (or stream sample position) in
  `_StreamResult` and timing windows from it.
- The ZMS/NATS envelope has the same bug: `MetadataRecord.timestamp` is set at
  publish (the window's end) and `length` is always `DURATION_SEC`. Low risk to
  fix now that the real start and duration are available at `_publish_processed`,
  but it changes what the RFS ingest receives, so tell its owner first.
- The gap limit is fixed from the chunk duration at the start of the consumer
  loop; a reconfigure that changes the chunk length does not update it (the
  0.2 s floor dominates at the default chunk of 36.6 ms).

## 11. Measurement on nano-super (2026-09-28)

The second review simulated the Jetson with about 0.85 s of awaited work per window
(inferred from the screenshot's 1.85 s spacing). It found that closing the window
before that work makes windows come more often and drop more results when the work is
close to DURATION_SEC. The actual work was measured before deciding.

Procedure: nano-super (15W), `main` 22bd52f against `fix/avg-window-times` 99c2a71,
each run in its own clone. Mock receiver, `BANDWIDTH=26000000`, 915 MHz,
`DURATION_SEC=1.0`, 150 s per run, `GET /api/averaged`, first 3 windows skipped.

```
main: n=141 spacing p10/p50/p90=1.017/1.046/1.090 duration p50=1.000
fix:  n=141 spacing p10/p50/p90=1.014/1.035/1.066 duration p50=1.035
main  TIMING recv#2150: recv=51.7ms dropped=0 (IQ=39.4ms) handoff_dropped=0/0 ovf=0 lost=0
fix   TIMING recv#2150: recv=51.1ms dropped=0 (IQ=39.4ms) handoff_dropped=0/0 ovf=0 lost=0
```

- On this hardware the per-window work is about 46 ms (main's spacing 1.046 s against
  1.000 s), not 0.85 s. The fix tiles (spacing equals stored duration) and nothing is
  dropped in either build.
- REJECTED (do not retry): "the Jetson spends about 0.85 s per window in the awaited
  work" as an explanation for the screenshot's 1.85 s spacing. It does not hold on
  nano-super with the mock receiver.
- Open: what makes the deployed sensor's windows about 1.85 s apart (a different
  DURATION_SEC, a real USRP at a higher rate, ZMS or NATS publishing, add-on modules,
  or a slow disk). Needs the sensor's `RFOBS_DURATION_SEC` and a few minutes of its
  `TIMING` and `PROC` log lines.

## 12. Per-window DB writes moved off the averaging loop (2026-09-29)

### The question

A reviewer showed by simulation that when the consumer loop's awaited per-window
work takes close to DURATION_SEC (0.85 s at 1.0 s), closing the window before that
work makes windows come more often, each average fewer chunks, and the bounded
result queue (8) drop more results than on main. Task: take the slow work off the
loop's critical path, so the loop only accumulates and closes windows.

### The answer

The only awaited step that can be slow is the DB insert. It was awaited inline
for no stated reason: there is no back-pressure to gain, since the result queue
drops rather than blocks when the loop falls behind, and the comment at
`pipeline/app.py:44` already described the inline await as a hazard. Both
inserts (averaged window, tone check) now go through `_OrderedDbWriter` in
commit 9bda933: one bounded `asyncio.Queue` (256 writes) and one worker task
owned by the processor. With an injected 0.85 s insert on nano-super, main
drops 14% of results and spaces windows 1.90 s apart. The fix drops none and
spaces them 1.045 s apart, the same as with no delay.

### What changed

- `_publish_processed` queues `_persist_avg_window` on the writer and
  returns. `replay_mode` still skips; `skip_psd_blobs` and disk-full
  reporting are unchanged because they stay inside `_persist_avg_window`, now
  evaluated when the write runs. ZMS/NATS stay fire-and-forget tasks.
- `_run_tone_check` still evaluates and logs inline (one pass over the bins,
  1.6 ms p50 on nano-super including the log). Only its insert is queued.
- The writer keeps submission order. A full queue drops the write, counts it
  (`db_writes_dropped` in `/api/health` under `pipeline`), and logs a WARNING
  at most once a minute. Each job logs its own errors; the worker also logs
  anything that escapes, per item.
- `run()` drains the writer for up to 3 s (`_DB_WRITE_DRAIN_SEC`, inside the
  watchdog's 5 s stop timeout) as soon as the consumer loop exits. If the drain
  times out, it logs how many writes were discarded and cancels the worker.
- The broadcast is unchanged: `LiveBroadcast.publish` is a `put_nowait` per
  subscriber queue (drops when a client's queue of 10 is full) and never
  awaits a socket. Each client's `send_loop` runs on its own task.

### Procedure

1. **Per-step cost, main, instrumented.** `timing_wrap.py` (scratch, not
   committed) wraps `_run_tone_check`, `_broadcast_averaged`,
   `_publish_processed`, `_persist_avg_window`,
   `SensorDatabase.insert_avg_window` and `insert_tone_check` with timers,
   then calls `cli.main()`. It isolates each awaited step with no product code
   change. The tone check was enabled (`TONE_CHECK_FREQ_HZ=915.5 MHz`) so its
   insert was exercised.
2. **Result-queue drops.** The same wrapper counts `_put_nowait_drop_full`
   calls on the maxsize-8 queue, split into put and dropped (see trap below).
3. **Injected delay.** `AVGM_INSERT_DELAY=0.85` makes the wrapped
   `insert_avg_window` sleep 0.85 s before inserting. This stands in for a slow
   writer thread on the deployed sensor. Run on both builds.
4. nano-super at 15 W. Mock receiver, `BANDWIDTH=26000000`, 915 MHz,
   `DURATION_SEC=1.0`, 150 s per run. Read with `GET /api/averaged` and skip
   the first 3 windows. Clones of `main` 22bd52f and the fix 9bda933 were
   shipped with a git bundle. The chunk is 1,024,000 samples (39.4 ms), with
   3 PSD workers.

### Evidence

Per-step cost, ms (p50 / p90 / max, about 120 windows):

| Step | workstation, branch HEAD | nano-super, main |
|---|---|---|
| `insert_avg_window` | 12.7 / 23.5 / 26.2 | 1.3 / 2.0 / 9.6 |
| `_run_tone_check` (eval + insert + log) | 0.6 / 4.4 / 7.1 | 2.8 / 3.7 / 7.9 |
| `insert_tone_check` | 0.2 / 4.0 / 6.8 | 1.3 / 1.7 / 3.2 |
| `_broadcast_averaged` | 0.3 / 0.4 / 2.5 | 1.3 / 1.7 / 2.4 |

nano-super, second round, with the queue counters (the first round agreed within
15 ms on every spacing figure):
```
maindelay: n=76  spacing p10/p50/p90=1.871/1.904/1.970 duration p50=1.000 gap max=1002.8ms  resultq put=1381 dropped=232
fixdelay:  n=139 spacing p10/p50/p90=1.013/1.045/1.110 duration p50=1.045 gap max=0.0ms     resultq put=1818 dropped=0
main:      n=139 spacing p10/p50/p90=1.023/1.063/1.117 duration p50=1.000 gap max=148.6ms   resultq put=1791 dropped=0
fix:       n=140 spacing p10/p50/p90=1.014/1.049/1.104 duration p50=1.049 gap max=0.0ms     resultq put=1811 dropped=0
fixdelay   _publish_processed p50=0.0 max=0.1ms, insert_avg_window p50=852.5ms, db_writes_dropped=0
```
The fix's windows last a little over 1.0 s because a window closes on the first
result at or past DURATION_SEC. That adds up to one chunk (39.4 ms) plus arrival
jitter.

Unit test, `test_slow_db_insert_does_not_bend_windows_or_drop_results`
(fake clock, results every 36.6 ms into a capped queue of 8, 60 s,
DURATION_SEC 1.0, 0.85 s inserts). On 9bda933's parent it failed with
`assert 861 == 0` (results dropped against a free insert). Now: 58 windows,
1639 results taken, 0 dropped, 28.3 chunks per window, every window 1.0248 s
long, and writer-queue high-water 0.

### Findings

- On nano-super with the mock receiver, the awaited work is small, about 5.5 ms
  p50 per window in total. The rest of main's 1.06 s spacing is the one-chunk
  close granularity.
- Main with an injected 0.85 s insert reproduces the deployed sensor's
  screenshot (1.85 s spacing, 1.0 s stored duration) almost exactly: 1.904 s.
  This makes a slow insert on the sensor's writer connection the leading
  explanation for the screenshot. It is not proven: the sensor itself was not
  measured.
- On main, that slow insert also lost 14% of results (232 of 1613) at the
  result queue. Section 9's open question, "are results dropped while that
  work runs", is answered yes for main. It is 0 with the fix.

### Measured and REJECTED (do not retry)

- "The tone check or the broadcast is what costs time." They cost 1.3 to
  2.8 ms p50 on nano-super. Only the DB insert can be slow, because it waits
  behind every other statement on the single aiosqlite writer thread.
- "The inline await was deliberate back-pressure." Nothing in the history
  says so (667a657 added it without comment), and back-pressure is not possible
  here: `_put_nowait_drop_full` drops at the result queue instead of blocking.

### Measurement traps hit

- The `TIMING recv#` line's `handoff_dropped=` counts only `_LoopHandoff`'s
  in-flight cap. It does NOT count results dropped by `_put_nowait_drop_full`
  when the result queue itself is full. Main with the injected delay printed
  `handoff_dropped=0/0` while it had dropped 232 results. That counter was
  used in section 11 to say "nothing is dropped". It was right there only
  because the work was small. Count queue-full drops directly.
- `fuser -k` sends SIGKILL, so an `atexit` report never runs. The wrapper
  prints its percentiles every 30 inserts instead.
- The existing fake-clock tests modelled insert cost as `clock.t += cost`
  inside the insert. With a background writer, that moves the loop's clock
  from another task, which is wrong. The new harness advances fake time only
  in the queue's `get()`. It lets an insert on the writer task finish once
  fake time passes its deadline, and advances the clock directly only when the
  insert runs on the consumer task itself (the old inline path, for the RED
  run).

### Open, not yet answered

- The deployed sensor's real insert cost. Needs the sensor's
  `RFOBS_DURATION_SEC`, and some of its `TIMING`/`PROC` lines or a
  `db_writes_dropped` reading once this is deployed. If the writer there
  averages longer than a window per window's writes, the queue grows. At
  DURATION_SEC=1.0 with the tone check on (2 writes a window), a writer that
  stops entirely fills the 256 slots in about 2 minutes. After that, writes are
  dropped and counted rather than stalling the loop.
- `_drain_burst_results` still awaits `insert_detections_batch` inline on the
  consumer loop, on the same writer connection. It runs every iteration, so
  a slow writer can still stall the loop through that path whenever bursts are
  pending. Not moved here, since detection persistence has its own ordering and
  error paths; it is the next candidate.
- `skip_psd_blobs` is now read when the write runs, not when the window
  closed. Under a backlog, a window closed before the governor tripped may be
  stored without its blob. This is the conservative side, and it was not
  tested.
- `handoff_dropped` should probably also count queue-full drops, or the TIMING
  line should print them separately. Not changed here.

### Review follow-up 2 (2026-09-29)

A review of 9bda933 found two defects and three smaller items. They are fixed in one follow-up commit.

1. **CORRECTION: a cancel during the drain skipped the thread shutdown.** 9bda933
   put the writer drain first in `run()`'s `finally`. A supervisor cancel that
   landed while it ran (after the watchdog's 5 s or the supervisor's 15 s)
   aborted the recording stop, `_signal_stop`, the joins, `_stop_isolation`,
   the recctl stop and the final burst drain. The drain now runs last, in an
   inner `finally`, so the thread shutdown always comes first. The drain also
   still runs if that shutdown is cancelled. The blocking joins hold the loop,
   so the writer makes no progress during them. The claim in "What changed"
   that the drain runs "as soon as the consumer loop exits" is withdrawn.
2. **Late rows escaped the minute rollup.** With a backlog, a window can be
   written up to 256 writes late, far past `_rollup_lag`. A minute folded
   before its window landed is never revisited. `_OrderedDbWriter` now tracks
   `oldest_pending_start`: the window start of the oldest unfinished write,
   counting the one in flight. The processor exposes it as
   `oldest_pending_write_start`. `_rollup_loop` reads it through the
   supervisor's processor, and `_rollup_forward` clamps its `closed` bound so
   it never folds that window's minute or later.
3. The queue-size comment was wrong. 256 writes is about 64 s at
   DURATION_SEC=0.5 with the tone check on. It is about 128 s with the tone
   check off (the default), or at 1.0 s with it on. Section 12's "about 2
   minutes" for a stopped writer at 1.0 s with the tone check on stands.
4. New test: an insert slower than a window (2.5 s at 1.0 s, writer queue
   of 8). The loop's windows and result drops are unchanged, and
   `db_writes_dropped` goes nonzero. The written rows are in window order, and
   the closed windows minus the written rows equal the drops.
5. The `/api/health` comment now says `db_writes_dropped` is per processor
   instance. It resets when the supervisor rebuilds the processor.

Tests: `test_cancel_during_the_drain_does_not_skip_thread_shutdown`,
`test_oldest_pending_write_start_tracks_the_backlog`,
`test_forward_pass_holds_back_to_the_oldest_pending_write` and
`test_pending_write_start_reads_the_live_processor` failed on 9bda933.
`test_insert_slower_than_a_window_fills_the_writer_not_the_loop` covers item 4.
It already passed on 9bda933, as expected.

Open: the clamp holds the whole forward pass back to the oldest pending
window's minute. If the writer is stuck, folding stops until it recovers or
the processor stops, and the drain then discards the rest. This is by design:
nothing is folded early. A supervisor rebuild also loses the old processor's
pending set, but the drain has already written or discarded it by then.
