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
