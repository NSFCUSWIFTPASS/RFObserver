# Burst overlay times are on the detector's clock, not the capture clock

## The question

2026-10-23. While adding the burst overlay to Fleet Live: which clock are a
sensor's live burst times (`bursts[].start_time_ms` / `stop_time_ms` in the
High Res `psd` frame) on, and can the fleet place them on its capture-time
timeline (frame `t` = `capture_time_ms`)?

Hardware/build: nano-super (Jetson Orin Nano Super, B200mini, 915 MHz,
56 Msps, CUDA PSD, v0.11.0b0, burst detection and attribution on), and a
local mock sensor (`RFOBS_MOCK_RECEIVER=true`, workstation `orenslab-dell`,
main at 0fd5f92 plus the branch `feat/fleet-live-bursts`).

Symptom numbers: on nano-super, bursts land 300 to 500 ms after the capture
time of the frame that shows their energy (median 414 ms after the frame's
capture time, 318 ms after its `chunk_time_ms`), n = 6 matched bursts.

## The answer

Burst times are `datetime.now()` when the rolling detector evaluates, minus
the number of rows back times the row duration
(`processing/rolling_burst.py`, `_to_fingerprint` and `detect_bursts(capture_time=now)`).
They run late of capture by however far the burst thread lags, and counting
rows back is also wrong whenever a chunk's rows are not back to back in time
(dropped grids, gaps between chunks). Exact capture-clock times come from the
rows' recorded stream samples (`_row_pos`) and each chunk's capture time;
the sensor now sends them as `capture_start_ms` / `capture_stop_ms`.

## Procedure

1. **Direct read of nano-super's `/ws/live` in High Res** (read-only), printing
   per frame `chunk_time_ms - capture_time_ms`, arrival - capture, and the
   newest burst's stop relative to both. Isolates: how late bursts are
   reported. Result: ~0.5 s after capture (the rolling window), which by itself
   cannot tell which clock the times are on.
2. **Match each burst to the frame where its band lights up** (`max_powers` in
   the burst's band vs that bin's median over 25 s), then the offset of the
   burst's mid time from that frame's capture time and chunk time. Isolates:
   the time base, against the spectrum as ground truth.
3. **Read the detector code** for where times come from (step 2 had n = 6).
4. **Mock ground truth.** `MockReceiver.recv_chunk` puts a full-chunk tone in
   every 5th chunk, so each burst should start exactly at a lit frame's capture
   time and end at its end. For each burst: error of start vs the nearest lit
   frame's capture time, and of stop vs that frame's end. Count within 5 ms.
   This is the reusable measurement.
5. **Span check:** `capture_stop - capture_start` vs `duration_ms`.

## Evidence

Step 1 (nano-super, 20 s):

```
chunk-cap=  131.1  arrive-cap=  155.9  dur=36.6  burst.stop-cap=  -465.7  burst.stop-chunk=  -596.8
chunk-cap=  143.7  arrive-cap=  152.2  dur=36.6  burst.stop-cap=  -502.3  burst.stop-chunk=  -646.0
chunk-cap=  122.4  arrive-cap=  133.8  dur=36.6  burst.stop-cap=  -538.9  burst.stop-chunk=  -661.3
```

Step 2 (nano-super, 25 s, n = 6):

```
mid - capture_time: median   413.8  p10   334.9  p90   519.5
mid - chunk_time:   median   318.4  p10   239.6  p90   399.4
```

Step 4, mock, final code (n = 96):

```
old     within 5 ms:  1 of 96   start err median  -5.5 ms (p10 -239.7, p90 +195.8)
capture within 5 ms: 84 of 96   start err median  +0.0 ms (p10  +0.0, p90  +0.0); stop err median +0.0 ms
```

The capture-time outliers are bursts from before the 25 s capture began
(errors of -533, -1079, ... ms, in pairs) and four +19 ms bursts that are
half a chunk long and start mid-frame.

Step 5, final code: `(capture span, old span, duration_ms)`:
`(36.6, 36.6, 36.57) x88, (17.6, 17.6, 17.55) x4, (36.4, 36.4, 36.39) x4`.

## Measured and REJECTED (do not retry)

- **Shift by the evaluated window's end.** Capture time of the window's last
  row (`capture_time + rows * tres`) minus the detection's `now`, added to the
  detector times. Mock: 7 of 101 within 5 ms (p10 -241, p90 +166 ms). Counting
  rows back assumes rows are contiguous in time, which dropped grids and gaps
  between chunks break.
- **Fleet-side lag estimate** (subtract the frame's
  `chunk_time_ms - capture_time_ms`): about 100 to 140 ms on nano-super against
  a measured ~300 to 500 ms lag. Kept only as the fallback for sensors that do
  not send capture times; it is not accurate.
- **Peak of the band-energy profile vs frame offset on the mock.** The mock's
  tone repeats every 5 frames (183 ms), so any lag that is a multiple of 183 ms
  aliases to zero; every timebase showed the same phase.

## Measurement traps hit

- Matching a burst to the brightest frame within ±2 s on the mock is
  ambiguous for the same reason (periodic tone); use the nearest lit frame and
  report within-5 ms counts, not medians alone. A first median of 0.0 ms for
  the shift method looked exact while only 7 of 101 were.
- An exclusive stop sample that falls on a chunk boundary timed from the next
  chunk's capture time made spans 50 to 59 ms instead of 36.6 (the mock's
  chunks are up to ~20 ms apart). It is timed from the chunk it ends.
- The workstation's burst thread barely lags, so old times look nearly right
  here (+3 to +36 ms medians) while the Jetson lags 300 to 500 ms; measure on
  the Jetson for lag, on the mock for exactness.

## Open, not yet answered

- **The sensor's own Live overlay likely has the same misplacement.** It places
  detector-clock burst times against rows stamped with `chunk_time_ms`; on the
  mock those match within 5 ms for 1 of 96 bursts. Not fixed here; the page
  could place rows by `capture_time_ms` and bursts by `capture_start_ms`.
- Stored detection times (`detections.start_time`) are the same detector
  clock; not changed.
- Step 2 on nano-super had only 6 matched bursts; the capture-time path has
  not been measured on real hardware yet (needs a sensor running this branch).
