# Scheduled runs that recorded show "missed, not_running" in the fleet

## The question

2026-11-06, about 22:55 UTC. "On the fleet I noticed a lot of missed and partial, check." HCRO
fleet on 10.1.42.31:8888, six sensors (.31-.36) on 0.14.0b0. Fleet `/fleet/api/schedules` "recent"
(the 20 newest outcomes):

```
21:48 SSM1_neg12  .35 missed not_running   .36 recorded
21:49 SSM1_0      .31 .32 .33 .35 missed not_running   .34 partial (lost 1,992,823)   .36 partial (lost 9,450,490)
21:55 SSM1_12     .31 .32 .33 .35 missed not_running   .34 recorded                   .36 partial (lost 13,048,012)
22:53 (no label)  all six recorded, lost 0
```

The same symptom on E450 (.32) on 2026-11-05: Test1 (20:00) and a 21:04 run "missed,
not_running" although the log said "recorded" and both files were on disk.

## The answer

Two separate things.

1. **"missed, not_running" on runs that recorded is a bug.** When a pipeline starts (process start,
   or a new processor in the same process), `ScheduleEngine.mark_missed` looks for runs that came due
   while nothing was recording, starting from `run_start_ms(s, last + 1)`. For a one-shot schedule
   (`every_ms` None) that is run 0's start again, so run 0 was reported once more as
   missed/not_running, overwriting its "recorded" outcome in the book and the database. The capture
   files and their `.json` (which carry the real outcome) were never touched. `_refresh` had the same
   arithmetic. Fixed: a schedule whose runs all have an outcome is skipped; the refresh steps to
   `last + 1` only while it is a real run.
2. **"partial" is real sample loss** on .34 and .36: receiver overflows during the run (7-10% of
   the 130M samples on .36). .34 had 360 overflow events (575M samples) and .36 106 (206M) since
   21:08 UTC; .32 had 1. No overflows in a 60 s check at about 23:00; the losses come in bursts. Not
   explained here (see Open).

## The procedure

1. **Fleet `/fleet/api/schedules`**, grouped by sensor and status: separates the two symptoms.
2. **Each sensor's start time** (`/api/health` uptime): .32, .33, .35 started at 21:08 UTC, before
   the runs, and did not restart; .31 restarted at 22:48 (CUDA change). Controls for "the sensor was
   down at run time".
3. **The capture files**: `/captures/list?origin=scheduled` on .31, .32, .33, .35 lists a `.sc16` for
   every SSM1 run. `/captures/detail` of .32's 21:49 run: 520,000,000 bytes, `schedule.status:
   recorded`, `start_error_ns: 1`, `lost_samples: 0`. So the run recorded; only the run record says
   missed.
4. **.32's `/api/schedules`**: every older one-shot (back to Test1 on 11-05) is also "missed,
   not_running"; the newest (22:53) is "recorded". Points at something that rewrites old outcomes.
5. **Read `schedules/engine.py` and `schedules/model.py`**: the only writers of reason
   "not_running" are `_start_run` (window before the ring) and `mark_missed`;
   `run_start_ms(s, k) = start_ms + k * (every_ms or 0)` makes run 1 of a one-shot equal run 0.
6. **Unit tests reproducing it** (`tests/unit/test_schedule_engine.py`): a recorded one-shot, then a
   second engine's `mark_missed` (same process), and a reloaded book's (process restart). Both
   failed with status "missed" before the fix.

## Evidence

```
.32 /api/schedules (23:00 UTC)
  466d68eb330d 21:49 last: missed not_running filename None
  3c61e8889c4e 22:53 last: recorded 323E282-hcro-jet-002-20261106T225300000-sched-3c61e8889c4e-0.sc16
  recent: 3c61e8889c4e recorded; a86b65342cae, 466d68eb330d, 8e5ae451bd56, 0b20b6709433,
          1d8f83609956, aa71cfd0faf3, 8a6d821a1b7d all missed not_running

.32 capture 323E282-hcro-jet-002-20261106T214900000-sched-466d68eb330d-0.sc16
  size 520000000  duration_sec 5.0  total_samples 130000000  lost_samples 0  stopped_reason complete
  schedule: status recorded, start_error_ns 1, clock ntp time1.hcro.org

Sensor starts (UTC): .31 22:48:21  .32 21:08:32  .33 21:08:33  .34 21:08:27  .35 21:08:38  .36 21:08:27
```

## Measured and REJECTED (do not retry)

- **"The sensors were down at 21:48-21:55."** .32, .33, .35 ran continuously from 21:08 and wrote a
  complete capture for each run.
- **"The run records were lost in E450's power cut (2026-11-05)."** Same mechanism: the restart's
  `mark_missed` overwrote them. The database survived the power cut (detections up to 21:16:25 UTC
  were there).

## Measurement traps hit

- I could not tell from the API what started a new pipeline on .32, .33 and .35 between 21:55 and
  22:55 without a process restart (a reconfigure, Standby/Active, or a supervisor restart all build
  a new engine). It does not matter for the fix: any pipeline start triggers it.
- The fleet's "recent" list shows only the newest 20 outcomes; older overwritten runs are visible
  per sensor in `/api/schedules`.

## Open, not yet answered

- Why .34 and .36 overflow in bursts (360 and 106 events since 21:08 UTC) when .32, .33, .35 have
  1-5.
- Runs already overwritten stay "missed" in each sensor's database; their capture `.json` has the
  real outcome. No repair was written.
