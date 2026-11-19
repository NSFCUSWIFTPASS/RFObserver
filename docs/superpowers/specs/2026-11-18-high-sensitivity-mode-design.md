# High-sensitivity mode

2026-11-18. Branch `feat/high-sensitivity`.

## Intent

Once a day each sensor runs a short status and RF-chain linearity check: test tones (914.85 and
915.10 MHz) and SSM hops at 0 dB and +12 dB transmit gain, recorded by a scheduled capture across the
fleet. The owner wants SSM decodes on every sensor that can hear the hops, and the tone levels, out of
that check. At the routine 30 dB burst threshold only the sensor next to the transmitter decodes them.
Lower thresholds find them, but at a cost no sensor should pay all day (NbE540: 17 detections/s at
30 dB, 1161/s at 12 dB). So the sensitivity is raised only for the few minutes of the check.

Measured on the 2026-11-06 SSM1 captures, replayed with main 4f143f4 (hops labelled SSM of the 390
hop-sensor pairs; docs/debugging/2026-11-18_ssm-coverage-hcro.md):

```
threshold  labelled  N830 SSM1_0  NbE540 SSM1_0  N830/NbE540 SSM1_12  NbE540 load
30 dB       59        0/20         0/20            0/19  /  0/19          17/s
15 dB      104        5/20         6/20           17/19  / 17/19         601/s
12 dB      110        9/20         8/20           17/19  / 17/19        1161/s
10 dB      115       11/20         9/20           17/19  / 16/19        1678/s
```

NNE60, E450 and W230 label at most one SSM1_0 hop even at 10 dB. The owner chose 15 dB as the
default (it decodes hops on N830 and NbE540 at 0 dB transmit gain and almost all at +12), with the
threshold configurable per call and per schedule.

## Goals

- A sensor enters high-sensitivity (HS) mode for at most 5 minutes and leaves it by itself.
- While on: burst threshold lowered (default 15 dB, configurable 10 to 30), the tone detector
  running, the isolation budget raised.
- Started three ways: a sensor API call, a flag on a schedule (so the fleet switches at the
  capture's instant), and a fleet API call that is all-or-nothing over the sensors that are up.
- Detections made in HS mode are tagged, and each HS run has a summary.

## Non-goals and anti-goals

- No persistence. A restart mid-run ends it; the sensor never comes back up in HS mode.
- No new UI beyond a status line on the Config page.
- It must not lower the routine sensitivity or change the stored settings (`.env`, Config page).
- A failed fleet call must not leave some sensors in HS mode.

## Sensor behaviour

**Overlay, not settings.** HS mode is an in-memory overlay read where the values are used:

```
BURST_THRESHOLD_HIGH_DB   -> HS_BURST_THRESHOLD_DB      (new setting, default 15, allowed 10..30)
TONE_DETECTOR_ENABLED     -> true                       (frequencies, threshold unchanged)
ISOLATION_MAX_PER_SEC     -> HS_ISOLATION_MAX_PER_SEC   (new setting, default 60)
```

`ISOLATION_LONG_MAX_PER_SEC` (10) and the other isolation settings stay. The burst threshold is read
per grid, so it applies at once. The tone detector needs a live start and stop (today it starts with
the pipeline); its floor settles in 1 s. When the routine setting already has the tone detector on,
HS mode leaves it running and does not stop it at the end.

**Lifetime.** Each run has an `id` (given or generated), a start, and an end at most 300 s later. A
run ends at its end time, on `DELETE`, on a pipeline stop or restart, or on a retune (the threshold
and tone floors no longer apply). A second start while one is running is refused (`409`) unless it
carries the same `id`, which extends the end to at most 300 s from the run's start.

**Tagging.** A nullable `hs_run` column on `detections` holds the run id for rows whose burst started
inside the run (tone rows included). Routine statistics (hourly counts, collection stats) exclude
tagged rows unless asked; the Detections page can filter on it.

**Summary.** Kept per run (last 50 runs, in the database so a restart keeps them): id, source (api,
schedule, fleet), start, end, end reason, threshold used, detections, SSM labels by `src_id`, tones
(frequency, level, SNR, duration), receiver overflows and lost samples during the run, isolation
counters (received, picked, gated, copies), and whether the burst thread stalled.

## API

Sensor:

- `POST /api/high-sensitivity` `{"seconds": 1..300, "id"?: str, "threshold_db"?: 10..30}`: `200`
  with the run (`id`, `start`, `end`); `409` when another run is on; `503` when the pipeline is not
  running.
- `DELETE /api/high-sensitivity/{id}`: ends it now; `404` if not running.
- `GET /api/high-sensitivity`: the current run or none, and the last runs' summaries.
- `GET /api/high-sensitivity/{id}`: one run's summary.

Schedule flag: `"high_sensitivity": true` (and optional `"hs_threshold_db"`) on `POST
/api/schedules`. Each run of the schedule starts HS mode `HS_LEAD_SEC` (2 s, for the tone floor)
before the capture and ends it when the capture ends. Refused at creation when lead plus duration
exceeds 300 s. The run's capture `.json` records the HS run id.

Fleet (`rfobserver fleet`):

- `POST /fleet/api/high-sensitivity` `{"seconds", "sensors"?: [...], "min_sensors"?: int,
  "threshold_db"?: ...}`. Steps:
  1. Probe the target sensors (default: all enabled) with `GET /api/health`, in parallel, short
     timeout. Those that do not answer are `offline` and skipped.
  2. Start one run, with one `id`, on every reachable sensor in parallel.
  3. If any reachable sensor refused or timed out, or fewer than `min_sensors` (default: all
     reachable) succeeded, `DELETE` the run on every sensor that started it, and answer `409` with
     each sensor's result. Otherwise `200`.
  The answer always lists every target: `entered`, `offline`, `failed` (with the reason) or
  `rolled_back`.
- `DELETE /fleet/api/high-sensitivity/{id}`: ends it on every sensor.
- `GET /fleet/api/high-sensitivity/{id}`: each sensor's summary for that run.
- `POST /fleet/api/schedules` passes `high_sensitivity` through (each sensor applies it; the
  existing per-sensor results apply, no rollback, as for schedules today).

A sensor whose `DELETE` fails during rollback is still bounded by its own end time; the answer marks
it `rollback_failed`.

## Risks

- **Overflow on busy sensors.** NbE540's load at 15 dB is about 35x routine (70x at 12 dB). If the receiver
  overflows during the check, the linearity readings are compromised. The summary reports overflows
  for the run; before relying on the mode at 12 or 10 dB, run NbE540 at that threshold for 2 minutes live.
  Live trial so far: N830 at 15 dB (0.15.0b0) for 35 minutes, 31 detections/s, 3-5 overflows per 5
  minutes (0.2-0.3 s lost), no isolation backlog.
- **Weak hopper.** A frequency hopper near N830 (82 channels, 310 kHz grid, 2-FSK, a packet every
  33.4 ms, about -110 dB) is most of N830's extra load at low thresholds. It is real; it gets tagged
  like everything else in the run.

## Left to the builder

- The schema change mechanics for `hs_run` and the summary table (follow the existing migrations).
- How the overlay is threaded to the burst config, isolation gate and tone detector.
- Timeouts for the fleet probe and start.
