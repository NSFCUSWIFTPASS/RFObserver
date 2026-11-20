# High-sensitivity API

High-sensitivity mode is for the daily status and RF-chain linearity check: for
a few minutes a sensor lowers its burst threshold, runs the tone detector and
raises its isolation budget, so weak transmissions (test tones, SSM hops at low
power) are detected and attributed on every sensor that hears them. Routine
sensitivity is too costly to lower all day (NbE540: 17 detections/s at 30 dB,
601/s at 15 dB).

A run lasts at most 300 s and ends by itself. It is never stored in the
settings: a restart, a retune or a reconfigure ends it, and the sensor always
comes back in routine mode.

While a run is on:

- the burst threshold is `HS_BURST_THRESHOLD_DB` (15 dB) or the run's own
  `threshold_db` (10 to 30);
- the tone detector runs (if the routine one is off, the run starts one and
  stops it at the end);
- the isolation budget is `HS_ISOLATION_MAX_PER_SEC` (60 a second);
- every detection it makes carries the run id (`hs_run`). Routine hourly
  counts leave these rows out; `/api/detections.json?hs_run=<id>` lists them.

## Start a run

```bash
curl -s -X POST "$SENSOR/api/high-sensitivity" -H "Content-Type: application/json" \
  -d '{"seconds": 120, "id": "daily-2026-11-19", "threshold_db": 15}'
```

`200`:

```json
{"run": {"id": "daily-2026-11-19", "source": "api",
         "start": "2026-11-19T16:00:00.000Z", "end": "2026-11-19T16:02:00.000Z",
         "threshold_db": 15.0, "seconds_left": 120, "ended": null, "end_reason": null}}
```

- `seconds`: required, 1 to 300.
- `id`: optional, 1 to 64 letters, digits, `.`, `-` or `_`; generated if left out; never reused (see Run ids). Sending the same
  `id` again while it runs extends it, never past its start + 300 s.
- `threshold_db`: optional, 10 to 30.

`400` for a bad body, `409` while another run is on, `503` when the pipeline is
not running.

## End, read

- `DELETE /api/high-sensitivity/{id}`: ends it now (`404` if it is not on).
- `GET /api/high-sensitivity`: `{"run": the current run or null, "runs": [the last 50 runs]}`.
- `GET /api/high-sensitivity/{id}`: one run with its summary (`404` if unknown).
- `GET /api/health`: `high_sensitivity: {"active", "run"}`.

A run's summary is written about 3 s after it ends:

```json
{"id": "daily-2026-11-19", "source": "api", "start": 1795104000.0, "end": 1795104120.0,
 "ended": 1795104120.0, "end_reason": "expired", "threshold_db": 15.0,
 "summary": {"detections": 812, "ssm": {"0200574952470001": 17},
             "tones": [{"freq_hz": 914850000.0, "level_db": -79.3, "snr_db": 33.6, "duration_ms": 2999.1}],
             "overflow_events": 0, "overflow_lost_samples": 0, "recv_stalls": 0,
             "isolation": {"received": 900, "picked": 610, "gated_out": 290, "copies": 41}}}
```

`end_reason` is `expired`, `deleted`, `retune`, `stopped` or `restart` (a run
the sensor found still open when it started). Check `overflow_events` before
trusting a run's levels: samples lost to receiver overflow during the check
compromise them.

## With a schedule

Add `"high_sensitivity": true` (and optionally `"hs_threshold_db"`) to a
schedule (docs/schedules-api.md). Each run turns the mode on `HS_LEAD_SEC`
(2 s, so the tone detector's floor settles) before the capture and off when it
ends, plus the same lead after it (the pipeline processes the capture a
little behind the radio); lead, duration and tail may be at most 300 s, so
`duration_ms` at most 296000. The run id is `<schedule id>-<run>-<start ms>`,
and every scheduled capture's `.json` records the run in
force as it started (`hs_run`). If another run is already on, the schedule's
run is skipped (logged) and the capture records the other run's id.

```bash
curl -s -X POST "$SENSOR/api/schedules" -H "Content-Type: application/json" \
  -d '{"id": "daily", "start": "2026-11-19T16:00:00Z", "duration_ms": 120000,
       "every_ms": 86400000, "count": 30, "high_sensitivity": true}'
```

Through the fleet (`POST /fleet/api/schedules`) the flag reaches each sensor
unchanged; each sensor's result is reported on its own, as for any schedule.

## High sensitivity only, on a schedule

Add `"capture": false` too and the schedule records no IQ: each run turns the
mode on at its start and off at start + `duration_ms`, with no lead or tail,
so `duration_ms` may be up to 300000. It repeats like any schedule
(`every_ms` with `count` or `until`), and takes `hs_threshold_db`.

```bash
curl -s -X POST "$SENSOR/api/schedules" -H "Content-Type: application/json" \
  -d '{"id": "daily-hs", "start": "2026-11-20T16:00:00Z", "duration_ms": 300000,
       "every_ms": 86400000, "count": 30, "high_sensitivity": true, "capture": false}'
```

The same body with `"sensors": [...]` goes to `POST /fleet/api/schedules` to put
it on several sensors; each starts its runs from its own clock. Each run's
outcome is in the schedule's runs (`GET /api/schedules/{id}`), with
`info.hs_run` naming the run (its summary is at
`GET /api/high-sensitivity/{hs_run}`):

- `running`: on now.
- `done`: it ran its whole window.
- `partial`: it ended early; `reason` is `retune`, `stopped` or `deleted`
  (`stopped` also after a restart cut it off).
- `missed`: `reason` `hs_busy` when another run was already on (`info.detail`),
  or `not_running` when the sensor was off or not processing at the time.

A sensor on 0.16.0b0 or older ignores `capture` and records the window as a
capture instead (up to 300 s of IQ); check the sensors' versions before
sending one through the fleet.

## Through the fleet

`POST /fleet/api/high-sensitivity` starts one run (one `id`) on every sensor
that is up, or leaves none in it:

```bash
curl -s -X POST "$FLEET/fleet/api/high-sensitivity" -H "Content-Type: application/json" \
  -d '{"seconds": 120, "id": "daily-2026-11-19", "min_sensors": 4}'
```

1. The targets (`sensors`, or every enabled sensor) are probed with
   `GET /api/health` (3 s). Those that do not answer are `offline` and skipped.
2. The run starts on the rest in parallel (5 s each).
3. If every reachable sensor entered and at least `min_sensors` did (default:
   all reachable), the answer is `200`. Otherwise the run is ended again on
   every sensor that entered, or may have (a start that timed out), and the
   answer is `409`. `502` when no target answered the probe.

```json
{"id": "daily-2026-11-19", "ok": true,
 "results": {"hcro-jet-001": {"state": "entered", "detail": null, "run": {...}},
             "hcro-jet-002": {"state": "offline", "detail": "ConnectError: ...", "run": null}}}
```

States: `entered`, `offline`, `failed` (with `detail`), `rolled_back`, and
`rollback_failed` (that sensor still ends the run by itself at its end time).

- `DELETE /fleet/api/high-sensitivity/{id}`: ends it on every enabled sensor
  (`200` if any had it).
- `GET /fleet/api/high-sensitivity/{id}`: each sensor's summary, or its error.

## Run ids

An `id` is 1 to 64 letters, digits, `.`, `-` or `_` (it goes in URLs). An id
already used by a stored run is refused (`409`), except to extend the run that
is on, so each run keeps its own summary.
