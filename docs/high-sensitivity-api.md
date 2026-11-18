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
- `id`: optional, 1 to 64 characters; generated if left out. Sending the same
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
