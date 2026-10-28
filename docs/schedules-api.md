# Scheduled captures API

Record raw IQ at a set instant, to the millisecond: once, or repeating. Each
run is cut from the live stream at the sample nearest the requested start on
the sensor's clock and holds exactly `duration x sample rate` samples. Give
several sensors the same schedule and their files cover the same instants, as
closely as their clocks agree (see [Timing](#timing)).

- Times are UTC: ISO 8601 with a zone (`2026-10-28T18:30:00.250Z`) or epoch
  milliseconds (`start_ms`). Responses also give each time in the sensor's
  local time zone (`*_local`, the zone at its location, as in the
  [Collection statistics API](collection-api.md#time-zone)) for convenience.
- A scheduled capture does not change the radio: it records whatever the
  sensor is tuned to at that moment.
- It records even while another capture (manual, armed trigger, another
  schedule) is running: it is its own file, written in parallel. At most two
  scheduled runs record at once; a third is reported missed (`busy`).
- Files are saved with the manual captures (never deleted by the storage cap)
  and are shown on the Captures page as "Scheduled". The Captures page also
  has a form for all of this.
- JSON in and out, no authentication, no CORS: call it from a server or a
  script. On a sensor started with `rfobserver web` only (no capture
  pipeline) every endpoint answers 503.

Every example below runs in the test suite (`tests/unit/test_schedules_api_doc.py`),
in order, against a sensor holding the example schedule, and must return what
is shown. The endpoints were also exercised against a live mock sensor.

## The example sensor

A sensor at Hat Creek (`LATITUDE=40.8165`, `LONGITUDE=-121.4639`, so
`America/Los_Angeles`, UTC-7 in October) recording 26 MS/s, with 900 GB free.
It is now 18:29:00 UTC (11:29 PDT) on 2026-10-28. It already has one schedule,
`hourly`: 2 s every hour from 17:00 UTC, 24 times. The 17:00 run recorded; the
18:00 run was cut short when the radio retuned.

```bash
SENSOR=http://sensor:8888
```

## Schedule one window

```bash
curl -s -X POST "$SENSOR/api/schedules" -H "Content-Type: application/json" \
  -d '{"id": "pass-1", "start": "2026-10-28T18:30:00.250Z", "duration_ms": 2000, "label": "Pass 1"}'
```

`201 Created`:

```json
{
  "schedule": {
    "id": "pass-1",
    "start_ms": 1793212200250,
    "duration_ms": 2000,
    "every_ms": null,
    "count": null,
    "until_ms": null,
    "label": "Pass 1",
    "source": "api",
    "created_ms": 1793212140000,
    "start": "2026-10-28T18:30:00.250Z",
    "until": null,
    "runs": 1
  },
  "next_runs": [
    {"run": 0, "start": "2026-10-28T18:30:00.250Z", "start_local": "11:30:00.250 PDT"}
  ],
  "warnings": []
}
```

Body fields:

- `start` (ISO 8601 with a zone) or `start_ms` (epoch ms): required. May be up
  to `TRIGGER_PRE_SEC` (1 s by default) in the past: the sensor still holds
  that much of the stream, which covers the time a request takes to arrive.
- `duration_ms`: required, 1 to 600000.
- `id`: optional, 1 to 64 letters, digits, `-` or `_`; generated if left out.
  Sending the same `id` and body again is a no-op (`200` with the stored
  schedule), so a script can retry safely. Use the same `id` on every sensor
  for one shared window; the files then carry the same `schedule.id` and
  `schedule.run`.
- `label`: optional text, shown on the Captures page and saved in the file.
- `every_ms` with `count` or `until`: repeat (below).

## Schedule a repeating window

Run `k` starts at `start + k x every_ms`. Counting from the schedule's start
keeps the instants the same on every sensor, whenever each received it.
`every_ms` must be longer than `duration_ms`. Give `count` (number of runs) or
`until` (UTC; the last run starts before it), not both.

```bash
curl -s -X POST "$SENSOR/api/schedules" -H "Content-Type: application/json" \
  -d '{"id": "every-min", "start": "2026-10-28T18:31:00.000Z", "duration_ms": 500, "every_ms": 60000, "count": 5}'
```

```json
{
  "schedule": {"id": "every-min", "every_ms": 60000, "count": 5, "runs": 5},
  "next_runs": [
    {"run": 0, "start": "2026-10-28T18:31:00.000Z", "start_local": "11:31:00.000 PDT"},
    {"run": 1, "start": "2026-10-28T18:32:00.000Z", "start_local": "11:32:00.000 PDT"},
    {"run": 2, "start": "2026-10-28T18:33:00.000Z", "start_local": "11:33:00.000 PDT"}
  ],
  "warnings": []
}
```

The same request again changes nothing (`200`):

```bash
curl -s -X POST "$SENSOR/api/schedules" -H "Content-Type: application/json" \
  -d '{"id": "every-min", "start": "2026-10-28T18:31:00.000Z", "duration_ms": 500, "every_ms": 60000, "count": 5}'
```

```json
{
  "schedule": {"id": "every-min", "created_ms": 1793212140000}
}
```

A sensor that sweeps frequencies (`FREQUENCY_STEP` above 0) accepts schedules
but warns in `warnings` that a window longer than the dwell is cut at the
retune.

## List schedules and recent runs

```bash
curl -s "$SENSOR/api/schedules"
```

```json
{
  "now": "2026-10-28T18:29:00.000Z",
  "now_ms": 1793212140000,
  "now_local": "11:29:00.000 PDT",
  "timezone": "America/Los_Angeles",
  "warnings": [],
  "schedules": [
    {
      "schedule": {"id": "hourly", "label": "Hourly sample", "runs": 24},
      "next_run": {"run": 2, "start": "2026-10-28T19:00:00.000Z", "start_local": "12:00:00.000 PDT"},
      "last_run": {"run": 1, "status": "partial", "reason": "retuned"}
    },
    {
      "schedule": {"id": "pass-1"},
      "next_run": {"run": 0, "start": "2026-10-28T18:30:00.250Z", "start_local": "11:30:00.250 PDT"},
      "last_run": null
    },
    {
      "schedule": {"id": "every-min"},
      "next_run": {"run": 0, "start": "2026-10-28T18:31:00.000Z", "start_local": "11:31:00.000 PDT"},
      "last_run": null
    }
  ],
  "recent": [
    {"id": "hourly", "label": "Hourly sample", "run": 1, "status": "partial", "reason": "retuned",
     "start": "2026-10-28T18:00:00.000Z", "start_local": "11:00:00.000 PDT"},
    {"id": "hourly", "label": "Hourly sample", "run": 0, "status": "recorded", "reason": null,
     "filename": "323E282-hcro-jet-001-20261028T170000000-sched-hourly-0.sc16"}
  ]
}
```

`schedules` are ordered by start; a schedule whose runs are all done stays
listed (with `next_run: null`) until it is deleted. `recent` holds the newest
20 runs of all schedules.

## One schedule and its runs

```bash
curl -s "$SENSOR/api/schedules/hourly"
```

```json
{
  "schedule": {"id": "hourly", "start": "2026-10-28T17:00:00.000Z", "every_ms": 3600000},
  "next_runs": [
    {"run": 2, "start": "2026-10-28T19:00:00.000Z", "start_local": "12:00:00.000 PDT"},
    {"run": 3, "start": "2026-10-28T20:00:00.000Z", "start_local": "13:00:00.000 PDT"}
  ],
  "runs": [
    {
      "run": 1,
      "status": "partial",
      "reason": "retuned",
      "start_ms": 1793210400000,
      "start": "2026-10-28T18:00:00.000Z",
      "start_local": "11:00:00.000 PDT",
      "filename": "323E282-hcro-jet-001-20261028T180000000-sched-hourly-1.sc16",
      "info": {"samples": 31200000, "start_error_ns": 12}
    },
    {
      "run": 0,
      "status": "recorded",
      "reason": null,
      "start_ms": 1793206800000,
      "start": "2026-10-28T17:00:00.000Z",
      "start_local": "10:00:00.000 PDT",
      "filename": "323E282-hcro-jet-001-20261028T170000000-sched-hourly-0.sc16",
      "info": {
        "first_sample_time": "2026-10-28T17:00:00.000000031Z",
        "start_error_ns": 31,
        "late_start_samples": 0,
        "lost_samples": 0,
        "writer_refused_samples": 0,
        "clock_source": "ntp",
        "clock_server": "0.fr.pool.ntp.org",
        "samples": 52000000
      }
    }
  ]
}
```

Run `status`:

- `recording`: in progress.
- `recorded`: every sample of the window, nothing filled.
- `partial`: a file exists but is not the whole clean window. `reason` says
  why: `retuned` (the radio retuned or was reconfigured; the file ends there),
  `stopped` (the pipeline stopped), or no reason when samples were lost and
  zero-filled (see `info`).
- `missed`: nothing recorded. `reason`: `not_running` (the sensor was off or
  not recording when the window came), `busy` (two other scheduled runs were
  recording), `storage` (storage refused recording; `info.detail` says why).
- `failed`: the file could not be written (`reason` holds the error).

## Delete a schedule

Cancels its future runs. A run already recording finishes, and recorded files
are kept.

```bash
curl -s -X DELETE "$SENSOR/api/schedules/every-min"
```

```json
{"deleted": "every-min"}
```

## Errors

`400` for a request that cannot be a schedule, `409` when it conflicts with
the sensor's state, `404` for an unknown id, each with a `detail`:

```bash
curl -s -X POST "$SENSOR/api/schedules" -H "Content-Type: application/json" \
  -d '{"start": "2026-10-28T18:00:00Z", "duration_ms": 1000}'
```

```json
{"detail": "start is 1740.000 s in the past; at most 1.000 s is allowed"}
```

```bash
curl -s -X POST "$SENSOR/api/schedules" -H "Content-Type: application/json" \
  -d '{"id": "pass-1", "start": "2026-10-28T18:30:00.250Z", "duration_ms": 3000}'
```

```json
{"detail": "id pass-1 is already used by another schedule"}
```

A schedule is refused when all its runs would not fit in the free space above
the storage floor (here 24 runs of 600 s at 26 MS/s):

```bash
curl -s -X POST "$SENSOR/api/schedules" -H "Content-Type: application/json" \
  -d '{"start": "2026-10-28T18:35:00Z", "duration_ms": 600000, "every_ms": 3600000, "count": 24}'
```

```json
{"detail": "The schedule needs 1497.6 GB but only 850.0 GB is free above the storage floor"}
```

```bash
curl -s "$SENSOR/api/schedules/nope"
```

```json
{"detail": "No schedule nope"}
```

## The capture files

A run's file is a normal capture in `manual/`
(`<serial>-<host>-<UTC start with ms>-sched-<id>-<run>.sc16`) with the same
companions as a manual capture (`.json`, the PSD grid for the waterfall, the
detections sidecar) and the same downloads, including SigMF. Its `.json` has
`"origin": "scheduled"` and a `schedule` block:

```
"schedule": {
  "id": "hourly", "run": 0, "label": "Hourly sample", "status": "recorded", "reason": null,
  "requested_start": "2026-10-28T17:00:00.000Z",
  "first_sample_time": "2026-10-28T17:00:00.000000031Z",
  "start_error_ns": 31,
  "late_start_samples": 0,
  "requested_samples": 52000000,
  "zero_filled": [], "zero_filled_samples": 0, "grid_rows_dropped": 0,
  "clock_source": "ntp", "clock_server": "0.fr.pool.ntp.org",
  "clock_offset_ms": null, "clock_synced": true
}
```

Samples lost on the way (USB overflow, or the disk falling behind) are written
as zeros at their place, so sample `i` is always `i / sample_rate` seconds
after `first_sample_time`, and files from different sensors line up by index.
The filled stretches are `zero_filled` (`[index, count]` pairs); the SigMF
export marks each as an annotation. The top-level `gaps` stays empty because
the file has no missing time.

## Timing

The start is placed on the sensor's host clock (the same clock its burst times
and live timestamps use), to the nearest sample: `start_error_ns` is how far
the first sample is from the requested instant on that clock, within half a
sample. How well two sensors agree is a matter of their clocks: with NTP
(systemd-timesyncd against a public pool) expect a few milliseconds.
`clock_source`, `clock_server` and, with chrony, `clock_offset_ms` record what
the clock was disciplined by. See
`docs/debugging/2026-10-28_hcro-time-sync-ptp.md` for the HCRO measurements and
the options for tighter time (a local NTP server, PTP from White Rabbit, or
PPS into the radios).

## Field reference

| Field | Meaning |
|---|---|
| `schedule` | The schedule (fields below) |
| `id` | The schedule's id |
| `start_ms` | First run's start, epoch ms (UTC); in a run, that run's start |
| `start` | The same instant, ISO 8601 UTC |
| `start_local` | The same instant in the sensor's time zone |
| `duration_ms` | Length of each run |
| `every_ms` | Interval between run starts, or null for one window |
| `count` | Number of runs, or null |
| `until_ms` | Runs start before this instant (epoch ms), or null |
| `until` | The same, ISO 8601 UTC |
| `runs` | Total runs of the schedule |
| `label` | Free text |
| `source` | Who created it: `api`, `ui` (Captures page) or `fleet` |
| `created_ms` | When it was created, epoch ms |
| `next_runs` | The next five runs not yet started |
| `next_run` | The next run not yet started, or null |
| `run` | A run's number, from 0 |
| `last_run` | The newest run with an outcome, or null |
| `recent` | The newest 20 runs of all schedules |
| `runs` | In one schedule's response: its runs, newest first (the newest 500 are kept) |
| `status` | A run's status (see above) |
| `reason` | Why a run is partial, missed or failed |
| `filename` | The run's capture file |
| `info` | Details of a run (fields below) |
| `first_sample_time` | The first sample's time on the sensor's clock (ns) |
| `start_error_ns` | First sample minus requested start |
| `late_start_samples` | Zero-filled samples at the start (the start was older than the stream the sensor held) |
| `lost_samples` | Samples lost in reception, zero-filled |
| `writer_refused_samples` | Samples the disk could not take in time, zero-filled |
| `clock_source` | `ntp`, or `unknown` |
| `clock_server` | The NTP server, if known |
| `samples` | Samples in the file |
| `now` | The sensor's time now, ISO 8601 UTC |
| `now_ms` | The same, epoch ms |
| `now_local` | The same in the sensor's time zone |
| `timezone` | The sensor's time zone |
| `warnings` | Notes that do not stop the schedule (sweeping sensors) |
| `schedules` | Every schedule not deleted |
| `deleted` | The id that was deleted |
