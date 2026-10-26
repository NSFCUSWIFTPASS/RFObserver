# Collection statistics API

How much raw IQ a sensor has recorded, per hour and per day, plus what the
storage cap deleted, how long the radio was live, and what is on disk now.
Meant for an outside tool (a server or a script) that shows data stored per
day.

- Plain HTTP GET, JSON out. No authentication, no CORS: call it from a server
  or a script, not from a browser page on another origin.
- Counting starts the first time a sensor running this version flushes its
  counters (every 15 s). There is no backfill: captures recorded before that
  are not counted. `covered_since` in every response says when counting began.
- Days are local days in a time zone: the one you ask for with `tz`, else the
  zone at the sensor's location, else its `TIMEZONE` setting, else Pacific
  (see [Time zone](#time-zone)).
  Hours are stored in UTC, so a daylight-saving day has 23 or 25 hours.

Every example below runs in the test suite (`tests/unit/test_collection_api_doc.py`)
against a sensor holding the example data, and must return what is shown. The
same requests were also run against a live mock sensor.

## The example sensor

The examples use a sensor in Boulder, Colorado (`LATITUDE=40.01`,
`LONGITUDE=-105.27`, so `America/Denver`, UTC-6 in October) that started counting at local midnight on 2026-10-18. It is now 12:00 local on
2026-10-26. Every hour it recorded one 1 s automatic capture at 56 MS/s
(224,000,000 bytes) and was live the whole hour. On 2026-10-25 it also recorded
a 4 s manual capture at 09:00, and the storage cap deleted two captures at
15:00.

```bash
SENSOR=http://sensor:8888
```

## One day

```bash
curl -s "$SENSOR/api/collection/daily?date=2026-10-25"
```

```json
{
  "timezone": "America/Denver",
  "timezone_source": "location",
  "covered_since": "2026-10-18T00:00:00-06:00",
  "date": "2026-10-25",
  "start": "2026-10-25T00:00:00-06:00",
  "end": "2026-10-26T00:00:00-06:00",
  "hours": 24,
  "complete": true,
  "covered": true,
  "auto_captures": 24,
  "manual_captures": 1,
  "auto_bytes": 5376000000,
  "manual_bytes": 896000000,
  "auto_seconds": 24.0,
  "manual_seconds": 4.0,
  "deleted_cap_captures": 2,
  "deleted_cap_bytes": 448000000,
  "deleted_space_captures": 0,
  "deleted_space_bytes": 0,
  "active_seconds": 86400.0,
  "captures": 25,
  "bytes": 6272000000,
  "seconds": 28.0
}
```

`bytes` is the figure to plot as "data stored that day". Today is returned
too, with `complete: false`:

```bash
curl -s "$SENSOR/api/collection/daily?date=2026-10-26"
```

```json
{
  "date": "2026-10-26",
  "complete": false,
  "covered": true,
  "bytes": 2688000000
}
```

A day before counting began is all zeros with `covered: false`, so a caller
can tell "nothing recorded" from "not counted":

```bash
curl -s "$SENSOR/api/collection/daily?date=2026-10-17"
```

```json
{
  "date": "2026-10-17",
  "covered": false,
  "bytes": 0
}
```

## A date range

`start` and `end` are inclusive local dates, at most 366 days apart. The
response has each day (same fields as above) and their `total`.

```bash
curl -s "$SENSOR/api/collection/daily?start=2026-10-24&end=2026-10-25"
```

```json
{
  "timezone": "America/Denver",
  "timezone_source": "location",
  "covered_since": "2026-10-18T00:00:00-06:00",
  "days": [
    {"date": "2026-10-24", "covered": true, "captures": 24, "bytes": 5376000000},
    {"date": "2026-10-25", "covered": true, "captures": 25, "bytes": 6272000000}
  ],
  "total": {
    "auto_captures": 48,
    "manual_captures": 1,
    "auto_bytes": 10752000000,
    "manual_bytes": 896000000,
    "auto_seconds": 48.0,
    "manual_seconds": 4.0,
    "deleted_cap_captures": 2,
    "deleted_cap_bytes": 448000000,
    "deleted_space_captures": 0,
    "deleted_space_bytes": 0,
    "active_seconds": 172800.0,
    "captures": 49,
    "bytes": 11648000000,
    "seconds": 52.0
  }
}
```

## 7-day average

The mean per day over the last `days` complete local days (default 7, from 1
to 60), not counting today. `average` is `null` until every one of those days
was fully counted; `days_counted` says how many were. `days` lists the window.

```bash
curl -s "$SENSOR/api/collection/average"
```

```json
{
  "timezone": "America/Denver",
  "timezone_source": "location",
  "covered_since": "2026-10-18T00:00:00-06:00",
  "days_counted": 7,
  "average": {
    "auto_captures": 24.0,
    "manual_captures": 0.14285714285714285,
    "auto_bytes": 5376000000.0,
    "manual_bytes": 128000000.0,
    "auto_seconds": 24.0,
    "manual_seconds": 0.5714285714285714,
    "deleted_cap_captures": 0.2857142857142857,
    "deleted_cap_bytes": 64000000.0,
    "deleted_space_captures": 0.0,
    "deleted_space_bytes": 0.0,
    "active_seconds": 86400.0,
    "captures": 24.142857142857142,
    "bytes": 5504000000.0,
    "seconds": 24.571428571428573
  },
  "days": [
    {"date": "2026-10-19", "covered": true, "complete": true, "bytes": 5376000000}
  ]
}
```

Asking for more days than have been counted gives no average:

```bash
curl -s "$SENSOR/api/collection/average?days=10"
```

```json
{
  "days_counted": 8,
  "average": null,
  "days": [
    {"date": "2026-10-16", "covered": false, "bytes": 0}
  ]
}
```

## By hour

`since` and `until` are ISO times (a trailing `Z` or an offset; no offset
means UTC), at most 31 days apart. Only hours with something counted are
listed, each with its UTC and local start.

```bash
curl -s "$SENSOR/api/collection/hourly?since=2026-10-25T15:00:00Z&until=2026-10-25T17:00:00Z"
```

```json
{
  "timezone": "America/Denver",
  "timezone_source": "location",
  "covered_since": "2026-10-18T00:00:00-06:00",
  "hours": [
    {
      "hour_utc": "2026-10-25T15:00:00+00:00",
      "hour_local": "2026-10-25T09:00:00-06:00",
      "auto_captures": 1,
      "manual_captures": 1,
      "auto_bytes": 224000000,
      "manual_bytes": 896000000,
      "auto_seconds": 1.0,
      "manual_seconds": 4.0,
      "deleted_cap_captures": 0,
      "deleted_cap_bytes": 0,
      "deleted_space_captures": 0,
      "deleted_space_bytes": 0,
      "active_seconds": 3600.0
    },
    {"hour_utc": "2026-10-25T16:00:00+00:00", "auto_bytes": 224000000}
  ]
}
```

## Storage now

What is on disk at this moment, unlike the figures above, which never go
down. Byte counts here include each capture's side files (metadata, PSD), so
they are a little larger than the IQ alone.

```bash
curl -s "$SENSOR/api/collection/storage"
```

```json
{
  "timezone": "America/Denver",
  "timezone_source": "location",
  "covered_since": "2026-10-18T00:00:00-06:00",
  "auto_captures": 180,
  "manual_captures": 1,
  "auto_bytes": 40320036000,
  "manual_bytes": 896000200,
  "cap_bytes": 53687091200,
  "volume_free_bytes": 412316860416,
  "volume_total_bytes": 982141468672
}
```

## Errors

Bad input is `400` with a `detail`; `503` means the sensor's database is not
up yet.

```bash
curl -s "$SENSOR/api/collection/daily?date=2026-13-01"
```

```json
{"detail": "date must be YYYY-MM-DD"}
```

```bash
curl -s "$SENSOR/api/collection/daily?start=2026-01-01&end=2027-06-01"
```

```json
{"detail": "at most 366 days"}
```

```bash
curl -s "$SENSOR/api/collection/average?days=0"
```

```json
{"detail": "days must be a whole number from 1 to 60"}
```

```bash
curl -s "$SENSOR/api/collection/hourly?since=yesterday&until=today"
```

```json
{"detail": "since and until must be ISO times"}
```

## Time zone

Every endpoint takes an optional `tz`, an IANA name such as `America/New_York`.
Without it, days are in the sensor's zone, the first of:

1. the zone at the sensor's `LATITUDE`/`LONGITUDE`, looked up offline on the
   sensor;
2. `TIMEZONE`, for a sensor with no location: set it by hand in the sensor's
   environment or `.env` (`RFOBS_TIMEZONE=America/Denver`);
3. Pacific time (`America/Los_Angeles`).

The sensor works this out once when it starts, and again as soon as its
location is changed on the Config page, and logs it ("Collection statistics
time zone: America/Denver (location)"). A zone found at the location is
written into `TIMEZONE` (and saved to `.env` with the next Config change), so
if the location is later cleared, the sensor keeps the last zone it found.
`timezone_source` in every response says which was used: `request`,
`location`, `setting` or `default`.

```bash
curl -s "$SENSOR/api/collection/daily?date=2026-10-25&tz=America/New_York"
```

```json
{
  "timezone": "America/New_York",
  "timezone_source": "request",
  "covered_since": "2026-10-18T02:00:00-04:00",
  "date": "2026-10-25",
  "start": "2026-10-25T00:00:00-04:00",
  "end": "2026-10-26T00:00:00-04:00",
  "bytes": 6272000000
}
```

Only zones a whole number of hours from UTC work, since days are built from
whole UTC hours. Asking for another (for example `Asia/Kolkata`) is `400`. A
location in such a zone is passed over for `TIMEZONE`, and an unusable
`TIMEZONE` for Pacific time, each with a warning in the sensor's log.
Changing the zone never changes what is stored, only how hours are grouped
into days.

```bash
curl -s "$SENSOR/api/collection/average?tz=Asia/Kolkata"
```

```json
{"detail": "tz must be an IANA zone a whole number of hours from UTC: time zone 'Asia/Kolkata' is not a whole number of hours from UTC"}
```

## Field reference

Figures (in days, totals, averages and hours):

| Field | Meaning |
|---|---|
| `auto_captures` | Captures started by the power trigger, counted in the hour they started |
| `manual_captures` | Captures started by hand (Record) |
| `auto_bytes` | Raw IQ bytes recorded by triggered captures, split over hours by time |
| `manual_bytes` | Raw IQ bytes recorded by manual captures |
| `auto_seconds` | Seconds of signal recorded by triggered captures |
| `manual_seconds` | Seconds of signal recorded by manual captures |
| `deleted_cap_captures` | Captures deleted to stay under the storage cap |
| `deleted_cap_bytes` | Raw IQ bytes of those captures |
| `deleted_space_captures` | Captures deleted to keep the volume's free-space floor |
| `deleted_space_bytes` | Raw IQ bytes of those captures |
| `active_seconds` | Seconds of live RF received (replays are not counted) |
| `captures` | `auto_captures + manual_captures` |
| `bytes` | `auto_bytes + manual_bytes`: data stored |
| `seconds` | `auto_seconds + manual_seconds` |

Day and response fields:

| Field | Meaning |
|---|---|
| `timezone` | The time zone the days are in |
| `timezone_source` | Where it came from: `request` (`tz`), `location`, `setting` (`TIMEZONE`) or `default` (Pacific) |
| `covered_since` | When counting began (local time), or `null` before the first flush |
| `date` | The local date |
| `start` | Local midnight starting the day |
| `end` | Local midnight ending the day |
| `hours` | Hours in the day: 24, or 23/25 on a daylight-saving change |
| `complete` | The day has ended |
| `covered` | Counting had begun by the start of the day, so its figures are whole |
| `days` | The days of a range or of an average's window |
| `total` | The figures summed over a range |
| `days_counted` | Days in an average's window that are complete and covered |
| `average` | The figures divided by the window's days, or `null` |
| `hours` | In `/hourly`: the counted hours |
| `hour_utc` | The hour's start in UTC |
| `hour_local` | The hour's start in local time |

Storage fields:

| Field | Meaning |
|---|---|
| `cap_bytes` | The storage cap (`ARCHIVE_MAX_GB`) in bytes |
| `volume_free_bytes` | Free space on the storage volume, or `null` if unknown |
| `volume_total_bytes` | Size of the storage volume, or `null` if unknown |

`auto_captures`, `manual_captures`, `auto_bytes` and `manual_bytes` in
`/storage` are what is on disk now (see above).
