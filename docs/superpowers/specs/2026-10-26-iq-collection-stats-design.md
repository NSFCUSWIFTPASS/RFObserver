# IQ collection statistics per hour and per day

Status: draft, 2026-10-26.

## Intent

An outside tool shows how much raw IQ each sensor records per day. It reads
the sensor's API: the amount for a given date, the total for a date range, and
a 7-day average. Alongside: how much the sensor deleted automatically, how long
it was active, and a snapshot of its storage now. No RFObserver page shows
this yet.

## Decisions (the user's)

- **What counts:** raw IQ recordings only, the `.sc16` captures, auto
  (triggered) and manual. Not the isolated burst clips, the PSD grids saved
  with a recording, the JSON sidecars or the database.
- **Recorded, not on disk:** a day's figure is what was recorded that day. When
  the storage cap later deletes a capture, the day it was recorded keeps its
  figure.
- **Days are local to the sensor:** each sensor has a time-zone setting; every
  caller sees the same day boundaries for that sensor.
- **No backfill:** a sensor counts from the hour it first runs this code. The
  API says when that was.
- **Extras:** bytes and captures deleted automatically, per hour and day;
  seconds the sensor was active, per hour and day; a current storage snapshot.
  A fleet-wide endpoint was offered and not chosen.
- **Consumer:** an outside tool, through the API, calling from a server or
  script (no browser): no CORS and no token.
- **7-day average:** none until 7 complete local days are counted; before
  that the endpoint says so (`average: null` with the count of days so far).

## Builder's calls (made in the design, check while reading)

- **Streaming pipeline only.** Captures written by the sweep (batch) pipeline,
  used when the trigger is off, are not counted, nor are its deletions
  (`_enforce_limit`). The sensors run the streaming pipeline.
- **Active means live RF received:** `active_seconds` is the sum of the
  durations of live chunks the streaming pipeline received. Replay, Standby, a
  crashed pipeline waiting to restart and dropped chunks add nothing.
- **Snapshot bytes are the capture footprint** (the `.sc16` plus its sidecars,
  as the storage index and `ARCHIVE_MAX_GB` measure it), so `cap - on_disk` is
  the real headroom. Recorded and deleted bytes are `.sc16` bytes only, as
  decided; a deletion stats the `.sc16` before removing it.
- **Default zone `UTC`** when `TIMEZONE` is unset.

## Data

A new table, one row per UTC hour (epoch seconds of the hour start), holding
sums that only ever increase:

| Column | Meaning |
|---|---|
| `hour` | UTC hour start, epoch seconds (primary key) |
| `auto_captures`, `manual_captures` | Captures recorded, by origin |
| `auto_bytes`, `manual_bytes` | Raw IQ bytes recorded (`.sc16` bytes, 4 per sample) |
| `auto_seconds`, `manual_seconds` | Recorded signal seconds |
| `deleted_cap_captures`, `deleted_cap_bytes` | Removed by the storage cap (`enforce_cap`, `ARCHIVE_MAX_GB`) |
| `deleted_space_captures`, `deleted_space_bytes` | Removed by the low-free-space guard (`evict_until_free`) |
| `active_seconds` | Seconds the sensor was active (capturing, not Standby) |

The first hour counted is kept in `config` (`collection_hours_since`), as
`detection_hours` keeps its own.

- **Recorded:** added when a capture is finalized, in the same place the
  `iq_captures` row is written (`streaming.py`, the recording-finalize path;
  skipped in replay, as that row is). A capture that spans an hour boundary is
  split between the hours in proportion to time; its count goes to the hour it
  started in.
- **Deleted:** added when `LocalStorage._delete_capture` removes a capture for
  the cap or the free-space guard, to the hour of the deletion, with the
  `.sc16` file's bytes (not its companions). Nothing else is counted as
  deleted: there is no delete action in the app, and files removed outside it
  are only dropped from the storage index.
- **Active seconds:** each live chunk's duration, accumulated in memory and
  added to the table about once a minute and on shutdown, so a crash loses at
  most about a minute.

Writes add to the existing row (`INSERT ... ON CONFLICT DO UPDATE SET x = x +
excluded.x`), so concurrent additions from different paths never overwrite each
other. Rows are never deleted by retention (one row per hour, about 8,800 a
year).

## Time zone

A setting `TIMEZONE` (IANA name, e.g. `America/Denver`), default `UTC`, in
`.env` like the other settings and settable through the Config apply endpoint.
Local days are assembled from the UTC hours that fall inside them, using the
zone's offset for each day (daylight saving days are 23 or 25 hours). Zones
whose offset is not a whole number of hours are not supported; the setting
rejects them.

## API

All on the sensor, JSON, read-only. Every response carries `timezone` and
`covered_since` (ISO local time of the first counted hour, or null when nothing
is counted yet). Bytes are integers; seconds are floats.

- `GET /api/collection/hourly?since=&until=` (ISO times): one entry per
  counted UTC hour in the range, each with its UTC and local start and the
  table's figures.
- `GET /api/collection/daily?date=YYYY-MM-DD`: one local day, the figures
  summed, plus `complete` (false for today) and `covered` (false for a day
  before `covered_since` or only partly after it).
- `GET /api/collection/daily?start=YYYY-MM-DD&end=YYYY-MM-DD` (inclusive): one
  entry per local day and a `total` over the range.
- `GET /api/collection/average?days=7`: the average per day over the last
  `days` complete local days (default 7), with the days used; `null` until that
  many complete days have been counted, with how many have.
- `GET /api/collection/storage`: now: captures and their footprint on disk
  (auto and manual), the cap (`ARCHIVE_MAX_GB` in bytes), the volume's free and
  total bytes. From the storage index, not a disk scan.

Figures in every daily and average response: captures, bytes and seconds (auto,
manual, total), deleted by cap and by low space (captures, bytes), active
seconds.

## Not in scope

- Backfilling from `iq_captures` or the files on disk.
- Burst clips, PSD grids, sidecars, database size.
- A page in RFObserver, and a fleet-wide endpoint.
- Time zones with fractional-hour offsets.

## Testing

- A capture inside one hour, and one spanning two hours, add the right bytes,
  seconds and count to the right hours.
- Cap and low-space deletions add the `.sc16` bytes to the deletion hour.
- Active seconds accrue while active, not in Standby, and are flushed.
- Daily totals in `UTC` and in `America/Denver` on a normal day and on both
  daylight-saving change days.
- Range totals, `complete` and `covered`, and `covered_since`; the 7-day
  average with 6 and with 7 complete counted days.
- Concurrent additions to one hour do not lose counts.
- The storage snapshot matches the storage index.
