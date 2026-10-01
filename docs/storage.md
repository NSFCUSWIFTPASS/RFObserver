# Storage

RFObserver defends a free-space floor on the volume that holds `STORAGE_PATH`
(and, separately, on the volume that holds `DB_PATH` if that is a different
device), whatever else is filling it. The floor is `RFOBS_DISK_MIN_FREE_GB`; the
default `0` means auto: 5% of the volume, at least 2 GB (46 GB on a 916 GB SSD).
`RFOBS_ARCHIVE_MAX_GB` still caps automatic captures on its own. Every
`RFOBS_STORAGE_CHECK_SEC` (10 s) the sensor samples free space and moves along
this ladder:

| Step | When | What RFObserver does |
|---|---|---|
| 0 | free >= floor | nothing |
| 1 | free < floor, automatic captures or burst files exist | deletes the oldest isolated bursts, then the oldest `auto/` captures, until free >= floor x 1.15 |
| 2 | free < floor, no automatic capture left | prunes PSD history to 7 days and detections to 90 days |
| 3 | still below the floor on the next check | refuses to start recordings, automatic and manual |
| 4 | free < floor / 2 | stops storing PSD blobs; the per-window stats rows are still written |

Steps are cumulative, and leaving one takes three checks in a row at floor x 1.15
or more, so a sensor sitting at the floor does not flap. A recording in progress
also checks free space about once a second and ends itself early
(`stopped_reason: "disk_floor"` in its `.json`) when free drops below half the
floor, before the disk is actually full.

Manual captures (`manual/`) and the capture being recorded are never deleted.
From step 2 on, nothing the sensor does raises free space: someone has to
download and delete manual captures or remove whatever else filled the volume.

With continuous triggering, step 1 can settle into deleting each new automatic
capture shortly after it is saved. When a storage check evicts a capture less
than 10 minutes old, the sensor flags `evicting_young` in health (and logs a
warning naming the capture and its age) for 30 minutes after the last such
eviction, without changing `step` or `status` -- steps 1-2 stay "working as
designed".

The database file does not shrink. Pruning PSD blobs and deleting old rows free
pages inside the file, which later inserts reuse, so the file stops growing
instead of getting smaller (`db_reusable_gb` shows how much is free inside it).
Stats rows, detections and minute rollups are kept for
`RFOBS_STATS_RETENTION_DAYS` (730); PSD blobs for `RFOBS_DB_RETENTION_DAYS`.

`GET /api/health` reports all of this in its `storage` block:

```bash
curl -s $SENSOR/api/health | python3 -m json.tool
```

```json
"storage": {
  "free_gb": 42.1, "floor_gb": 45.8, "volume_gb": 916.0,
  "db_gb": 44.0, "db_reusable_gb": 3.2,
  "auto_gb": 480.3, "manual_gb": 118.7, "bursts_gb": 1.2,
  "step": 1, "step_text": "evicting the oldest automatic captures",
  "step_since": "2026-09-23T10:00:00+00:00",
  "last_write_error": null,
  "degraded_since": null,
  "db_volume": null
}
```

`db_volume` is `{free_gb, floor_gb}` when `DB_PATH` is on another device. The
health `status` is `degraded` at step 3 or above, and also while the sticky
warning is set: any write failure (for example a full disk, recorded in
`last_write_error` and in the capture's `.json` as `write_failed` /
`write_error`) or reaching step 3 sets `degraded_since`. The warning survives a
restart and stays after space recovers, so it is seen; the Dashboard shows it
as a banner. Once you have dealt with the cause, clear it from the banner or
with:

```bash
curl -X POST $SENSOR/api/storage/clear-degraded
```
