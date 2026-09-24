# Storage Budgeting: Design

Date: 2026-09-23
Status: approved in conversation, section by section; this document is for review.

## Intent

The user, verbatim:

> "we need to create space for the PSD storage and tie that into IQ storage space using
> the available space on the path somehow. Another thing to consider (the disk RFObs is
> pointed at is usually for RFObs only but incase something else fill that up, how does
> RFObs behave. For example, on the deployed sensor, we have a 1TB NVMe, PSD history is set
> to 30 days and 600GB on Archive Max"

> "for avg statistics, we want to keep them much longer since they are just a few numbers,
> like 2 years or so"

Field reference (user-supplied `df`): OS on a 114 GB SD card; RFObserver data on a 916 GB
NVMe at `/mnt/ssd`, 631 GB used, 239 GB free; `ARCHIVE_MAX_GB=600`, 30-day PSD retention.

**Success means:** RFObserver keeps its volume from filling regardless of what fills it;
when space is short it gives up the most re-acquirable data first and says so loudly; a
write failure is never recorded as a successful capture; stats survive about two years.

## Current behaviour (verified against `main` on 2026-09-23)

| Finding | Where |
|---|---|
| Nothing measures free space; `get_disk_usage()` has no callers | `utils/hardware.py:37` |
| `ARCHIVE_MAX_GB` is enforced only after a recording finalizes, and only over `auto/` | `storage/local.py:99`, `pipeline/streaming.py:1435` |
| `manual/` captures are never counted and never evicted | `storage/local.py:106` |
| IQ bytes are counted at enqueue, not at write | `pipeline/streaming.py:1001` |
| The file writer logs `File writer crashed` on any exception and exits; the recording continues and every later chunk becomes a "dropped chunk" gap | `_file_writer_loop` |
| RAM-mode `tofile()` is unguarded; a failure skips metadata, DB insert and eviction | `_finalize_recording` |
| `avg_windows`, `detections`, `avg_minutes` rows are never deleted; retention only nulls PSD blobs | `_cleanup_loop`, `prune_avg_psd_blobs` |
| The DB is `auto_vacuum=0`: nulling blobs frees pages for reuse but never shrinks the file | measured: 3,000 blobs nulled, file unchanged at 50.7 MB, 12,000 freelist pages, reused by later inserts |

Measured costs: PSD blobs ~1.4 GB/day; stats ~308 B/row; detections **372 B/row**
including indexes (95,340 rows measured). Detections at a noisy 10 dB threshold on
nano-super: ~11 M rows/day = **4.1 GB/day**; at the 30 dB default: ~17 K rows/day = 6 MB/day.

## Decisions

| Question | Decision |
|---|---|
| Shape of the policy | A free-space floor RFObserver defends. `ARCHIVE_MAX_GB` and `DB_RETENTION_DAYS` remain upper bounds. |
| What goes first under pressure | IQ, then PSD age, then stop recording, then stop PSD blobs. Never manual captures, never stats rows outside their retention. |
| Meaning of `ARCHIVE_MAX_GB` | Unchanged: `auto/` only. Manual usage is reported separately and bounded only by the floor. |
| Surfacing | `/api/health` storage block, UI state, and a sticky degraded flag. |
| DB file never shrinking | Accept the plateau. Pruning stops DB growth; only IQ eviction returns disk. No vacuuming on a live sensor. |
| Detection retention | Same as stats: 2 years. |

## Architecture

```
storage/governor.py     StorageGovernor: pure decisions over a sampled StorageSample.
                        No filesystem or DB access in the decision logic.
pipeline/app.py         _storage_loop: every STORAGE_CHECK_SEC (10 s) sample the disk and
                        DB, tick the governor, carry out its actions, publish the result
                        to app.state.storage.
```

Consumers read `app.state.storage` (the published `StorageState`); none measures the disk
itself, except the mid-recording guard, which must react faster than a 10 s tick.

### Inputs sampled per tick (`StorageSample`)

- `free_bytes`, `total_bytes`: `shutil.disk_usage(STORAGE_PATH)` (its `free` is the
  space available to a non-root process).
- If `DB_PATH` is on a different device (`st_dev` differs): the same pair for the DB's
  volume, checked against its own floor.
- `db_file_bytes`, `db_reusable_bytes` (`freelist_count * page_size`) so the plateau is
  visible.
- `auto_bytes`, `manual_bytes`, and whether any evictable `auto/` capture exists (every
  `auto/` capture except the one currently being recorded).

### The floor

`DISK_MIN_FREE_GB`, default `0` meaning **auto: 5% of the volume, minimum 2 GB**
(46 GB on the field's 916 GB SSD). An explicit value overrides. Recovery margin
`floor * 1.15`.

### The ladder

The step is recomputed every tick. Each step has its own trigger because only step 1
returns disk.

| Step | Trigger | Action | Effect |
|---|---|---|---|
| 0 | free >= floor (recovery: >= floor x 1.15 for 3 consecutive ticks) | none | |
| 1 | free < floor and an evictable `auto/` capture exists | evict oldest `auto/` captures until free >= floor x 1.15 or none evictable | returns disk |
| 2 | free < floor and no evictable `auto/` capture | prune PSD blobs to 7 days and detections to 90 days (pressure cutoffs) | stops DB growth; no disk returned |
| 3 | free < floor after steps 1 and 2 | refuse to start recordings, auto and manual | stops IQ growth |
| 4 | free < floor / 2 | stop writing PSD blobs; stats rows still written | stops nearly all growth |

Steps are cumulative: at step 3, steps 1 and 2 are also in force. Leaving any step
requires the recovery condition, so a sensor sitting at the floor does not flap.

Once at step 2 or beyond, nothing RFObserver does will raise free space (there is no
evictable IQ left and pruning does not shrink the DB file). The sensor stays there until
space is freed from outside: a person deletes or downloads-then-deletes manual captures,
lowers retention, or removes whatever else filled the volume. That is intended; it is why
step 3 sets the sticky flag.

At field rates the step 2 pressure cutoff frees ~23 days of PSD blobs inside the DB, so
the DB file stops growing for ~23 days. That time is for a person to intervene.

**Never touched by the governor:** `manual/` captures, the capture being recorded, stats
rows (other than normal retention), `tone_checks`, `iq_captures`.

### Mid-recording guard

A 10 s tick is too slow for a live recording: 56 Msps is 224 MB/s, ~2 GB per tick. The
file writer checks `shutil.disk_usage` after roughly every second of written IQ. If free
drops below floor / 2 it ends the recording cleanly and early through the normal
finalize path, with `stopped_reason: "disk_floor"`. This is the recording-side
counterpart of step 4 and must fire before ENOSPC, not after.

### Consumers

| Consumer | Reads | Behaviour |
|---|---|---|
| `start_recording`, trigger arming | step | step >= 3: refuse, with a reason the API and UI show |
| `_storage_loop` itself | step 1 | calls `LocalStorage.evict_until_free(target, exclude=active)` |
| `_cleanup_loop` | step 2 | uses the pressure cutoffs instead of the configured ones |
| PSD blob write in `insert_avg_window` | step 4 | writes the row with `psd_powers = NULL` |
| file writer | own sampling | mid-recording guard |

`enforce_cap` after each recording is unchanged (it bounds `auto/` by `ARCHIVE_MAX_GB`).

## Write failures and accounting

- **The writer owns the truth.** It counts IQ and grid bytes after each successful
  `write`. On an exception it records `(errno, message)` in a shared error slot and
  sets a flag instead of silently exiting.
- **A writer error ends the recording promptly** (`stopped_reason: "write_error"`) rather
  than turning the remainder into dropped-chunk gaps.
- **Metadata comes from the file.** `total_bytes`, `total_samples`, `duration_sec` are
  derived from the `.sc16` size after the writer has closed it; a partial trailing sample
  is truncated. Same rule already applied to the `.psd` row count.
- **Every capture `.json` gains `stopped_reason`**: `manual`, `max_duration`,
  `trigger_end`, `disk_floor`, `write_error`. On failure also `write_failed: true` and
  `write_error: "<errno name>: <message>"`.
- **RAM-mode `tofile()` is guarded.** On failure whatever reached disk is kept, and the
  metadata write, DB insert and eviction pass still run, flagged as failed.
- **Other write failures report to the governor**: SQLite "database or disk is full" from
  the pipeline's inserts, and failures writing `.json` / `.psd.json` sidecars, are recorded
  as `last_write_error` with a timestamp.

## Surfacing

`GET /api/health` gains:

```
"storage": {
  "free_gb": 42.1, "floor_gb": 45.8, "volume_gb": 916.0,
  "db_gb": 44.0, "db_reusable_gb": 3.2,
  "auto_gb": 480.3, "manual_gb": 118.7,
  "step": 1, "step_since": "2026-09-23T10:00:00Z",
  "last_write_error": null,
  "degraded_since": null,
  "db_volume": null            # {free_gb, floor_gb} when DB_PATH is on another device
}
```

- Health `status` is `degraded` at step >= 3 or while the sticky flag is set. Steps 1-2
  are the system working as designed: reported, not degraded.
- **Sticky flag**: set by any write failure or by reaching step 3. Persisted in the DB
  `config` table (key `storage_degraded`) so a restart keeps the evidence. Cleared only
  by `POST /api/storage/clear-degraded`, and stays set after space recovers.
- **Dashboard**: a persistent banner while step > 0 or the flag is set, naming the step
  and the last error, with the clear action.
- **Config page**: a storage bar for the volume: free, floor, DB, `auto/`, `manual/`.

## Retention

- New setting `STATS_RETENTION_DAYS`, default 730. Deletes `avg_windows`, `detections` and
  `avg_minutes` rows older than the cutoff. `DB_RETENTION_DAYS` keeps its meaning (PSD
  blobs only).
- **Every delete is chunked**: `DELETE FROM t WHERE rowid IN (SELECT rowid FROM t WHERE
  <time> < ? LIMIT :n)`, awaiting between chunks. Statement size on the writer
  connection is what starves the pipeline (peak-finder rollup, 2026-09-21), and the first
  run on a field DB has up to two years of backlog. The chunk size is set from a
  measurement on nano-super against a real-size DB, with a per-statement budget well under
  the ~300 ms line at which the pipeline begins to drop.
- Step 2 pressure cutoffs (PSD 7 days, detections 90 days) use the same chunked path.
- Deleted rows free pages for reuse; the file does not shrink (plateau accepted).

## New settings

| Setting | Default | Meaning |
|---|---|---|
| `DISK_MIN_FREE_GB` | `0` = auto (5% of volume, min 2 GB) | the floor |
| `STATS_RETENTION_DAYS` | `730` | row retention for stats, detections, minute rollups |
| `STORAGE_CHECK_SEC` | `10` | governor tick |

All three appear on the config page with help text. Pressure cutoffs (7 / 90 days),
recovery margin (1.15), recovery ticks (3) and the step-4 fraction (0.5) are constants.

## Testing

- **Governor unit tests** (pure): every step's trigger; cumulative steps; recovery
  hysteresis with no flapping at the floor; step 2 not treated as reclaiming; manual and
  active captures never selected; auto floor computation; separate DB volume.
- **Eviction**: `evict_until_free` with an injected free-space function.
- **Injected write failures**: `OSError(ENOSPC)` in the writer and in RAM-mode `tofile()`
  must yield a finalized capture, file-derived counts, `write_failed`, the sticky flag, and
  a prompt stop rather than gaps.
- **Real ENOSPC on nano-super**: a small tmpfs (sudo) as `STORAGE_PATH`, a live
  recording run into it. Injected errors cannot prove the real path.
- **Floor scenario on nano-super**: a filler file pushes free below the floor; watch
  steps 1 to 4 in health and UI; remove it and confirm recovery only after hysteresis.
- **Retention**: correct rows and only those; per-chunk timing on a seeded real-size DB
  on nano-super, which fixes the chunk size.
- **Surfacing**: health block shape; sticky flag survives restart and clears via the
  endpoint; headless browser check of the banner and storage bar.

## Out of scope

- Web UI authentication.
- Reclaiming DB file space (vacuum); plateau accepted.
- Publishing storage state over NATS or OpenZMS.
- Deleting stale `iq_captures` rows for evicted captures.

## Open items

- Chunk size for retention deletes: set by measurement during implementation.
- Exact field `DB_PATH` placement (same device as `STORAGE_PATH` or not) is not known
  from here; the design handles both.
- The field sensor's burst threshold is unknown; at a low threshold detections dominate
  DB growth, and only retention and the step 2 cutoff bound them.
