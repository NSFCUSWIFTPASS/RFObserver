# Storage budgeting: hardware validation on nano-super

## 1. The question

Date: 2026-09-23. Build: `feat/storage-budgeting` at `e0bcee5` ("feat(web): storage banner,
refusal notice, and config storage bar"), run from a worktree (`~/rfobs-storage`) with
`PYTHONPATH=$HOME/rfobs-storage/src`, `rfobserver.__file__` confirmed under the worktree in every
run log.

Hardware: nano-super (Jetson Orin Nano Super, JetPack 6.2 / L4T R36.5.0, Ubuntu 22.04 aarch64,
Python 3.10, system SQLite 3.37.2), 15 W power profile (the box default, not MAXN), rootfs on a
467 GB NVMe (`/dev/nvme0n1p1`, about 430 GB free at the start). SDR: Ettus B200mini, serial
`322750B`, UHD 4.1.0.5, over USB 3.

What was validated, against `docs/superpowers/specs/2026-09-23-storage-budgeting-design.md`:

1. The retention chunk size (`RETENTION_CHUNK_ROWS`): per-statement timing on a real-size DB, and
   one full retention pass while the pipeline runs against that DB.
2. The real ENOSPC path: a live B200mini recording into a 256 MB tmpfs.
3. The floor ladder, steps 0 to 4 and recovery, with a filler file on the NVMe; the dashboard
   banner and config storage bar at step 3; the mid-recording floor guard (`disk_floor`).
4. Open question from review: does continuous triggering churn short captures after a
   `disk_floor` stop, before the governor's next tick refuses?

## 2. The answer

The ladder, the refusals, the floor guard, the sticky flag and the ENOSPC stop all behave as
designed; `RETENTION_CHUNK_ROWS` is set to **250** (the largest chunk whose p99 statement time is
under 100 ms for every retention statement, cold cache). But the retention pass is not harmless:
nulling PSD blobs while the pipeline runs stalled the DB writer for 10 to 30 s at a time in every
run (4 of 4), because this SQLite build has `SECURE_DELETE` compiled in and the zero-fill writes
provoke this NVMe's lost-completion stalls; with `PRAGMA secure_delete=OFF` the same work ran clean
(2 of 2). That and five smaller gaps are under "Findings"; none was fixed here.

## 3. Procedure (probes in the order run)

Every server was started with `RFOBS_WEB_PORT=8888 RFOBS_SENSOR_ACTIVE=true`, with
`RFOBS_STORAGE_PATH` and `RFOBS_DB_PATH` under a scratch dir (`~/rfobs-val/`, now deleted), and
stopped with `fuser -k 8888/tcp`. No `.env` is read (the worktree has none).

| # | Probe | What it isolates / controls for |
|---|---|---|
| 1 | Seed a 27.7 GB DB (schema from `SensorDatabase.connect()`, rows by stdlib `sqlite3`, day by day so tables interleave in the file as on a live sensor) | A DB larger than the box's 7.4 GB RAM, so old rows are read from disk as on a field DB |
| 2 | Time every retention statement at chunk 1000, 2000, 5000, 10000, then 500, 250, 100 (wrapping `_delete_older_chunk` and `_prune_blob_chunk` on the instance, driving the real `delete_older_than` / `prune_avg_psd_blobs` with their 0.05 s pause). `drop_caches` before each run | Statement cost vs chunk size, cold cache (worst case), no concurrent load |
| 3 | Baseline: mock pipeline, fresh empty DB, 4 min | The pipeline's own drop counter and beacon with no retention work |
| 4 | Full pass: mock pipeline on the seeded DB, chunk forced to 250, rollup off (`RFOBS_PEAKS_ROLLUP_INTERVAL_SEC=0`, so the 2026-09-21 rollup cost is not mixed in), `/api/health` beacon sampled every 5 s from the workstation | Retention under load: drops, beacon age, and afterwards the gaps between consecutive `avg_windows.start_time` (each row is stamped at insert, so a gap is a stalled writer) |
| 5 | Traced rerun of the blob-null phase (aiosqlite `_execute` calls over 200 ms logged, event-loop lag over 200 ms logged, blob watermark preset so the run skips the 20 M-row stats scan) | Which statement stalls, and whether the loop itself blocks |
| 6 | A/B: 49,383 blobs restored on two 2-day ranges; null one with the default connection, the other with `PRAGMA secure_delete=OFF` on the writer; `drop_caches` before each | Whether the stall follows the zero-fill writes |
| 7 | tmpfs 256 MB as `STORAGE_PATH`, B200mini at 2 Msps, `DISK_MIN_FREE_GB=0.001`, `RECORDING_MAX_SEC=0`, `STORAGE_CHECK_SEC=2`, manual `POST /api/recording/start` | The real ENOSPC path (the floor guard cannot pre-empt it at a 1 MB floor) |
| 8 | Restart on the same DB; then `POST /api/storage/clear-degraded` | Sticky flag persistence and clearing |
| 9 | Same tmpfs, default floor (`DISK_MIN_FREE_GB` unset: 2 GB auto on a 256 MB volume) | Refusal at step 3/4 instead of ENOSPC |
| 10 | NVMe, floor 10 GB, continuous trigger (threshold -300 dB so every chunk crosses), `RECORDING_MAX_SEC=10`, default `STORAGE_CHECK_SEC=10`; `fallocate` a filler to leave 4 GiB (below floor / 2) mid-recording | The churn question: captures started between the writer's `disk_floor` stop and the governor tick |
| 11 | Same volume, no trigger, filler at 12 GiB free, manual recording, filler grown to 4.5 GiB free | A `disk_floor` capture whose `.json` survives (manual captures are never evicted) |
| 12 | Ladder A: floor 10 GB, `STORAGE_CHECK_SEC=2` (shortened from 10 to save time), 4 Msps continuous trigger, `RECORDING_MAX_SEC=2`, 2.6 GB of `auto/` captures accumulated, then filler to 9 GiB free | Step 1 eviction to floor x 1.15, oldest first, active capture untouched |
| 13 | Ladder B: restart without trigger (so no new `auto/` captures appear), filler to 7 GiB free; ladder DB pre-seeded with 5,000 blob windows aged 8 to 20 days and 5,000 detections aged 91 to 100 days, `DB_RETENTION_DAYS=30` so only the pressure cutoffs can touch them | Steps 1, 2 (pressure pass acts on real rows), 3 on the next tick; screenshots |
| 14 | Filler to 4 GiB free | Step 4: new `avg_windows` rows with `psd_powers IS NULL` |
| 15 | Remove the filler | Recovery only after 3 ticks at >= floor x 1.15; sticky flag kept |

Screenshots (headless Chrome via puppeteer) are in the session scratchpad:
`step3-dashboard.png`, `step3-config-storage.png`.

## 4. Evidence

### 4.1 Seeded DB

```
DONE avg_windows=20000000 blobs=2222222 detections=9999990 avg_minutes=1166400 t=1081s size=27661352960
```

`avg_windows`: 20 M rows over 810 days (one per 3.5 s; the field rate is 2/s, so two field years
are about 126 M rows; this is a representative size, not the full one), blobs of 8192 B on the
last 90 days (2.22 M). `detections`: 10 M over the last 110 days, random `burst_id` (uuid4) and
random `center_freq_hz` so the unique and frequency indexes are updated at random pages as in
production. `avg_minutes`: 1440 per day for 810 days.

### 4.2 Statement timing (ms per statement, cold cache, pause 0.05 s between statements)

Large chunks (first run):

```
 chunk what             stmts     rows     p50     p90     p99     max   rows/s
  1000 blob_scan_noop     100   100000     6.4     7.4     8.9     9.3   152858
  1000 blob_null          100    99091    79.4    98.5   197.6   219.8    11324
  1000 del_avg_windows    100    99095     7.5    10.6    83.1    83.2    77579
  1000 del_detections      93    92145   213.8   383.4   529.0   629.3     4287
  2000 blob_scan_noop     100   200000    10.1    11.1    12.1    13.6   200176
  2000 blob_null           99   197546   144.8   405.8   613.6   618.2     9919
  2000 del_avg_windows     99   197550    15.4    33.2    40.7    41.8   109760
  2000 del_detections      91   181835   379.0   579.3   671.8  1083.1     4838
  5000 blob_scan_noop     100   500000    22.7    25.5    30.9    38.6   216952
  5000 blob_null           99   493851   329.2   470.8  1009.1  1010.5    12841
  5000 del_avg_windows    104   518547    36.0   124.2   135.6   136.4   100108
  5000 del_detections      91   454710   583.4  1271.7  1480.0  2866.4     6730
 10000 blob_scan_noop     100  1000000    45.4    50.2    65.4  4098.5   115899
 10000 blob_null           99   987694   635.4  1258.2  1974.8  2002.0    12922
 10000 del_avg_windows    104  1037088    85.2   100.4   114.2   115.5   120793
 10000 del_detections      91   909287   960.5  1824.6  2801.9  4220.5     8397
```

No chunk in the brief's range meets the 100 ms p99 budget for detections or blob nulling, so
smaller chunks were measured:

```
 chunk what             stmts     rows     p50     p90     p99     max   rows/s
   500 blob_scan_noop     100    50000     3.5     4.7     5.5     6.1   139316
   500 blob_null          100    49524    44.3    47.8    54.5    67.9    11331
   500 del_avg_windows    100    49503     4.6     6.0    23.4    27.3    91334
   500 del_detections     183    91325   114.5   149.0   182.8   246.3     4272
   250 blob_scan_noop     100    25000     2.2     4.3    15.5    61.8    78538
   250 blob_null           99    24706    15.1    35.3    37.5    68.6    10776
   250 del_avg_windows     99    24706     2.9     3.5    20.5    25.3    71599
   250 del_detections     363    90657    56.7    84.9    93.9 30075.1     1788
   100 blob_scan_noop     100    10000     1.7     3.4     4.0     5.3    47483
   100 blob_null          248    24717     7.5    31.5    33.4    39.7     8277
   100 del_avg_windows    248    24719     2.2     3.2    16.4    21.9    37317
   100 del_detections     911    91093    15.6    41.9    47.8    67.1     4812
```

The 30,075 ms maximum at chunk 250 is one statement stuck behind an NVMe completion timeout
(dmesg below); it is the max, not the p99. Detections dominate: random-order unique and frequency
indexes make them about 4,300 rows/s regardless of chunk size, so the statement time is linear in
the chunk. Choice: **250** (detections p99 93.9 ms, blob null 37.5 ms, avg_windows 20.5 ms).

```
[Wed Sep 23 21:42:39 2026] nvme nvme0: I/O 762 QID 6 timeout, completion polled
...
DB write _delete_older_chunk stuck >30s -- abandoning connection
Database reconnected after stuck write
250 detections deleted 90657 68.9s
```

(The product log line uses an em-dash; it is rendered `--` here.)

### 4.3 Full pass with the pipeline running (chunk 250)

Baseline, fresh DB, 4 min: `TIMING recv#2550: recv=59.8ms dropped=0 ... handoff_dropped=0/0`,
beacon age never above 0.2 s.

Pass on the seeded DB (process start 21:55:05):

```
2026-09-23 23:00:37,769 rfobserver.storage.database INFO Pruned PSD blobs for 173042 avg windows (cutoff: 2026-09-16T21:55:06.158537)
2026-09-23 23:00:43,375 rfobserver.storage.database INFO Retention: deleted 26013 avg_windows rows older than 730 days
2026-09-23 23:01:07,879 rfobserver.storage.database INFO Retention: deleted 115312 avg_minutes rows older than 730 days
2026-09-23 23:01:11,202 rfobserver.pipeline.streaming INFO TIMING recv#44600: recv=59.9ms dropped=0 (IQ=36.6ms) handoff_dropped=0/0 ovf=0 lost=0
```

The blob pass took 65.5 min: the in-memory watermark starts empty on every process start, so the
pass walks all 20 M stats-only rows at 250 per statement plus 50 ms pause before reaching the 11
days of blobs it nulls. `dropped=0` throughout. But beacon age and insert gaps show the writer
stalled during the last 2 minutes, which is exactly when blobs were being nulled:

```
beacon age (s) from /api/health, every 5 s:
22:58:53 3.2   22:58:58 8.2   22:59:03 13.2
22:59:19 4.6   22:59:24 9.6
22:59:29 1.2   22:59:34 6.3   22:59:39 11.3   22:59:44 16.3   22:59:49 21.3
23:00:04 2.4   23:00:09 7.5   23:00:14 12.5   23:00:19 17.5

gaps between consecutive avg_windows rows written during the pass (6,880 rows):
median gap 0.553393  p99 0.710625
(21.997747, '2026-09-23T22:59:27.819929+00:00', '2026-09-23T22:59:49.817676+00:00')
(19.064211, '2026-09-23T23:00:01.773443+00:00', '2026-09-23T23:00:20.837654+00:00')
(15.043106, '2026-09-23T22:58:50.736768+00:00', '2026-09-23T22:59:05.779874+00:00')
(10.646352, '2026-09-23T22:59:14.389812+00:00', '2026-09-23T22:59:25.036164+00:00')
(0.777224, ...)   <- next largest, normal
```

### 4.4 Traced blob-null rerun (98,932 blobs, same DB, chunk 250)

```
VALTRACE slow _execute(commit) 16154 ms:
VALTRACE slow _execute(executemany) 15766 ms: INSERT OR IGNORE INTO detections ...
VALTRACE slow _execute(execute) 8733 ms: PRAGMA page_size
VALTRACE slow _execute(commit) 29993 ms:
rfobserver.storage.database ERROR DB write _prune_blob_chunk stuck >30s -- abandoning connection
VALTRACE slow _execute(commit) 5371 ms:
VALTRACE slow _execute(commit) 29992 ms:
rfobserver.storage.database ERROR DB write _prune_blob_chunk stuck >30s -- abandoning connection
VALTRACE slow _execute(execute) 29977 ms: INSERT INTO avg_windows ...
rfobserver.pipeline.streaming ERROR insert_detections failed for 64 bursts; skipping
  sqlite3.OperationalError: database is locked
VALTRACE slow _execute(commit) 29989 ms:
rfobserver.storage.database ERROR DB write _prune_blob_chunk stuck >30s -- abandoning connection
rfobserver.pipeline.streaming ERROR insert_detections failed for 66 bursts; skipping
VALTRACE slow _execute(commit) 29996 ms:
rfobserver.storage.database ERROR DB write _prune_blob_chunk stuck >30s -- abandoning connection
rfobserver.pipeline.streaming ERROR insert_detections failed for 66 bursts; skipping
rfobserver.storage.database INFO Pruned PSD blobs for 98932 avg windows (cutoff: 2026-09-20T23:03:06.332344)

dmesg:
[Wed Sep 23 23:04:14 2026] nvme nvme0: I/O 984 QID 6 timeout, completion polled
[Wed Sep 23 23:04:48 2026] nvme nvme0: I/O 970 QID 6 timeout, completion polled
[Wed Sep 23 23:05:34 2026] nvme nvme0: I/O 149 QID 5 timeout, completion polled
```

The slow operation is always the prune's `commit` (250 blobs); the pipeline's inserts and the
storage tick's `PRAGMA page_size` wait behind it on the same writer thread. The one event-loop lag
logged was 381 ms at startup; the loop itself did not block.

SQLite build on the box: `3.37.2`, `pragma secure_delete` = 1, compile option `SECURE_DELETE`.

### 4.5 A/B: secure_delete (49,383 blobs each, pipeline running, cold cache)

| Run | Writer | Wall time for the pass | Operations over 200 ms |
|---|---|---|---|
| sdoff (first try, warm) | `secure_delete=OFF` | 12 s | none |
| ab_on | default (`secure_delete=1`) | 56 s | `commit 27288 ms` (plus the inserts queued behind it) |
| ab_off | `secure_delete=OFF` | 25 s | none |

### 4.6 Real ENOSPC on a 256 MB tmpfs (B200mini, 2 Msps)

```
23:10:54 {"state":"recording","file":"322750B-nano-super-20260923T231054.sc16","bytes":9638400,...}
23:11:11 {"state":"finalizing",...,"bytes":140710400,...}
23:11:13 {"state":"idle",...,"bytes":135725056,...}

2026-09-23 23:11:11,276 ERROR Recording write failed (ENOSPC: No space left on device); ending the recording
2026-09-23 23:11:11,778 ERROR Write failed: 322750B-nano-super-20260923T231054.sc16: ENOSPC: No space left on device
2026-09-23 23:11:11,785 WARNING PSD grid rows on disk (16200) differ from rows queued (17000); reporting the file
2026-09-23 23:11:11,864 ERROR Write failed: 322750B-nano-super-20260923T231054.psd.json: ENOSPC: No space left on device
2026-09-23 23:11:11,918 ERROR Write failed: 322750B-nano-super-20260923T231054.sc16 metadata .json: ENOSPC: No space left on device
2026-09-23 23:11:12,736 WARNING Storage step 0 -> 4 (PSD history writes stopped): 0.0 GB free, floor 0.0 GB
2026-09-23 23:11:15,210 ERROR Detections sidecar write failed for 322750B-nano-super-20260923T231054.sc16

-rw-rw-r-- 1 ocollaco ocollaco         0 23:11 322750B-nano-super-20260923T231054.detections.json
-rw-rw-r-- 1 ocollaco ocollaco         0 23:11 322750B-nano-super-20260923T231054.json
-rw-rw-r-- 1 ocollaco ocollaco 132710400 23:11 322750B-nano-super-20260923T231054.psd
-rw-rw-r-- 1 ocollaco ocollaco         0 23:11 322750B-nano-super-20260923T231054.psd.json
-rw-rw-r-- 1 ocollaco ocollaco 135725056 23:11 322750B-nano-super-20260923T231054.sc16
```

`135725056 % 4 == 0`; 33,931,264 samples = 16.97 s at 2 Msps. The DB row carries the
file-derived counts (the `.json` could not be written):

```
iq_captures: (1, '322750B-nano-super-20260923T231054.sc16', 'manual', '2026-09-23T23:10:53.775766+00:00',
  '2026-09-23T23:11:10.741398+00:00', 16.966, 2437000000.0, 2000000.0, 40.0, 33931264, 0, ...)
config: [('storage_degraded', '2026-09-23T23:11:11.779382+00:00')]
```

`/captures/list` still lists it (`"meta":null`, the zero-byte files listed). Health after the stop:

```
{"status":"degraded",...,"pipeline":{"active":true,"gave_up":false,"consecutive_crashes":0,"beacon_age_sec":0.1,...},
 "storage":{"free_gb":0.0,"floor_gb":0.0,"volume_gb":0.2,"db_gb":0.0,"db_reusable_gb":0.0,"auto_gb":0.0,"manual_gb":0.2,
 "step":4,"step_text":"PSD history writes stopped","step_since":"2026-09-23T23:11:12.736377+00:00",
 "last_write_error":{"at":"2026-09-23T23:11:11.924040+00:00","error":"322750B-nano-super-20260923T231054.sc16 metadata .json: ENOSPC: No space left on device"},
 "degraded_since":"2026-09-23T23:11:11.779382+00:00","db_volume":{"free_gb":403.6,"floor_gb":0.0}}}
```

After a restart on the same DB, then after `POST /api/storage/clear-degraded`:

```
{"status":"degraded",...,"storage":{...,"step":4,...,"last_write_error":null,"degraded_since":"2026-09-23T23:11:11.779382+00:00",...}}
clear -> {...,"step":4,...,"last_write_error":null,"degraded_since":null,...}
config: [('storage_degraded', '', '2026-09-23 23:11:58')]
```

(`status` stays `degraded` after the clear because the step is still 4, as designed.)

Default floor on the same (now empty) tmpfs:

```
{"status":"degraded",...,"storage":{"free_gb":0.2,"floor_gb":2.0,"volume_gb":0.2,...,"step":4,...,
 "degraded_since":"2026-09-23T23:12:21.585987+00:00","db_volume":{"free_gb":403.6,"floor_gb":23.3}}}
POST /api/recording/start -> HTTP 409 {"detail":"Recording refused: free space 0.2 GB is below the 2.0 GB floor (storage step 4, PSD history writes stopped)"}
POST /api/recording/arm   -> HTTP 409 (same detail)
avg_windows per minute (start, rows, psd NULL, psd present):
('2026-09-23T23:10', 30, 0, 30)  ('2026-09-23T23:11', 61, 46, 15)  ('2026-09-23T23:12', 33, 33, 0)
```

### 4.7 Churn after a disk_floor stop (continuous trigger, STORAGE_CHECK_SEC=10)

Filler `fallocate`d at 23:14:12.76 to 13.51, leaving 3.99 GiB (floor 10, half-floor 5):

```
23:14:13,581 WARNING Free space 3.99 GB below half the 10.00 GB floor: ending the recording
23:14:14,293 INFO Recording saved: ...231407.sc16 (57152000 bytes, 6.7s, ...)
23:14:14,721 INFO Recording started (disk): ...231414.sc16
23:14:14,804 WARNING Free space 3.97 GB below half the 10.00 GB floor: ending the recording
23:14:15,383 INFO Recording saved: ...231414.sc16 (8000000 bytes, 0.7s, ...)
23:14:15,408 WARNING Free space 3.96 GB below half the 10.00 GB floor: ending the recording
23:14:15,408 INFO Recording started (disk): ...231415.sc16
23:14:15,627 INFO Recording saved: ...231415.sc16 (8000000 bytes, 0.2s, ...)
23:14:15,813 INFO Recording started (disk): ...231415.sc16          <- same name again
23:14:15,822 WARNING Free space 3.96 GB below half the 10.00 GB floor: ending the recording
23:14:16,066 INFO Recording saved: ...231415.sc16 (8000000 bytes, 0.2s, ...)
23:14:16,255 INFO Recording started (disk): ...231416.sc16
23:14:16,258 WARNING Free space 3.95 GB below half the 10.00 GB floor: ending the recording
23:14:16,638 INFO Recording saved: ...231416.sc16 (8000000 bytes, 0.3s, ...)
23:14:17,413 INFO Recording started (disk): ...231417.sc16
23:14:17,459 WARNING Free space 3.94 GB below half the 10.00 GB floor: ending the recording
23:14:17,706 WARNING Storage step 0 -> 4 (PSD history writes stopped): 3.9 GB free, floor 10.0 GB
23:14:18,667 WARNING Storage floor: evicted 0.5 GB of automatic captures
23:14:18,730 WARNING Recording refused: free space 3.9 GB is below the 10.0 GB floor (storage step 4, PSD history writes stopped)
```

Five captures of 0.2 to 0.9 s were started in the 4.1 s between the writer's stop and the tick.
All were then evicted by step 1 at the same tick; their `.detections.json` sidecars, written after
the 3 s grace, were left behind in `auto/` (`231407`, `231414`, `231415`, `231417`).

### 4.8 Floor guard with a surviving .json (manual capture)

```
23:15:32,189 INFO Recording started (disk): 322750B-nano-super-20260923T231532.sc16
(filler grown at 23:15:35.47 to leave 4.5 GiB)
23:15:36,126 WARNING Free space 4.49 GB below half the 10.00 GB floor: ending the recording
23:15:36,868 INFO Recording saved: 322750B-nano-super-20260923T231532.sc16 (40768000 bytes, 4.7s, ...)

  "duration_sec": 5.096, "total_bytes": 40768000, "total_samples": 10192000,
  "stopped_reason": "disk_floor", "write_failed": false,
```

The guard fired 0.65 s after the filler grew, before any governor tick. Health on this restart
still showed the flag from the churn run on the same DB:
`"step":0,...,"degraded_since":"2026-09-23T23:14:17.706100+00:00"`.

### 4.9 Ladder (floor 10 GB, STORAGE_CHECK_SEC=2)

Step 0, then step 1 (filler to 9 GiB at 23:18:42.59, 2.6 GB of `auto/` captures, 34 files):

```
23:18:46 {"status":"ok",...,"free_gb":17.4,"floor_gb":10.0,...,"auto_gb":2.6,...,"step":0,"step_text":"healthy",...}
23:18:48 {"status":"ok",...,"free_gb":9.0,"floor_gb":10.0,...,"auto_gb":2.6,...,"step":1,"step_text":"evicting the oldest automatic captures","step_since":"2026-09-23T23:18:46.737769+00:00",...} REC {"state":"recording","file":"...231846.sc16",...}
23:18:50 {"status":"ok",...,"free_gb":11.4,"floor_gb":10.0,...,"auto_gb":0.1,...,"step":1,...} REC {"state":"finalizing","file":"...231846.sc16",...}

23:18:46,949 INFO Rotated old capture: ...231649.sc16 (freed 80108785 bytes)
... 24 more, oldest first, through ...231834.sc16
23:18:48,174 WARNING Storage floor: evicted 2.5 GB of automatic captures
```

The capture being recorded (`231846`) was not in the eviction list; it was evicted on the next
tick after it finished (see Findings, item 3). Step stayed 1 while free hovered at 11.4 to
11.5 GB, each new capture evicted about one tick after it was saved.

Ladder B (restart, no trigger, filler to 7 GiB at 23:19:25):

```
23:19:28,980 WARNING Storage step 0 -> 1 (evicting the oldest automatic captures): 7.0 GB free, floor 10.0 GB
23:19:29,039 WARNING Storage floor: evicted 0.1 GB of automatic captures
23:19:31,044 WARNING Storage step 1 -> 2 (pruning PSD history and detections): 7.1 GB free, floor 10.0 GB
23:19:31,530 INFO Pruned PSD blobs for 5000 avg windows (cutoff: 2026-09-16T23:19:31.045431)
23:19:31,660 INFO Retention: deleted 5000 detections rows older than 90 days
23:19:33,225 WARNING Storage step 2 -> 3 (recordings refused): 7.0 GB free, floor 10.0 GB

23:19:29 {"status":"ok","uptime_sec":1.0,"storage":{"free_gb":7.0,"floor_gb":10.0,"volume_gb":467.0,"db_gb":0.1,"db_reusable_gb":0.0,"auto_gb":0.1,"manual_gb":0.0,"step":1,"step_text":"evicting the oldest automatic captures","step_since":"2026-09-23T23:19:28.980135+00:00","last_write_error":null,"degraded_since":null,"db_volume":null}}
23:19:32 {"status":"ok","uptime_sec":3.0,"storage":{"free_gb":7.1,"floor_gb":10.0,"volume_gb":467.0,"db_gb":0.1,"db_reusable_gb":0.0,"auto_gb":0.0,"manual_gb":0.0,"step":2,"step_text":"pruning PSD history and detections","step_since":"2026-09-23T23:19:31.044666+00:00","last_write_error":null,"degraded_since":null,"db_volume":null}}
23:19:34 {"status":"degraded","uptime_sec":5.2,"storage":{"free_gb":7.0,"floor_gb":10.0,"volume_gb":467.0,"db_gb":0.1,"db_reusable_gb":0.0,"auto_gb":0.0,"manual_gb":0.0,"step":3,"step_text":"recordings refused","step_since":"2026-09-23T23:19:33.225418+00:00","last_write_error":null,"degraded_since":"2026-09-23T23:19:33.225418+00:00","db_volume":null}}

POST /api/recording/start -> HTTP 409 {"detail":"Recording refused: free space 7.0 GB is below the 10.0 GB floor (storage step 3, recordings refused)"}
```

Step 3 UI, from headless Chrome:

```
#storage-banner (class "storage-banner storage-banner-critical", visible):
  Storage
  Step 3: recordings refused. 7 GB free, minimum 10 GB. A storage problem occurred at 9/23/2026, 5:19:33 PM.
  Clear Warning
config page, Volume bar caption:
  Manual 0 GB · Automatic 0 GB · Database 0.1 GB · Other 459.9 GB · Free 7 of 467 GB · Minimum free 10 GB · Step 3: recordings refused
```

Step 4 (filler to 4 GiB at 23:21:47.07):

```
23:21:47,812 WARNING Storage step 3 -> 4 (PSD history writes stopped): 4.0 GB free, floor 10.0 GB
avg_windows per 10 s (start, rows, psd NULL, psd present):
('2026-09-23T23:21:3', 12, 0, 12)  ('2026-09-23T23:21:4', 12, 2, 10)  ('2026-09-23T23:21:5', 12, 12, 0)
('2026-09-23T23:22:0', 12, 12, 0)  ('2026-09-23T23:22:1', 11, 4, 7)   ('2026-09-23T23:22:2', 1, 0, 1)
```

Recovery (filler removed at 23:22:06.67; ticks every 2 s):

```
23:22:08 {"status":"degraded",...,"free_gb":4.0,...,"step":4,...}
23:22:10 {"status":"degraded",...,"free_gb":403.4,...,"step":4,"step_since":"2026-09-23T23:21:47.812717+00:00",...,"degraded_since":"2026-09-23T23:19:33.225418+00:00"}
23:22:12 {"status":"degraded",...,"free_gb":403.4,...,"step":4,...}
23:22:13,719 INFO Storage step 4 -> 0 (healthy): 403.4 GB free, floor 10.0 GB
23:22:15 {"status":"degraded",...,"free_gb":403.4,...,"step":0,"step_text":"healthy","step_since":"2026-09-23T23:22:13.719092+00:00",...,"degraded_since":"2026-09-23T23:19:33.225418+00:00"}
```

A manual recording then started (HTTP 200, `stopped_reason: "manual"`), and after the clear:
`{"status":"ok",...,"step":0,...,"degraded_since":null}`.

## 5. Measured and REJECTED (do not retry)

- **Chunk 5000 (the pre-measurement default), 2000 and 1000.** Detections p99 1480 / 672 / 529 ms;
  blob null p99 1009 / 614 / 198 ms. All over the 100 ms budget, 5000 and 2000 over the ~300 ms
  drop line.
- **Chunk 500.** Detections p99 182.8 ms (blob null 54.5 ms is fine). Over budget.
- **Chunk 10000 for anything.** Detections p99 2.8 s, blob null 2.0 s.
- **"No dropped chunks in the log" as the pass criterion.** `dropped=0` held through a pass in
  which the writer stalled for 22 s (4.3). The drop counter is on the receive side; a stalled DB
  writer shows as beacon age and as gaps in `avg_windows.start_time`, not as drops.
- **Chunk size as the fix for the stalls.** The stalls happen at chunk 250 with statements that
  take 15 ms p50 standalone; they are 10 to 30 s device waits inside a `commit`, not statement
  cost. A smaller chunk would not remove them.

## 6. Measurement traps hit

- **Mock pacing.** The mock receiver produces a chunk every ~60 ms against 36.6 ms of IQ, so it
  runs slower than real time and its `dropped=` counter has slack a real 56 Msps stream does not.
  Mock drop counts understate starvation.
- **pgrep matching its own ssh command line.** Wait loops like
  `ssh box 'pgrep -f seed_db.py'` matched the remote `bash -c` carrying that string, so they never
  saw the process exit. Use `pgrep -f "[s]eed_db"`.
- **The prune watermark.** `_blob_prune_mark` is per process. Timing runs had to preset it to the
  first remaining blob, or every run would re-scan 20 M stats-only rows first. The full pass did
  not preset it, which is how its 65 min scan was found.
- **Cold vs warm.** All timing runs dropped the page cache first; this is the worst case. The first
  `secure_delete=OFF` run (12 s) followed other runs on warm pages; the controlled A/B pair
  (25 s vs 56 s) both started cold.
- **GB is GiB.** `/api/health` and `DISK_MIN_FREE_GB` use 1024**3; `df -h` shows 430 G "Avail"
  where health shows 403.6 GB.
- **tmpfs fill rate.** The brief expected 256 MB in about 30 s of IQ at 2 Msps; the `.psd` grid
  sidecar is the same byte rate as the IQ at 2048 bins, so the volume filled in 16 s.
- **Evidence evicted.** In the churn run the `disk_floor` captures were evicted by step 1 at the
  same tick, taking their `.json` with them; the `disk_floor` `.json` in 4.8 came from a separate
  manual run.
- **Recovery visible only in the log.** Health shows `step` and `step_since`, not the good-tick
  count; the 3-tick hysteresis is read from the log timestamps.

## 7. Findings (not fixed here; for the controller to route)

1. **Blob nulling stalls the writer 10 to 30 s (4 of 4 runs with the pipeline running).** The
   prune's `commit` blocks while the pipeline's inserts queue behind it; 3 to 4 of these per
   100 K blobs, `_guarded_write` abandoning the connection at 30 s, `insert_detections failed ...
   database is locked`, and 10 to 22 s gaps in `avg_windows`. Correlated with dmesg
   `nvme nvme0: I/O ... timeout, completion polled` (the known lost-completion hiccup in this
   module's docstring). This Ubuntu SQLite has `SECURE_DELETE` compiled in, so every freed blob
   page is also overwritten with zeros; with `PRAGMA secure_delete=OFF` on the writer the same
   work ran with no operation over 200 ms (2 of 2). Candidate mitigation: set
   `secure_delete=OFF` in `_configure_writer`. The field sensor at step 2 (pressure prune of
   about 23 days, about 4 M blobs) would hit this at scale.
2. **Every process start re-walks all stats-only history.** The blob-prune watermark is in memory
   only, so the first pass after a start scans every `avg_windows` row older than the cutoff at
   250 per statement plus a 50 ms pause: 65.5 min for 20 M rows here, about 7 h for the field's
   126 M rows at two years. While that pass runs the cleanup loop cannot start a step 2 pressure
   pass, since the wake is only checked between passes.
3. **Step 1 evicts captures while free is above the floor, and in steady state evicts each new
   capture.** Eviction targets floor x 1.15, so after the first eviction a sensor with continuous
   triggering stays at step 1 with free between 10 and 11.5 GB and deletes every capture about one
   tick after it is saved. The ladder never escalates (there is always an evictable capture), so
   nothing is refused and nothing warns beyond "step 1".
4. **Churn after `disk_floor` (the review question): yes.** With continuous triggering, 5 captures
   of 0.2 to 0.9 s were started in the 4.1 s before the governor tick (10 s tick). Each was ended
   by the writer's guard and saved. Two of them got the same name
   (`322750B-nano-super-20260923T231415.sc16`, started 405 ms apart), so the first was
   overwritten. The start path consults only the governor's step, not the writer's
   `_disk_floor_hit` or a fresh free-space sample.
5. **On a truly full volume the capture `.json` is 0 bytes.** The spec expects the `.json` to carry
   `write_failed`, `write_error`, `stopped_reason: "write_error"`; with nothing freed after ENOSPC
   it cannot be written, and the open truncates it to 0 bytes (same for `.psd.json` and
   `.detections.json`). The facts survive only in `iq_captures` (file-derived counts) and in
   `last_write_error`. The captures list tolerates it (`"meta":null`).
6. **`last_write_error` does not survive a restart.** Only `degraded_since` is persisted; after a
   restart health shows the flag with `last_write_error: null`, so the reason is lost.
7. **Orphaned `.detections.json` after eviction.** The sidecar is written 3 s after the capture;
   a capture evicted inside that window leaves its sidecar behind in `auto/` (4.7, and 7 more in
   ladder A).
8. **Stale `refused` in `/api/recording/status`.** After recovery to step 0 the status still read
   `"refused":"Recording refused: free space 7.0 GB ... (storage step 3 ...)"` until the next start
   attempt cleared it.
9. **Stale config comment.** `DB_CLEANUP_INTERVAL_SEC` says "0 disables the retention loop", but
   the loop now always runs (`app.py`: "Retention always runs"); at 0 it would re-run retention
   back to back.

## 8. Open, not yet answered

- Is Finding 1 specific to this NVMe and its interrupt setup (`nvme.use_threaded_interrupts=1` is
  on the kernel command line), or does the field sensor's `/mnt/ssd` NVMe stall the same way?
  Not tested on the field sensor.
- Does `secure_delete=OFF` remove the stalls at field scale (millions of blobs) and at 56 Msps
  real load? Only 49 K-blob runs on the mock pipeline were measured.
- Timing at MAXN (25 W) and at the full two-year row count (126 M `avg_windows`); both would move
  the numbers, likely in opposite directions.
- The field `DB_PATH` placement (same device as `STORAGE_PATH` or not). The separate-device path
  (`db_volume`) was exercised only incidentally (tmpfs runs, DB on the NVMe), with a 0.001 GB floor
  and the 23.3 GB auto floor on the DB volume.
- The field burst threshold: at a noisy threshold detections dominate DB growth, and they are
  also the slowest rows to delete (about 4,300 rows/s here), so the first retention pass on such a
  DB is long.
- Whether the real 56 Msps pipeline (not the mock) drops chunks during a 250-row retention pass.

## 9. CORRECTION / follow-up 2026-09-23: fixes for the section 7 findings

Section 7 is left as written. Fixed on `feat/storage-budgeting` in commits c6691b9 (1, 2),
fd44666 (4) and bf80c06 (5 to 9). Each fix has a unit test that failed before it. None of these
were re-measured on nano-super; the numbers in sections 4 and 7 are from before the fixes.

1. **Fixed (mitigation as proposed).** `_configure_writer` sets `PRAGMA secure_delete=OFF` on the
   writer connection, on connect and on the `_reconnect` path (same function). The web layer's
   read-only connection is unchanged. The stall itself is not re-measured at field scale (see
   section 8).
2. **Fixed.** The blob-prune watermark is persisted in `config` under `blob_prune_mark` as JSON
   `[start_time, rowid]`: loaded once per process at the first `prune_avg_psd_blobs`, saved every
   40 chunks (10,000 rows) and at the end of each pass, each save its own `set_config` statement.
   A missing or garbled value falls back to `("", 0)`, a full scan. The mark still advances only
   past committed, nulled rows.
3. **Not fixed; parked for the user** (eviction target and steady-state step 1).
4. **Fixed.** After a recording ends with `disk_floor` or `write_error`, starts (manual, arm,
   trigger fire, continuous re-arm fire) are refused with "Recording held: the last capture
   stopped for <reason>; waiting for the next storage check" until `StorageGovernor.ticks`
   (completed ticks, new) moves past its value at the end of that finalize. Capture names that
   already exist in `auto/` or `manual/` get `-2`, `-3`, ... before `.sc16`;
   `is_active_capture` and the captures route's `_drop` stem match both work with the suffix.
   Known limit: the tick counter increments in `tick()`, after the sample is taken, so a tick
   whose sample was taken just before the stop can release the hold. That still bounds churn to
   at most one capture per storage tick instead of several per second.
5. **Fixed.** The capture `.json` and the `.psd.json` are written to `<name>.tmp` and renamed;
   on OSError the tmp is removed, so a full disk leaves no `.json` at all rather than a 0-byte
   one. The facts stay in `iq_captures` and `last_write_error`, as before. The
   `.detections.json` sidecar (storage/detections_sidecar.py) was not changed.
6. **Fixed.** `last_write_error` is persisted under `storage_last_write_error` (JSON
   `{"at", "error"}` or ""), written by the storage loop with `storage_degraded` whenever either
   changes (every new write error now marks it), restored at startup, and cleared by
   `POST /api/storage/clear-degraded`.
7. **Fixed.** The deferred detections sidecar is skipped (debug log) when the `.sc16` no longer
   exists after the grace delay.
8. **Fixed.** `recording_status()["refused"]` is the refusal in force now; `_last_refusal` only
   de-duplicates the log line.
9. **Fixed.** The config comment now says the loop always runs, and `_cleanup_loop` clamps the
   interval to at least 60 s.

Open after these fixes: whether `secure_delete=OFF` removes the stalls at field scale and on the
field NVMe (unchanged from section 8), and a hardware re-run of the section 4.7 churn probe with
the hold in place.

### CORRECTION 2026-09-23 (to section 9, item 4)

Item 4's "Known limit" misstated the churn bound as "at most one capture per storage tick". The
hold as first committed (fd44666) released on the first tick to complete after the stop, and that
tick could have taken its sample before the stop, so one extra churned capture could still start
per `disk_floor` or `write_error` event. The hold now lasts while `governor.ticks <= hold + 1` and
releases at `ticks >= hold + 2`. Ticks run one at a time from one loop, so the second tick to
complete after the stop must have begun after it: zero extra captures before a fresh storage
check. Covered by `test_a_tick_in_flight_at_the_stop_does_not_release_the_hold`. The hold is no
longer cleared on read (that raced the recording-control thread); the comparison alone decides.
Also since then: a failed save of `blob_prune_mark` is logged and the prune pass continues (the
stored mark stays older but correct).

### FOLLOW-UP 2026-09-23 (final branch review)

Two further defects were found by the final whole-branch code review, not on hardware. Both are
fixed in code with unit tests; neither has been re-run on hardware yet.

- **I1: the storage tick could evict the capture being recorded.** `_storage_tick` took the
  active-capture snapshot once, then awaited `db.file_stats()` (which can queue 10-30 s behind the
  writer) and the threaded sample before `evict_until_free` re-globbed `auto/`. A capture begun in
  that window was neither excluded nor counted as non-evictable. Now `evict_until_free` takes
  `exclude_fn` (re-evaluated right before each delete) and `not_after` (the tick's start time);
  any `auto/` `.sc16` with mtime at or after `not_after` is neither evicted nor counted evictable
  by `sample()`. Also `_delete_capture` sizes with `_size_or_zero`, so a concurrent delete by
  `enforce_cap` cannot abort eviction.
- **I2: an abandoned writer could corrupt or falsely fail the next recording.** Finalize gives
  the disk writer 10 s, then drops its handle; all recordings shared one queue and one
  `_writer_error`, so a still-blocked old writer could consume the next recording's chunks and
  sentinel and flag it as failed. Each recording now gets its own queue and a generation number;
  the writer reads only its queue, sets `_writer_error` / `_disk_floor_hit` only while its
  generation is current, and exits once superseded and drained.
- Minors in the same wave: a RAM-mode flush failure now applies the start hold; a failing
  `file_stats` no longer skips the tick (sampled as 0 bytes); a failed persist of the degraded
  pair is retried on the next tick, persists are serialized by a lock so an older write cannot
  land after a newer one, and the clear route writes the governor's values after the clear
  instead of hard-coded blanks.

Open: a hardware re-run with a hung or slow storage volume (I2), and a tick under a slow DB with
continuous triggering (I1).

## 10. Hardware re-verification (2026-09-24)

### 10.1 The question

Date: 2026-09-24. Build: `feat/storage-budgeting` at `fd4094d` ("fix(storage): young-eviction
text only overrides step 1"), run from the worktree `~/rfobs-storage` (clean, `rfobserver.__file__`
= `/home/ocollaco/rfobs-storage/src/rfobserver/__init__.py` in every run log). Same box and SDR as
section 1: nano-super, 15 W profile, SQLite 3.37.2, B200mini `322750B`. Scratch dir `~/rfobs-val2/`
(now deleted).

Do the fixes from section 9 and the FOLLOW-UP (F1 `secure_delete=OFF`, F2 persisted prune
watermark, F4 start hold and `-2` names, I1 live-capture protection, I2 per-recording writer
queues) and the new step 1 young-eviction warning behave on the hardware as they do in the unit
tests; and does the section 4.6 ENOSPC path still behave?

### 10.2 The answer

All seven probes pass. F1 is confirmed with a control: forcing `secure_delete=ON` back on the
writer reproduced the 30 s stalls in 2 of 2 runs, the shipped `OFF` ran 2 of 2 with no operation
over 200 ms. F2 resumed a killed pass exactly at the saved mark and a restart after a completed
pass scanned 0 rows. F4 started zero captures between the `disk_floor` stop and the storage tick,
and a same-second manual start got `-2`. I1 held for 10 min of steady-state step 1 plus 6 min with
the tick's `file_stats` delayed 4 s (0 active captures evicted in 264 rotations). I2 was reproduced
with a FIFO and the next recording was unaffected. The young-eviction warning turns on, counts,
leaves `status` "ok", and clears. Four new gaps are under 10.6 Findings; the main one is that the
section 7 item 7 fix (orphaned `.detections.json`) still leaves orphans through a check-then-write
race (7 orphans in about 270 evictions).

### 10.3 Procedure (probes in the order run)

Every server ran through a launcher (`launch.py`) that only instruments: it logs
`PRAGMA secure_delete` as read back on every writer connection (connect and `_reconnect`), every
aiosqlite operation over 200 ms (`VALTRACE slow ...`, as in 4.4), and per blob-prune pass the
loaded mark, chunks, rows scanned, nulled and wall time. Two opt-in switches were used only where
stated: `VAL_SECURE_DELETE_ON=1` (control runs: re-issues `PRAGMA secure_delete=ON` after
`_configure_writer`) and a file `~/rfobs-val2/slow_file_stats` holding N (delays
`SensorDatabase.file_stats` by N s), plus `VAL_YOUNG_WINDOW_SEC` (overrides the module constant
`governor.YOUNG_EVICTION_WINDOW_SEC`). No product file was edited. `RFOBS_WEB_PORT=8888
RFOBS_SENSOR_ACTIVE=true RFOBS_PEAKS_ROLLUP_INTERVAL_SEC=0` throughout; stopped with
`fuser -k 8888/tcp`.

| # | Probe | What it isolates / controls for |
|---|---|---|
| 1 | Seed 2,000,000 stats-only `avg_windows` rows (700 to 30 days old) then 100,000 rows with 8192 B blobs (20 to 8 days old); 1.32 GB. Copy kept as the A/B source | A first pass that must walk 2 M rows before nulling 100 K blobs |
| 2 | Mock pipeline on it, cold cache; poll `config.blob_prune_mark` every 1 s; `fuser -k` (SIGKILL) once the mark passes rowid 1,000,000 | F2: a kill mid-pass, then what the restart loads |
| 3 | Restart on the same DB, cold; let the pass finish; `/api/health` every 5 s from the workstation; afterwards gaps between consecutive `avg_windows.start_time` of rows written during the pass | F2 resume; F1 with the shipped code on a full pass |
| 4 | Restart again after the completed pass | F2: no rescan |
| 5 | A/B x2 on fresh copies, mark preset to the last stats-only row so the pass only nulls the 100 K blobs; order ON, OFF, OFF, ON; `drop_caches` before each | F1: whether the stall follows `secure_delete`, on the same day, same device, same data (the control is what makes "no stall" mean something) |
| 6 | B200mini 2 Msps, continuous trigger (-300 dB), `RECORDING_MAX_SEC=10`, floor 10 GB, default `STORAGE_CHECK_SEC=10`; mid-recording `fallocate` a filler leaving 4 GiB; watch status every 0.1 s | F4 hold: captures between the writer's `disk_floor` stop and the tick |
| 7 | Same DB, no trigger; POST start, stop, wait for idle, start, timed to begin right after a second boundary; up to 5 tries | F4 unique names |
| 8 | B200mini 4 Msps, continuous trigger, `RECORDING_MAX_SEC=2`, floor 10 GB, `STORAGE_CHECK_SEC=2`; 2.6 GB of `auto/` captures, then filler to 9 GiB free; 10 min steady state; health every 2 s; screenshots | I1 and the young-eviction warning in the section 7 item 3 steady state |
| 9 | Same run, `slow_file_stats` = 4 for 6 min | I1 proper: a capture starting between the tick's active snapshot and its eviction |
| 10 | Restart with `VAL_YOUNG_WINDOW_SEC=120`; wait for a young eviction; remove the filler | The INFO clear line without waiting 30 min |
| 11 | B200mini 2 Msps, no trigger; pre-create FIFOs named `<stem>.psd` in `manual/` for the next 6 s, POST start (A), remove the unused FIFOs, stop A after 3 s, start B, open a reader on A's FIFO 3 s into B, stop B | I2: A's writer blocks in `open(<A>.psd)` (the writer opens the `.sc16`, then the grid); finalize abandons it; B starts while it is still blocked; then both writers run at once |
| 12 | 256 MB tmpfs as `STORAGE_PATH` (DB on the NVMe), 2 Msps, `DISK_MIN_FREE_GB=0.001`, `RECORDING_MAX_SEC=0`, `STORAGE_CHECK_SEC=2`, manual start; then restart on the same DB | Section 4.6 regression: real ENOSPC |

Screenshots (headless Chrome via puppeteer, session scratchpad): `young-dashboard.png`,
`young-config.png`.

### 10.4 Evidence

**F1, runtime pragma** (every writer connection in every run, including after `_reconnect`):

```
14:13:01,689 valtrace WARNING VALTRACE writer connection configured: PRAGMA secure_delete=0 (/home/ocollaco/rfobs-val2/p12/p12.db)
```

**F1, A/B on the blob phase** (100,000 blobs, pipeline running, cold, chunk 250):

| Run | Writer | Pass wall time | Ops over 200 ms | `_guarded_write` abandons | insert failures | Max `avg_windows` gap | Max beacon age | nvme timeouts in dmesg |
|---|---|---|---|---|---|---|---|---|
| abon | `secure_delete=1` (control) | 129.1 s | 24, max 29,991 ms | 2 | 2 x `insert_detections failed for 68 bursts` | 32.2 s | 30.8 s | 14:22:25, 14:23:05 |
| aboff | `0` (shipped) | 24.0 s | 0 | 0 | 0 | 0.719 s | 1.0 s or less | none |
| off2 | `0` (shipped) | 24.1 s | 0 | 0 | 0 | 0.708 s | 1.0 s or less | none |
| on2 | `secure_delete=1` (control) | 72.0 s | 12, max 30,001 ms | 2 (`_prune_blob_chunk`, `insert_detections`) | `avg-window persist failed (chunk #127)` | 33.1 s | 26.6 s | 14:26:51 |

```
abon:
14:22:52,173 VALTRACE slow _execute(commit) 29991 ms:
14:22:52,174 rfobserver.storage.database ERROR DB write _prune_blob_chunk stuck >30s -- abandoning connection
14:22:52,177 VALTRACE writer connection configured: PRAGMA secure_delete=1 (/home/ocollaco/rfobs-val2/p12/abon.db)
14:22:52,342 VALTRACE slow _execute(execute) 29523 ms: INSERT INTO avg_windows
14:22:54,351 rfobserver.pipeline.streaming ERROR insert_detections failed for 68 bursts; skipping
14:24:07,222 VALTRACE prune pass end: chunks=401 scanned=100000 nulled=99500 wall=129.1s
aboff:
14:24:42,140 VALTRACE writer connection configured: PRAGMA secure_delete=0 (/home/ocollaco/rfobs-val2/p12/aboff.db)
14:25:06,745 VALTRACE prune pass end: chunks=401 scanned=100000 nulled=100000 wall=24.0s
dmesg:
[Thu Sep 24 14:22:25 2026] nvme nvme0: I/O 762 QID 5 timeout, completion polled
[Thu Sep 24 14:23:05 2026] nvme nvme0: I/O 89 QID 6 timeout, completion polled
[Thu Sep 24 14:26:51 2026] nvme nvme0: I/O 16 QID 5 timeout, completion polled
```

(Product log lines with an em-dash are rendered `--` here.) The full first pass with the shipped
code (runs 1 and 2 below, 2.1 M rows scanned, 100 K nulled) also had 0 operations over 200 ms,
beacon age never above 1.0 s, and `avg_windows` gaps of median 0.575 s, max 0.729 s.

**F2, kill mid-pass and resume:**

```
run1 14:13:02,388 VALTRACE prune pass start: mark=('', 0) cutoff=2026-09-17T14:13:02.388185
     14:13:22 blob_prune_mark = ["2024-11-23T17:47:47.095104+00:00", 90000]
kill 14:16:39.36 blob_prune_mark = ["2025-09-24T14:11:47.095104+00:00", 1000000] (updated_at 14:16:38)
     fuser -k 8888/tcp
run2 14:16:48,488 VALTRACE prune pass start: mark=('2025-09-24T14:11:47.095104+00:00', 1000000)
     14:20:48,033 VALTRACE prune pass end: chunks=4401 scanned=1100000 nulled=100000 wall=239.5s mark=('2026-09-16T14:12:05.671104+00:00', 2100000)
     14:20:48,033 INFO Pruned PSD blobs for 100000 avg windows (cutoff: 2026-09-17T14:16:48.488026)
run3 14:21:19,643 VALTRACE prune pass start: mark=('2026-09-16T14:12:05.671104+00:00', 2100000)
     14:21:19,646 VALTRACE prune pass end: chunks=1 scanned=0 nulled=0 wall=0.0s
```

The restart resumed exactly at the last saved mark (saves every 40 chunks; the kill landed on a
save, so nothing was rescanned; in general up to 10,000 rows would be). About 4,600 rows/s scan
rate, so the full 2.1 M pass would have been about 7.7 min; the post-completion restart took 3 ms.

**F4, hold after `disk_floor`** (filler `fallocate`d 14:29:20.602 to 14:29:21.498, 4.0 GiB left):

```
14:29:22,179 WARNING Free space 3.98 GB below half the 10.00 GB floor: ending the recording
14:29:23,022 INFO Recording saved: ...142916.sc16 (57152000 bytes, 6.8s, ...)
14:29:23,406 WARNING Recording held: the last capture stopped for disk_floor; waiting for the next storage check
14:29:25,999 WARNING Storage step 0 -> 4 (PSD history writes stopped): 4.0 GB free, floor 10.0 GB
14:29:26,094 WARNING Recording refused: free space 4.0 GB is below the 10.0 GB floor (storage step 4, ...)
status (0.1 s poll):
14:29:22.428 {"state": "finalizing", "file": "...142916.sc16", "bytes": 57152000, "refused": null}
14:29:23.351 {"state": "idle", ..., "refused": "Recording held: the last capture stopped for disk_floor; waiting for the next storage check"}
14:29:23.675 {"state": "armed", ..., "refused": "Recording held: ..."}
```

Zero captures started in the 3.8 s between the stop and the tick (section 4.7: five). After the
filler was removed (14:30:11.9) the step went 4 -> 0 at 14:30:40,285 and the next capture started
at 14:30:40,527. No duplicate name in any `Recording started` line of the run.

**F4, same-second name** (5th try):

```
14:32:06,054 INFO Recording started (disk): 322750B-nano-super-20260924T143206.sc16
14:32:06,473 INFO Recording saved: 322750B-nano-super-20260924T143206.sc16 (9638400 bytes, 0.3s, ...)
14:32:07,047 INFO Recording started (disk): 322750B-nano-super-20260924T143206-2.sc16
14:32:08,700 INFO Recording saved: 322750B-nano-super-20260924T143206-2.sc16 (14553600 bytes, 1.6s, ...)
iq_captures:
(15, '322750B-nano-super-20260924T143206.sc16', 'manual', '2026-09-24T14:32:05.024703+00:00', 1.205, 2409600)
(16, '322750B-nano-super-20260924T143206-2.sc16', 'manual', '2026-09-24T14:32:05.988696+00:00', 1.819, 3638400)
```

Both keep their own `.json` (`total_bytes` 9638400 and 14553600), `.psd`, `.psd.json` and
`.detections.json`.

**I1, steady state** (filler to 9 GiB at 14:35:10.5; 2.6 GB, 34 captures in `auto/`):

```
14:35:12,955 WARNING Storage step 0 -> 1 (evicting the oldest automatic captures): 9.0 GB free, floor 10.0 GB
14:35:14,875 WARNING Storage floor: evicted 2.5 GB of automatic captures
```

Checker over the log (a rotation of a name that had `Recording started` and no `Recording saved`
yet counts as evicting the active capture) and over `auto/` at the end:

```
10 min steady state (14:35:10 to 14:45:20):
starts=151 saved=151 rotations=183 evicted_active=0 []
zero_byte_sc16=[] orphan_json=[] orphan_detections=6
error lines=0
file_stats delayed 4 s (14:45:41 to 14:51:31):
starts=81 saved=81 rotations=81 evicted_active=0 []
zero_byte_sc16=[] orphan_json=[] orphan_detections=6 (the same six)
error lines=0
delayed ticks 53; ticks with a capture started inside the delayed window 51
```

One of those ticks, traced:

```
14:45:55,595 VALTRACE file_stats delayed 4.0 s                       <- tick starts; snapshot empty of 144556
14:45:56,628 Recording started (disk): ...144556.sc16
14:45:59,566 Recording saved: ...144556.sc16
14:45:59,596 Recording started (disk): ...144559.sc16                   <- active at eviction
14:45:59,669 Rotated old capture: ...144548.sc16                       <- the only eviction this tick
14:46:05,814 Rotated old capture: ...144556.sc16                       <- next tick
14:46:12,723 Rotated old capture: ...144559.sc16                       <- after it was saved at 14:46:04,847
```

`144556` (saved but mtime after the tick start) was kept by `not_after`, `144559` (begun after the
snapshot, recording) by `exclude_fn`/`not_after`. No writer errors, no 0-byte `.sc16`, no `.json`
without its `.sc16`.

**Young-eviction warning** (same run):

```
14:35:14,900 WARNING Storage floor: evicted 33 automatic captures within 10 minutes of recording (...143251.sc16 139 s, ..., ...143505.sc16 5 s); free 9.0 GB, floor 10.0 GB
14:35:14,906 rfobserver.storage.governor WARNING Storage floor: evicting automatic captures within 600 s of recording (youngest 5 s); this warning stays up for 30 min
14:45:12,192 WARNING Storage floor: evicted 1 automatic captures within 10 minutes of recording (...144507.sc16 2 s); free 11.4 GB, floor 10.0 GB
```

150 per-check WARNING lines in the 10 min; the turn-on WARNING once. Health (349 samples at 2 s):
293 `("ok", step 1, evicting_young true, degraded_since null, "deleting automatic captures minutes
after they are recorded")`, 1 `("ok", step 1, false, null, "evicting the oldest automatic
captures")` (the tick before the first eviction finished), 55 at step 0 before the filler; never
`degraded`; max beacon age 1.9 s. `young_evictions` 36 at 14:35:24, 101 at 14:39:34, 184 at
14:45:26. One verbatim sample:

```
14:44:01 {"status":"ok",...,"storage":{"free_gb":11.4,"floor_gb":10.0,"volume_gb":467.0,"db_gb":0.0,"db_reusable_gb":0.0,"auto_gb":0.2,"manual_gb":0.0,"step":1,"step_text":"deleting automatic captures minutes after they are recorded","step_since":"2026-09-24T14:35:12.954924+00:00","last_write_error":null,"degraded_since":null,"db_volume":null,"evicting_young":true,"young_evictions":163,"last_young_eviction":"2026-09-24T14:44:01.003145+00:00"}}
```

UI, from headless Chrome:

```
/live/ #storage-banner (class "storage-banner", visible):
  Storage
  Step 1: deleting automatic captures minutes after they are recorded. 11.4 GB free, minimum 10 GB. Automatic captures are being deleted within minutes of recording (39 in the last 30 minutes). Free up space or lower Archive Max to keep them.
/config #storage-legend:
  Manual 0 GB · Automatic 0.1 GB · Database 0 GB · Other 455.5 GB · Free 11.4 of 467 GB · Minimum free 10 GB · Step 1: deleting automatic captures minutes after they are recorded · Deleting captures soon after recording
```

Clear, with the window overridden to 120 s (filler removed 14:53:34):

```
VALTRACE YOUNG_EVICTION_WINDOW_SEC overridden to 120
14:53:34,124 rfobserver.storage.governor WARNING Storage floor: evicting automatic captures within 600 s of recording (youngest 4 s); this warning stays up for 2 min
14:53:44,129 INFO Storage step 1 -> 0 (healthy): 427.8 GB free, floor 10.0 GB
14:55:35,378 rfobserver.storage.governor INFO Storage: no young evictions in 2 min; evicting_young warning cleared
health: {"status":"ok",...,"step":0,"step_text":"healthy",...,"degraded_since":null,...,"evicting_young":false,"young_evictions":19,"last_young_eviction":"2026-09-24T14:53:34.124259+00:00"}
```

The clear came 121.3 s after the last young eviction (first tick past the window). With the
shipped 1800 s constant the clear was not observed on hardware.

**I2, abandoned writer:**

```
14:56:18,350 INFO Recording started (disk): 322750B-nano-super-20260924T145618.sc16      (A; <A>.psd is a FIFO)
14:56:32,398 ERROR Recording writer did not exit; abandoning writer thread
14:56:32,404 WARNING IQ bytes on disk (0) differ from bytes queued (34214400); reporting the file
14:56:32,422 WARNING PSD grid rows on disk (0) differ from rows queued (4176); reporting the file
14:56:32,509 INFO Recording saved: 322750B-nano-super-20260924T145618.sc16 (0 bytes, 4.1s, ...)
14:56:32,877 WARNING A previous recording's writer is still blocked; the new recording uses its own queue and writer
14:56:32,923 INFO Recording started (disk): 322750B-nano-super-20260924T145632.sc16      (B)
14:56:35.976 reader opened on A's FIFO (A's writer unblocks while B records)
14:56:41,132 INFO Recording saved: 322750B-nano-super-20260924T145632.sc16 (66982400 bytes, 8.1s, 0 dropped, 0 grid rows dropped, ...)

B .json: duration_sec 8.373, total_bytes 66982400, total_samples 16745600, stopped_reason manual, write_failed false, write_error None
B .psd 66977792 bytes = 8176 rows x 2048 bins x 4 B (matches "PSD data saved ... (8176 rows)")
A .sc16 afterwards: 34214400 bytes (= A's bytes queued); A's grid drained through the FIFO: 34209792 bytes = 4176 rows
"Recording write failed" lines: 0
```

A's writer wrote exactly A's queued data and none of B's; B finished clean. Stop A took 11.2 s
(the 10 s join timeout).

**Regression, real ENOSPC on tmpfs:**

```
14:57:27,442 INFO Recording started (disk): 322750B-nano-super-20260924T145727.sc16
14:57:43,918 ERROR Recording write failed (ENOSPC: No space left on device); ending the recording
14:57:44,494 ERROR Write failed: 322750B-nano-super-20260924T145727.sc16: ENOSPC: No space left on device
14:57:44,570 ERROR Write failed: 322750B-nano-super-20260924T145727.psd.json: ENOSPC: No space left on device
14:57:44,624 ERROR Write failed: 322750B-nano-super-20260924T145727.sc16 metadata .json: ENOSPC: No space left on device
14:57:44,646 INFO Recording saved: ...145727.sc16 (135921664 bytes, 17.1s, 0 dropped, 0 grid rows dropped, 13 overflow gaps (13 samples lost))
14:57:44,781 WARNING Storage step 0 -> 4 (PSD history writes stopped): 0.0 GB free, floor 0.0 GB
14:57:47,889 ERROR Detections sidecar write failed for 322750B-nano-super-20260924T145727.sc16

manual/: 0 322750B-nano-super-20260924T145727.detections.json
         132513792 ...145727.psd
         135921664 ...145727.sc16        (no .json, no .psd.json)
iq_captures: ('322750B-nano-super-20260924T145727.sc16', 'manual', 16.99, 33980416)
config: ('storage_degraded', '2026-09-24T14:57:44.511041+00:00')
        ('storage_last_write_error', '{"at": "2026-09-24T14:57:44.635382+00:00", "error": "...145727.sc16 metadata .json: ENOSPC: No space left on device"}')
POST /api/recording/start -> 409 {"detail":"Recording refused: free space 0.0 GB is below the 0.0 GB floor (storage step 4, PSD history writes stopped)"}
14:57:56,733 INFO TIMING recv#200: recv=187.1ms dropped=0 (IQ=204.8ms) handoff_dropped=0/0 ovf=25 lost=25
after restart: {"status":"degraded",...,"step":4,...,"last_write_error":{"at":"2026-09-24T14:57:44.635382+00:00","error":"...145727.sc16 metadata .json: ENOSPC: No space left on device"},"degraded_since":"2026-09-24T14:57:44.511041+00:00",...}
```

`135921664 % 4 == 0`, 33,980,416 samples = 16.99 s at 2 Msps, file-derived. No 0-byte `.json` or
`.psd.json` (section 7 item 5 fixed); `last_write_error` now survives the restart (item 6 fixed).
`stopped_reason: write_error` cannot be read back, since no `.json` exists; the reason is in the
log line and `last_write_error`. The pipeline kept running (TIMING lines, beacon 0.5 s).

### 10.5 Measured and REJECTED (do not retry)

- **Throttling the capture volume to reproduce I2.** This kernel (5.15.185-tegra) has no
  `dm-delay`, `dm-flakey`, `dm-dust` or `CONFIG_BLK_DEV_THROTTLING` (`dmsetup targets`: verity,
  crypt, striped, linear, error), so a slow loop device or cgroup `io.max` is not available.
  `fsfreeze` on a loop ext4 was considered and not used: the finalize's own `.json` write would
  block on the frozen volume too, so the next recording could not start while the old writer was
  still blocked. A FIFO at `<stem>.psd` blocks only the writer (the finalize never opens the
  `.psd`, it only stats it, and `_unique_capture_name` checks only `.sc16`, so no suffix is added).
- **"No stall in one OFF run" as evidence for F1.** Without a same-day control it could just be a
  quiet NVMe; the ON control runs are what show the stall is still reproducible on this box today.

### 10.6 Findings (not fixed here; for the controller to route)

1. **Orphaned `.detections.json` still happens (section 7 item 7 is only partly fixed).**
   `_deferred_sidecar` checks `sc16_path.exists()` after the 3 s grace, then awaits the DB query
   in `write_sidecar`; an eviction landing between the check and the write leaves the sidecar
   behind. 1 orphan in probe 6 (`142916`), 6 in the 183 evictions of probe 8. Traced:

   ```
   14:36:52,898 Recording saved: ...143649.sc16            (grace ends about 14:36:55.9, check passes)
   14:36:56,254 Rotated old capture: ...143649.sc16
   ...143649.detections.json 120606 bytes, mtime 14:36:56.584
   ```

   The window is the `write_sidecar` query time (0.3 to 0.7 s here), so it bites exactly in the
   young-eviction steady state.
2. **An abandoned writer's capture is recorded as empty and never corrected.** In probe 11 A's
   `.json` and `iq_captures` row say `duration_sec 0.0, total_bytes 0, total_samples 0,
   write_failed false`, and there is no `.psd.json`, but the writer later wrote the full
   34,214,400 bytes to A's `.sc16` (and its grid). Nothing flags A as incomplete at finalize
   (`write_failed` is false with 0 bytes on disk against 34 MB queued), and nothing updates the
   metadata once the writer finishes. I2's goal (B not corrupted or falsely failed) holds.
3. **`young_evictions` is cumulative for as long as evictions keep coming, but the banner says
   "(N in the last 30 minutes)".** From the code (`note_young_evictions`: the count restarts only
   when the previous young eviction is 30 min old), a sensor that stays in this steady state for
   hours reports every eviction since it began. Not observed wrong on hardware (the 10 min run
   stayed inside one window: 184 in 10 min, all within 30 min); code reading only.
4. **Slow DB operations of 1 to 6.5 s while the real SDR records continuously, no retention
   running.** Runs with the B200mini and disk captures on the same NVMe had operations over 1 s
   (probe 6: 6, max 6,488 ms; probe 7: 15, max 4,738 ms; probe 8: 23, max 2,218 ms; probe 10: 29,
   max 2,074 ms), mostly a `fetchall` paired with an `INSERT INTO avg_windows` or `iq_captures`,
   on a DB under 0.1 GB, with no nvme timeout in dmesg after 14:26:51. Mock runs had none. The same
   runs show PSD workers saturated at 15 W (`PROC chunk#600: process=579.9ms latency=1279.8ms
   (IQ=204.8ms)`), so CPU/GIL starvation and page-cache writeback of the capture stream are both
   candidates; not isolated. None reached the 30 s guard, and beacon age stayed under 2 s.

Observations, not defects: at step 4 the eviction also turns `evicting_young` on (probe 6: the
`disk_floor` capture evicted 4 s after it was saved); `step_text` stays the step 4 text, as
`fd4094d` intends. The control runs' "Pruned PSD blobs for 99500" (and 99750) undercount: 0 blobs
were left, because the retry after an abandoned commit found the rows already nulled by the
abandoned connection's commit.

### 10.7 Measurement traps hit

- **`pkill -f` on the workstation killed the calling shell** (exit 144) because the pattern was in
  its own command line; use `pkill -f "[b]eacon.sh"`.
- **Health `free_gb` lags a fill by up to one tick.** At 14:29:24 health still read 427.8 GB, 2.5 s
  after the filler; it is the last tick's sample.
- **The delayed `file_stats` stretches the tick period** from 2 s to about 6 s in probe 9; captures
  then wait about two ticks before eviction, which is the point of that probe, not a regression.
- **Wall time of a pass includes the retries.** The ON control's 129 s includes two 30 s waits;
  compare operations over 200 ms and gaps, not only wall time.
- **Mark save granularity.** The kill in probe 2 happened to land right after a save; the resume
  point can trail by up to 40 chunks (10,000 rows) in general.

### 10.8 Open, not yet answered

- Section 8's field-scale questions are unchanged: F1 was measured at 100 K blobs on the mock
  pipeline, not millions of blobs at 56 Msps or on the field NVMe.
- The shipped 30 min young-eviction clear was not observed (only with the window overridden to
  120 s).
- I2 was reproduced with a blocked `open()`, not with a writer blocked mid-`write()` on a slow
  device; the latter needs a kernel with `dm-delay` or block throttling.
- The cause of Finding 4 (CPU starvation vs writeback contention).

### 10.9 Cleanup

`~/rfobs-val2` (DBs, captures, fillers, logs, launcher) deleted; tmpfs unmounted; no `launch.py`
or `rfobserver` process and nothing on 8888; `/` back to 430 G available. `~/rfobs-replay-data`,
`~/rfobs-stall`, `~/rfobs-stalltest`, `~/GitHub/RFObserver` (branch `feat/averaged-window-store`,
`stash@{0}` intact) and the `~/rfobs-storage` worktree (clean at `fd4094d`) were not modified.
