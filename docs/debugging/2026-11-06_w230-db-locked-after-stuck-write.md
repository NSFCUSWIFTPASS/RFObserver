# W230: every DB write fails with "database is locked" after one stuck write

## The question

2026-11-06. "W230 sensor was not responding to capture requests (return 500 I think)" and "why is
it giving that return value?"

- Sensor: W230, hcro-jet-003, 10.1.42.33, RFObserver 0.13.0b0 (systemd, Python 3.10, Jetson).
- Fleet showed `Not scheduled on: hcro-jet-003 (HTTP 500)` for the SSM3 schedules on 2026-11-05.
- `/api/health` said `ok`, pipeline active, `db_writes_dropped 0`, while `detection_count` stayed at
  5,794,185 from 2026-11-05 19:59:42 UTC to at least 2026-11-06 17:54 UTC (about 22 h).

## The answer

At 20:00:05 UTC on 2026-11-05 one write on the writer connection stalled for over 30 s.
`_guarded_write` abandoned that connection and opened a new one. The abandoned connection then ran
the INSERT that was still queued on its worker thread. The coroutine had been cancelled, so nothing
ever committed that INSERT. The old connection sat idle inside an open transaction and kept the WAL
write lock for good, so every later write on the new connection failed with
`sqlite3.OperationalError: database is locked`. That includes the schedule insert behind
`POST /api/schedules`, which surfaced as the 500.

What stalled the first write for more than 30 s is not determined (see "Open").

## The procedure

1. **Read-only API on .33** (`/api/health`, `/api/status`, `/api/detections.json`,
   `/api/averaged`, `/api/collection/hourly`, `/api/schedules`). Separates "pipeline dead" from
   "writes dead": the pipeline, isolation and attribution counters kept rising and reads worked,
   while every table stopped at about 19:59:42-20:00:05 UTC.
2. **Compare with a sensor that ran the same schedule** (.34, .36 and E450 .32 all recorded Test1
   at 20:00:00 and kept writing). Rules out "Test1 itself breaks the DB" as a sufficient cause.
3. **syslog on the box** (`ssh rf-nano-bb@10.1.42.33`, read-only; the journal is volatile).
   Reading 12:59:30-13:00:40 MST showed the first error and the sequence.
4. **Thread states of the live process** (`/proc/<pid>/task/*/stat`, no sudo, read twice 10 s
   apart). Tells a spinning statement (thread in R with rising CPU) from an idle lock holder. Only
   the receiver and pipeline threads used CPU, so the lock holder is idle.
5. **Local reproduction** with `SensorDatabase` (scratch DB, `_write_timeout` 1 s): occupy the
   writer connection's worker thread with a 3 s `time.sleep`, issue a guarded
   `insert_avg_window`, wait for the sleep to end, then write again with the 30 s timeout. This
   tests the mechanism without the sensor.

## Evidence

syslog on hcro-jet-003 (MST, UTC-7):

```
13:00:02 Scheduled capture 8a6d821a1b7d run 0: recorded (...-sched-8a6d821a1b7d-0.sc16, 52000000 samples)
13:00:09 Detected 5 bursts
13:00:18 rfobserver.transport.rfdb WARNING Saving the rf-db bookmark timed out; keeping it in memory
13:00:35 rfobserver.storage.database ERROR DB write _insert_avg_window_row stuck >30s - abandoning connection
13:00:35 rfobserver.storage.database ERROR Database reconnected after stuck write
13:00:37 rfobserver.pipeline.streaming ERROR avg-window persist failed (chunk #95163)
         sqlite3.OperationalError: database is locked
13:00:38 rfobserver.pipeline.app ERROR avg_minutes rollup pass failed ... database is locked
13:00:39 DB write insert_detections stuck >30s - abandoning connection
13:00:39 insert_detections failed for 5 bursts; skipping
...
2026-11-06 10:54:33 avg-window persist failed (chunk #2097481) ... database is locked
2026-11-06 10:54:33 Collection statistics flush failed ... database is locked
grep -c "database is locked" /var/log/syslog -> 302553
```

DB state from the API: newest detection 2026-11-05T19:59:42.663Z, newest avg_window id 2944105 at
20:00:04.28Z, no collection hours after 19:00 UTC, rf-db bookmark stuck at 2944105.

Local reproduction (output verbatim):

```
1 healthy write ok
2 stuck write: retried on the fresh connection, ok
3 corpse in_transaction: True
4.0 write after raised OperationalError database is locked
4.1 write after raised OperationalError database is locked
4.2 write after raised OperationalError database is locked
```

Mechanism in code (`storage/database.py`):

- `_guarded_write` runs the write under `asyncio.wait_for(..., timeout=30)`. On timeout it calls
  `_reconnect`, which leaks the old connection deliberately ("closing it awaits its worker thread,
  which is the thing that is stuck") and retries the write on a new one.
- Cancelling the coroutine does not take the queued call off aiosqlite's worker queue: the worker
  runs it when it reaches it. Python's sqlite3 opens an implicit `BEGIN` before an INSERT, and the
  `commit()` that would end it belonged to the cancelled coroutine.
- The `_reconnect` docstring assumed the old connection "still holds the WAL write lock" only while
  stuck. Here it is no longer stuck, yet it still holds the lock, and nothing ever releases it.

## Measured and REJECTED (do not retry)

- **"The storage device wedged"** (what the 30 s timeout was written for): the NVMe was fine
  throughout. The 208 MB Test1 capture was written, syslog kept writing about 1.8 GB, `df` showed
  819 GB free, and no thread is in uninterruptible sleep or spinning now.
- **"Test1 breaks the DB"**: .32, .34 and .36 recorded the same schedule and kept writing.
- **"The web read-only connection holds the lock"**: in WAL mode readers never block a writer, and
  that connection is `query_only`.
- **Health check showing the problem**: `db_writes_dropped` is 0 because the detection and
  avg-window paths log and skip on failure rather than counting a drop.

## Measurement traps hit

- `/api/health` `status ok` and `db_writes_dropped 0` looked healthy for 22 h. Use
  `detection_count` and the newest avg_window time.
- The SSH user differs per sensor: `rf-nano-aa` works on .32 and is refused on .33, while
  `rf-nano-bb` works on .33. SSH to .33 also timed out once while pings were being dropped; the
  retry worked.
- The repro's first runs used a 1 s write timeout, shorter than `busy_timeout` (2 s), so the later
  writes failed with `TimeoutError` instead of `database is locked`. With the sensor's 30 s timeout
  the message matches.
- `uptime_sec` and the clock: unrelated here, but on E450 (.32) the same day the clock booted at a
  stale saved time and was corrected by NTP about a minute later.

## Open, not yet answered

- What held the writer connection's worker thread for more than 30 s from about 20:00:05 UTC,
  right after Test1 finished recording at 20:00:02. The candidates queued on that connection then
  are `insert_iq_capture`, the detections sidecar query, and the schedule book's
  `upsert_run`/`prune_runs`. No log line names it.
- Why E450 lost its schedule run outcomes after its power loss (reported `missed, not_running`
  although both runs recorded). That may be a separate issue.
