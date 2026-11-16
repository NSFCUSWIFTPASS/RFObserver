# HCRO .33/.36 restarts: abandoned fleet polls back up the web reader and pin the WAL

## The question

2026-11-16. "Why did .33 and .36 restart?" HCRO sensors hcro-jet-003 (W230, 10.1.42.33) and
hcro-jet-006 (N1000, 10.1.42.36), Jetson Orin Nano, RFObserver 0.14.0b0, Python 3.10.12,
SQLite 3.37.2, aiosqlite 0.22.1, DB on NVMe (`/mnt/ssd/rfobs/rfobs.db`, 12 GB on .36, 29 GB on .33).

Symptom numbers:

| sensor | restart (local MST) | systemd | how |
|---|---|---|---|
| .33 | 2026-11-13 14:59:27 | NRestarts=1, status=90 | watchdog: pipeline stalled 32.0 s, in-process restart failed |
| .36 | 2026-11-14 18:40:03 | NRestarts=1, status=90 | watchdog: pipeline stalled 33.9 s, in-process restart failed |

Hosts did not reboot (.33 up since 09-30, .36 since 10-15).

## The answer

The fleet hub's Live page polls every sensor every 30 s (`averaged/stats`, `averaged/waterfall`,
`detections.json`, `iq-captures`) with a 5 s client timeout (30 s for the waterfall). When a round
takes longer than that on a sensor, the hub gives up and polls again, but the sensor keeps running
every abandoned request to completion on its single read-only web connection. The backlog then only
grows. Each queued request holds an open cursor between its `execute` and its `fetch`, so that
connection never runs without an open statement. Its read snapshot is never released, the WAL
cannot be checkpointed past it, the WAL grows without bound, every query and write slows down, and
eventually a write exceeds the 30 s write timeout and the pipeline watchdog exits the process.

What makes a round slow in the first place: `detections.json` filters on `sdr_center_freq_hz = ?`
plus a 15-minute `start_time` range, and SQLite (no `sqlite_stat1`) picks
`idx_detections_sdr_center` for the equality. Every detection on a sensor has the same centre
frequency, so the query reads and sorts the whole table: 1.8 s on .32 (2.5 M rows, warm cache)
against 0.001 s on the time index; .36 has 11.2 M rows and .33 more.

## Procedure (in the order run)

1. **systemd and host state** (`systemctl show rfobserver -p NRestarts`, `uptime -s`, `last -x`):
   service restarted once on each, hosts not rebooted. Separates a service exit from a reboot.
2. **OOM**: `memory.events` of the service and of `system.slice` (hierarchical since boot):
   `oom_kill 0` on every sensor. Rules out the 4 GB `MemoryMax`.
3. **syslog around the exit** (journal only reaches back to 11-15; `/var/log/syslog` is readable via
   `adm` and unrotated): the chain was `DB write ... stuck >30s -- abandoning connection`, then the
   pipeline watchdog, then `restart did not complete in 10.0s; exiting for systemd`, exit 90.
4. **Which write stuck first**: `_prune_tier_blob_chunk` on both. The hourly avg_tiers prune had been
   slowing for days before each exit (time from the "avg windows" to the "avg_tiers" prune log line):

   | .33 (day sample) | s | .36 (day sample) | s |
   |---|---|---|---|
   | 11-09 | 0 | 11-11 | 0 |
   | 11-10 | 9 | 11-12 | 2 |
   | 11-11 | 13 | 11-13 | 13 |
   | 11-12 | 32 | 11-14 | 45 |
   | 11-13 | 52 (crash that day) | after restart | 1 |

5. **WAL file sizes** (`ls -la /mnt/ssd/rfobs`): .33's `rfobs.db-wal` was 14.4 GB with the main DB
   last written 01:03; .31/.32 had 5-8 MB.
6. **WAL index header** (`rfobs.db-shm` is world-readable; parse `mxFrame` at offset 16,
   `nBackfill` at 96, `aReadMark[5]` at 100). This isolates "checkpoint blocked by a reader" from
   "checkpoint not running". On .33: `mxFrame=3497354 nBackfill=2945690 readMarks=(0, 2945690, ...)`:
   backfill stopped exactly at reader slot 1. Positive control (local, a cursor left open on a writer
   connection): backfill stops at that reader's mark while that connection's own writes still succeed.
7. **Who holds slot 1**: the only process with the DB open is rfobserver. Web reads through the
   read-only connection on .36 (`/api/detections`, `/api/detections/hourly`) hung > 10 min while
   `/api/schedules` (no DB read) answered in 0.4 s. .31/.32 answered the same reads in 0.4 s.
8. **Request completions per sensor** (uvicorn logs on completion): .36's averaged polls from the hub
   (10.1.42.31) stopped completing at 11:33:38, the minute its pin began; .33 completed almost none
   after its 11:48 restart and re-pinned at 11:50.
9. **py-spy dump** (`sudo ~/py-spy dump --pid <pid> --locals`, three dumps): all three aiosqlite
   worker threads alive, each completing different calls between dumps; the web reader's thread was
   inside a call in every dump. Not a hung SQLite call.
10. **asyncio task dump** (gdb attach, `PyRun_SimpleString` of a read-only snippet that walks
    `gc.get_objects()` for pending Tasks and their `cr_await` chains, written to `/dev/shm` because
    the unit has `ProtectSystem=strict`). On .36 at 14:04 local:

    ```
    DB read_only=False tx_qsize=0      (writer)
    DB read_only=True  tx_qsize=463    (web reader)
    DB read_only=True  tx_qsize=0      (rf-db reader)
    1449 pending tasks, of which:
    675 averaged_stats       waiting on stats_sem   (api.py:1383)
    284 averaged_waterfall   waiting on waterfall_sem (api.py:1345)
    278 detections_json      in query_detections (database.py:2000/2001)
    169 iq_captures          in query_iq_captures
      9 dashboard            in get_config (theme), queued behind the rest
    ```

    The stats request being served was for 18:26:50-18:41:50 UTC at 21:04 UTC: 2.4 h behind.
11. **Why one round is slow**: `EXPLAIN QUERY PLAN` on the sensors' own DB (opened with
    `?immutable=1`, no locks), and the query timed with each index:

    ```
    .36, .32: detections.json -> SEARCH detections USING INDEX idx_detections_sdr_center
              (sdr_center_freq_hz=?), USE TEMP B-TREE FOR ORDER BY;  sqlite_stat1: absent
    .32: rows=2504293  as planned 1.8 s   INDEXED BY idx_detections_time 0.001 s
    .36: rows=11203894 (the timed query failed, see traps)
    iq-captures -> idx_iq_captures_time (fine)
    ```

12. **Disconnect detection works**: the stats/waterfall handlers already return 499 for a gone
    client. Reproduced with the sensors' exact stack (Starlette 1.7.0, FastAPI 0.143.3, uvicorn
    0.54.0, httpx 0.28.1 as on the hub): five concurrent requests with a 0.5 s client timeout
    behind a one-slot gate; the server saw `[(0, False), (1, True), (2, True), (3, True), (4,
    True)]`. So queued stats/waterfall requests are dropped once they reach the gate; the
    unbounded part is the ungated `detections.json`/`iq-captures` calls, each a full-table read.

## Evidence

WAL index, all live sensors, 2026-11-16 ~19:12 UTC (two samples a minute apart):

```
.31: mxFrame=731   nBackfill=0   readMarks=(0, 731, 320, -, -)   wal=0.01GB db_mtime=12:10
.32: mxFrame=248   nBackfill=0   readMarks=(0, 240, -, -, -)     wal=0.01GB db_mtime=12:11
.33: mxFrame=16588 nBackfill=477 readMarks=(0, 477, 16580, -, -) wal=0.07GB db_mtime=11:51
.36: mxFrame=39225 nBackfill=881 readMarks=(0, 881, 39225, 24681, -) wal=0.16GB db_mtime=11:33
```

.36 by 21:20 UTC: `mxFrame=167804 nBackfill=881`, WAL 0.69 GB.

Hub timeouts (`fleet/sensors.py`): `TIMEOUT_SEC = 5.0`, `WATERFALL_TIMEOUT_SEC = 30.0`.

## Measured and REJECTED (do not retry)

- **Memory / OOM kill**: `oom_kill 0` in the service and `system.slice` `memory.events` on all
  sensors since boot. The 3.1-3.2 GB MemoryCurrent is mostly page cache.
- **Abandoned writer connection keeps its snapshot**: .33's corpse logged "closed" at 01:03:37;
  locally, closing a connection with a live stepped cursor leaves no pin (`wal_checkpoint(TRUNCATE)`
  returns busy=0).
- **Cancelled read leaves a statement open**: cancelling before the call runs and cancelling
  mid-cursor both leave no pin (local, aiosqlite 0.22.1, Python 3.10.12).
- **A single hung SQLite call**: py-spy shows every worker thread completing calls.
- **My SSH probing as the trigger**: .33's 11:50 pin has no SSH session of mine after 11:47.
- **Slow but draining**: .36 completed zero averaged requests in 1.5 h; the queue only grows.

## Measurement traps

- `pgrep -f "rfobserver run"` matched the new process after a restart I did not know about (the
  user restarted .33 at 11:47); use `systemctl show -p ExecMainPID`.
- `/api/detections` hanging after .33's restart looked like "slow reads with a small WAL"; it was
  the backlog rebuilding within minutes because the hub kept polling.
- py-spy `--native` and consecutive dumps can fail with "failed to get os threadid" when a thread
  exits mid-dump; retry in a loop.
- The first gdb snippet failed with PermissionError: the service user cannot read `~rf-nano-ee`, and
  `ProtectSystem=strict` makes `/tmp` read-only for it. Read from `/tmp`, write to `/dev/shm`.
- Thread names: "Thread-N" counts unnamed threads in creation order, so after a writer reconnect the
  numbers no longer map to writer/reader in the obvious way.
- Opening a live DB with `?immutable=1` reads the main file only. With frames pinned in the WAL the
  file alone is inconsistent and a scan fails with "database disk image is malformed". That is not
  corruption; do not run integrity checks on the strength of it.
- My own probing may have started .36's episode: between 11:26 and 11:36 I ran several full greps
  of its 2.3 GB `/var/log/syslog`, which evicts the DB from the page cache. With a cold cache the
  full-table `detections.json` read is slower still, and .36's backlog began at 11:33. .33's 11:50
  start followed a restart (also a cold cache). Grep the syslog with a time-bounded `sed -n` range
  or `journalctl --since` instead.

## Also found (separate bug, confirmed locally)

After a stuck write, the retry on the fresh writer connection fails with "database is locked" while
the abandoned connection still holds the write lock. Python's sqlite3 has already issued its
implicit `BEGIN`, so the fresh connection is left `in_transaction=True` and the next read pins a
snapshot. If the abandoned connection commits anything before it is closed, that snapshot is stale
and every later write on the writer fails with "database is locked" permanently (deterministic test:
0/20 writes). If it rolls back, the next successful write commits and it recovers (20/20).

## The change (branch fix/web-read-backlog)

1. `_detection_conditions`: with a since/until bound the sdr_center term is written
   `+sdr_center_freq_hz = ?`, which keeps the planner on `idx_detections_time`. The un-bounded
   History filter still uses the sdr_center index.
2. `detections.json` and `iq-captures` get a one-slot gate (`lists_sem`) and return 499 to a client
   that left while queued, like stats and waterfall.
3. The web reader's connection runs each statement and fetches all its rows in one worker call
   (`_ReadConnection`), so no statement stays open between calls and a busy reader cannot pin the
   WAL.
4. `_guarded_write` rolls back after a lock error, so a failed retry cannot leave the writer in a
   transaction with a stale snapshot.

Tests: `test_time_bounded_detection_query_walks_the_time_index`,
`test_disconnected_client_gets_499_without_running_the_aggregation` (now also detections.json and
iq-captures), `test_read_only_cursor_does_not_pin_the_wal`,
`test_failed_retry_does_not_leave_the_writer_in_a_stale_transaction`; each failed before its fix.

## Open, not yet answered

- Not yet verified on a sensor: needs a deploy, then the WAL index of .33/.36 watched while the
  fleet Live page is open.
- Whether the .33 crash on 11-13 and the .36 crash on 11-14 had this trigger. The prune slowdown
  over 11-10..11-13 fits a WAL growing behind a pinned reader, but the sensor logs were not checked
  for hub polls that far back.
- Why .31/.32 have not backed up under the same polling: .31 has 77 k detections a week and .32
  2.5 M rows in total, so their full-table read stays under the 5 s hub timeout while warm.
- .33 and .36 are still backed up now; they clear only on a restart, and re-pin while the fleet Live
  page polls them on the old code.
