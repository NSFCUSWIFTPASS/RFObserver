# Fix slow-to-load UI + resource growth over long runs

## Problem

After `rfobserver run` has been up for a long time, the web UI takes very long to
load on refresh. A two-part audit (in-memory growth + DB/page-load path) found three
compounding causes; the rest of the streaming pipeline is well-bounded.

1. **1 Hz `SELECT COUNT(*) FROM detections` (primary).** `_heartbeat_loop`
   (`pipeline/app.py:184`) calls `db.count_detections()` every second, whose body
   (`storage/database.py:398`) is a full-table `COUNT(*)`. SQLite has no cached row
   count, so this is O(N) and grows with the table. It runs on the **single shared
   aiosqlite connection**, so it serializes ahead of the page-load queries
   (`/api/detections`, `/api/status-bar`), making refresh crawl. The value is used
   ONLY as a monotonic change-trigger (`dashboard.html:1092`: refresh the detections
   table when `detection_count` increments); it is never displayed.
2. **WebSocket task leak (real RAM/task growth).** `web/websocket.py:86`
   `await asyncio.gather(send_loop(), recv_loop())`: on disconnect `recv_loop` raises
   `WebSocketDisconnect`; gather propagates it but does NOT cancel `send_loop`, which
   is left parked forever on `await sub.queue.get()` (`:68`, no timeout) and never
   collected. Every dashboard refresh / tab close / network blip leaks one Task +
   `_Subscriber` + queue. Accumulates over hours of reconnecting.
3. **Retention is dead code -> tables grow forever.** `cleanup_old_data()`
   (`storage/database.py:424`) has ZERO callers and does not cover `tone_checks`.
   `detections` (per burst) and `tone_checks` (per `DURATION_SEC` when tone-check is
   on) grow without bound; no `VACUUM`. This bloats the DB file and makes issue #1 and
   every scan progressively slower.

Everything else audited (streaming queues/buffers, rolling-burst window/`_tracked`,
PSD RAM/disk accumulation, module ring buffers, LocalStorage, LiveBroadcast subscriber
set) is bounded/cleared.

## Goal

Refresh stays fast regardless of uptime, and process RAM + DB size plateau instead of
growing without bound.

## Design

### Fix 1 - O(1) heartbeat change-marker (`storage/database.py`)

`count_detections` is a change-trigger, not a displayed count, so replace the O(N)
scan with an O(1) marker:

```python
async def count_detections(self) -> int:
    """Monotonic change-marker for the detections table (O(1)).

    Used only by the heartbeat as a "did a new detection arrive" trigger
    (clients refresh when it increments), so the exact value is irrelevant;
    MAX(id) is O(1) via the integer PK and stays monotonic across retention
    deletes (SQLite does not reuse rowids without VACUUM).
    """
    assert self._db is not None
    async with self._db.execute("SELECT MAX(id) FROM detections") as cursor:
        row = await cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
```

Method name/signature unchanged (caller `app.py:185` untouched). Empty table -> 0.

### Fix 2 - cancel the sibling loop on WS disconnect (`web/websocket.py`)

Run both loops as explicit tasks; when either finishes (normal disconnect via
`recv_loop`, or error), cancel the other so `send_loop` cannot orphan:

```python
async def websocket_endpoint(websocket, broadcast) -> None:
    await websocket.accept()
    sub = broadcast.subscribe()

    async def send_loop() -> None:
        while True:
            data = await sub.queue.get()
            await websocket.send_json(data)

    async def recv_loop() -> None:
        while True:
            text = await websocket.receive_text()
            ...  # unchanged set_mode / set_view handling

    tasks = [asyncio.create_task(send_loop()), asyncio.create_task(recv_loop())]
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        # surface a non-cancel, non-disconnect error from the finished task
        for t in done:
            exc = t.exception()
            if exc and not isinstance(exc, WebSocketDisconnect):
                raise exc
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("WebSocket handler error")
    finally:
        for t in tasks:
            t.cancel()
        with contextlib.suppress(BaseException):
            await asyncio.gather(*tasks, return_exceptions=True)
        broadcast.unsubscribe(sub)
```

Net: on any exit path both tasks are cancelled and awaited (draining
`CancelledError`), then the subscriber is removed. No orphaned `send_loop`.

### Fix 3 - scheduled retention covering all growing tables (`database.py`, `config.py`, `pipeline/app.py`)

- `cleanup_old_data(days)` (`database.py:424`): add a third delete for the
  per-interval table, and commit once:
  ```python
  cursor = await self._db.execute("DELETE FROM tone_checks WHERE timestamp < ?", (cutoff,))
  tc_count = cursor.rowcount
  ...
  await self._db.commit()
  return det_count + stat_count + tc_count
  ```
- `config.py`: add
  ```python
  DB_RETENTION_DAYS: int = 7            # 0 disables retention
  DB_CLEANUP_INTERVAL_SEC: float = 3600.0
  ```
- `pipeline/app.py`: add a `_cleanup_loop(settings, db)` and schedule it in the
  `tasks` list (unconditionally when `settings.DB_RETENTION_DAYS > 0`, so it runs
  headless too). It runs one cleanup shortly after start (so a long-accumulated DB is
  trimmed on the next restart), then every `DB_CLEANUP_INTERVAL_SEC`; wrapped in
  try/except so a failure never kills the process:
  ```python
  async def _cleanup_loop(settings, db) -> None:
      while True:
          try:
              deleted = await db.cleanup_old_data(settings.DB_RETENTION_DAYS)
              if deleted:
                  logger.info("Retention: pruned %d rows older than %d days",
                              deleted, settings.DB_RETENTION_DAYS)
          except Exception:
              logger.exception("Retention cleanup failed; continuing")
          await asyncio.sleep(settings.DB_CLEANUP_INTERVAL_SEC)
  ```

**No automatic VACUUM.** Retention caps *row count* (which is what fixes query
speed); the DB file then plateaus at ~`DB_RETENTION_DAYS` of data (freed pages are
reused). A full `VACUUM` would take an exclusive lock and block the single shared
connection for the whole rewrite -- unsafe to run live. Disk reclaim, if ever needed,
is a manual offline `VACUUM`. Documented, not automated.

## Testing

- **Fix 1 (`tests/unit/test_database.py`):** insert 3 detections; `count_detections()`
  returns the max rowid (== 3); equals a marker that increments on insert and does not
  decrease after deleting the oldest row (monotonic across deletes). Empty table -> 0.
- **Fix 2 (`tests/unit/test_websocket.py`):** drive `websocket_endpoint` with a fake
  WebSocket whose `receive_text` raises `WebSocketDisconnect` after one message and
  whose `send_json` records calls; assert the handler returns, `broadcast` has zero
  subscribers afterward, and no task from the handler is still pending (e.g. assert the
  send-side coroutine was cancelled -- capture created tasks or assert
  `asyncio.all_tasks()` returns to baseline). Also assert `set_view`/`set_mode` still
  work before disconnect.
- **Fix 3 (`tests/unit/test_database.py`):** seed detections/stats/tone_checks with old
  and recent timestamps; `cleanup_old_data(days=7)` deletes only the old rows in ALL
  THREE tables and returns the total; recent rows remain. Add a small app-level test
  (or unit) that `_cleanup_loop` invokes `cleanup_old_data` (can assert one call with a
  monkeypatched db + a cancelled sleep).
- Full CI per CLAUDE.md (ruff, mypy, unit, integration).

## Files

- `src/rfobserver/storage/database.py` - `count_detections` -> MAX(id); `cleanup_old_data`
  covers `tone_checks` + returns total.
- `src/rfobserver/web/websocket.py` - cancel sibling loop on exit (needs `contextlib`).
- `src/rfobserver/config.py` - `DB_RETENTION_DAYS`, `DB_CLEANUP_INTERVAL_SEC`.
- `src/rfobserver/pipeline/app.py` - `_cleanup_loop` + schedule in `tasks`.
- `tests/unit/test_database.py`, `tests/unit/test_websocket.py` (+ small app test).

## Out of scope (secondary, noted in the audit)

- History-page full scans: `duration_histogram` (`database.py:337`, no LIMIT) and
  `capture_configs` DISTINCT (`:382`). Retention shrinks their input; bounding/
  aggregating them in SQL is a separate improvement.
- ZMS/NATS `create_task` fan-out (`streaming.py:1441/1444`) has no NATS-publish timeout
  (fragility, self-completing, not a growth leak).
- Per-request DB connection pooling / read replica (single shared connection is fine
  once #1 removes the 1 Hz scan).
