# Slow-UI + Resource-Growth Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`).

**Goal:** Keep the web UI fast regardless of uptime and make process RAM + DB size plateau, by (1) making the 1 Hz heartbeat detection-marker O(1), (2) fixing the WebSocket `send_loop` task leak, and (3) wiring scheduled retention that covers all growing tables.

**Architecture:** Three independent fixes in `storage/database.py`, `web/websocket.py`, and `config.py` + `pipeline/app.py`. No schema change, no new deps.

**Tech Stack:** aiosqlite, asyncio, FastAPI WebSocket. Env: prefix Python with `PYTHONPATH=`, use `.venv/bin`. ruff global.

## Global Constraints

- No emojis, no em-dashes anywhere. No "Co-Authored-By" in commits.
- Every Python command prefixed with `PYTHONPATH=`; `.venv/bin/...`. All CLAUDE.md checks stay green (ruff check + format, mypy, unit, integration@NATS:4222).
- No schema migration, no new runtime dependency. Single shared aiosqlite connection stays.
- `count_detections` keeps its name/signature (caller `pipeline/app.py:185` is untouched); it is a change-marker, not a displayed count.
- No automatic VACUUM (would exclusively lock the shared connection); retention caps row count only.

---

### Task 1: O(1) heartbeat change-marker (`storage/database.py`)

**Files:** Modify `src/rfobserver/storage/database.py:390-402` (`count_detections`). Test: `tests/unit/test_database.py`.

- [ ] **Step 1: Write the failing test** in `tests/unit/test_database.py` (mirror the existing DB test setup — temp `DB_PATH`, `await db.connect()`):
```python
@pytest.mark.asyncio
async def test_count_detections_is_monotonic_marker(tmp_path):
    db = SensorDatabase(str(tmp_path / "m.db"))
    await db.connect()
    try:
        assert await db.count_detections() == 0            # empty -> 0
        ids = []
        for i in range(3):
            await db.insert_detection(
                burst_id=f"b{i}", start_time=_dt(i), stop_time=_dt(i),
                center_freq_hz=1e6, bandwidth_hz=1e3, peak_power_db=-50.0,
                duration_ms=1.0, detection_timestamp=_dt(i),
            )
        m3 = await db.count_detections()
        assert m3 == 3                                     # MAX(id) after 3 inserts
        # monotonic across a delete of the oldest row (marker must not go backward)
        await db._db.execute("DELETE FROM detections WHERE id = 1"); await db._db.commit()
        assert await db.count_detections() == m3           # still 3 (MAX(id) unchanged)
    finally:
        await db.close()
```
(Add a `_dt(i)` helper returning `datetime(2026, 1, 1, 0, 0, i, tzinfo=timezone.utc)` if the module lacks one; reuse the existing insert-detection test's pattern for kwargs.)

- [ ] **Step 2: Run, expect fail** — `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py::test_count_detections_is_monotonic_marker -x -q` (current `COUNT(*)` returns 2 after the delete, not 3).

- [ ] **Step 3: Implement.** Replace the `count_detections` body's query `SELECT COUNT(*) FROM detections` with `SELECT MAX(id) FROM detections`, and return `int(row[0]) if row and row[0] is not None else 0`. Update the docstring to say it is an O(1) monotonic change-marker (MAX(id), stable across retention deletes). Keep name/signature.

- [ ] **Step 4: Run tests -> PASS.** Also `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -x -q`.

- [ ] **Step 5: Commit** — `git commit -am "db: heartbeat detection-marker uses MAX(id) (O(1)) instead of COUNT(*) scan"`.

---

### Task 2: Cancel the orphaned WebSocket send_loop (`web/websocket.py`)

**Files:** Modify `src/rfobserver/web/websocket.py:57-86` (`websocket_endpoint`); it already imports `asyncio`, `contextlib`, `WebSocketDisconnect`. Test: `tests/unit/test_websocket.py`.

- [ ] **Step 1: Write the failing test** in `tests/unit/test_websocket.py`:
```python
@pytest.mark.asyncio
async def test_disconnect_cancels_send_loop_no_leak():
    import asyncio
    from rfobserver.web.websocket import LiveBroadcast, websocket_endpoint

    b = LiveBroadcast()
    sent = []

    class FakeWS:
        def __init__(self):
            self._recv = 0
        async def accept(self): pass
        async def send_json(self, data): sent.append(data)
        async def receive_text(self):
            # one control message, then disconnect
            self._recv += 1
            if self._recv == 1:
                return '{"type": "set_view", "psd_visible": false}'
            raise WebSocketDisconnect(1000)

    before = len(asyncio.all_tasks())
    await websocket_endpoint(FakeWS(), b)
    await asyncio.sleep(0)  # let cancellations settle
    assert b._subscribers == set()                       # unsubscribed
    # no lingering handler task (send_loop not orphaned)
    assert len([t for t in asyncio.all_tasks() if not t.done()]) <= before
```
(Import `WebSocketDisconnect` from `fastapi` / `starlette.websockets` as the module does.)

- [ ] **Step 2: Run, expect fail/hang guard** — `PYTHONPATH= .venv/bin/pytest tests/unit/test_websocket.py::test_disconnect_cancels_send_loop_no_leak -x -q`. With the current `gather`, `send_loop` stays pending -> the leftover-task assertion fails (the test itself returns because `gather` re-raises `WebSocketDisconnect`, but the orphaned task lingers).

- [ ] **Step 3: Implement** the sibling-cancel rewrite from the spec: run `send_loop`/`recv_loop` as `asyncio.create_task`, `await asyncio.wait(..., return_when=FIRST_COMPLETED)`, cancel `pending`, re-raise any non-`WebSocketDisconnect`/non-`CancelledError` exception from `done`; in `finally` cancel both tasks, `await asyncio.gather(*tasks, return_exceptions=True)` under `contextlib.suppress(BaseException)`, then `broadcast.unsubscribe(sub)`. Keep the `set_mode` / `set_view` handling inside `recv_loop` exactly as-is.

- [ ] **Step 4: Run tests -> PASS.** Also run the existing websocket tests: `PYTHONPATH= .venv/bin/pytest tests/unit/test_websocket.py -x -q`.

- [ ] **Step 5: Commit** — `git commit -am "ws: cancel sibling loop on disconnect (fix orphaned send_loop task leak)"`.

---

### Task 3: Scheduled retention covering all growing tables (`database.py`, `config.py`, `pipeline/app.py`)

**Files:** Modify `src/rfobserver/storage/database.py:424-438` (`cleanup_old_data`), `src/rfobserver/config.py` (after `ARCHIVE_MAX_GB`, ~line 97), `src/rfobserver/pipeline/app.py` (new `_cleanup_loop` + schedule near `:126-132`). Test: `tests/unit/test_database.py` (+ a small `_cleanup_loop` test).

- [ ] **Step 1: Write failing tests** in `tests/unit/test_database.py`:
```python
@pytest.mark.asyncio
async def test_cleanup_covers_detections_stats_tone_checks(tmp_path):
    db = SensorDatabase(str(tmp_path / "c.db"))
    await db.connect()
    try:
        old = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        new = datetime.now(timezone.utc).isoformat()
        # one old + one recent row in each of the three tables
        for ts in (old, new):
            await db._db.execute(
                "INSERT INTO detections (burst_id,start_time,stop_time,center_freq_hz,"
                "bandwidth_hz,peak_power_db,duration_ms,detection_timestamp) "
                "VALUES (?,?,?,?,?,?,?,?)", (ts, ts, ts, 1e6, 1e3, -50.0, 1.0, ts))
            await db._db.execute(
                "INSERT INTO stats (timestamp,data) VALUES (?,?)", (ts, "{}"))
            await db._db.execute(
                "INSERT INTO tone_checks (timestamp,tone_freq_hz,sdr_center_freq_hz,"
                "in_band,tone_power_db,noise_floor_db,snr_db,detected) "
                "VALUES (?,?,?,?,?,?,?,?)", (ts, 1e6, 1e6, 1, -50.0, -90.0, 40.0, 1))
        await db._db.commit()
        deleted = await db.cleanup_old_data(days=7)
        assert deleted == 3                                # one old row per table
        for tbl in ("detections", "stats", "tone_checks"):
            async with db._db.execute(f"SELECT COUNT(*) FROM {tbl}") as c:
                assert (await c.fetchone())[0] == 1        # recent row survives
    finally:
        await db.close()
```
(Match the real `stats` / `tone_checks` column names from the CREATE TABLE in `database.py`; adjust the INSERTs if columns differ. Reuse module imports `datetime, timezone, timedelta`.)

- [ ] **Step 2: Run, expect fail** — `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py::test_cleanup_covers_detections_stats_tone_checks -x -q` (current cleanup deletes 2, leaves the old `tone_checks` row).

- [ ] **Step 3: Implement.**
  - `cleanup_old_data` (`database.py:429-438`): after the `stats` delete, add
    ```python
    cursor = await self._db.execute("DELETE FROM tone_checks WHERE timestamp < ?", (cutoff,))
    tc_count = cursor.rowcount
    ```
    and change `total = det_count + stats_count` to `total = det_count + stats_count + tc_count`.
  - `config.py` (after `ARCHIVE_MAX_GB`): add
    ```python
    DB_RETENTION_DAYS: int = 7            # rows older than this are pruned; 0 disables
    DB_CLEANUP_INTERVAL_SEC: float = 3600.0
    ```
  - `pipeline/app.py`: add the `_cleanup_loop` coroutine (spec body) near `_heartbeat_loop`, and in the `tasks` block (`:124-132`) append it when retention is enabled:
    ```python
    if settings.DB_RETENTION_DAYS > 0:
        tasks.append(_cleanup_loop(settings, db))
    ```
    `_cleanup_loop` runs one cleanup immediately, then every `DB_CLEANUP_INTERVAL_SEC`, each wrapped in try/except with a log line; `db` is the same `SensorDatabase`.

- [ ] **Step 4: `_cleanup_loop` unit test** (`tests/unit/test_app_cleanup.py` or append to an app test): monkeypatch a fake `db` with an async `cleanup_old_data` recording its `days` arg, run `_cleanup_loop` as a task with `DB_CLEANUP_INTERVAL_SEC` tiny, `await asyncio.sleep(0)` a couple times, cancel the task, assert `cleanup_old_data` was called at least once with `settings.DB_RETENTION_DAYS`. Keep it fast and cancellation-clean.

- [ ] **Step 5: Run** — `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py tests/unit/test_app_cleanup.py -x -q` -> PASS. Confirm `DB_RETENTION_DAYS`/`DB_CLEANUP_INTERVAL_SEC` appear in `PYTHONPATH= .venv/bin/pytest tests/unit/test_config.py -x -q` if it enumerates settings (update if it asserts the full set).

- [ ] **Step 6: Commit** — `git commit -am "db: scheduled retention (covers tone_checks) with DB_RETENTION_DAYS; wire into app"`.

---

### Task 4: Full verification + finish

- [ ] **Step 1:** `ruff check src/ tests/ && ruff format --check src/ tests/`
- [ ] **Step 2:** `PYTHONPATH= .venv/bin/mypy src/rfobserver/`
- [ ] **Step 3:** `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`
- [ ] **Step 4:** `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q`
- [ ] **Step 5:** If green, use superpowers:finishing-a-development-branch.

## Self-Review (author)

- Coverage: Task 1 = O(1) marker; Task 2 = WS leak; Task 3 = retention (cleanup+config+schedule); Task 4 = CI/finish. Matches spec's three primary fixes.
- Placeholders: none; each fix's code is in the spec/plan; test column names flagged to verify against the live CREATE TABLE.
- Consistency: `count_detections` name unchanged; `cleanup_old_data(days)` signature unchanged (return value now includes tone_checks); new config names used identically in `config.py` and `app.py`.
