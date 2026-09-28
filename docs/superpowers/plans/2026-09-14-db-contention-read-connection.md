# DB Contention Fix (Branch B) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix issue 1, where a single Dashboard tab trips the watchdog, by
giving the web layer its own read-only SQLite connection and by saving each
drain's bursts in one batched write. Then prove on nano-super that
field-scale Dashboard load no longer trips the watchdog.

**Architecture:**

- Today every DB call goes through one aiosqlite connection and its single
  worker thread: pipeline writes, heartbeat reads, and 12 to 24 s Dashboard
  reads.
- Measured on hardware: with a 24 h query running, each
  `insert_detection` (execute plus commit) waited 2.7 to 3.8 s, and
  `_drain_burst_results` inserted bursts one at a time before the beacon mark.
  The consumer starved and the watchdog fired.
- Fix, as spec Cut 3b:
  - A second `SensorDatabase` opened read-only (`PRAGMA query_only=ON`, WAL)
    serves all web reads and the heartbeat. WAL gives true concurrent
    read/write across two connections.
  - The one web write (`PUT /api/ui-prefs`) uses the pipeline's write
    connection.
  - `_drain_burst_results` collects every pending burst and calls a new
    `insert_detections` batch (one `executemany`, one commit).
- Cut 3a (a separate web thread) stays out of scope unless the hardware check
  still trips.

**Tech Stack:** Python 3.10 to 3.12, asyncio, aiosqlite (SQLite WAL),
FastAPI, pytest and pytest-asyncio.

**Spec:** `docs/debugging/2026-09-14_stall-safety-net-hardware-validation.md`
(issue 1, the Dashboard evidence, and its CORRECTION), together with
`docs/superpowers/specs/2026-09-08-pipeline-stall-resilience-design.md`
section "3b. Read-only DB connection for the web layer". The user chose
"Batch + read conn" on 2026-09-14: re-measure on nano-super, and do Cut 3a
only if it still trips.

## Global Constraints

- Code must run on Python >= 3.10 (the Jetsons run 3.10.12). For asyncio
  timeouts use `except (TimeoutError, asyncio.TimeoutError):  # noqa: UP041`.
- Always prefix commands with `PYTHONPATH=`. The 3.10 test venv is
  `$V310` =
  `/tmp/claude-1000/-home-orencollaco-GitHub-RFObserver/50689e78-60fa-453e-89bd-c3c811638a7b/scratchpad/venv310`.
  Run each task's tests on `.venv` (3.11) and on `$V310`.
- No em-dashes and no emojis anywhere.
- Stage explicit paths only. Never `git add -A` or `git add .`. Never stage
  `docs/` or `.superpowers/`.
- Commit messages carry no `Co-Authored-By:` or `Claude-Session:` trailers.
  Check with `git log -1 --format=%B` and amend them away if the environment
  appends them.
- Before the merge, run the full CI set: ruff check, ruff format --check, mypy,
  the unit suite on 3.11 and 3.10, and the integration tests (throwaway NATS:
  `docker run -d --rm --name rfobs-test-nats -p 4222:4222 nats:2.10-alpine -js`).

---

### Task 0: Branch setup

- [ ] Create the branch from local `main` (currently 901dadf, branch A merged).
  The uncommitted docs stay in the tree, unstaged.

```bash
git checkout -b fix/db-contention-read-connection main
```

### Task 1: Read-only SensorDatabase mode

**Files:**
- Modify: `src/rfobserver/storage/database.py` (`SensorDatabase.__init__` at about line 180, `connect` at about line 238)
- Test: `tests/unit/test_database.py`

**Interfaces:**
- Produces: `SensorDatabase(db_path: str, *, read_only: bool = False)` and a `read_only` property. A read-only instance's `connect()` opens the same file, sets `PRAGMA busy_timeout=2000` and `PRAGMA query_only=ON`, and does NOT run the schema script, migrations or index creation. The writer instance must connect first, because it creates the schema.

- [ ] **Step 1: Write the failing tests** in `tests/unit/test_database.py`. Reuse the file's existing detection-kwargs helper or fixture if there is one; otherwise define `_det_kwargs(i: int) -> dict[str, Any]` with the required `insert_detection` fields.

```python
@pytest.mark.asyncio
async def test_read_only_connection_reads_but_rejects_writes(tmp_path):
    path = str(tmp_path / "ro.sqlite")
    writer = SensorDatabase(path)
    await writer.connect()
    reader = SensorDatabase(path, read_only=True)
    await reader.connect()
    try:
        assert reader.read_only and not writer.read_only
        await writer.insert_detection(**_det_kwargs(0))
        assert await reader.count_detections() >= 1
        with pytest.raises(sqlite3.OperationalError):
            await reader.set_config("k", "v")
    finally:
        await reader.close()
        await writer.close()


@pytest.mark.asyncio
async def test_busy_reader_does_not_delay_writer(tmp_path):
    """The whole point of the split: a long web read must not queue pipeline writes."""
    path = str(tmp_path / "split.sqlite")
    writer = SensorDatabase(path)
    await writer.connect()
    reader = SensorDatabase(path, read_only=True)
    await reader.connect()
    try:
        assert reader._db is not None

        def _slow(seconds: float) -> int:
            time.sleep(seconds)
            return 0

        await reader._db.create_function("slow", 1, _slow)
        slow_read = asyncio.ensure_future(reader._db.execute("SELECT slow(1.0)"))
        await asyncio.sleep(0.1)  # the reader's worker thread is now busy
        t0 = time.monotonic()
        await writer.insert_detection(**_det_kwargs(1))
        assert time.monotonic() - t0 < 0.5, "a busy reader must not delay the writer"
        await slow_read
    finally:
        await reader.close()
        await writer.close()
```

Add `import asyncio`, `import sqlite3` and `import time` if missing. The
`set_config` write raises through `_guarded_write`, whose retry path also
fails fast, so `OperationalError` ("attempt to write a readonly database")
propagates. If `_guarded_write` wraps it differently, assert on whatever
`sqlite3.Error` subclass actually propagates, and say so in the report.

- [ ] **Step 2: Run the tests and verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -k "read_only or busy_reader" -v -p no:cacheprovider`
Expected: FAIL with `TypeError: SensorDatabase.__init__() got an unexpected keyword argument 'read_only'`.

- [ ] **Step 3: Implement**

```python
    def __init__(self, db_path: str, *, read_only: bool = False) -> None:
        self._db_path = db_path
        self._read_only = read_only
        ...

    @property
    def read_only(self) -> bool:
        """True for the web layer's reader: query_only, no schema/migrations."""
        return self._read_only
```

At the top of `connect()`, before the writer-only work:

```python
        if self._read_only:
            # Web-layer reader (spec Cut 3b): its own connection and worker
            # thread, so long Dashboard reads never queue pipeline writes. WAL
            # (set by the writer) gives concurrent read/write. query_only makes
            # any accidental write fail instead of contending with the pipeline.
            # The writer must connect first: it owns schema and migrations.
            self._db = await aiosqlite.connect(self._db_path)
            await self._db.execute("PRAGMA busy_timeout=2000")
            await self._db.execute("PRAGMA query_only=ON")
            logger.info("Database connected read-only: %s", self._db_path)
            return
```

`_reconnect` (the stuck-write recovery) is only reached from writes, so a
read-only instance never uses it. No change is needed there.

- [ ] **Step 4: Run the tests on 3.11 and 3.10, plus the whole database test file**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -q -p no:cacheprovider && PYTHONPATH= $V310/bin/pytest tests/unit/test_database.py -q -p no:cacheprovider`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/storage/database.py tests/unit/test_database.py
git commit -m "feat(db): read-only SensorDatabase mode for the web layer

A second connection with its own aiosqlite worker thread and
PRAGMA query_only=ON. With WAL, long Dashboard reads on it no longer queue
behind or ahead of pipeline writes on the write connection (spec Cut 3b)."
```

### Task 2: Batched detection insert

**Files:**
- Modify: `src/rfobserver/storage/database.py` (`insert_detection` at about line 339; add `insert_detections` after it)
- Test: `tests/unit/test_database.py`

**Interfaces:**
- Produces: `SensorDatabase.insert_detections(self, detections: Sequence[Mapping[str, Any]]) -> int`, decorated with `@_guarded_write`. Each mapping carries exactly the keyword arguments of `insert_detection`. It runs one `executemany` with the same `INSERT OR IGNORE` SQL and one commit, and returns `len(detections)`. An empty sequence returns 0 without touching the DB. `insert_detection` stays, with an unchanged signature, and both share one private row builder so the SQL and column order are defined once.

- [ ] **Step 1: Write the failing tests**

```python
@pytest.mark.asyncio
async def test_insert_detections_batch_inserts_all_rows_in_one_commit(tmp_path):
    db = SensorDatabase(str(tmp_path / "batch.sqlite"))
    await db.connect()
    try:
        assert db._db is not None
        commits = 0
        real_commit = db._db.commit

        async def counting_commit() -> None:
            nonlocal commits
            commits += 1
            await real_commit()

        db._db.commit = counting_commit  # type: ignore[method-assign]
        n = await db.insert_detections([_det_kwargs(i) for i in range(25)])
        assert n == 25
        assert commits == 1, "one commit per batch, not per row"
        assert len(await db.query_detections(limit=100)) == 25
        # INSERT OR IGNORE semantics preserved: re-inserting the same burst_ids is a no-op.
        await db.insert_detections([_det_kwargs(i) for i in range(25)])
        assert len(await db.query_detections(limit=100)) == 25
        assert await db.insert_detections([]) == 0
    finally:
        await db.close()
```

Adjust `query_detections(limit=100)` to the method's real signature (read it),
and make `_det_kwargs(i)` produce a unique `burst_id` per `i`.

- [ ] **Step 2: Run it and verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -k insert_detections -v -p no:cacheprovider`
Expected: FAIL with `AttributeError: 'SensorDatabase' object has no attribute 'insert_detections'`.

- [ ] **Step 3: Implement.** Extract the SQL into a module constant and the tuple into a helper:

```python
_INSERT_DETECTION_SQL = """INSERT OR IGNORE INTO detections
   (burst_id, start_time, stop_time, center_freq_hz, bandwidth_hz,
    peak_power_db, duration_ms, detection_timestamp,
    sdr_center_freq_hz, sample_rate_hz, lo_offset_hz, analog_bw_hz,
    gain_db, antenna, device_serial, peak_freq_hz)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""


def _detection_row(
    burst_id: str,
    start_time: datetime,
    stop_time: datetime,
    center_freq_hz: float,
    bandwidth_hz: float,
    peak_power_db: float,
    duration_ms: float,
    detection_timestamp: datetime,
    sdr_center_freq_hz: float | None = None,
    sample_rate_hz: float | None = None,
    lo_offset_hz: float | None = None,
    analog_bw_hz: float | None = None,
    gain_db: float | None = None,
    antenna: str | None = None,
    device_serial: str | None = None,
    peak_freq_hz: float = 0.0,
) -> tuple[Any, ...]:
    return (
        burst_id,
        start_time.isoformat(),
        stop_time.isoformat(),
        center_freq_hz,
        bandwidth_hz,
        peak_power_db,
        duration_ms,
        detection_timestamp.isoformat(),
        sdr_center_freq_hz,
        sample_rate_hz,
        lo_offset_hz,
        analog_bw_hz,
        gain_db,
        antenna,
        device_serial,
        peak_freq_hz,
    )
```

`insert_detection` keeps its exact signature and becomes:

```python
        assert self._db is not None
        await self._db.execute(
            _INSERT_DETECTION_SQL,
            _detection_row(
                burst_id, start_time, stop_time, center_freq_hz, bandwidth_hz,
                peak_power_db, duration_ms, detection_timestamp, sdr_center_freq_hz,
                sample_rate_hz, lo_offset_hz, analog_bw_hz, gain_db, antenna,
                device_serial, peak_freq_hz,
            ),
        )
        await self._db.commit()
```

The new batch method:

```python
    @_guarded_write
    async def insert_detections(self, detections: Sequence[Mapping[str, Any]]) -> int:
        """Insert many detections with one executemany and one commit.

        The pipeline persists a whole drain's bursts in one call: one trip
        through the connection's worker queue instead of two per burst.
        """
        if not detections:
            return 0
        assert self._db is not None
        await self._db.executemany(
            _INSERT_DETECTION_SQL, [_detection_row(**d) for d in detections]
        )
        await self._db.commit()
        return len(detections)
```

Import `Mapping` and `Sequence` from `collections.abc`, inside
`TYPE_CHECKING` if the module already follows that pattern.

- [ ] **Step 4: Run the database tests on 3.11 and 3.10, then mypy**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -q -p no:cacheprovider && PYTHONPATH= $V310/bin/pytest tests/unit/test_database.py -q -p no:cacheprovider && PYTHONPATH= .venv/bin/mypy src/rfobserver/`
Expected: all pass, and mypy is clean.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/storage/database.py tests/unit/test_database.py
git commit -m "feat(db): batched insert_detections (one executemany, one commit)"
```

### Task 3: Drain saves a whole batch in one write

**Files:**
- Modify: `src/rfobserver/pipeline/streaming.py` (`_drain_burst_results`, at about line 1874)
- Modify: `tests/integration/test_pipeline_db_resilience.py`, `tests/unit/test_streaming_replay_mode.py` (both mock `insert_detection`)
- Test: `tests/unit/test_streaming_drain_batch.py` (create)

**Interfaces:**
- Consumes: `SensorDatabase.insert_detections(detections) -> int` (Task 2).
- Behavior:
  - `_drain_burst_results` drains every queued item.
  - For each non-replay item it builds the detection kwargs (the same fields
    it passes today) into one list, and logs `Detected %d bursts` per item as
    today.
  - After the loop, if the list is non-empty, it calls
    `await self._db.insert_detections(rows)` exactly once.
  - On any exception it logs
    `logger.exception("insert_detections failed for %d bursts; skipping", len(rows))`
    and returns normally. The consumer loop must never die from a DB error;
    issue F1 is already verified on hardware.
  - In replay mode no DB call is made.

- [ ] **Step 1: Write the failing test** `tests/unit/test_streaming_drain_batch.py`:

```python
"""A drain persists all pending bursts in one batched DB write."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest

from rfobserver.models import BurstFingerprint
from rfobserver.pipeline.streaming import StreamingProcessor


class _BatchDB:
    def __init__(self) -> None:
        self.batches: list[list[dict[str, Any]]] = []

    async def insert_detections(self, detections: Any) -> int:
        self.batches.append(list(detections))
        return len(self.batches[-1])


def _proc(db: Any, *, replay: bool = False) -> StreamingProcessor:
    proc = StreamingProcessor.__new__(StreamingProcessor)
    proc._db = db
    proc._replay_mode = replay
    proc._receiver = object()
    proc._settings = type("S", (), {"BANDWIDTH": 2_000_000, "GAIN": 30})()
    proc._burst_result_queue = asyncio.Queue()
    return proc


def _burst() -> BurstFingerprint:
    now = datetime.now(timezone.utc)
    return BurstFingerprint(
        start_time=now,
        stop_time=now,
        center_freq_hz=915e6,
        peak_freq_hz=915e6,
        bandwidth_hz=1e5,
        peak_power_db=-40.0,
        duration_ms=10.0,
    )


@pytest.mark.asyncio
async def test_drain_writes_all_pending_bursts_in_one_batch() -> None:
    db = _BatchDB()
    proc = _proc(db)
    for n in (3, 1, 4):
        await proc._burst_result_queue.put(([_burst() for _ in range(n)], 915e6))
    await proc._drain_burst_results()
    assert len(db.batches) == 1, "one DB write per drain, not per burst"
    assert len(db.batches[0]) == 8
    row = db.batches[0][0]
    assert row["sdr_center_freq_hz"] == 915e6 and row["antenna"] == "RX2"


@pytest.mark.asyncio
async def test_drain_in_replay_mode_writes_nothing() -> None:
    db = _BatchDB()
    proc = _proc(db, replay=True)
    await proc._burst_result_queue.put(([_burst()], 915e6))
    await proc._drain_burst_results()
    assert db.batches == []


@pytest.mark.asyncio
async def test_drain_with_nothing_queued_writes_nothing() -> None:
    db = _BatchDB()
    await _proc(db)._drain_burst_results()
    assert db.batches == []
```

- [ ] **Step 2: Run it and verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_streaming_drain_batch.py -v -p no:cacheprovider`
Expected: FAIL with `AttributeError: '_BatchDB' object has no attribute 'insert_detection'` (the drain still calls the per-row method).

- [ ] **Step 3: Implement**. Replace the per-burst `try: await self._db.insert_detection(...)` block in `_drain_burst_results` with building a dict per burst (same keys and values as the current call) appended to a `rows` list declared before the `while True`. Keep the `Detected %d bursts` log per item. After the loop:

```python
        if rows:
            try:
                await self._db.insert_detections(rows)
            except Exception:
                logger.exception("insert_detections failed for %d bursts; skipping", len(rows))
```

Update `tests/integration/test_pipeline_db_resilience.py`: `_BoomDB` defines
`insert_detections(self, detections)`, which counts calls and raises; the test
asserts one call and that the drain does not raise. Update
`tests/unit/test_streaming_replay_mode.py`, which mocks
`db.insert_detection = AsyncMock()`: mock `insert_detections` instead, and
assert `not_called` in replay and `called` in non-replay.

- [ ] **Step 4: Run the tests on 3.11 and 3.10**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_streaming_drain_batch.py tests/unit/test_streaming_replay_mode.py tests/unit/test_streaming.py tests/integration/test_pipeline_db_resilience.py -q -p no:cacheprovider`, then the same with `$V310`.
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/pipeline/streaming.py tests/unit/test_streaming_drain_batch.py tests/unit/test_streaming_replay_mode.py tests/integration/test_pipeline_db_resilience.py
git commit -m "fix(streaming): persist each drain's bursts in one batched write

_drain_burst_results inserted bursts one at a time (execute + commit each)
before the consumer could mark progress. Under a Dashboard query each write
waited ~3 s on the shared connection, so ~6.5 bursts/s arriving against
~0.3/s persisted starved the consumer and tripped the watchdog on
nano-super. One executemany + one commit per drain."
```

### Task 4: Wire the reader into the web layer and heartbeat

**Files:**
- Modify: `src/rfobserver/pipeline/app.py` (`run()`: open and close the reader; `_run_web_server`: take it; the heartbeat task gets the reader)
- Modify: `src/rfobserver/web/routes/api.py` (`put_ui_prefs` at about line 1113 uses the write connection; add `_get_write_db`)
- Modify: `src/rfobserver/web/app.py` (`create_app`: initialize `app.state.write_database = None` next to `app.state.database = None`)
- Test: `tests/unit/test_web_routes.py`

**Interfaces:**
- Consumes: `SensorDatabase(path, read_only=True)` (Task 1).
- Produces:
  - `app.state.database` is the read-only reader for all web reads.
  - `app.state.write_database` is the pipeline's writer, used only by
    `PUT /api/ui-prefs`.
  - `_get_write_db(request)` returns `write_database`, falling back to
    `database` when `write_database` is None, so existing tests and non-split
    setups keep working.

- [ ] **Step 1: Write the failing test** in `tests/unit/test_web_routes.py`:

```python
@pytest.mark.asyncio
async def test_ui_prefs_put_uses_write_connection_when_split(settings, tmp_path):
    from rfobserver.storage.database import SensorDatabase

    path = str(tmp_path / "prefs.sqlite")
    writer = SensorDatabase(path)
    await writer.connect()
    reader = SensorDatabase(path, read_only=True)
    await reader.connect()
    try:
        app = create_app(settings)
        app.state.database = reader
        app.state.write_database = writer
        client = TestClient(app)
        r = client.put("/api/ui-prefs", json={"theme": "dark"})
        assert r.status_code == 200, r.text
        assert client.get("/api/ui-prefs").json()["theme"] == "dark"
    finally:
        await reader.close()
        await writer.close()
```

If `TestClient` inside an async test conflicts with the running loop in this
suite, restructure it to create and connect the databases through
`asyncio.run` in a sync test, and say so in the report.

- [ ] **Step 2: Run it and verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_web_routes.py -k write_connection -v -p no:cacheprovider`
Expected: FAIL with HTTP 500 (the PUT writes through the read-only connection and gets "attempt to write a readonly database").

- [ ] **Step 3: Implement**

`api.py`:

```python
def _get_write_db(request: Request) -> Any:
    """The pipeline's write connection; the web layer's reader is query_only.

    Falls back to ``database`` when no split is configured (tests, tools).
    """
    return getattr(request.app.state, "write_database", None) or _get_db(request)
```

In `put_ui_prefs`, keep reading the current document with `db = _get_db(request)`,
and write with `await _get_write_db(request).set_config(UI_PREFS_KEY, json.dumps(doc))`.
The 503 check stays on `db`.

`web/app.py`: add `app.state.write_database = None`.

`pipeline/app.py` `run()`: right after `await db.connect()`:

```python
    # Web-layer reader (spec Cut 3b): Dashboard reads get their own connection so
    # they never queue pipeline writes. Connect after the writer (schema owner).
    read_db = SensorDatabase(settings.DB_PATH, read_only=True)
    await read_db.connect()
```

Pass it to the web server and the heartbeat:
`_run_web_server(settings, supervisor, read_db, db, broadcast, beacon)` and
`_heartbeat_loop(settings, supervisor, read_db, local_storage, broadcast)`.
In `_run_web_server`, add a `write_database` parameter after `database` and set
`app.state.write_database = write_database`. In the `finally`, close the reader
before `await db.close()`: `await read_db.close()`. The cleanup loop
(`_cleanup_loop`, which prunes blobs, a write) keeps `db`.

- [ ] **Step 4: Run the web and pipeline tests on 3.11 and 3.10, then mypy**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/ -q -p no:cacheprovider && PYTHONPATH= $V310/bin/pytest tests/unit/ -q -p no:cacheprovider && PYTHONPATH= .venv/bin/mypy src/rfobserver/`
Expected: all pass, and mypy is clean.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/pipeline/app.py src/rfobserver/web/routes/api.py src/rfobserver/web/app.py tests/unit/test_web_routes.py
git commit -m "feat(web): serve the web layer and heartbeat from a read-only connection

Dashboard queries (12-24 s on a field-size DB) and the per-second heartbeat
count now run on their own SQLite connection, so pipeline writes no longer
wait behind them. The one web write (PUT /api/ui-prefs) uses the pipeline's
write connection."
```

### Task 4b: Cap concurrent heavy Dashboard aggregations (added 2026-09-14 after the hardware check)

Why: on nano-super, after Tasks 1 to 4, 1 Dashboard tab passes (beacon at
most 1.2 s, no slow writes). But 4 concurrent 24 h tabs still trip the
watchdog: the main event-loop thread was pegged at 100% (system 82%) decoding
up to four waterfalls at once, loop lag was 3 to 5 s, and beacon age reached
36 s. The user chose "Cap heavy queries": bound the load to the 1-tab profile
(one waterfall aggregation plus one stats aggregation at a time).

**Files:**
- Modify: `src/rfobserver/web/app.py` (`create_app`: create the two semaphores on `app.state`)
- Modify: `src/rfobserver/web/routes/api.py` (`averaged_waterfall` at about line 946, `averaged_stats` at about line 985)
- Test: `tests/unit/test_heavy_query_cap.py` (create)

**Interfaces:**
- Produces:
  - `app.state.waterfall_sem: asyncio.Semaphore` (value 1)
  - `app.state.stats_sem: asyncio.Semaphore` (value 1)

  Both are created in `create_app`, per app rather than at module level. On
  Python 3.10 a module-level asyncio primitive binds to the first loop that
  contends on it, and breaks across test event loops.
- Behavior:
  - `averaged_waterfall` keeps its cache check before waiting. It awaits
    `db.query_avg_waterfall(...)` only while holding `waterfall_sem`, and
    re-checks the cache once it holds the semaphore.
  - `averaged_stats` awaits `db.query_avg_stats(...)` only while holding
    `stats_sem`.
  - After acquiring either semaphore, if `await request.is_disconnected()` is
    true, the handler returns `Response(status_code=499)` without running the
    query. A Dashboard that aborted a superseded load must not leave stale
    24 h queries queued.

- [ ] **Step 1: Write the failing tests** (`tests/unit/test_heavy_query_cap.py`):

```python
"""Heavy Dashboard aggregations run one at a time per kind, bounding event-loop load."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest

from rfobserver.config import AppSettings
from rfobserver.web.app import create_app

_RANGE = {"since": "2026-09-13T00:00:00Z", "until": "2026-09-14T00:00:00Z"}


class _SlowDB:
    def __init__(self) -> None:
        self.active = {"wf": 0, "stats": 0}
        self.max_active = {"wf": 0, "stats": 0, "total": 0}

    async def _run(self, kind: str) -> None:
        self.active[kind] += 1
        self.max_active[kind] = max(self.max_active[kind], self.active[kind])
        self.max_active["total"] = max(self.max_active["total"], sum(self.active.values()))
        await asyncio.sleep(0.1)
        self.active[kind] -= 1

    async def query_avg_waterfall(self, **_: Any) -> dict[str, Any]:
        await self._run("wf")
        return {
            "bucket_sec": 0.0, "num_bins": 0, "min_db": 0.0, "max_db": 0.0,
            "total_windows": 0, "freq_start_hz": 0.0, "freq_step_hz": 0.0,
            "mode": 0, "buckets": [], "psd_rows": [],
        }

    async def query_avg_stats(self, **_: Any) -> dict[str, Any]:
        await self._run("stats")
        return {"bucket_sec": 0.0, "min_pwr": 0.0, "max_pwr": 0.0, "points": []}


@pytest.mark.asyncio
async def test_heavy_aggregations_capped_one_per_kind() -> None:
    app = create_app(AppSettings(_env_file=None))
    db = _SlowDB()
    app.state.database = db
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        reqs = []
        for i in range(4):
            # Distinct until per request so the waterfall cache never hits.
            params = {"since": _RANGE["since"], "until": f"2026-09-14T00:00:0{i}Z"}
            reqs.append(client.get("/api/averaged/waterfall", params=params))
            reqs.append(client.get("/api/averaged/stats", params=params))
        responses = await asyncio.gather(*reqs)
    assert all(r.status_code == 200 for r in responses), [r.status_code for r in responses]
    assert db.max_active["wf"] == 1, "waterfall aggregations must not overlap"
    assert db.max_active["stats"] == 1, "stats aggregations must not overlap"
    assert db.max_active["total"] == 2, "one waterfall and one stats may run together"
```

If `_RANGE` parsing or the packed waterfall response needs different empty-result
keys, read `api.py`'s `_waterfall_cached` and `_parse_range` and adjust the fake's
return values to what they accept. The concurrency assertions must stay unchanged.

- [ ] **Step 2: Run it and verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_heavy_query_cap.py -v -p no:cacheprovider`
Expected: FAIL on `max_active["wf"] == 1` (it is 4 today).

- [ ] **Step 3: Implement.** In `create_app` next to the other `app.state` initializers:

```python
    # One heavy Dashboard aggregation of each kind at a time. On a field-size DB a
    # 24 h waterfall decodes ~74k PSD blobs on the shared event loop; four at once
    # pegged the loop and tripped the pipeline watchdog on nano-super. Extra tabs
    # wait their turn instead of starving the pipeline.
    app.state.waterfall_sem = asyncio.Semaphore(1)
    app.state.stats_sem = asyncio.Semaphore(1)
```

In `averaged_waterfall`, after the existing cache check:

```python
    async with request.app.state.waterfall_sem:
        if await request.is_disconnected():
            return Response(status_code=499)
        cached = _WATERFALL_CACHE.get(key)
        if cached is not None:
            return Response(content=cached, media_type="application/octet-stream")
        result = await db.query_avg_waterfall(...)  # unchanged arguments
    return Response(content=_waterfall_cached(key, result), media_type="application/octet-stream")
```

In `averaged_stats`, wrap the `query_avg_stats` await the same way (with the
`is_disconnected` early return, and no cache).

- [ ] **Step 4: Run the new and existing web tests on 3.11 and 3.10, then mypy**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_heavy_query_cap.py tests/unit/test_web_routes.py -q -p no:cacheprovider`, then the same with `$V310`, then `PYTHONPATH= .venv/bin/mypy src/rfobserver/`.
Expected: all pass, and mypy is clean.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/web/app.py src/rfobserver/web/routes/api.py tests/unit/test_heavy_query_cap.py
git commit -m "fix(web): run heavy Dashboard aggregations one at a time per kind

With the read connection split, one 24 h Dashboard tab no longer disturbs
the pipeline, but four concurrent tabs pegged the shared event loop at 100%
decoding waterfalls and still tripped the watchdog on nano-super. Cap the
waterfall and stats aggregations at one each (the load profile of the tab
that passes) and skip queued requests whose client already disconnected."
```

### Task 5: Hardware verification (controller-run)

- [ ] **Step 1:** Run the full CI set locally (see Global Constraints).
- [ ] **Step 2:** Deploy the branch head to nano-super with `git archive`. Copy the harness from `docs/debugging/2026-09-14_stall-safety-net-hardware-validation/`.
- [ ] **Step 3:** Start with the watchdog on. Run the pipeline for about 60 s so it writes real rows, then stop it and seed with `seed_windows.py data/rfobserver.db 74000 24`. Restart the pipeline.
- [ ] **Step 4:** Run `dash_load.py http://127.0.0.1:8888 1 60 24`, then `dash_load.py ... 4 120 24`. Pass criteria:
  - no "Watchdog:" lines
  - `NRestarts` unchanged
  - maximum `beacon_age` under 5 s
  - no `db-slow insert_*` lines over 2 s
  - loop lag recorded, expected at most about 2 s
  - Dashboard requests all return 200

  If it still trips, record the evidence and escalate to Cut 3a as a separate
  plan. Do not merge a branch that fails this check without telling the user.
- [ ] **Step 5:** Clean up the box: stop the unit, remove the worktree copy and the data.

### Task 6: Merge and record

- [ ] Merge into local `main` with `--no-ff` (no push). Update issue 1's status in the validation doc with the hardware numbers. Leave the doc uncommitted.
