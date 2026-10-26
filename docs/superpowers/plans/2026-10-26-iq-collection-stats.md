# IQ Collection Statistics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Count raw IQ recorded, IQ deleted automatically and live RF time per UTC hour in the sensor's database, and serve hourly, daily, range, 7-day-average and storage-snapshot figures over the API, with days in the sensor's time zone.

**Architecture:** One thread-safe in-memory accumulator (`CollectionStats`) that the recording-finalize path, the storage evictions and the receiver loop add to; a pipeline loop flushes it every 15 s into a `collection_hours` table with additive upserts. A route module reads the table and assembles local days from UTC hours with `zoneinfo`.

**Tech Stack:** Python 3.10+, aiosqlite, FastAPI, `zoneinfo` (stdlib), pytest / pytest-asyncio.

**Spec:** `docs/superpowers/specs/2026-10-26-iq-collection-stats-design.md`

## Global Constraints

- Raw IQ only: `.sc16` bytes (4 per sample); auto and manual by origin.
- Recorded figures never decrease: deletions are separate counters.
- No backfill: counting starts at the hour the table is first flushed (`collection_hours_since` in `config`).
- Streaming pipeline only; replay adds nothing (recorded or active).
- Deletion reasons counted: `cap` (`enforce_cap`) and `space` (`evict_until_free`); `_enforce_limit` (sweep pipeline) is not counted.
- `active_seconds` = durations of live chunks received.
- `TIMEZONE` setting, IANA name, default `UTC`; whole-hour offsets only.
- 7-day average is `null` until that many complete local days are counted.
- Server-to-server API: no CORS, no auth.
- Checks before every commit (CLAUDE.md): `ruff check src/ tests/`, `ruff format --check src/ tests/`, `PYTHONPATH= .venv/bin/mypy src/rfobserver/`, unit and integration pytest. No em-dashes; no Co-Authored-By lines.

## Review Focus

1. A day or range before `covered_since` (or straddling it): zeros with `covered: false`, never an error.
2. Bad input: malformed date, `end` before `start`, a range over 366 days, `days` < 1 or > 60: HTTP 400 with a reason.
3. Daylight-saving change days in `America/Denver` (2026-03-08 is 23 h, 2026-11-01 is 25 h): the day's hours are exactly those local hours.
4. An invalid `TIMEZONE` in `.env` must not stop the sensor from starting: it logs a warning and uses `UTC`, and responses report the zone actually used. Config apply rejects it with 400.
5. A clean shutdown flushes pending counts; a restart keeps `collection_hours_since` and adds to the same hour rows.

---

### Task 1: Accumulator and table

**Files:**
- Create: `src/rfobserver/storage/collection.py`
- Modify: `src/rfobserver/storage/database.py` (schema string near `detection_hours`, new methods beside `store_detection_hours`)
- Test: `tests/unit/test_collection_stats.py`

**Interfaces:**
- Produces:
  - `FIELDS: tuple[str, ...]` = (`auto_captures`, `manual_captures`, `auto_bytes`, `manual_bytes`, `auto_seconds`, `manual_seconds`, `deleted_cap_captures`, `deleted_cap_bytes`, `deleted_space_captures`, `deleted_space_bytes`, `active_seconds`)
  - `class CollectionStats` (thread-safe, a `threading.Lock`):
    - `add_recorded(origin: str, start_ts: float, stop_ts: float, nbytes: int) -> None` (origin `"auto"`/`"manual"`; bytes and seconds split over the hours in proportion to time, the count to the start hour; integer bytes so the parts sum exactly to `nbytes`)
    - `add_deleted(reason: str, nbytes: int, at_ts: float) -> None` (reason `"cap"`/`"space"`, anything else ignored)
    - `add_active(seconds: float, at_ts: float) -> None`
    - `take() -> dict[int, dict[str, float]]` (pending deltas by hour epoch, cleared)
    - `async flush(db: SensorDatabase, now: float) -> None` (on first use sets `COLLECTION_SINCE_KEY` to the hour of `now` if absent; writes `take()`; on a DB error puts the deltas back)
  - `COLLECTION_SINCE_KEY = "collection_hours_since"` in `storage/database.py`
  - `SensorDatabase.add_collection_hours(rows: dict[int, dict[str, float]]) -> None` (one `INSERT ... ON CONFLICT(hour) DO UPDATE SET f = f + excluded.f` per row)
  - `SensorDatabase.collection_hours(since: int, until: int) -> list[dict[str, Any]]` (`hour` plus every field, `since <= hour < until`, ordered)

Table: `collection_hours(hour INTEGER PRIMARY KEY, <FIELDS> ... NOT NULL DEFAULT 0)`; counts and bytes `INTEGER`, seconds `REAL`. Not added to `_RETENTION_TABLES`.

- [ ] **Step 1: Write the failing tests**

```python
H = 1_792_800_000  # an hour start (multiple of 3600)

def test_capture_within_an_hour():
    cs = CollectionStats()
    cs.add_recorded("auto", H + 60, H + 70, 4_000)
    assert cs.take() == {H: {"auto_captures": 1, "auto_bytes": 4_000, "auto_seconds": 10.0}}

def test_capture_spanning_two_hours_splits_by_time():
    cs = CollectionStats()
    cs.add_recorded("manual", H + 3590, H + 3610, 2_000)
    d = cs.take()
    assert d[H] == {"manual_captures": 1, "manual_bytes": 1_000, "manual_seconds": 10.0}
    assert d[H + 3600] == {"manual_bytes": 1_000, "manual_seconds": 10.0}

def test_deleted_and_active_go_to_their_hour_and_unknown_reason_is_ignored():
    cs = CollectionStats()
    cs.add_deleted("cap", 500, H + 5)
    cs.add_deleted("space", 300, H + 3605)
    cs.add_deleted("user", 999, H + 5)
    cs.add_active(0.25, H + 5)
    d = cs.take()
    assert d[H] == {"deleted_cap_captures": 1, "deleted_cap_bytes": 500, "active_seconds": 0.25}
    assert d[H + 3600] == {"deleted_space_captures": 1, "deleted_space_bytes": 300}

def test_concurrent_adds_lose_nothing():
    # 8 threads x 10_000 add_active(0.001) -> 80.0 s in one hour

async def test_flush_sets_since_once_and_adds_to_rows(db):
    # flush at H+100 sets since=H; a second CollectionStats flushing more into H adds to the row;
    # since is unchanged by the second flush; db.collection_hours(H, H+3600)[0]["auto_bytes"] is the sum

async def test_flush_keeps_deltas_when_the_write_fails(db, monkeypatch):
    # add_collection_hours raises once -> the next flush writes the same totals
```

- [ ] **Step 2: Run** `PYTHONPATH= .venv/bin/pytest tests/unit/test_collection_stats.py -q`. Expected: FAIL (import error).
- [ ] **Step 3: Implement `storage/collection.py` and the database table and methods** as in Interfaces.
- [ ] **Step 4: Run** the same command. Expected: all pass.
- [ ] **Step 5: Commit** `storage/collection.py`, `storage/database.py`, the test: "collection: hourly IQ collection counters and their table".

### Task 2: Producers and the flush loop

**Files:**
- Modify: `src/rfobserver/storage/local.py` (`_delete_capture`, `enforce_cap`, `evict_until_free`, new attribute)
- Modify: `src/rfobserver/pipeline/streaming.py` (constructor kwarg; recording finalize beside `insert_iq_capture` ~line 2585; receiver loop after `recv_chunk` ~line 1601)
- Modify: `src/rfobserver/pipeline/app.py` (create, wire, flush loop beside `_detection_hours_loop` ~line 401, final flush on shutdown, `app.state.collection_stats`)
- Test: `tests/unit/test_collection_wiring.py`

**Interfaces:**
- Consumes: `CollectionStats` (Task 1).
- Produces:
  - `LocalStorage.on_deleted: Callable[[str, int], None] | None` (reason, `.sc16` bytes), called by `_delete_capture(path, reason: str | None = None)` when `reason` is set; the `.sc16` size is taken with `stat` before unlinking (0 if gone). `enforce_cap` passes `"cap"`, `evict_until_free` passes `"space"`, `_enforce_limit` passes nothing.
  - `StreamingProcessor(..., collection: CollectionStats | None = None)`: at finalize, unless replay, `collection.add_recorded(origin, start_dt.timestamp(), stop_dt.timestamp(), total_samples * 4)` with the same `origin`, `start_dt`, `stop_dt` as the `iq_captures` row; in the receiver loop, unless replay, `collection.add_active(n / sample_rate, time.time())` per chunk with `n > 0`.
  - `pipeline/app.py`: `_collection_loop(stats, db)` flushes every `COLLECTION_FLUSH_SEC = 15`; after the workers stop, one final `await stats.flush(db, time.time())`.

- [ ] **Step 1: Write the failing tests**

```python
def test_cap_eviction_reports_sc16_bytes(tmp_path):
    # LocalStorage with a tiny cap; two auto captures (.sc16 of 4000 and 8000 bytes plus a .json);
    # on_deleted records calls; enforce_cap() -> [("cap", 4000)]

def test_free_space_eviction_reports_space(tmp_path):
    # evict_until_free with free_bytes stub -> reasons == ["space"]

def test_sweep_limit_eviction_is_not_reported(tmp_path):
    # _enforce_limit(incoming) deletes, on_deleted not called

def test_finalize_adds_the_capture_and_replay_does_not(tmp_path):
    # drive the processor's finalize (as tests/unit/test_recording_gaps.py does) with a CollectionStats;
    # take() has the capture's bytes == total_samples * 4 under its origin; replay_mode=True -> {}

def test_receiver_loop_adds_live_seconds(tmp_path):
    # one mock chunk of N samples at rate R -> active_seconds == N / R in the current hour
```

- [ ] **Step 2: Run** `PYTHONPATH= .venv/bin/pytest tests/unit/test_collection_wiring.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement** the hooks and the loop as in Interfaces.
- [ ] **Step 4: Run** the new tests and `tests/unit/test_storage_local*.py tests/unit/test_recording_gaps.py`. Expected: all pass.
- [ ] **Step 5: Commit**: "collection: count recorded IQ, automatic deletions and live time".

### Task 3: Time-zone setting

**Files:**
- Modify: `src/rfobserver/config.py` (setting), `src/rfobserver/web/routes/config.py` (apply mapping near `"sensor_name"`)
- Create in `src/rfobserver/storage/collection.py`: zone helpers
- Test: `tests/unit/test_collection_days.py`

**Interfaces:**
- Produces:
  - `AppSettings.TIMEZONE: str = "UTC"`
  - `whole_hour_zone(name: str) -> ZoneInfo` (raises `ValueError` for an unknown zone, or one whose UTC offset on any day of the current and next year is not a whole number of hours)
  - `sensor_zone(settings) -> ZoneInfo` (the setting if valid, else logs a warning once and returns `ZoneInfo("UTC")`)
  - `local_day_bounds(day: date, tz: ZoneInfo) -> tuple[int, int]` (UTC epoch seconds of the local midnight and the next local midnight)
  - Config apply key `"timezone"` -> `TIMEZONE`, validated with `whole_hour_zone`, 400 when invalid.

- [ ] **Step 1: Write the failing tests**

```python
def test_day_bounds_utc_and_denver():
    assert local_day_bounds(date(2026, 10, 26), ZoneInfo("UTC")) == (1_792_972_800, 1_793_059_200)
    lo, hi = local_day_bounds(date(2026, 3, 8), ZoneInfo("America/Denver"))
    assert hi - lo == 23 * 3600
    lo, hi = local_day_bounds(date(2026, 11, 1), ZoneInfo("America/Denver"))
    assert hi - lo == 25 * 3600

def test_whole_hour_zone_rejects_fractional_and_unknown():
    whole_hour_zone("America/Denver")
    for bad in ("Asia/Kolkata", "Not/AZone"):
        with pytest.raises(ValueError):
            whole_hour_zone(bad)

def test_sensor_zone_falls_back_to_utc(caplog):
    # AppSettings(_env_file=None, TIMEZONE="Bad/Zone") constructs fine; sensor_zone(...) is UTC with a warning

def test_config_apply_timezone(client):
    # POST /config/apply {"timezone": "America/Denver"} -> 200 and persisted; {"timezone": "Asia/Kolkata"} -> 400
```

- [ ] **Step 2: Run** `PYTHONPATH= .venv/bin/pytest tests/unit/test_collection_days.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement** as in Interfaces (check `config.py`'s apply path for how other keys validate and persist).
- [ ] **Step 4: Run** the same command. Expected: all pass.
- [ ] **Step 5: Commit**: "collection: TIMEZONE setting and local day bounds".

### Task 4: The API

**Files:**
- Create: `src/rfobserver/web/routes/collection.py` (router mounted at `/api/collection` in `web/app.py` beside `api.router`)
- Modify: `src/rfobserver/storage/local.py` (snapshot method)
- Test: `tests/unit/test_collection_api.py`

**Interfaces:**
- Consumes: `SensorDatabase.collection_hours`, `COLLECTION_SINCE_KEY`, `FIELDS`, `sensor_zone`, `local_day_bounds`; `request.app.state.database`, `.settings`, `.local_storage`.
- Produces:
  - `LocalStorage.snapshot() -> dict[str, int]`: `auto_captures`, `manual_captures`, `auto_bytes`, `manual_bytes` (footprint, from the index under its lock), `cap_bytes`.
  - Routes (all add `timezone` and `covered_since`, ISO local or null):
    - `GET /hourly?since=&until=` -> `{"hours": [{"hour_utc", "hour_local", <FIELDS>}...]}`; range at most 31 days.
    - `GET /daily?date=` -> one day object; `GET /daily?start=&end=` -> `{"days": [...], "total": {...}}` (inclusive, at most 366 days). A day object: `date`, `complete` (its end is in the past), `covered` (`covered_since <= day start`), every field, plus `captures`, `bytes`, `seconds` (auto + manual).
    - `GET /average?days=7` (1..60): the last `days` complete local days; `average` = per-field mean over them, or `null` when fewer than `days` complete covered days exist; `days_counted`.
    - `GET /storage` -> `snapshot()` plus `volume_free_bytes`, `volume_total_bytes` (`shutil.disk_usage(storage_path)`).
  - 503 when the database is not connected.

- [ ] **Step 1: Write the failing tests** (seed `collection_hours` and `COLLECTION_SINCE_KEY` directly)

```python
async def test_daily_sums_local_hours_in_denver(app_denver):
    # rows at the UTC hours of 2026-10-25 06:00Z..2026-10-26 05:00Z; one row outside
    # GET /api/collection/daily?date=2026-10-25 -> bytes == sum of the 24 rows, complete True, covered True

async def test_dst_day_has_23_hours(app_denver):
    # one row per hour 2026-03-08 07:00Z..2026-03-09 06:00Z (23 rows, plus one each side)
    # daily?date=2026-03-08 -> active_seconds == 23 * 3600

async def test_range_total_and_days(app_utc):
    # start=2026-10-20&end=2026-10-22 -> 3 days, total == their sum

async def test_before_coverage_is_zero_and_not_covered(app_utc):
    # since = 2026-10-21T12:00Z; date=2026-10-20 -> covered False, bytes 0; 2026-10-21 -> covered False

async def test_average_null_until_seven_complete_days(app_utc):
    # 6 complete covered days -> average None, days_counted 6; 7 -> mean of the 7

@pytest.mark.parametrize("q", ["date=2026-13-01", "start=2026-10-05&end=2026-10-01",
                               "start=2025-01-01&end=2026-10-01", "x=1"])
async def test_bad_daily_queries_are_400(app_utc, q): ...

async def test_average_days_out_of_range_is_400(app_utc): ...  # days=0, days=61

async def test_storage_snapshot(app_utc):
    # local_storage with one auto and one manual capture -> counts, footprint bytes, cap_bytes, volume figures > 0

async def test_hourly_lists_hours_with_local_time(app_denver): ...
```

- [ ] **Step 2: Run** `PYTHONPATH= .venv/bin/pytest tests/unit/test_collection_api.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement** the routes and `snapshot()`; aggregate days in Python from one `collection_hours` read spanning the requested range.
- [ ] **Step 4: Run** the same command. Expected: all pass.
- [ ] **Step 5: Full checks** (Global Constraints list). Expected: all clean.
- [ ] **Step 6: Commit**: "api: collection statistics by hour, day, range, 7-day average and storage".
