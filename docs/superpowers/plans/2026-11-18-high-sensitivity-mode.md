# High-sensitivity Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A sensor (or the whole fleet) can lower its burst threshold, run the tone detector and raise the isolation budget for at most 5 minutes, tagging what it finds and summarising each run.

**Architecture:** A pure, clock-injected `HighSensitivity` controller owns the run (id, start, end, threshold) and is the overlay the pipeline reads: the burst thread swaps the threshold into its running detector, the isolation gate reads its per-second cap through it, and the event loop starts and stops a live tone detector on its transitions. Runs and their summaries persist in SQLite; the API, the schedule flag and the fleet hub only call the controller.

**Tech Stack:** Python 3.10+, FastAPI, aiosqlite, pytest/pytest-asyncio, httpx (fleet hub).

**Spec:** `docs/superpowers/specs/2026-11-18-high-sensitivity-mode-design.md`

## Global Constraints

- Run length 1..300 s; a same-id extension never moves the end past start + 300 s.
- `HS_BURST_THRESHOLD_DB` default 15.0, allowed 10..30 (per-call `threshold_db` same range).
- `HS_ISOLATION_MAX_PER_SEC` default 60; `ISOLATION_LONG_MAX_PER_SEC` unchanged.
- `HS_LEAD_SEC` default 2.0 (schedule flag: start this long before the capture).
- Never written to `.env` or the stored settings; a restart never resumes a run.
- Keep the last 50 run summaries.
- No emojis, no em-dashes in code, UI or docs. Apple-style UI (match config.html).
- Checks before every commit: `ruff check src/ tests/`, `ruff format --check src/ tests/`, mypy (both venvs), unit, integration.

## Review Focus

1. A run that expires while the burst thread is mid-grid: the routine threshold is back within one grid, and no row produced after the end is tagged.
2. A restart mid-run: the sensor starts in routine mode and the run's stored summary is closed with `end_reason "restart"`.
3. Fleet start where a sensor times out but did enter: rollback also sends DELETE to timed-out sensors, not only to the ones that answered 200.
4. The routine tone detector already on: HS start does not create a second detector and HS end does not stop it.
5. A retune or pipeline reconfigure during a run ends it with `end_reason "retune"` and restores routine values.

Each is pinned by a test in the owning task below.

---

### Task 1: Settings and the `HighSensitivity` controller

**Files:**
- Modify: `src/rfobserver/config.py` (after `ISOLATION_LONG_MAX_PER_SEC`)
- Create: `src/rfobserver/pipeline/high_sensitivity.py`
- Test: `tests/unit/test_high_sensitivity.py`

**Interfaces:**
- Produces:
  - Settings `HS_BURST_THRESHOLD_DB: float = 15.0`, `HS_ISOLATION_MAX_PER_SEC: int = 60`, `HS_LEAD_SEC: float = 2.0`.
  - `class HSError(ValueError)` with `.status: int` (400 bad input, 409 conflict, 404 not running).
  - `@dataclass class HSRun: id: str; source: str; start: float; end: float; threshold_db: float; ended: float | None = None; end_reason: str | None = None`.
  - `class HighSensitivity(settings, clock: Callable[[], float] = time.time)`:
    - `start(seconds: float, *, run_id: str | None = None, threshold_db: float | None = None, source: str = "api") -> HSRun`
    - `stop(run_id: str, reason: str = "deleted") -> HSRun`
    - `end_now(reason: str) -> HSRun | None` (ends whatever is running; used for retune/stop)
    - `current() -> HSRun | None` (expires the run first if `clock() >= end`, with reason `"expired"`)
    - `burst_threshold_db() -> float` and `isolation_max_per_sec() -> int` (HS values while a run is on, else the settings')
    - `listeners: list[Callable[[str, HSRun], None]]`, called with `"start"` / `"end"`.

- [ ] **Step 1: Write the failing tests**

```python
def test_start_overlays_and_expiry_restores():
    clk = Clock(1000.0); hs = HighSensitivity(_settings(), clock=clk)
    run = hs.start(120)
    assert hs.burst_threshold_db() == 15.0 and hs.isolation_max_per_sec() == 60
    clk.t = 1000.0 + 120
    assert hs.current() is None
    assert hs.burst_threshold_db() == 30.0 and hs.isolation_max_per_sec() == 20
    assert run.end_reason == "expired"

def test_seconds_and_threshold_are_bounded():
    hs = HighSensitivity(_settings(), clock=Clock(0.0))
    for bad in (0, 301, -1):
        with pytest.raises(HSError) as e: hs.start(bad)
        assert e.value.status == 400
    for bad in (9.9, 30.1):
        with pytest.raises(HSError): hs.start(60, threshold_db=bad)

def test_second_run_conflicts_but_same_id_extends_to_300_s():
    clk = Clock(0.0); hs = HighSensitivity(_settings(), clock=clk)
    hs.start(100, run_id="a")
    with pytest.raises(HSError) as e: hs.start(60, run_id="b")
    assert e.value.status == 409
    clk.t = 50.0
    assert hs.start(300, run_id="a").end == 300.0  # start 0 + 300, not 50 + 300

def test_listeners_see_start_and_end_once():
    seen = []; clk = Clock(0.0); hs = HighSensitivity(_settings(), clock=clk)
    hs.listeners.append(lambda ev, run: seen.append((ev, run.id, run.end_reason)))
    hs.start(10, run_id="x"); hs.end_now("retune"); hs.end_now("retune")
    assert seen == [("start", "x", None), ("end", "x", "retune")]
```

- [ ] **Step 2: Run them; expect `ModuleNotFoundError` for `rfobserver.pipeline.high_sensitivity`**

Run: `PYTHONPATH=src .venv310/bin/python -m pytest tests/unit/test_high_sensitivity.py -q`

- [ ] **Step 3: Implement the settings and the controller.** Thread-safe (a `threading.Lock`): the burst thread and isolation thread read it, the event loop writes it. Generated ids: `uuid4().hex[:12]`. Listener exceptions are logged, never raised.

- [ ] **Step 4: Run them; expect PASS**

- [ ] **Step 5: Commit** `pipeline: high-sensitivity controller and settings`

---

### Task 2: Storage: `hs_run` column, `hs_runs` table, routine counts exclude HS rows

**Files:**
- Modify: `src/rfobserver/storage/database.py` (SCHEMA, `_DETECTION_ATTRIBUTION_COLUMNS` neighbour, `_detection_row`, `_INSERT_DETECTION_SQL`, the hourly query at the `COUNT(*) FROM detections WHERE start_time >= ?` site, a new `_migrate_hs_columns`)
- Test: `tests/unit/test_database.py`

**Interfaces:**
- Produces on `SensorDatabase`:
  - detections accept an optional `"hs_run": str | None` key on insert.
  - `async def hs_run_save(run: dict[str, Any]) -> None` (upsert by `id`; keeps the newest 50 by `start`).
  - `async def hs_runs(limit: int = 50) -> list[dict[str, Any]]`, `async def hs_run_get(run_id: str) -> dict[str, Any] | None`.
  - `async def hs_run_close_open(reason: str, ended: float) -> int` (closes rows with no `ended`; used at startup).
  - `async def hs_run_counts(run_id: str) -> dict[str, Any]`: `{"detections": int, "ssm": {src_id: n}, "tones": [{"freq_hz", "level_db", "snr_db", "duration_ms"}]}` from rows with that `hs_run` (SSM `src_id` via `json_extract(attribution, '$.frames[0].src_id')`, tones from `model = 'Tone'`).
- `hs_runs` columns: `id TEXT PRIMARY KEY, source TEXT, start REAL, end REAL, ended REAL, end_reason TEXT, threshold_db REAL, summary TEXT` (JSON).

- [ ] **Step 1: Failing tests**

```python
async def test_hourly_counts_leave_out_high_sensitivity_rows(db):
    await db.insert_detections([_det(t0), {**_det(t0), "burst_id": "hs1", "hs_run": "r1"}])
    rows = await db.count_detection_hours(since, until)  # the existing hourly-count method
    assert sum(n for *_, n in rows) == 1

async def test_hs_run_counts_ssm_and_tones(db): ...  # one SSM row src 0200574952470001, one Tone row
    assert c == {"detections": 2, "ssm": {"0200574952470001": 1}, "tones": [ {...} ]}

async def test_hs_runs_keep_the_newest_50_and_close_open_ones(db): ...
    assert len(await db.hs_runs()) == 50
    assert await db.hs_run_close_open("restart", ended=9.0) == 1

async def test_existing_db_gains_hs_run_column(tmp_path): ...  # create with old SCHEMA, reopen, insert with hs_run
```

(Use the existing hourly-count method's real name at the line 2168 site; `count_detection_hours` above stands for it.)

- [ ] **Step 2: Run; expect failures on the unknown key / missing methods**
- [ ] **Step 3: Implement.** Hourly query gains `AND hs_run IS NULL`, and so do the collection-stats queries behind `/api/collection/*`. `query_detections` (and `/api/detections.json`) gain an `hs_run: str | None` filter (`"any"` = only tagged rows). Index `detections(hs_run)` partial `WHERE hs_run IS NOT NULL`. Add tests for both: collection stats leave a tagged row out, and `hs_run="r1"` returns only that run's rows.
- [ ] **Step 4: Run `tests/unit/test_database.py`; expect PASS**
- [ ] **Step 5: Commit** `storage: hs_run tag, hs_runs table; routine hourly counts leave HS rows out`

---

### Task 3: Pipeline overlay: threshold swap, isolation cap, live tone detector, lifecycle

**Files:**
- Modify: `src/rfobserver/processing/rolling_burst.py` (add `set_config`)
- Modify: `src/rfobserver/pipeline/isolation.py` (`IsolationStage.__init__` gains `max_per_sec: Callable[[], int] | None = None`; `_gate` uses it instead of `s.ISOLATION_MAX_PER_SEC` when given)
- Modify: `src/rfobserver/pipeline/streaming.py` (`_burst_detection_loop`, `_make_burst_config`, isolation construction, tone start/stop, `reconfigure`, stop path)
- Test: `tests/unit/test_rolling_burst.py`, `tests/unit/test_isolation_stage.py`, new `tests/unit/test_high_sensitivity_streaming.py`

**Interfaces:**
- Consumes: Task 1 `HighSensitivity`.
- Produces:
  - `RollingBurstDetector.set_config(config: BurstDetectionConfig) -> None` (keeps the window and tracks).
  - `StreamingProcessor.high_sensitivity: HighSensitivity` (created in `__init__`).
  - `StreamingProcessor._hs_tone_started: bool` (true only when HS created the tone detector).

Behaviour:
- Burst thread: each grid compares `hs.burst_threshold_db()` with the config's; on change, `set_config(replace(config, threshold_high_db=...))` on the running detector. It records which run produced each completed batch (the run id in force when `feed` returned it) and passes it along the existing handoff tuple as a third element.
- Isolation: `max_per_sec=self.high_sensitivity.isolation_max_per_sec`.
- Event loop listener on `"start"`: if no tone detector is running, start one (as `_start_tone_detector` does, ignoring `TONE_DETECTOR_ENABLED`) and set `_hs_tone_started`; on `"end"`: if `_hs_tone_started`, stop it via `_stop_tone_detector()` and clear the flag. Listener calls arrive on any thread: hop to the loop with `call_soon_threadsafe`.
- `reconfigure()` and a retune: `hs.end_now("retune")`. Pipeline stop: `hs.end_now("stopped")`.
- A periodic check on the event loop (the result consumer, once per result) calls `hs.current()` so expiry fires even with no API traffic.

- [ ] **Step 1: Failing tests**

```python
def test_set_config_changes_threshold_without_losing_the_window(): ...
    # feed half a burst at 30 dB config, set_config(15 dB), feed the rest: one burst emitted, full length

def test_gate_reads_the_cap_through_the_callable(tmp_path):
    cap = {"n": 2}
    st, *_ = _stage(tmp_path, max_per_sec=lambda: cap["n"])  # extend _stage to pass it through
    ... 3 cands -> 2 picked; cap["n"] = 3 -> next window 3 picked

async def test_hs_start_runs_a_tone_detector_and_end_stops_only_its_own(proc):
    proc.high_sensitivity.start(30); await _settle()
    assert proc.tone_detector_status()["enabled"] is True
    proc.high_sensitivity.end_now("deleted"); await _settle()
    assert proc.tone_detector_status()["enabled"] is False

async def test_routine_tone_detector_survives_hs_end(proc_with_tone_on): ...   # Review Focus 4

async def test_reconfigure_ends_the_run_with_retune(proc): ...                 # Review Focus 5

def test_threshold_reverts_within_one_grid_after_expiry(): ...                 # Review Focus 1
    # drive _burst_detection_loop's per-grid check with a fake clock: grid k under HS, clock passes end,
    # grid k+1 config threshold == 30 and its bursts carry hs run None
```

Use the existing mock-receiver fixtures in `tests/unit/test_tone_detector_streaming.py` for `proc`.

- [ ] **Step 2: Run the four files; expect failures (no `set_config`, unknown kwarg, no `high_sensitivity`)**
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run them, then `tests/unit/` whole; expect PASS**
- [ ] **Step 5: Commit** `pipeline: high-sensitivity overlay (threshold, isolation cap, tone detector)`

---

### Task 4: Tagging and run summaries

**Files:**
- Modify: `src/rfobserver/pipeline/streaming.py` (the detection row build near `"detection_timestamp": burst.detection_timestamp`, the HS listeners)
- Modify: `src/rfobserver/pipeline/tone_detector.py` (`ToneDetector.__init__` gains `hs_run: Callable[[], str | None] = lambda: None`; `_insert` adds `"hs_run"`)
- Test: `tests/unit/test_high_sensitivity_streaming.py`, `tests/unit/test_tone_detector_pipeline.py`

**Interfaces:**
- Consumes: Task 2 storage methods, Task 3 handoff run id.
- Produces: `StreamingProcessor.hs_summary(run: HSRun) -> Awaitable[dict[str, Any]]` with keys `id, source, start, end, ended, end_reason, threshold_db, detections, ssm, tones, overflow_events, overflow_lost_samples, recv_stalls, isolation` (isolation = delta of `IsolationStats.snapshot()` keys received, picked, gated_out, copies).

Behaviour:
- Burst rows carry the run id from the handoff (Task 3); tone rows carry the run in force when the tone ended.
- On `"start"`: snapshot receiver `overflow_events`, `overflow_lost_samples`, `_recv_stalls`, isolation stats; save the run (`hs_run_save`) with no `ended`.
- On `"end"`: after 3 s (late rows land), build the summary from the deltas and `hs_run_counts`, save it.
- Startup: `hs_run_close_open("restart", now)` before the pipeline runs (Review Focus 2).

- [ ] **Step 1: Failing tests**

```python
async def test_rows_made_under_hs_are_tagged_and_others_not(proc): ...
async def test_summary_has_counter_deltas_and_counts(proc): ...
    assert s["overflow_events"] == 2 and s["detections"] == 3 and s["end_reason"] == "deleted"
async def test_startup_closes_a_run_left_open_by_a_restart(tmp_path): ...     # Review Focus 2
```

- [ ] **Step 2: Run; expect failures**
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run; expect PASS**
- [ ] **Step 5: Commit** `pipeline: tag high-sensitivity detections, keep a summary per run`

---

### Task 5: Sensor API, health and Config status

**Files:**
- Create: `src/rfobserver/web/routes/high_sensitivity.py` (router, prefix `/api/high-sensitivity`, registered in `web/app.py` before `api.router`)
- Modify: `src/rfobserver/web/app.py` (health `body["high_sensitivity"]`)
- Modify: `src/rfobserver/web/templates/config.html` (one status line under the Tone Detector card: "High sensitivity until 14:02:31 (15 dB)" or "Off")
- Create: `docs/high-sensitivity-api.md`
- Test: `tests/unit/test_high_sensitivity_api.py`

**Interfaces:**
- Consumes: `proc.high_sensitivity`, `proc.hs_summary`, Task 2 reads.
- Produces routes (JSON):
  - `POST ""` body `{"seconds", "id"?, "threshold_db"?}` -> `200 {"run": {...}}`; `400`/`409` from `HSError.status`; `503 {"detail": "pipeline not running"}`.
  - `DELETE "/{run_id}"` -> `200 {"run"}` or `404`.
  - `GET ""` -> `{"run": current or null, "runs": [summaries]}`; `GET "/{run_id}"` -> summary or `404`.
  - Run JSON: `id, source, start, end` as ISO ms strings (`schedules.model.iso_ms`), `threshold_db`, `seconds_left`.

- [ ] **Step 1: Failing tests** for each status code above, and health carrying `{"active": bool, "run": ...}`.
- [ ] **Step 2: Run; expect 404s on the missing routes**
- [ ] **Step 3: Implement routes, health, the status line, the API doc**
- [ ] **Step 4: Run; expect PASS**
- [ ] **Step 5: Commit** `web: high-sensitivity API, health and Config status`

---

### Task 6: Schedule flag

**Files:**
- Modify: `src/rfobserver/schedules/model.py` (`Schedule` gains `high_sensitivity: bool = False`, `hs_threshold_db: float | None = None`; `parse_schedule` validates; `to_json`/`from_json`)
- Modify: `src/rfobserver/pipeline/streaming.py` (event-loop check, once per result: for flagged schedules, a run starting within `HS_LEAD_SEC` starts HS with `run_id=f"{schedule_id}-{k}"`, `source="schedule"`, `seconds = lead + duration`; the scheduled capture `.json` gets `"hs_run"`)
- Test: `tests/unit/test_schedules_model.py`, `tests/unit/test_high_sensitivity_streaming.py`

**Interfaces:**
- Consumes: `runs_between(s, t0_ms, t1_ms, limit)`, `HighSensitivity.start`.

- [ ] **Step 1: Failing tests**

```python
def test_flagged_schedule_longer_than_the_hs_limit_is_refused():
    with pytest.raises(ScheduleError): parse_schedule({"start": ..., "duration_ms": 299_000, "high_sensitivity": True}, ...)  # 2 s lead + 299 s > 300
def test_flag_round_trips(): ...
async def test_flagged_run_starts_hs_lead_seconds_early_and_ends_with_the_capture(proc): ...
async def test_flagged_run_while_another_hs_run_is_on_is_skipped_and_reported(proc): ...
```

A conflict (another run on) does not block the capture; the capture `.json` records `"hs_run": null` and `"hs_error": "conflict"`.

- [ ] **Step 2: Run; expect failures**
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run; expect PASS**
- [ ] **Step 5: Commit** `schedules: high_sensitivity flag starts the mode around each run`

---

### Task 7: Fleet endpoints with rollback

**Files:**
- Modify: `src/rfobserver/fleet/app.py` (beside `/fleet/api/schedules`)
- Test: `tests/unit/test_fleet_high_sensitivity.py` (use the existing fleet test doubles for `hub.send`/`hub.get`)

**Interfaces:**
- Consumes: `hub.send(sensor, method, path, body) -> Reply`, `hub.get(sensor, "health", cache=False)`, `registry.all()`, `registry.get(id)`.
- Produces:
  - `POST /fleet/api/high-sensitivity` body `{"seconds", "sensors"?, "min_sensors"?, "threshold_db"?, "id"?}`.
    1. Targets: `sensors` or every enabled sensor. Probe health in parallel, timeout 3 s: no answer -> `offline`.
    2. `POST api/high-sensitivity` with one id to the rest in parallel, timeout 5 s.
    3. Success when every reachable sensor answered 200 and the count is at least `min_sensors` (default: all reachable, at least 1). Otherwise DELETE the run on every sensor that answered 200 **or timed out** (Review Focus 3), mark them `rolled_back` (or `rollback_failed`), answer `409`.
    4. Body: `{"id", "ok": bool, "results": {sensor_id: {"state": "entered"|"offline"|"failed"|"rolled_back"|"rollback_failed", "detail": str|None, "run": {...}|None}}}`; `200` on success.
  - `DELETE /fleet/api/high-sensitivity/{id}` -> per-sensor results, `200` if any ended.
  - `GET /fleet/api/high-sensitivity/{id}` -> `{sensor_id: summary or error}`.
  - `POST /fleet/api/schedules` already forwards unknown keys; add a test that `high_sensitivity` reaches the sensor payload.

- [ ] **Step 1: Failing tests**

```python
async def test_all_reachable_enter_offline_is_skipped(): ...      # 3 sensors, 1 offline -> 200, states entered x2, offline x1
async def test_one_refusal_rolls_back_the_others(): ...           # 409, DELETE sent to the 2 that entered
async def test_timeout_on_start_is_rolled_back_too(): ...         # Review Focus 3
async def test_min_sensors_not_met_rolls_back(): ...
async def test_all_offline_is_502(): ...
```

- [ ] **Step 2: Run; expect 404s**
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run; expect PASS**
- [ ] **Step 5: Commit** `fleet: high-sensitivity start with rollback, stop and summaries`

---

### Task 8: Whole-branch checks and docs

**Files:**
- Modify: `docs/high-sensitivity-api.md` (fleet section, schedule flag, examples)
- Modify: `docs/schedules-api.md` (field reference: `high_sensitivity`, `hs_threshold_db`)

- [ ] **Step 1: Run every check** (Global Constraints list). Expected: all pass.
- [ ] **Step 2: Mock-run once:** `PYTHONPATH= RFOBS_MOCK_RECEIVER=true RFOBS_WEB_PORT=8888 .venv310/bin/rfobserver run`, then `POST /api/high-sensitivity {"seconds": 10}`, `GET` it, wait 12 s, `GET` the summary (`end_reason "expired"`). Stop the server by port afterwards.
- [ ] **Step 3: Commit** `docs: high-sensitivity API and schedule flag`
