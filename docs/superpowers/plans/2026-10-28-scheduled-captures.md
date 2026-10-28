# Scheduled IQ Captures Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Schedule one-off and repeating IQ captures at a UTC instant to the ms, cut to the sample, recorded in parallel with any other capture, settable from the sensor UI/API and the fleet.

**Architecture:** A new `rfobserver.schedules` package holds the pure parts (schedule model and run math, the sample cutter) and the shared `ScheduleBook` (persisted in SQLite). Inside `StreamingProcessor`, a `ScheduleEngine` is fed every chunk by the receiver thread right after the ring write, cuts runs by stream position and capture time, and hands write ops to its own `ScheduledWriter` thread, independent of the manual/trigger recording state machine. Web routes (`/api/schedules`) and fleet routes (`/fleet/api/schedules`) sit on the book; the Captures pages get a Schedule card.

**Tech Stack:** Python 3.10+, FastAPI, aiosqlite (`SensorDatabase`), numpy, httpx (fleet), vanilla JS templates.

**Spec:** `docs/superpowers/specs/2026-10-28-scheduled-captures-design.md`

## Global Constraints

- Python >= 3.10 (Jetsons run 3.10); no new dependencies.
- The receiver thread never blocks on scheduling or writing (no blocking put, no disk I/O, no DB call on it).
- Manual/trigger recording behaviour, files and tests unchanged.
- SDR parameters are never changed by a schedule.
- All API and UI times are UTC (ISO 8601 with ms, `Z`); the UI also shows the sensor's local time (zone from `app.state.collection_zone`), entry is UTC only.
- Scheduled files live in `manual/` (never evicted), origin `"scheduled"` in iq_captures, the captures list and collection stats.
- Lost samples in scheduled files are zero-filled and listed in `gaps`; a retune ends the file (`partial`, reason `retuned`).
- Limits (spec leaves them to the builder, decided here): `MAX_DURATION_MS = 600_000`; `MAX_CONCURRENT_RUNS = 2`; keep the newest `RUNS_KEPT = 500` runs per schedule; lookback grace = `settings.TRIGGER_PRE_SEC` (the ring always holds at least that).
- Copy rules: no emojis, no em-dashes in code, UI or docs; Apple-style UI; pages work offline.
- Checks before every commit: `ruff check src/ tests/`, `ruff format --check src/ tests/`, `PYTHONPATH= .venv310/bin/mypy src/rfobserver/`, unit and integration tests. Commit with `-c user.name="Oren Collaco" -c user.email="orencollaco97@gmail.com"`, explicit paths only, no Claude co-author line.
- Tests never touch the repo `.env` (`monkeypatch.chdir(tmp_path)` around config apply).

## Review Focus

1. Host clock steps (NTP correction) during a run: the cut must stay contiguous by stream position (only the first sample is placed by time); test a capture_time jump mid-run keeps the sample count exact.
2. Pipeline not running when a run is due (replay, inactive sensor, restart): run marked `missed` (reason), never recorded late; a processor stopped mid-run ends it `partial` (reason `stopped`) with a valid file.
3. Writer behind (slow disk): IQ ops dropped by the byte budget become zeros, file length stays exact, status `partial`; receiver thread never waits.
4. Many runs (`every_ms` small, long `until`): next-run computation is O(1) per schedule, missed marking after downtime is bounded (newest `RUNS_KEPT` only).
5. Fleet fan-out with one sensor down: other sensors get the schedule with the same id; the response lists the failure; a retry with the same id/body is a no-op on sensors that have it.

---

### Task 1: Schedule model and run math

**Files:**
- Create: `src/rfobserver/schedules/__init__.py` (docstring only), `src/rfobserver/schedules/model.py`
- Test: `tests/unit/test_schedule_model.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class Schedule: id: str; start_ms: int; duration_ms: int; every_ms: int | None; count: int | None; until_ms: int | None; label: str; source: str; created_ms: int`
  - `def parse_schedule(body: dict[str, Any], *, now_ms: int, grace_ms: int, source: str, new_id: Callable[[], str]) -> Schedule` raising `ScheduleError(detail: str)` (subclass of ValueError)
  - `def run_start_ms(s: Schedule, k: int) -> int`
  - `def total_runs(s: Schedule) -> int` (1 for one-off; `count`, or derived from `until_ms`)
  - `def first_run_from(s: Schedule, t_ms: int) -> int | None` (smallest k with run start >= t_ms, None if past the last)
  - `def runs_between(s: Schedule, t0_ms: int, t1_ms: int, limit: int) -> list[int]` (k with start in [t0, t1), newest `limit` only)
  - `def schedule_bytes(s: Schedule, rate_hz: float) -> int` (runs x duration x rate x 4)
  - `def to_json(s: Schedule) -> dict[str, Any]` (adds `start` ISO ms Z, `until` ISO or null)
  - constants `MAX_DURATION_MS = 600_000`, `ID_RE = r"^[A-Za-z0-9_-]{1,64}$"`

- [ ] **Step 1: Write the failing tests**

```python
def test_one_off_parses_iso_with_ms():
    s = parse_schedule({"start": "2026-10-28T18:30:00.250Z", "duration_ms": 2000},
                       now_ms=NOW, grace_ms=1000, source="api", new_id=lambda: "abc")
    assert s.start_ms == 1793212200250 and s.duration_ms == 2000 and total_runs(s) == 1
    assert to_json(s)["start"] == "2026-10-28T18:30:00.250Z"

def test_start_ms_and_offset_forms_agree():
    a = parse_schedule({"start_ms": 1793212200250, "duration_ms": 1}, ...)
    b = parse_schedule({"start": "2026-10-28T11:30:00.250-07:00", "duration_ms": 1}, ...)
    assert a.start_ms == b.start_ms

def test_repeat_instants_are_from_start():
    s = parse_schedule({"start_ms": T, "duration_ms": 500, "every_ms": 60000, "count": 3}, ...)
    assert [run_start_ms(s, k) for k in range(3)] == [T, T + 60000, T + 120000]
    assert first_run_from(s, T + 1) == 1 and first_run_from(s, T + 120001) is None

def test_until_bounds_runs():
    s = parse_schedule({"start_ms": T, "duration_ms": 500, "every_ms": 1000, "until": iso(T + 3000)}, ...)
    assert total_runs(s) == 3   # starts T, T+1000, T+2000 (T+3000 is not before until)

@pytest.mark.parametrize("body,detail", [
    ({"duration_ms": 1}, "start"),
    ({"start_ms": NOW - 1001, "duration_ms": 1}, "past"),
    ({"start_ms": T, "duration_ms": 0}, "duration_ms"),
    ({"start_ms": T, "duration_ms": 600_001}, "duration_ms"),
    ({"start_ms": T, "duration_ms": 500, "every_ms": 500, "count": 2}, "every_ms"),
    ({"start_ms": T, "duration_ms": 500, "every_ms": 1000}, "count or until"),
    ({"start_ms": T, "duration_ms": 500, "every_ms": 1000, "count": 2, "until": iso(T + 9000)}, "count or until"),
    ({"start_ms": T, "duration_ms": 500, "count": 2}, "every_ms"),
    ({"start": "2026-10-28 18:30", "duration_ms": 1}, "start"),          # no zone
    ({"id": "bad id!", "start_ms": T, "duration_ms": 1}, "id"),
])
def test_invalid_bodies(body, detail):
    with pytest.raises(ScheduleError, match=detail): parse_schedule(body, ...)

def test_runs_between_is_bounded_and_newest():
    s = parse_schedule({"start_ms": T, "duration_ms": 1, "every_ms": 10, "count": 10**9}, ...)
    ks = runs_between(s, T, T + 10**7, limit=500)
    assert len(ks) == 500 and ks[-1] == 10**6 - 1

def test_schedule_bytes():
    s = parse_schedule({"start_ms": T, "duration_ms": 1000, "every_ms": 2000, "count": 3}, ...)
    assert schedule_bytes(s, 1e6) == 3 * 1_000_000 * 4
```

(`NOW`, `T = NOW + 60_000`, `iso()` are test helpers; `...` stands for the same keyword arguments as the first test.)

- [ ] **Step 2: Run, expect FAIL** `PYTHONPATH= .venv310/bin/pytest tests/unit/test_schedule_model.py -q` (ModuleNotFoundError).
- [ ] **Step 3: Implement** `model.py`. Naive ISO strings (no zone) are rejected. `id` defaults to `new_id()`. `runs_between` computes k range arithmetically, never iterates over all runs.
- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Commit** `git add src/rfobserver/schedules/__init__.py src/rfobserver/schedules/model.py tests/unit/test_schedule_model.py && git commit -m "schedules: model, parsing and run instants"`

### Task 2: Persistence (schedules, runs, scheduled stats columns)

**Files:**
- Modify: `src/rfobserver/storage/database.py` (schema, migrations, methods)
- Modify: `src/rfobserver/storage/collection.py` (`FIELDS`, `_ORIGINS`)
- Test: `tests/unit/test_schedule_db.py`, extend `tests/unit/test_collection_stats.py`

**Interfaces:**
- Consumes: `Schedule`, `to_json` (Task 1).
- Produces on `SensorDatabase`:
  - `async insert_schedule(s: Schedule) -> None`; `async list_schedules(include_deleted: bool = False) -> list[Schedule]`; `async get_schedule(id: str) -> Schedule | None`; `async delete_schedule(id: str, deleted_ms: int) -> bool`
  - `async upsert_run(schedule_id: str, run: int, status: str, *, start_ms: int, filename: str | None = None, reason: str | None = None, info: dict[str, Any] | None = None) -> None`
  - `async list_runs(schedule_id: str, limit: int = 50) -> list[dict[str, Any]]` (newest first: run, status, start_ms, filename, reason, info)
  - `async last_run(schedule_id: str) -> int | None`; `async prune_runs(schedule_id: str, keep: int) -> None`
- Tables: `schedules(id TEXT PRIMARY KEY, body TEXT NOT NULL, created_ms INTEGER, deleted_ms INTEGER)` (body = `to_json` JSON); `schedule_runs(schedule_id TEXT, run INTEGER, status TEXT, start_ms INTEGER, filename TEXT, reason TEXT, info TEXT, PRIMARY KEY(schedule_id, run))`.
- `collection_hours` gains `scheduled_captures INTEGER`, `scheduled_bytes INTEGER`, `scheduled_seconds REAL` (DEFAULT 0) in the schema and via a `_migrate_collection_columns` ALTER like `_migrate_iq_capture_columns`. `FIELDS` gets the three names (after the manual ones), `_ORIGINS = ("auto", "manual", "scheduled")`.

- [ ] **Step 1: Failing tests:** round trip insert/get/list; delete hides from `list_schedules()` but `include_deleted=True` returns it; `upsert_run` twice keeps one row with the newer status; `prune_runs(keep=2)` leaves the 2 newest; an old DB file created with the pre-change `collection_hours` (CREATE the old table by hand) gains the columns on `connect()`; `CollectionStats.add_recorded("scheduled", ...)` then `flush` writes `scheduled_*`.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run these plus `tests/unit/test_collection_*.py`, expect PASS** (collection API tests may need their expected key sets extended; the doc test fails until Task 9 updates the doc field table: mark it `xfail(strict=True, reason="doc updated in Task 9")` here and remove the mark in Task 9).
- [ ] **Step 5: Commit** "schedules: persistence and scheduled collection columns".

### Task 3: Sample cutter (pure)

**Files:**
- Create: `src/rfobserver/schedules/cutter.py`
- Test: `tests/unit/test_schedule_cutter.py`

**Interfaces:**
- Produces:
  - `@dataclass class RunSpec: schedule_id: str; run: int; start_ms: int; n_samples: int; rate_hz: float; center_freq_hz: float; label: str`
  - `class RunCutter(spec: RunSpec)` with
    - `feed(chunk: np.ndarray, chunk_start: int, capture_time: float, gaps: Sequence[tuple[int, int]], center_freq_hz: float, ring: RingReader | None) -> list[Op]` where `Op = tuple[Literal["iq"], np.ndarray] | tuple[Literal["zeros"], int]` (iq arrays are copies)
    - `end(reason: str) -> list[Op]` (ends early; pads nothing)
    - properties `done: bool`, `written: int` (file samples incl. zeros), `status: str` (`recording`/`recorded`/`partial`), `reason: str | None`, `info() -> dict` with `requested_start` (ISO ms Z), `first_sample_time` (ISO ns), `start_error_ns`, `gaps: list[[index, lost]]`, `lost_samples`, `late_start_samples`, `stream_start` (position)
  - `class RingReader(Protocol): def read_range(self, start: int, end: int) -> np.ndarray | None; def bounds(self) -> tuple[int, int]` (CircularBuffer already matches)
- Rules (spec "Cutting to the sample"): the first chunk whose span reaches `start` places the start: `p0 = chunk_start + round((start_s - capture_time) * rate)`. If `p0 < chunk_start`, samples `[p0, chunk_start)` come from `ring.read_range` (the ring already holds this chunk, read `[p0, chunk_start + n)` in one call); if the ring no longer has `p0`, start at the ring's oldest sample and zero-fill the head (`late_start_samples`, status partial). After the first chunk, placement is by stream position only. A chunk gap `(off, lost)` inserts `lost` zeros at `off`. `center_freq_hz` differing from the spec ends the run (`partial`, `retuned`) before that chunk. The run is `recorded` when `written == n_samples` with no zeros, else `partial`. Ops never exceed `n_samples` in total.

- [ ] **Step 1: Failing tests** (rate 1000 Hz, chunks of 100 samples with `capture_time = t0 + chunk_start/1000`):
  - start mid-chunk: start at t0+0.2505 s, 50 samples: ops concatenated == stream[250:300] (round half to even documented by asserting 250 or 251 explicitly from the formula), `start_error_ns` within +-0.5 sample.
  - start inside an earlier chunk (feed begins after start): ring read used, output exact.
  - start older than the ring: head zero-filled, `late_start_samples` > 0, status partial, total == n.
  - spans 3 chunks with a gap `(30, 7)` in the middle chunk: 7 zeros at the right index, `gaps == [[idx, 7]]`, total == n, partial.
  - capture_time jumps +0.3 s on the second chunk: output still contiguous stream samples (position-only after start).
  - center freq change on the second chunk: ends early, `reason == "retuned"`, `written` == samples from chunk 1.
  - `end("stopped")` mid-run: status partial, reason stopped.
- [ ] **Step 2: Run, expect FAIL.**  **Step 3: Implement.**  **Step 4: PASS.**
- [ ] **Step 5: Commit** "schedules: sample cutter".

### Task 4: Scheduled writer thread

**Files:**
- Create: `src/rfobserver/schedules/writer.py`
- Test: `tests/unit/test_schedule_writer.py`

**Interfaces:**
- Consumes: `Op`, `RunSpec` (Task 3); `psd_grid.grid_paths`, `psd_grid.write_meta`, `psd_grid.write_text_atomic`.
- Produces: `class ScheduledWriter(directory_for: Callable[[], Path], on_finished: Callable[[FinishedRun], None], max_bytes_in_flight: int = 256 * 2**20)` with
  - `start()`, `stop(timeout: float)`
  - `open(key: tuple[str, int], filename: str, spec: RunSpec) -> None` (non-blocking enqueue)
  - `put_ops(key, ops: list[Op]) -> int` (non-blocking; returns the number of IQ samples it could not take because the byte budget was full; the caller turns those into a `("zeros", n)` op, which is always accepted)
  - `put_grid(key, rows: np.ndarray, freq_axis, time_res, first_sample, slice_samples, cal_offset_db) -> None` (drop silently if over budget)
  - `close(key, info: dict[str, Any]) -> None`
  - `@dataclass FinishedRun: key; path: Path; spec: RunSpec; info: dict; total_samples: int; write_error: str | None`
- Behaviour: one thread, one `queue.SimpleQueue` of tagged items; bytes in flight counted at put and released after write; zeros written in 1 MiB blocks; on close writes `<base>.json` (same keys as manual captures' `.json` for the fields that apply: file, format, sample_rate_hz, center_freq_hz, bandwidth_hz, gain_db (from spec/settings passed in info), start_time = first_sample_time, duration_sec, total_bytes, total_samples, lost_samples, gaps, gaps_truncated false, stopped_reason, write_failed, hostname, serial; plus `"origin": "scheduled"` and `"schedule": {id, run, label, requested_start, first_sample_time, start_error_ns, late_start_samples, clock_source, clock_server, clock_offset_ms, status, reason}`) and the PSD grid meta if rows were written, then calls `on_finished`. Total samples read back from the file size (like `_bytes_from_file`). OSError: record `write_error`, keep going (finish still called).

- SigMF: `storage/sigmf_export.iq_sigmf_meta` adds the json's `schedule` block to the global object as `rfobs:schedule` when present (add `src/rfobserver/storage/sigmf_export.py` to this task's files and a test that a scheduled capture's SigMF meta carries it).

- [ ] **Step 1: Failing tests** (tmp dir): ops iq/zeros/iq produce a file equal to the expected int32 array; `put_ops` over budget returns the refused count; close writes the json with the schedule block and calls on_finished with `total_samples`; a grid put produces `.psd` + `.psd.json` loadable by `psd_grid.load_grid`; an unwritable dir yields `write_error` and still finishes.
- [ ] **Step 2: FAIL. Step 3: Implement. Step 4: PASS.**
- [ ] **Step 5: Commit** "schedules: writer thread".

### Task 5: Clock source info

**Files:**
- Create: `src/rfobserver/schedules/clockinfo.py`
- Test: `tests/unit/test_schedule_clockinfo.py`

**Interfaces:**
- Produces: `def clock_info() -> dict[str, Any]` returning `{"clock_source": "ntp"|"unknown", "clock_server": str | None, "clock_offset_ms": float | None, "clock_synced": bool | None}`, cached for 60 s (`functools` + monotonic), never raising, never taking more than 1 s (subprocess timeout).
- Source: `timedatectl show-timesync -p ServerName -p NTPMessage --value` and `timedatectl show -p NTPSynchronized --value`; chrony (`chronyc -c tracking`) if present. Offset from chrony only (timesyncd does not expose it: None). Missing tools give `unknown`.

- [ ] **Step 1: Failing tests** with `subprocess.run` monkeypatched: timesyncd output parsed; chrony csv parsed (offset s to ms); FileNotFoundError gives unknown; TimeoutExpired gives unknown; second call within 60 s does not run subprocess again.
- [ ] **Step 2-4: FAIL, implement, PASS.**  **Step 5: Commit** "schedules: clock source info for metadata".

### Task 6: ScheduleBook and ScheduleEngine, wired into the pipeline

**Files:**
- Create: `src/rfobserver/schedules/book.py`, `src/rfobserver/schedules/engine.py`
- Modify: `src/rfobserver/pipeline/streaming.py` (constructor param, receiver-loop hook after the ring write and before `_check_trigger_and_record`, dispatch hook in `_handle_chunk_result`, stop hook, finalize callback), `src/rfobserver/pipeline/app.py` (create and load the book, `app.state.schedules`, pass to `StreamingProcessor`)
- Test: `tests/unit/test_schedule_engine.py`, `tests/unit/test_schedule_book.py`

**Interfaces:**
- Consumes: Tasks 1 to 5; `SensorDatabase` methods (Task 2); `CollectionStats.add_recorded("scheduled", ...)`; `LocalStorage.manual_dir`, `.track()`; `db.insert_iq_capture(origin="scheduled", ...)`; `trim_grid_rows` (already in streaming).
- Produces:
  - `class ScheduleBook(db: SensorDatabase)`: `async load() -> None`; `async add(s: Schedule) -> tuple[Schedule, bool]` (bool: created; same id with identical body returns existing and False; different body raises `ScheduleConflict`); `async delete(id) -> bool`; `snapshot() -> tuple[int, list[Schedule]]` (version, active schedules; thread-safe, used by the receiver thread); `report_run(schedule_id, run, status, **fields) -> None` (thread-safe: schedules the DB upsert on the loop via `call_soon_threadsafe`; also keeps the latest status per run in memory for the API); `async runs(schedule_id, limit) -> list[dict]`; `bind_loop(loop)`.
  - `class ScheduleEngine(book, ring_getter, writer, settings, storage, refusal: Callable[[], str | None], now: Callable[[], float] = time.time)`:
    - `on_chunk(chunk, chunk_start, capture_time, gaps, center_freq_hz, rate_hz) -> None` (receiver thread; refreshes its run plan only when `book.snapshot()` version changes; starts a `RunCutter` when a run's start falls before this chunk's end; runs above `MAX_CONCURRENT_RUNS` are reported `missed` with reason `busy`; a refusal from storage reports `missed` reason `storage`; feeds active cutters, forwards ops to the writer, turns refused samples into zeros)
    - `on_grid(psd_grid, chunk_start, slice_samples) -> None` (dispatch thread; rows trimmed to each active run's stream range with `trim_grid_rows`)
    - `stop(reason: str) -> None` (ends active runs `partial` with `reason`)
    - `mark_missed(now_ms) -> None` (at start: for each schedule, runs in `[max(created_ms, last recorded run start), now - grace)` without a row are reported `missed`, reason `not_running`, newest `RUNS_KEPT` only via `runs_between`)
  - `StreamingProcessor(..., schedules: ScheduleBook | None = None)`; replay processors never get one.
  - On writer finish (writer thread): `storage.track(path)`, `collection.add_recorded("scheduled", first_ts, first_ts + samples/rate, bytes)`, `db.insert_iq_capture(origin="scheduled", trigger_initiated=False, ...)` via the loop, `book.report_run(..., status, filename, info)`, `book` prunes runs to `RUNS_KEPT`. The detections sidecar is scheduled exactly as for a manual capture (`_deferred_sidecar(sc16, DETECTIONS_SIDECAR_GRACE_SEC)` on the loop).
- Retune detection: `center_freq_hz` per chunk (sweep) inside the cutter; a config-generation change or processor stop calls `engine.stop("retuned")` / `engine.stop("stopped")`.

- [ ] **Step 1: Failing tests** with a fake ring (CircularBuffer), fake writer capturing ops, in-memory book over a temp `SensorDatabase`:
  - a one-off due mid-stream is cut exactly (concatenated ops equal the expected stream slice) and reported `recorded` with the filename;
  - a repeating schedule (every 300 ms, count 3) produces 3 runs at the right stream positions;
  - three overlapping schedules: two record, the third is `missed`/`busy`;
  - storage refusal: `missed`/`storage`;
  - adding a schedule mid-stream is picked up (version change) without restarting;
  - deleting a schedule mid-run lets the run finish and no further runs start;
  - `stop("stopped")` mid-run: `partial`/`stopped`;
  - `mark_missed` after a gap reports the due runs `missed` and is bounded;
  - `ScheduleBook.add` idempotent create and 409-style conflict;
  - writer byte budget exhausted: zeros op emitted, run `partial`, the receiver call returns without waiting (fake writer that refuses).
- [ ] **Step 2: FAIL. Step 3: Implement, then wire `streaming.py` and `pipeline/app.py`.**
- [ ] **Step 4: PASS, plus the full unit suite** (`PYTHONPATH= .venv310/bin/pytest tests/unit -x -q`), since streaming.py changed.
- [ ] **Step 5: Commit** "schedules: engine in the receiver loop, book, pipeline wiring".

### Task 7: Sensor API `/api/schedules`

**Files:**
- Create: `src/rfobserver/web/routes/schedules.py`
- Modify: `src/rfobserver/web/app.py` (`include_router(schedules.router, prefix="/api/schedules")` before `api.router`)
- Test: `tests/unit/test_schedules_api.py`

**Interfaces:**
- Consumes: `app.state.schedules` (Task 6), `app.state.collection_zone`, `parse_schedule`, `schedule_bytes`, storage free space (same source as `/api/collection/storage`: `shutil.disk_usage` on the storage path, minus the floor).
- Produces (JSON):
  - `POST /api/schedules` body per spec: 201 `{schedule, next_runs: [{run, start, start_local}], warnings: [..]}`; 200 with the same body for an idempotent repeat; 400 `{detail}` invalid; 409 `{detail}` id conflict or too large for free space; 503 when no book (web-only mode).
  - `GET /api/schedules`: `{now, now_local, timezone, schedules: [{schedule, next_run, runs_done, last_status}]}`.
  - `GET /api/schedules/{id}`: `{schedule, next_runs, runs: [...] }` (runs newest first, each with `start`, `start_local`, `status`, `reason`, `filename`, `info`); 404.
  - `DELETE /api/schedules/{id}`: 200 `{deleted: id}`; 404.
  - Warning text when `settings.FREQUENCY_STEP > 0`: "This sensor is sweeping; windows longer than the dwell (X s) will be cut at the retune."
- `start_local` format `HH:MM:SS.mmm ZZZ` in the sensor zone, date included when it differs from the UTC date.

- [ ] **Step 1: Failing tests** (TestClient over `create_app` with a book on a temp DB, `_now` monkeypatched): create, list, get, delete, idempotent repeat, conflict 409, invalid 400 with detail text, too-large 409, sweep warning, 503 without a book, `start_local` in America/Los_Angeles for HCRO coordinates.
- [ ] **Step 2-4. Step 5: Commit** "schedules: sensor API".

### Task 8: Captures list origin and the sensor Schedule card

**Files:**
- Modify: `src/rfobserver/web/routes/captures.py` (`_describe_captures`: origin `"scheduled"` when the json has `"schedule"`, plus `entry["schedule_label"]`), `src/rfobserver/web/templates/captures.html`, `src/rfobserver/web/static/style.css`
- Test: extend `tests/unit/test_web_routes.py` or a new `tests/unit/test_captures_schedule_ui.py`

**Interfaces:**
- Consumes: Task 7 endpoints.
- UI (spec "UI"): Schedule card above the list: live clock line `Now 18:29:47.312 UTC  11:29:47 PDT (America/Los_Angeles)`; form fields Start date + time (UTC, `HH:MM:SS.mmm` text input validated client-side), Duration (s, 3 decimals), Repeat checkbox revealing every (s) and until (UTC) or times, Label; Schedule button; inline error from `detail`; warnings shown. Upcoming list (next run UTC + local, duration, repeat summary "every 60 s (3 of 10)", countdown, Delete). Recent runs (status chip recorded/partial/missed with reason, link `?file=` to the capture). Polls `GET /api/schedules` every 2 s while visible (pauses when hidden, like PSD). Captures list rows: a `scheduled` chip with the label. Hidden in fleet mode for now (Task 10 provides the fleet variant).

- [ ] **Step 1: Failing tests:** list entry origin `scheduled` and label from a json with a schedule block; page HTML contains `id="schedule-card"`, `/api/schedules`, `Start (UTC)`; no em-dash characters in the template (`"—" not in html`).
- [ ] **Step 2-4. Manual check** with the mock pipeline (`PYTHONPATH= RFOBS_MOCK_RECEIVER=true RFOBS_SENSOR_ACTIVE=true RFOBS_WEB_PORT=8890 RFOBS_MDNS_ENABLED=false .venv310/bin/rfobserver run` in a temp dir) and headless Chrome (scratchpad playwright-core): schedule 1 s, 3 s ahead; the run appears as recorded, the file opens in the detail view, no console errors.
- [ ] **Step 5: Commit** "schedules: Captures page card and scheduled chip".

### Task 9: Docs (schedules API doc with executed examples, collection doc)

**Files:**
- Create: `docs/schedules-api.md`, `tests/unit/test_schedules_api_doc.py`
- Modify: `docs/collection-api.md` (scheduled fields in the field table and examples), `tests/unit/test_collection_api_doc.py` (remove the xfail from Task 2), `README.md` (Documentation list: "- [Scheduled captures API](docs/schedules-api.md): ms-timed one-off and repeating IQ captures, per sensor and through the fleet.")

**Interfaces:**
- Consumes: Task 7 API; the doc-test harness pattern of `test_collection_api_doc.py` (curl/json block pairs, subset match, field table == keys).
- [ ] **Step 1:** Write the doc test first against a doc stub; FAIL.
- [ ] **Step 2:** Write the doc (overview, create one-off, create repeating, list, get with runs, delete, errors, timing and accuracy note pointing to `docs/debugging/2026-10-28_hcro-time-sync-ptp.md`, file metadata `schedule` block reference, fleet section). PASS; mutate one value to confirm the test catches drift, restore.
- [ ] **Step 3: Commit** "docs: scheduled captures API, examples run by the test suite".

### Task 10: Fleet schedules (API and Captures card)

**Files:**
- Modify: `src/rfobserver/fleet/sensors.py` (`async send(sensor, method: str, path: str, json: dict | None = None) -> Reply`, no cache, through the `other` limit, marks up/down), `src/rfobserver/fleet/app.py`, `src/rfobserver/web/templates/captures.html` (fleet variant of the card), `src/rfobserver/web/static/style.css`
- Test: `tests/unit/test_fleet_schedules.py`

**Interfaces:**
- Produces:
  - `POST /fleet/api/schedules` body = schedule body + `sensors: [id]` (empty or missing: 400). Generates the id (`uuid4().hex[:12]`) if absent, sends to each sensor in parallel, returns `{id, results: {sensor_id: {ok: bool, status: int, detail: str | None, schedule: ...}}}`; status 201 if any ok, else 502.
  - `GET /fleet/api/schedules`: `{now, schedules: [{id, schedule, sensors: {sensor_id: {next_run, last_status, runs_done}}}], errors: {sensor_id: detail}}` merged by id.
  - `DELETE /fleet/api/schedules/{id}`: deletes on every online sensor that lists it; `{results: {...}}`.
- Card in fleet mode: sensor picker (all online selected), per-sensor status row in Upcoming and Recent, each sensor's local time from its identity lat/long is not available to the fleet, so show the sensor's `now_local`/timezone from its `GET /api/schedules` reply.
- [ ] **Step 1: Failing tests** with `httpx.MockTransport` (pattern of `test_fleet_captures.py`): fan-out sends the same id to both sensors; one sensor down gives a partial result with 201; all down 502; merged list groups by id; delete reaches both; fleet page contains the picker.
- [ ] **Step 2-4. Manual check** with two mock sensors (`RFOBS_MDNS_ENABLED=false`, ports 8890/8891) and a local fleet on 8892: one schedule reaches both, both files have the same `schedule.id`/`run` and `requested_start`.
- [ ] **Step 5: Commit** "fleet: schedule captures on several sensors".

### Task 11: End-to-end integration test

**Files:**
- Create: `tests/integration/test_scheduled_capture.py`

- [ ] **Step 1:** With the mock receiver pipeline as the existing integration tests run it: POST a schedule starting 1.5 s ahead, 300 ms; wait up to 5 s; assert the run is `recorded`, the file is in `manual/`, `total_samples == round(0.3 * rate)`, json `schedule.start_error_ns` within one sample, the captures list shows origin `scheduled`, `/api/collection/daily` for today has `scheduled_captures == 1`; and a manual recording started before the window and stopped after it is unaffected (its own file, same length as without the schedule within one chunk).
- [ ] **Step 2:** Run all checks (Global Constraints list). **Step 3: Commit** "tests: scheduled capture end to end".
