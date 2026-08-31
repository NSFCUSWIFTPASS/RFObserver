# Averaged-Window Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist every DURATION_SEC-averaged window (PSD + IQ stats + flags) to SQLite with start+duration, expose a datetime-range query API, and associate burst detections to the averaged window they started in.

**Architecture:** A new `avg_windows` SQLite table stores the averaged PSD as a compact little-endian float32 BLOB plus averaged stats and nullable flags. Persistence hooks into `StreamingProcessor._publish_processed` before the ZMS/NATS gate so it runs whenever the pipeline is live (not replay), independent of any sink. Detection association is a time-range + tuning join computed at query time by reusing `query_detections`. Three JSON endpoints under `/api/averaged` expose the data.

**Tech Stack:** Python 3.11, aiosqlite, numpy (float32 BLOB codec), FastAPI, pytest + pytest-asyncio, httpx ASGITransport for async endpoint tests.

**Spec:** `docs/superpowers/specs/2026-08-31-averaged-window-store-design.md`

## Global Constraints

- **Run all commands with `PYTHONPATH=` prefix** and the venv: e.g. `PYTHONPATH= .venv/bin/pytest ...`. The host PYTHONPATH leaks system Python 3.10 packages otherwise.
- **No emojis, no em-dashes** anywhere in code, comments, or docs.
- **Stored PSD is raw dBFS**, encoded little-endian float32 (`numpy dtype "<f4"`). Do not apply calibration or scale before storing.
- **mypy runs without numpy stubs** (numpy is treated as `Any`). Keep return values concrete: decode blobs with an explicit `float()` comprehension, never `return np.frombuffer(...).tolist()` (that leaks `Any` and trips `no-any-return`).
- **Commit staging is explicit paths only.** Never `git add -A` or `git add .`.
- **No `Co-Authored-By` / Claude trailer** in commits.
- **Full check suite before each commit** where practical (at minimum the files' unit tests each task; the final task runs the whole suite): `ruff check src/ tests/`, `ruff format --check src/ tests/`, `PYTHONPATH= .venv/bin/mypy src/rfobserver/`, `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`.
- Branch: `feat/averaged-window-store` (already created off `main`).

---

### Task 1: `avg_windows` schema + BLOB codec + insert + range query

**Files:**
- Modify: `src/rfobserver/storage/database.py` (add table to `SCHEMA`, add `import numpy as np`, add `insert_avg_window` and `query_avg_windows`)
- Test: `tests/unit/test_database.py`

**Interfaces:**
- Produces:
  - `SensorDatabase.insert_avg_window(*, start_time: datetime, duration_sec: float, sdr_center_freq_hz: float, sample_rate_hz: float, gain_db: float | None, num_bins: int, freq_start_hz: float, freq_step_hz: float, pwr_avg: float, pwr_max: float, pwr_median: float, pwr_std: float, kurtosis: float, powers: list[float], interference: bool | None = None, violations: bytes | None = None) -> None`
  - `SensorDatabase.query_avg_windows(*, since: datetime | None = None, until: datetime | None = None, sdr_center_freq: float | None = None, sample_rate: float | None = None, gain: float | None = None, limit: int = 500) -> list[dict[str, Any]]` (no `psd_powers`/`violations` blobs in the returned dicts)

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_database.py`:

```python
async def test_insert_and_query_avg_window(db):
    await db.insert_avg_window(
        start_time=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        duration_sec=0.5,
        sdr_center_freq_hz=2_437_000_000.0,
        sample_rate_hz=56_000_000.0,
        gain_db=40.0,
        num_bins=4,
        freq_start_hz=2_409_000_000.0,
        freq_step_hz=14_000_000.0,
        pwr_avg=-70.0,
        pwr_max=-50.0,
        pwr_median=-72.0,
        pwr_std=3.0,
        kurtosis=1.2,
        powers=[-80.0, -70.0, -60.0, -50.0],
    )
    rows = await db.query_avg_windows(limit=10)
    assert len(rows) == 1
    r = rows[0]
    assert r["num_bins"] == 4
    assert r["pwr_avg"] == -70.0
    assert r["sdr_center_freq_hz"] == 2_437_000_000.0
    assert r["interference"] is None
    # The light query does not carry the heavy blobs.
    assert "psd_powers" not in r
    assert "violations" not in r


async def test_query_avg_windows_time_and_tuning_filters(db):
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    common = dict(
        duration_sec=0.5, sample_rate_hz=56e6, gain_db=40.0, num_bins=2,
        freq_start_hz=0.0, freq_step_hz=1.0, pwr_avg=-70.0, pwr_max=-50.0,
        pwr_median=-72.0, pwr_std=3.0, kurtosis=1.0, powers=[-70.0, -60.0],
    )
    await db.insert_avg_window(start_time=base, sdr_center_freq_hz=100e6, **common)
    await db.insert_avg_window(
        start_time=base + timedelta(seconds=10), sdr_center_freq_hz=200e6, **common
    )
    # Time window excludes the first.
    rows = await db.query_avg_windows(since=base + timedelta(seconds=5))
    assert [r["sdr_center_freq_hz"] for r in rows] == [200e6]
    # Tuning filter selects one center.
    rows = await db.query_avg_windows(sdr_center_freq=100e6)
    assert [r["sdr_center_freq_hz"] for r in rows] == [100e6]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -k avg_window -x -q`
Expected: FAIL with `AttributeError: 'SensorDatabase' object has no attribute 'insert_avg_window'`.

- [ ] **Step 3: Add the schema**

In `src/rfobserver/storage/database.py`, append to the `SCHEMA` string (before the closing `"""`, after the `tone_checks` block), inside the CREATE section and add its indexes with the others:

```sql
CREATE TABLE IF NOT EXISTS avg_windows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time TEXT NOT NULL,
    duration_sec REAL NOT NULL,
    sdr_center_freq_hz REAL NOT NULL,
    sample_rate_hz REAL NOT NULL,
    gain_db REAL,
    num_bins INTEGER NOT NULL,
    freq_start_hz REAL NOT NULL,
    freq_step_hz REAL NOT NULL,
    pwr_avg REAL,
    pwr_max REAL,
    pwr_median REAL,
    pwr_std REAL,
    kurtosis REAL,
    interference INTEGER,
    psd_powers BLOB NOT NULL,
    violations BLOB,
    created_at TEXT DEFAULT (datetime('now'))
);
```

And add to the index block at the end of `SCHEMA`:

```sql
CREATE INDEX IF NOT EXISTS idx_avg_windows_time ON avg_windows(start_time);
CREATE INDEX IF NOT EXISTS idx_avg_windows_center_time ON avg_windows(sdr_center_freq_hz, start_time);
```

- [ ] **Step 4: Add the codec import and methods**

At the top of `database.py` add `import numpy as np` with the other imports. Add these methods to `SensorDatabase` (near `insert_tone_check`):

```python
async def insert_avg_window(
    self,
    *,
    start_time: datetime,
    duration_sec: float,
    sdr_center_freq_hz: float,
    sample_rate_hz: float,
    gain_db: float | None,
    num_bins: int,
    freq_start_hz: float,
    freq_step_hz: float,
    pwr_avg: float,
    pwr_max: float,
    pwr_median: float,
    pwr_std: float,
    kurtosis: float,
    powers: list[float],
    interference: bool | None = None,
    violations: bytes | None = None,
) -> None:
    """Persist one DURATION_SEC-averaged window. ``powers`` is stored as a
    little-endian float32 BLOB (raw dBFS)."""
    assert self._db is not None
    psd_blob = np.asarray(powers, dtype="<f4").tobytes()
    await self._db.execute(
        """INSERT INTO avg_windows
           (start_time, duration_sec, sdr_center_freq_hz, sample_rate_hz,
            gain_db, num_bins, freq_start_hz, freq_step_hz, pwr_avg, pwr_max,
            pwr_median, pwr_std, kurtosis, interference, psd_powers, violations)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            start_time.isoformat(),
            duration_sec,
            sdr_center_freq_hz,
            sample_rate_hz,
            gain_db,
            num_bins,
            freq_start_hz,
            freq_step_hz,
            pwr_avg,
            pwr_max,
            pwr_median,
            pwr_std,
            kurtosis,
            None if interference is None else int(interference),
            psd_blob,
            violations,
        ),
    )
    await self._db.commit()

# Columns returned by the light range query -- everything except the heavy blobs.
_AVG_LIGHT_COLUMNS = (
    "id, start_time, duration_sec, sdr_center_freq_hz, sample_rate_hz, gain_db, "
    "num_bins, freq_start_hz, freq_step_hz, pwr_avg, pwr_max, pwr_median, "
    "pwr_std, kurtosis, interference, created_at"
)

async def query_avg_windows(
    self,
    *,
    since: datetime | None = None,
    until: datetime | None = None,
    sdr_center_freq: float | None = None,
    sample_rate: float | None = None,
    gain: float | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Averaged windows in a time/tuning range, newest first. Excludes the
    psd_powers/violations blobs to keep range listings light."""
    assert self._db is not None
    conditions: list[str] = []
    params: list[Any] = []
    if since is not None:
        conditions.append("start_time >= ?")
        params.append(since.isoformat())
    if until is not None:
        conditions.append("start_time < ?")
        params.append(until.isoformat())
    sdr_conditions, sdr_params = self._sdr_conditions(sdr_center_freq, sample_rate, gain)
    conditions.extend(sdr_conditions)
    params.extend(sdr_params)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = (
        f"SELECT {self._AVG_LIGHT_COLUMNS} FROM avg_windows "
        f"{where} ORDER BY start_time DESC LIMIT ?"
    )
    params.append(limit)
    self._db.row_factory = aiosqlite.Row
    async with self._db.execute(query, params) as cursor:
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
```

Note: `_sdr_conditions` is a static method that filters on `sdr_center_freq_hz`, `sample_rate_hz`, `gain_db` -- the exact column names in `avg_windows`, so it is reusable verbatim.

- [ ] **Step 5: Run tests to verify they pass**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -k avg_window -x -q`
Expected: PASS.

- [ ] **Step 6: Lint + type check**

Run: `ruff check src/ tests/ && ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/`
Expected: all pass. If `ruff format` complains, run `ruff format src/ tests/` and re-check.

- [ ] **Step 7: Commit**

```bash
git add src/rfobserver/storage/database.py tests/unit/test_database.py
git commit -m "avg-windows: schema + insert_avg_window + query_avg_windows"
```

---

### Task 2: `get_avg_window` (full record with decoded PSD + reconstructed frequencies)

**Files:**
- Modify: `src/rfobserver/storage/database.py`
- Test: `tests/unit/test_database.py`

**Interfaces:**
- Consumes: `insert_avg_window` (Task 1)
- Produces: `SensorDatabase.get_avg_window(window_id: int) -> dict[str, Any] | None` -- full record; adds `powers: list[float]` and `frequencies: list[float]` (length `num_bins`), drops the raw `psd_powers` blob.

- [ ] **Step 1: Write the failing test**

```python
async def test_get_avg_window_decodes_psd_and_frequencies(db):
    await db.insert_avg_window(
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        duration_sec=0.5, sdr_center_freq_hz=100e6, sample_rate_hz=4.0,
        gain_db=40.0, num_bins=4, freq_start_hz=98.0, freq_step_hz=1.0,
        pwr_avg=-70.0, pwr_max=-50.0, pwr_median=-72.0, pwr_std=3.0,
        kurtosis=1.0, powers=[-80.0, -70.0, -60.0, -50.0],
    )
    rows = await db.query_avg_windows(limit=1)
    full = await db.get_avg_window(rows[0]["id"])
    assert full is not None
    assert full["powers"] == pytest.approx([-80.0, -70.0, -60.0, -50.0], abs=1e-3)
    assert full["frequencies"] == pytest.approx([98.0, 99.0, 100.0, 101.0], abs=1e-6)
    assert "psd_powers" not in full


async def test_get_avg_window_missing_returns_none(db):
    assert await db.get_avg_window(9999) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -k "get_avg_window" -x -q`
Expected: FAIL with `AttributeError: ... 'get_avg_window'`.

- [ ] **Step 3: Implement**

Add to `SensorDatabase`:

```python
async def get_avg_window(self, window_id: int) -> dict[str, Any] | None:
    """One averaged window with its PSD decoded to a list and the frequency
    axis reconstructed from freq_start_hz + i * freq_step_hz."""
    assert self._db is not None
    self._db.row_factory = aiosqlite.Row
    async with self._db.execute(
        "SELECT * FROM avg_windows WHERE id = ?", (window_id,)
    ) as cursor:
        row = await cursor.fetchone()
    if row is None:
        return None
    record = dict(row)
    blob = record.pop("psd_powers")
    num_bins = int(record["num_bins"])
    # Explicit float() comprehension keeps the return concrete (numpy is
    # untyped under the CI mypy config; a bare .tolist() would leak Any).
    record["powers"] = [float(x) for x in np.frombuffer(blob, dtype="<f4")]
    start = float(record["freq_start_hz"])
    step = float(record["freq_step_hz"])
    record["frequencies"] = [start + i * step for i in range(num_bins)]
    return record
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -k "get_avg_window" -x -q`
Expected: PASS.

- [ ] **Step 5: Lint + type check**

Run: `ruff check src/ tests/ && ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/rfobserver/storage/database.py tests/unit/test_database.py
git commit -m "avg-windows: get_avg_window with decoded PSD + reconstructed freqs"
```

---

### Task 3: `detections_for_window` association + retention

**Files:**
- Modify: `src/rfobserver/storage/database.py` (add `detections_for_window`; extend `cleanup_old_data`)
- Test: `tests/unit/test_database.py`

**Interfaces:**
- Consumes: `get_avg_window` (Task 2), existing `query_detections`, existing `cleanup_old_data`
- Produces: `SensorDatabase.detections_for_window(window: dict[str, Any]) -> list[dict[str, Any]]`

- [ ] **Step 1: Write the failing tests**

```python
async def test_detections_for_window_associates_by_start_and_tuning(db):
    win_start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    await db.insert_avg_window(
        start_time=win_start, duration_sec=2.0, sdr_center_freq_hz=915e6,
        sample_rate_hz=56e6, gain_db=40.0, num_bins=2, freq_start_hz=0.0,
        freq_step_hz=1.0, pwr_avg=-70.0, pwr_max=-50.0, pwr_median=-72.0,
        pwr_std=3.0, kurtosis=1.0, powers=[-70.0, -60.0],
    )
    win = (await db.query_avg_windows(limit=1))[0]
    full = await db.get_avg_window(win["id"])

    def _det(bid, sec, center=915e6):
        return dict(
            burst_id=bid,
            start_time=win_start + timedelta(seconds=sec),
            stop_time=win_start + timedelta(seconds=sec + 0.1),
            center_freq_hz=915.1e6, bandwidth_hz=1e6, peak_power_db=-30.0,
            duration_ms=100.0,
            detection_timestamp=win_start + timedelta(seconds=sec),
            sdr_center_freq_hz=center, sample_rate_hz=56e6, gain_db=40.0,
        )

    await db.insert_detection(**_det("in", 0.5))          # inside window
    await db.insert_detection(**_det("out", 5.0))         # after window
    await db.insert_detection(**_det("wrong-tune", 0.6, center=100e6))  # inside time, wrong center

    dets = await db.detections_for_window(full)
    assert {d["burst_id"] for d in dets} == {"in"}


async def test_cleanup_prunes_old_avg_windows(db):
    old = datetime.now(timezone.utc) - timedelta(days=30)
    recent = datetime.now(timezone.utc)
    common = dict(
        duration_sec=0.5, sdr_center_freq_hz=100e6, sample_rate_hz=4.0,
        gain_db=40.0, num_bins=2, freq_start_hz=0.0, freq_step_hz=1.0,
        pwr_avg=-70.0, pwr_max=-50.0, pwr_median=-72.0, pwr_std=3.0,
        kurtosis=1.0, powers=[-70.0, -60.0],
    )
    await db.insert_avg_window(start_time=old, **common)
    await db.insert_avg_window(start_time=recent, **common)
    removed = await db.cleanup_old_data(days=7)
    assert removed >= 1
    rows = await db.query_avg_windows(limit=10)
    assert len(rows) == 1  # only the recent one survives
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -k "detections_for_window or prunes_old_avg" -x -q`
Expected: FAIL (`detections_for_window` missing; cleanup count/rows wrong).

- [ ] **Step 3: Implement the association helper**

Add to `SensorDatabase`:

```python
async def detections_for_window(self, window: dict[str, Any]) -> list[dict[str, Any]]:
    """Detections that started inside this averaged window's time span and
    match its SDR tuning. Reuses query_detections' since/until (which range on
    the burst start_time) so a burst is associated with the window it began in."""
    start = datetime.fromisoformat(window["start_time"])
    until = start + timedelta(seconds=float(window["duration_sec"]))
    return await self.query_detections(
        since=start,
        until=until,
        sdr_center_freq=window["sdr_center_freq_hz"],
        sample_rate=window["sample_rate_hz"],
        gain=window["gain_db"],
        limit=1000,
    )
```

- [ ] **Step 4: Extend retention**

In `cleanup_old_data`, after the `tone_checks` delete and before `await self._db.commit()`, add:

```python
cursor = await self._db.execute("DELETE FROM avg_windows WHERE start_time < ?", (cutoff,))
avg_count = cursor.rowcount
```

Then include it in the total:

```python
total: int = det_count + stats_count + tc_count + avg_count
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -k "detections_for_window or prunes_old_avg" -x -q`
Expected: PASS.

- [ ] **Step 6: Full DB test file + lint + types**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -x -q && ruff check src/ tests/ && ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/rfobserver/storage/database.py tests/unit/test_database.py
git commit -m "avg-windows: detection association + retention pruning"
```

---

### Task 4: Persist averaged window from the streaming pipeline

**Files:**
- Modify: `src/rfobserver/pipeline/streaming.py` (add `_persist_avg_window`; call it in `_publish_processed` before the sink gate)
- Test: `tests/unit/test_streaming_avg_window.py` (new)

**Interfaces:**
- Consumes: `SensorDatabase.insert_avg_window` (Task 1)
- Produces: `StreamingProcessor._persist_avg_window(avg_powers: list[float], result: _StreamResult, iq_stats: IQStatistics) -> None`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_streaming_avg_window.py`:

```python
"""Averaged-window persistence runs for every live window, independent of sinks."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from rfobserver.config import AppSettings
from rfobserver.models import IQStatistics


def _proc(tmp_path, *, replay_mode=False, with_sinks=True):
    from rfobserver.pipeline.streaming import StreamingProcessor

    settings = AppSettings(
        _env_file=None, STORAGE_PATH=str(tmp_path), DB_PATH=str(tmp_path / "d.db")
    )
    db = MagicMock()
    db.insert_avg_window = AsyncMock()
    storage = MagicMock()
    storage.storage_path = tmp_path
    storage.auto_dir = tmp_path / "auto"
    storage.manual_dir = tmp_path / "manual"
    storage.auto_dir.mkdir(exist_ok=True)
    storage.manual_dir.mkdir(exist_ok=True)
    receiver = MagicMock()
    receiver.serial = "sim0"
    proc = StreamingProcessor(
        receiver=receiver,
        database=db,
        local_storage=storage,
        settings=settings,
        broadcast=None,
        zms_monitor=(MagicMock() if with_sinks else None),
        nats_producer=(MagicMock() if with_sinks else None),
        replay_mode=replay_mode,
    )
    return proc, db


def _result():
    summary = SimpleNamespace(
        powers=[-80.0, -70.0, -60.0, -50.0],
        frequencies=[2.409e9, 2.423e9, 2.437e9, 2.451e9],
        center_freq=2.437e9,
        sample_rate=56_000_000,
        num_bins=4,
    )
    return SimpleNamespace(summary_psd=summary, center_freq_hz=2_437_000_000, capture_num=1)


def _stats():
    return IQStatistics(average=-70.0, max=-50.0, median=-72.0, std=3.0, kurtosis=1.2)


@pytest.mark.asyncio
async def test_persist_avg_window_inserts_expected_fields(tmp_path):
    proc, db = _proc(tmp_path)
    await proc._persist_avg_window([-80.0, -70.0, -60.0, -50.0], _result(), _stats())
    db.insert_avg_window.assert_called_once()
    kwargs = db.insert_avg_window.call_args.kwargs
    assert kwargs["num_bins"] == 4
    assert kwargs["sdr_center_freq_hz"] == 2_437_000_000.0
    assert kwargs["freq_start_hz"] == pytest.approx(2.409e9)
    assert kwargs["freq_step_hz"] == pytest.approx(0.014e9, rel=1e-6)
    assert kwargs["pwr_avg"] == -70.0
    assert kwargs["powers"] == [-80.0, -70.0, -60.0, -50.0]


@pytest.mark.asyncio
async def test_publish_persists_even_with_no_sinks(tmp_path):
    proc, db = _proc(tmp_path, with_sinks=False)
    await proc._publish_processed([-80.0, -70.0, -60.0, -50.0], _result(), _stats())
    db.insert_avg_window.assert_called_once()


@pytest.mark.asyncio
async def test_replay_mode_skips_avg_window_persist(tmp_path):
    proc, db = _proc(tmp_path, replay_mode=True)
    await proc._publish_processed([-80.0, -70.0, -60.0, -50.0], _result(), _stats())
    db.insert_avg_window.assert_not_called()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_streaming_avg_window.py -x -q`
Expected: FAIL with `AttributeError: ... '_persist_avg_window'`.

- [ ] **Step 3: Implement `_persist_avg_window`**

In `src/rfobserver/pipeline/streaming.py`, add this method next to `_build_envelope` (imports `datetime`, `timezone`, and `logger` already exist in the module):

```python
async def _persist_avg_window(
    self, avg_powers: list[float], result: _StreamResult, iq_stats: IQStatistics
) -> None:
    """Store the averaged window locally. Runs for every live window,
    independent of whether ZMS/NATS are attached. Flags (interference /
    violations) are not computed in the streaming path yet, so they are left
    NULL until the PSDProcessor gap is closed."""
    s = self._settings
    freqs = result.summary_psd.frequencies
    freq_start = float(freqs[0]) if freqs else 0.0
    freq_step = float(freqs[1] - freqs[0]) if len(freqs) > 1 else 0.0
    try:
        await self._db.insert_avg_window(
            start_time=datetime.now(timezone.utc),
            duration_sec=s.DURATION_SEC,
            sdr_center_freq_hz=float(result.center_freq_hz),
            sample_rate_hz=float(s.BANDWIDTH),
            gain_db=float(s.GAIN),
            num_bins=result.summary_psd.num_bins,
            freq_start_hz=freq_start,
            freq_step_hz=freq_step,
            pwr_avg=iq_stats.average,
            pwr_max=iq_stats.max,
            pwr_median=iq_stats.median,
            pwr_std=iq_stats.std,
            kurtosis=iq_stats.kurtosis,
            powers=avg_powers,
        )
    except Exception:
        logger.exception("avg-window persist failed (chunk #%d)", result.capture_num)
```

- [ ] **Step 4: Call it in `_publish_processed` before the sink gate**

In `_publish_processed`, change the top so persistence runs before the early return when both sinks are absent:

```python
        if self._replay_mode:
            return
        await self._persist_avg_window(avg_powers, result, iq_stats)
        if self._zms_monitor is None and self._nats_producer is None:
            return
```

(The rest of the method -- envelope build and ZMS/NATS fanout -- is unchanged.)

- [ ] **Step 5: Run test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_streaming_avg_window.py -x -q`
Expected: PASS (all three).

- [ ] **Step 6: Lint + types + full unit suite**

Run: `ruff check src/ tests/ && ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/ && PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/rfobserver/pipeline/streaming.py tests/unit/test_streaming_avg_window.py
git commit -m "streaming: persist averaged window per live window (sink-independent)"
```

---

### Task 5: `/api/averaged` endpoints

**Files:**
- Modify: `src/rfobserver/web/routes/api.py` (add `_opt_dt` helper + three routes)
- Test: `tests/integration/test_web_integration.py`

**Interfaces:**
- Consumes: `query_avg_windows`, `get_avg_window`, `detections_for_window` (Tasks 1-3), existing `_get_db`, `_opt_float`
- Produces: HTTP routes `GET /api/averaged`, `GET /api/averaged/{window_id}`, `GET /api/averaged/{window_id}/detections`

- [ ] **Step 1: Write the failing tests**

Add to `tests/integration/test_web_integration.py`:

```python
@pytest.fixture
async def _seed_avg(app_with_db):
    from datetime import datetime, timedelta, timezone

    app, db = app_with_db
    start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    await db.insert_avg_window(
        start_time=start, duration_sec=2.0, sdr_center_freq_hz=915e6,
        sample_rate_hz=56e6, gain_db=40.0, num_bins=4, freq_start_hz=0.0,
        freq_step_hz=1.0, pwr_avg=-70.0, pwr_max=-50.0, pwr_median=-72.0,
        pwr_std=3.0, kurtosis=1.0, powers=[-80.0, -70.0, -60.0, -50.0],
    )
    await db.insert_detection(
        burst_id="b1", start_time=start + timedelta(seconds=0.5),
        stop_time=start + timedelta(seconds=0.6), center_freq_hz=915.1e6,
        bandwidth_hz=1e6, peak_power_db=-30.0, duration_ms=100.0,
        detection_timestamp=start + timedelta(seconds=0.5),
        sdr_center_freq_hz=915e6, sample_rate_hz=56e6, gain_db=40.0,
    )
    return app, db


@pytest.mark.asyncio
async def test_api_averaged_list(_seed_avg):
    from httpx import ASGITransport, AsyncClient

    app, _ = _seed_avg
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.get("/api/averaged")
        assert r.status_code == 200
        windows = r.json()["windows"]
        assert len(windows) == 1
        assert windows[0]["sdr_center_freq_hz"] == 915e6
        assert "psd_powers" not in windows[0]


@pytest.mark.asyncio
async def test_api_averaged_detail_and_detections(_seed_avg):
    from httpx import ASGITransport, AsyncClient

    app, _ = _seed_avg
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        wid = (await c.get("/api/averaged")).json()["windows"][0]["id"]
        detail = await c.get(f"/api/averaged/{wid}")
        assert detail.status_code == 200
        body = detail.json()
        assert body["powers"] == pytest.approx([-80.0, -70.0, -60.0, -50.0], abs=1e-3)
        assert len(body["frequencies"]) == 4

        dets = await c.get(f"/api/averaged/{wid}/detections")
        assert dets.status_code == 200
        assert [d["burst_id"] for d in dets.json()["detections"]] == ["b1"]

        assert (await c.get("/api/averaged/9999")).status_code == 404
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_web_integration.py -k averaged -x -q`
Expected: FAIL with 404 on `/api/averaged` (route not defined).

- [ ] **Step 3: Implement the helper + routes**

In `src/rfobserver/web/routes/api.py`, add near `_opt_float`:

```python
def _opt_dt(raw: str | None) -> datetime | None:
    """Parse an ISO 8601 timestamp query param, or None when empty/invalid."""
    if raw is None or raw == "":
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None
```

Ensure `from datetime import datetime` is imported at the top of the module (add it if absent). Then add the routes (place them alongside the other `@router.get` handlers):

```python
@router.get("/averaged")
async def averaged_list(
    request: Request,
    since: str | None = None,
    until: str | None = None,
    sdr_center: str | None = None,
    sample_rate: str | None = None,
    gain: str | None = None,
    limit: str | None = None,
) -> dict[str, Any]:
    """Averaged windows in a datetime/tuning range (no PSD blobs)."""
    db = _get_db(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    rows = await db.query_avg_windows(
        since=_opt_dt(since),
        until=_opt_dt(until),
        sdr_center_freq=_opt_float(sdr_center),
        sample_rate=_opt_float(sample_rate),
        gain=_opt_float(gain),
        limit=int(limit) if limit else 500,
    )
    return {"windows": rows}


@router.get("/averaged/{window_id}")
async def averaged_detail(request: Request, window_id: int) -> dict[str, Any]:
    """One averaged window with decoded PSD + reconstructed frequencies."""
    db = _get_db(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    row = await db.get_avg_window(window_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Averaged window not found")
    return row


@router.get("/averaged/{window_id}/detections")
async def averaged_detections(request: Request, window_id: int) -> dict[str, Any]:
    """Detections associated with an averaged window (time + tuning join)."""
    db = _get_db(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    row = await db.get_avg_window(window_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Averaged window not found")
    return {"detections": await db.detections_for_window(row)}
```

Confirm `HTTPException` and `Request` are already imported in this module (they are used by existing routes). If `Any` is not imported, add `from typing import Any`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_web_integration.py -k averaged -x -q`
Expected: PASS.

- [ ] **Step 5: Lint + types**

Run: `ruff check src/ tests/ && ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/rfobserver/web/routes/api.py tests/integration/test_web_integration.py
git commit -m "api: /api/averaged list, detail, and detection-association endpoints"
```

---

### Task 6: Integration proof + full-suite verification

**Files:**
- Modify: `tests/integration/test_web_integration.py` (add a sink-independent persistence proof) OR reuse an existing mock-pipeline integration test if one already drives `_publish_processed`.
- Test: same file.

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Write the failing test**

Add to `tests/integration/test_web_integration.py` a direct proof that persistence works end-to-end through `_publish_processed` with no sinks and is then queryable via the API:

```python
@pytest.mark.asyncio
async def test_publish_processed_persists_and_is_queryable(app_with_db):
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from httpx import ASGITransport, AsyncClient

    from rfobserver.config import AppSettings
    from rfobserver.models import IQStatistics
    from rfobserver.pipeline.streaming import StreamingProcessor

    app, db = app_with_db
    settings = AppSettings(_env_file=None)
    storage = MagicMock()
    storage.auto_dir = MagicMock()
    storage.manual_dir = MagicMock()
    receiver = MagicMock()
    receiver.serial = "sim0"
    proc = StreamingProcessor(
        receiver=receiver, database=db, local_storage=storage, settings=settings,
        broadcast=None, zms_monitor=None, nats_producer=None, replay_mode=False,
    )
    summary = SimpleNamespace(
        powers=[-80.0, -70.0], frequencies=[915e6, 916e6],
        center_freq=915e6, sample_rate=56_000_000, num_bins=2,
    )
    result = SimpleNamespace(summary_psd=summary, center_freq_hz=915_000_000, capture_num=1)
    stats = IQStatistics(average=-70.0, max=-50.0, median=-72.0, std=3.0, kurtosis=1.0)

    await proc._publish_processed([-80.0, -70.0], result, stats)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        windows = (await c.get("/api/averaged")).json()["windows"]
        assert len(windows) == 1
        assert windows[0]["sdr_center_freq_hz"] == 915_000_000.0
```

- [ ] **Step 2: Run it to verify it fails, then passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_web_integration.py -k "publish_processed_persists" -x -q`
Expected: PASS once Tasks 1-5 are in (it exercises real code; if a signature is off it fails here, which is the point of the integration proof). If it fails, fix the offending task, do not weaken the test.

- [ ] **Step 3: Run the FULL check suite exactly as CI does**

Start a throwaway NATS for the integration suite:

```bash
docker run -d --rm --name rfobs-test-nats -p 4222:4222 nats:2.10-alpine -js
```

Then:

```bash
ruff check src/ tests/
ruff format --check src/ tests/
PYTHONPATH= .venv/bin/mypy src/rfobserver/
PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q
PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q
docker stop rfobs-test-nats
```

Expected: all green. The integration suite takes ~4-5 min.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_web_integration.py
git commit -m "test: end-to-end averaged-window persistence via _publish_processed"
```

---

### Task 7: Deploy to local Jetson and verify (no HCRO)

**Files:** none (deployment/verification only).

- [ ] **Step 1: Push the branch**

```bash
git push -u origin feat/averaged-window-store
```

- [ ] **Step 2: Deploy to the mock Jetson (`nano-super`, no SDR)**

```bash
ssh ocollaco@192.168.97.153 'cd ~/GitHub/RFObserver && git fetch --quiet origin && git checkout feat/averaged-window-store && git pull --quiet --ff-only origin feat/averaged-window-store && git log --oneline -1'
```

- [ ] **Step 3: Run the mock pipeline with ZMS/NATS off and confirm rows accrue**

```bash
ssh ocollaco@192.168.97.153 'cd ~/GitHub/RFObserver && fuser -k 8888/tcp 2>/dev/null; sleep 1; PYTHONPATH= RFOBS_MOCK_RECEIVER=true RFOBS_SENSOR_ACTIVE=true RFOBS_WEB_PORT=8888 nohup .venv/bin/rfobserver run >/tmp/rfobs-run.log 2>&1 & sleep 12; curl -s "http://127.0.0.1:8888/api/averaged?limit=5"'
```

Expected: JSON with a non-empty `windows` array (proves persistence runs with sinks off).

- [ ] **Step 4: Spot-check detail + association**

```bash
ssh ocollaco@192.168.97.153 'cd ~/GitHub/RFObserver && wid=$(curl -s "http://127.0.0.1:8888/api/averaged?limit=1" | python3 -c "import sys,json;print(json.load(sys.stdin)[\"windows\"][0][\"id\"])"); echo "id=$wid"; curl -s "http://127.0.0.1:8888/api/averaged/$wid" | python3 -c "import sys,json;d=json.load(sys.stdin);print(\"bins\",len(d[\"powers\"]),\"freqs\",len(d[\"frequencies\"]))"; curl -s "http://127.0.0.1:8888/api/averaged/$wid/detections" | head -c 200'
```

Expected: `powers`/`frequencies` lengths equal `num_bins`; detections endpoint returns a JSON object (possibly empty list if no bursts in that window).

- [ ] **Step 5: Stop the Jetson pipeline and clean up**

```bash
ssh ocollaco@192.168.97.153 'fuser -k 8888/tcp 2>/dev/null; echo stopped'
```

- [ ] **Step 6: Report to the user** that the local-Jetson verification passed, and that the HCRO deployment (the live box) is theirs to deploy and test. Do NOT touch any HCRO host.

---

## Self-Review

**Spec coverage:**
- Store averaged PSD/stats/flags with start+duration → Task 1 (schema + insert). Yes.
- Full-fidelity float32 BLOB → Task 1 codec. Yes.
- Persist always while live, independent of ZMS/NATS → Task 4 (before sink gate) + Task 6 proof. Yes.
- datetime start/stop selection → Task 1 `query_avg_windows` since/until + Task 5 `/api/averaged`. Yes.
- Detection association → Task 3 `detections_for_window` + Task 5 `/api/averaged/{id}/detections`. Yes.
- Reconstructed frequency axis from start+step → Task 2. Yes.
- Retention via existing cleanup → Task 3. Yes.
- Flags nullable (PSDProcessor gap honest) → Task 1 columns + Task 4 leaves them NULL. Yes.
- No viewer UI → out of scope, not planned. Correct.

**Placeholder scan:** No TBD/TODO; all steps carry real code and exact commands.

**Type consistency:** `insert_avg_window`/`query_avg_windows`/`get_avg_window`/`detections_for_window` signatures match across Tasks 1-5 and their call sites. `_persist_avg_window` and `_opt_dt` names are used consistently. Column names in `_sdr_conditions` (`sdr_center_freq_hz`, `sample_rate_hz`, `gain_db`) match the `avg_windows` schema, so the reuse is valid.
