# Dashboard Peak Finder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Peaks control to the Dashboard toolbar that finds the top N power peaks over a 3-day to 1-month lookback and opens a 15-minute to 3-hour window centred on each one.

**Architecture:** A per-minute rollup table (`avg_minutes`) summarises `avg_windows` so a search is cheap at any lookback. A background loop rolls new minutes forward and backfills history newest-first, persisting watermarks in the existing `config` table. A new read-only endpoint ranks rollup rows and applies a minimum-separation rule. The Dashboard treats a chosen peak as an ordinary absolute time range, so the existing render path is untouched.

**Tech Stack:** SQLite via aiosqlite, FastAPI, vanilla JS (no framework), pytest, Puppeteer.

**Spec:** `docs/superpowers/specs/2026-09-21-dashboard-peak-finder-design.md`

## Global Constraints

- Python >= 3.10 clean. The Jetsons run 3.10; CI covers 3.10, 3.11 and 3.12. No `match`, no PEP 604 runtime-only syntax problems (the codebase uses `from __future__ import annotations` everywhere).
- No emojis anywhere. No em-dashes in code, UI strings, comments or docs.
- Prefix every command with `PYTHONPATH=` to clear the host's leaked system Python path. Example: `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`.
- `ruff` is global (`~/.local/bin/ruff`), not in the venv, so it runs without the prefix.
- Line length limit is 100 characters (ruff E501).
- The lint CI job installs only ruff, mypy, pydantic and pydantic-settings, so any subclass of a third-party class needs `# type: ignore[misc,unused-ignore]`.
- Heavy read queries use the read-only DB connection, a semaphore, and return 499 when the client disconnects, matching the existing Dashboard endpoints.
- The peak search must add no work to the per-window insert path, which runs at about 2 inserts per second on the field sensor.
- Never use `git add -A` or `git add .`. Stage explicit paths only.
- Run the full check set before every commit: `ruff check src/ tests/`, `ruff format --check src/ tests/`, `PYTHONPATH= .venv/bin/mypy src/rfobserver/`, `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`, and for tasks touching routes or the pipeline also `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q` (needs NATS on localhost:4222).

## File Structure

| File | Responsibility |
|---|---|
| `src/rfobserver/storage/rollup.py` (new) | Pure arithmetic: fold windows into minute summaries, select separated peaks. No I/O, no clock, no DB. |
| `src/rfobserver/storage/database.py` (modify) | `avg_minutes` in `SCHEMA`; `upsert_avg_minutes`, `query_avg_minute_peaks`, `oldest_avg_window_time`. |
| `src/rfobserver/pipeline/app.py` (modify) | `_rollup_loop`: forward pass plus newest-first backfill, watermarks in `config`. |
| `src/rfobserver/config.py` (modify) | `PEAKS_ROLLUP_INTERVAL_SEC`. |
| `src/rfobserver/web/routes/api.py` (modify) | `GET /api/averaged/peaks`. |
| `src/rfobserver/web/app.py` (modify) | `peaks_sem` on app state. |
| `src/rfobserver/web/templates/averaged.html` (modify) | Peaks button and popover markup. |
| `src/rfobserver/web/static/averaged.js` (modify) | Peak state, fetch, list render, pick, prev/next. |
| `src/rfobserver/web/static/style.css` (modify) | Peaks popover and list styling. |

`rollup.py` exists because `database.py` (1428 lines) and `averaged.js` (1400 lines) are already large, and because the two rules most likely to be wrong (which minute a window belongs to, and which peaks are far enough apart) are worth testing without a database in the way.

---

### Task 1: Rollup arithmetic and peak selection

Pure functions with no database. Everything that follows depends on these names.

**Files:**
- Create: `src/rfobserver/storage/rollup.py`
- Test: `tests/unit/test_rollup.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `MINUTE_KEY_LEN = 16`
  - `ROLLUP_NEWEST_KEY = "rollup_newest"` and `ROLLUP_OLDEST_KEY = "rollup_oldest"`, the `config` keys the rollup watermarks live under. They live here rather than in `pipeline/app.py` so the web layer can read them without importing the pipeline.
  - `METRICS: tuple[str, ...] = ("pwr_max", "pwr_snr", "pwr_avg")`
  - `PEAK_TIME_COLUMN: dict[str, str]` mapping each metric to its argmax column name
  - `class WindowRow(NamedTuple)` with fields `start_time: str`, `sdr_center_freq_hz: float`, `sample_rate_hz: float | None`, `gain_db: float | None`, `pwr_max: float | None`, `pwr_median: float | None`, `pwr_avg: float | None`
  - `class MinuteSummary(NamedTuple)` with fields `minute_start: str`, `sdr_center_freq_hz: float`, `n: int`, `sample_rate_hz: float | None`, `gain_db: float | None`, `pwr_max: float | None`, `pwr_snr: float | None`, `pwr_avg: float | None`, `peak_max_time: str | None`, `peak_snr_time: str | None`, `peak_avg_time: str | None`
  - `class Candidate(NamedTuple)` with fields `peak_time: datetime`, `value: float`, `pwr_max: float | None`, `pwr_snr: float | None`, `pwr_avg: float | None`
  - `class Peak(NamedTuple)` with fields `rank: int`, `peak_time: datetime`, `since: datetime`, `until: datetime`, `value: float`, `pwr_max: float | None`, `pwr_snr: float | None`, `pwr_avg: float | None`
  - `def minute_key(start_time: str) -> str`
  - `def fold_windows(rows: Iterable[WindowRow]) -> list[MinuteSummary]`
  - `def select_peaks(candidates: Sequence[Candidate], *, window_sec: int, count: int, now: datetime) -> list[Peak]`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_rollup.py`:

```python
"""Tests for the per-minute rollup arithmetic and peak selection."""

from datetime import datetime, timedelta, timezone

from rfobserver.storage.rollup import (
    Candidate,
    MinuteSummary,
    WindowRow,
    fold_windows,
    minute_key,
    select_peaks,
)

UTC = timezone.utc


def _w(ts: str, pwr_max: float, pwr_median: float, pwr_avg: float, center: float = 2.437e9):
    return WindowRow(
        start_time=ts,
        sdr_center_freq_hz=center,
        sample_rate_hz=56e6,
        gain_db=40.0,
        pwr_max=pwr_max,
        pwr_median=pwr_median,
        pwr_avg=pwr_avg,
    )


def test_minute_key_truncates_to_the_minute():
    assert minute_key("2026-09-19T03:14:22.108192+00:00") == "2026-09-19T03:14"
    # isoformat() omits microseconds when they are zero; the key must not shift.
    assert minute_key("2026-09-19T03:14:22+00:00") == "2026-09-19T03:14"


def test_fold_takes_the_maximum_of_each_metric_with_its_own_timestamp():
    rows = [
        _w("2026-09-19T03:14:01+00:00", pwr_max=-30.0, pwr_median=-50.0, pwr_avg=-45.0),
        # highest pwr_max, but a smaller rise above its own noise floor
        _w("2026-09-19T03:14:30+00:00", pwr_max=-20.0, pwr_median=-25.0, pwr_avg=-44.0),
        # highest pwr_snr (40 dB) and highest pwr_avg
        _w("2026-09-19T03:14:59+00:00", pwr_max=-22.0, pwr_median=-62.0, pwr_avg=-40.0),
    ]
    (s,) = fold_windows(rows)
    assert s.minute_start == "2026-09-19T03:14"
    assert s.n == 3
    assert s.pwr_max == -20.0
    assert s.peak_max_time == "2026-09-19T03:14:30+00:00"
    assert s.pwr_snr == 40.0
    assert s.peak_snr_time == "2026-09-19T03:14:59+00:00"
    assert s.pwr_avg == -40.0
    assert s.peak_avg_time == "2026-09-19T03:14:59+00:00"


def test_fold_separates_minutes_and_centre_frequencies():
    rows = [
        _w("2026-09-19T03:14:01+00:00", -30.0, -50.0, -45.0, center=2.437e9),
        _w("2026-09-19T03:15:01+00:00", -31.0, -50.0, -45.0, center=2.437e9),
        _w("2026-09-19T03:14:02+00:00", -32.0, -50.0, -45.0, center=5.8e9),
    ]
    out = {(s.minute_start, s.sdr_center_freq_hz) for s in fold_windows(rows)}
    assert out == {
        ("2026-09-19T03:14", 2.437e9),
        ("2026-09-19T03:15", 2.437e9),
        ("2026-09-19T03:14", 5.8e9),
    }


def test_fold_tolerates_missing_statistics():
    # A window with no pwr_median cannot contribute a pwr_snr, but still counts.
    rows = [
        _w("2026-09-19T03:14:01+00:00", -30.0, -50.0, -45.0),
        WindowRow("2026-09-19T03:14:02+00:00", 2.437e9, 56e6, 40.0, -10.0, None, None),
    ]
    (s,) = fold_windows(rows)
    assert s.n == 2
    assert s.pwr_max == -10.0
    assert s.peak_max_time == "2026-09-19T03:14:02+00:00"
    assert s.pwr_snr == 20.0
    assert s.peak_snr_time == "2026-09-19T03:14:01+00:00"


def test_fold_returns_nothing_for_no_rows():
    assert fold_windows([]) == []


def _c(minutes_ago: float, value: float, now: datetime) -> Candidate:
    t = now - timedelta(minutes=minutes_ago)
    return Candidate(peak_time=t, value=value, pwr_max=value, pwr_snr=30.0, pwr_avg=-45.0)


def test_select_rejects_peaks_closer_together_than_the_window():
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    # Ten candidates inside one minute: one real event, not ten peaks.
    candidates = [_c(600 + i * 0.1, -20.0 - i, now) for i in range(10)]
    peaks = select_peaks(candidates, window_sec=1800, count=10, now=now)
    assert len(peaks) == 1
    assert peaks[0].rank == 1


def test_select_returns_separated_peaks_in_descending_order():
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    candidates = [
        _c(60, -25.0, now),
        _c(600, -20.0, now),   # strongest
        _c(1200, -30.0, now),
    ]
    peaks = select_peaks(candidates, window_sec=1800, count=10, now=now)
    assert [p.rank for p in peaks] == [1, 2, 3]
    assert [p.value for p in peaks] == [-20.0, -25.0, -30.0]


def test_select_centres_the_window_on_the_peak():
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    (peak,) = select_peaks([_c(600, -20.0, now)], window_sec=1800, count=1, now=now)
    assert peak.since == peak.peak_time - timedelta(seconds=900)
    assert peak.until == peak.peak_time + timedelta(seconds=900)


def test_select_clamps_the_window_to_now():
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    # A peak one minute old cannot have fifteen minutes of future in its window.
    (peak,) = select_peaks([_c(1, -20.0, now)], window_sec=1800, count=1, now=now)
    assert peak.until == now
    assert peak.since == peak.peak_time - timedelta(seconds=900)


def test_select_stops_at_count():
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    candidates = [_c(60 * (i + 1), -20.0 - i, now) for i in range(20)]
    assert len(select_peaks(candidates, window_sec=60, count=5, now=now)) == 5


def test_select_returns_fewer_than_count_when_candidates_run_out():
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    assert len(select_peaks([_c(600, -20.0, now)], window_sec=1800, count=10, now=now)) == 1


def test_select_returns_nothing_for_no_candidates():
    now = datetime(2026, 9, 20, 12, 0, tzinfo=UTC)
    assert select_peaks([], window_sec=1800, count=10, now=now) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_rollup.py -x -q`

Expected: collection error, `ModuleNotFoundError: No module named 'rfobserver.storage.rollup'`.

- [ ] **Step 3: Write the implementation**

Create `src/rfobserver/storage/rollup.py`:

```python
"""Per-minute rollup of averaged windows, and peak selection over it.

Pure functions: no database, no clock, no I/O. The rollup loop and the peaks
endpoint own all the I/O and call in here for the arithmetic, so the two rules
most likely to be wrong (which minute a window belongs to, and which peaks are
far enough apart to count as separate events) can be tested on their own.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

# 'YYYY-MM-DDTHH:MM' is the first 16 characters of an ISO-8601 timestamp, and
# sorts lexicographically in the same order as the timestamps themselves, which
# is what lets the rollup be range-filtered without parsing any dates.
MINUTE_KEY_LEN = 16

# How far the rollup has folded forward, and how far back its backfill has
# reached. Stored in the `config` table so a restart resumes instead of
# starting over. Defined here so both the pipeline loop that writes them and
# the web endpoint that reports coverage can use the same names.
ROLLUP_NEWEST_KEY = "rollup_newest"
ROLLUP_OLDEST_KEY = "rollup_oldest"

METRICS: tuple[str, ...] = ("pwr_max", "pwr_snr", "pwr_avg")

# Each metric records the timestamp of the window that achieved it, so the
# Dashboard can centre on the real peak rather than on a minute boundary.
PEAK_TIME_COLUMN: dict[str, str] = {
    "pwr_max": "peak_max_time",
    "pwr_snr": "peak_snr_time",
    "pwr_avg": "peak_avg_time",
}


class WindowRow(NamedTuple):
    """One `avg_windows` row, light columns only."""

    start_time: str
    sdr_center_freq_hz: float
    sample_rate_hz: float | None
    gain_db: float | None
    pwr_max: float | None
    pwr_median: float | None
    pwr_avg: float | None


class MinuteSummary(NamedTuple):
    """One `avg_minutes` row."""

    minute_start: str
    sdr_center_freq_hz: float
    n: int
    sample_rate_hz: float | None
    gain_db: float | None
    pwr_max: float | None
    pwr_snr: float | None
    pwr_avg: float | None
    peak_max_time: str | None
    peak_snr_time: str | None
    peak_avg_time: str | None


class Candidate(NamedTuple):
    """A rollup row considered for selection, already ranked by `value`."""

    peak_time: datetime
    value: float
    pwr_max: float | None
    pwr_snr: float | None
    pwr_avg: float | None


class Peak(NamedTuple):
    """A selected peak and the time range the Dashboard should open for it."""

    rank: int
    peak_time: datetime
    since: datetime
    until: datetime
    value: float
    pwr_max: float | None
    pwr_snr: float | None
    pwr_avg: float | None


def minute_key(start_time: str) -> str:
    """Minute bucket for an ISO-8601 window start, as stored in `avg_windows`."""
    return start_time[:MINUTE_KEY_LEN]


def _metric_value(row: WindowRow, metric: str) -> float | None:
    if metric == "pwr_max":
        return row.pwr_max
    if metric == "pwr_avg":
        return row.pwr_avg
    # pwr_snr is how far the peak rose above that window's own noise floor.
    if row.pwr_max is None or row.pwr_median is None:
        return None
    return row.pwr_max - row.pwr_median


def fold_windows(rows: Iterable[WindowRow]) -> list[MinuteSummary]:
    """Collapse windows into one summary per (minute, centre frequency).

    Each metric keeps its own maximum and the timestamp of the window that
    achieved it; a window missing the statistics a metric needs is skipped for
    that metric but still counted in `n`.
    """
    best: dict[tuple[str, float], dict[str, object]] = {}
    for row in rows:
        key = (minute_key(row.start_time), row.sdr_center_freq_hz)
        acc = best.get(key)
        if acc is None:
            acc = {
                "n": 0,
                "sample_rate_hz": row.sample_rate_hz,
                "gain_db": row.gain_db,
            }
            for metric in METRICS:
                acc[metric] = None
                acc[PEAK_TIME_COLUMN[metric]] = None
            best[key] = acc
        acc["n"] = int(acc["n"]) + 1  # type: ignore[call-overload]
        for metric in METRICS:
            value = _metric_value(row, metric)
            if value is None:
                continue
            current = acc[metric]
            if current is None or value > float(current):  # type: ignore[arg-type]
                acc[metric] = value
                acc[PEAK_TIME_COLUMN[metric]] = row.start_time

    out: list[MinuteSummary] = []
    for (minute, center), acc in best.items():
        out.append(
            MinuteSummary(
                minute_start=minute,
                sdr_center_freq_hz=center,
                n=int(acc["n"]),  # type: ignore[call-overload]
                sample_rate_hz=acc["sample_rate_hz"],  # type: ignore[arg-type]
                gain_db=acc["gain_db"],  # type: ignore[arg-type]
                pwr_max=acc["pwr_max"],  # type: ignore[arg-type]
                pwr_snr=acc["pwr_snr"],  # type: ignore[arg-type]
                pwr_avg=acc["pwr_avg"],  # type: ignore[arg-type]
                peak_max_time=acc["peak_max_time"],  # type: ignore[arg-type]
                peak_snr_time=acc["peak_snr_time"],  # type: ignore[arg-type]
                peak_avg_time=acc["peak_avg_time"],  # type: ignore[arg-type]
            )
        )
    return out


def select_peaks(
    candidates: Sequence[Candidate],
    *,
    window_sec: int,
    count: int,
    now: datetime,
) -> list[Peak]:
    """Pick up to `count` peaks no closer together than `window_sec`.

    Without the separation rule the top N over a week is usually N samples of
    one burst. Candidates must already be ordered by `value` descending; the
    first acceptable candidate wins, so a rejected one is always weaker than
    the peak that displaced it.
    """
    half = timedelta(seconds=window_sec / 2)
    gap = timedelta(seconds=window_sec)
    chosen: list[Candidate] = []
    for candidate in candidates:
        if len(chosen) == count:
            break
        if any(abs(candidate.peak_time - c.peak_time) < gap for c in chosen):
            continue
        chosen.append(candidate)

    peaks: list[Peak] = []
    for rank, candidate in enumerate(chosen, start=1):
        until = candidate.peak_time + half
        peaks.append(
            Peak(
                rank=rank,
                peak_time=candidate.peak_time,
                since=candidate.peak_time - half,
                until=min(until, now),
                value=candidate.value,
                pwr_max=candidate.pwr_max,
                pwr_snr=candidate.pwr_snr,
                pwr_avg=candidate.pwr_avg,
            )
        )
    return peaks
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_rollup.py -x -q`

Expected: 12 passed.

- [ ] **Step 5: Run the full check set**

```bash
ruff check src/ tests/
ruff format --check src/ tests/
PYTHONPATH= .venv/bin/mypy src/rfobserver/
PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q
```

All must pass. If mypy objects to the `dict[str, object]` accumulator, replace it with a small mutable dataclass rather than widening the ignores.

- [ ] **Step 6: Commit**

```bash
git add src/rfobserver/storage/rollup.py tests/unit/test_rollup.py
git commit -m "feat(rollup): minute-bucket folding and separated peak selection"
```

---

### Task 2: `avg_minutes` table and its database methods

**Files:**
- Modify: `src/rfobserver/storage/database.py` (SCHEMA string ending at line 190; new methods next to `query_avg_stats` at line 1071)
- Test: `tests/unit/test_avg_minutes.py`

**Interfaces:**
- Consumes: `MinuteSummary`, `WindowRow`, `METRICS`, `PEAK_TIME_COLUMN`, `fold_windows`, `minute_key` from Task 1.
- Produces, all on `SensorDatabase`:
  - `async def upsert_avg_minutes(self, summaries: Sequence[MinuteSummary]) -> int`
  - `async def iter_rollup_windows(self, *, since: datetime, until: datetime, chunk: int = 5000) -> AsyncIterator[list[WindowRow]]`
  - `async def query_avg_minute_peaks(self, *, since: datetime, until: datetime, metric: str, sdr_center_freq: float | None = None, sample_rate: float | None = None, gain: float | None = None, limit: int = 2000) -> list[tuple[str, float, float | None, float | None, float | None]]` returning `(peak_time, value, pwr_max, pwr_snr, pwr_avg)` ordered by value descending
  - `async def oldest_avg_window_time(self) -> datetime | None`

**Background the implementer needs:**

Timestamps are stored as `datetime.now(timezone.utc).isoformat()`, for example
`2026-09-21T10:00:00.123456+00:00`. All range filtering on `avg_windows` and
`avg_minutes` is therefore a lexicographic string comparison, which sorts
identically to chronological order for this format. Do not add date parsing to
any query.

`_scan_avg_windows` asserts that `where` starts with exactly
`WHERE start_time >= ?`, and it appends `start_time, id` to every row it
yields, so a row has two more elements than the columns requested.

This file has no `ON CONFLICT` anywhere. Its upsert convention is a UNIQUE or
PRIMARY KEY constraint plus `INSERT OR REPLACE` (see `set_config` line 1386 and
`iq_captures` line 596). Follow it. Replace is exact here rather than lossy
because a minute is always folded from all of its windows at once, never
incrementally.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_avg_minutes.py`:

```python
"""Tests for the avg_minutes rollup table and its queries."""

from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.rollup import MinuteSummary, fold_windows

UTC = timezone.utc
T0 = datetime(2026, 9, 19, 3, 0, tzinfo=UTC)


@pytest.fixture
async def db(tmp_path):
    database = SensorDatabase(str(tmp_path / "test.db"))
    await database.connect()
    yield database
    await database.close()


async def _insert_window(db, offset_sec: float, pwr_max: float, center: float = 2.437e9):
    await db.insert_avg_window(
        start_time=T0 + timedelta(seconds=offset_sec),
        duration_sec=0.5,
        sdr_center_freq_hz=center,
        sample_rate_hz=56e6,
        gain_db=40.0,
        num_bins=4,
        freq_start_hz=2.409e9,
        freq_step_hz=1e6,
        powers=np.array([pwr_max, -60.0, -60.0, -60.0], dtype="<f4"),
        pwr_avg=-50.0,
        pwr_max=pwr_max,
        pwr_median=-60.0,
        pwr_std=1.0,
        kurtosis=2.0,
        interference=0,
    )


async def test_rollup_round_trips_through_the_real_insert_path(db):
    # Uses insert_avg_window so the stored timestamp format is the real one.
    await _insert_window(db, 5, -30.0)
    await _insert_window(db, 40, -22.0)
    await _insert_window(db, 70, -35.0)

    rows = []
    async for chunk in db.iter_rollup_windows(since=T0, until=T0 + timedelta(minutes=5)):
        rows.extend(chunk)
    assert len(rows) == 3

    summaries = fold_windows(rows)
    assert await db.upsert_avg_minutes(summaries) == 2

    peaks = await db.query_avg_minute_peaks(
        since=T0, until=T0 + timedelta(minutes=5), metric="pwr_max"
    )
    assert [round(p[1], 1) for p in peaks] == [-22.0, -35.0]
    # The peak timestamp must be the winning window's, not the minute boundary.
    assert peaks[0][0].startswith("2026-09-19T03:00:40")


async def test_upsert_replaces_a_minute_rather_than_duplicating_it(db):
    s = MinuteSummary(
        minute_start="2026-09-19T03:00",
        sdr_center_freq_hz=2.437e9,
        n=1,
        sample_rate_hz=56e6,
        gain_db=40.0,
        pwr_max=-30.0,
        pwr_snr=20.0,
        pwr_avg=-50.0,
        peak_max_time="2026-09-19T03:00:05+00:00",
        peak_snr_time="2026-09-19T03:00:05+00:00",
        peak_avg_time="2026-09-19T03:00:05+00:00",
    )
    await db.upsert_avg_minutes([s])
    await db.upsert_avg_minutes([s._replace(pwr_max=-10.0, n=2)])
    peaks = await db.query_avg_minute_peaks(
        since=T0, until=T0 + timedelta(minutes=5), metric="pwr_max"
    )
    assert len(peaks) == 1
    assert peaks[0][1] == -10.0


async def test_peaks_are_ordered_by_the_requested_metric(db):
    # Minute A: loud peak, high noise floor. Minute B: weaker peak, quiet floor.
    await _insert_window(db, 5, -20.0)
    await _insert_window(db, 65, -30.0)
    rows = []
    async for chunk in db.iter_rollup_windows(since=T0, until=T0 + timedelta(minutes=5)):
        rows.extend(chunk)
    await db.upsert_avg_minutes(fold_windows(rows))

    by_max = await db.query_avg_minute_peaks(
        since=T0, until=T0 + timedelta(minutes=5), metric="pwr_max"
    )
    assert round(by_max[0][1], 1) == -20.0
    by_avg = await db.query_avg_minute_peaks(
        since=T0, until=T0 + timedelta(minutes=5), metric="pwr_avg"
    )
    assert len(by_avg) == 2


async def test_peaks_filter_by_centre_frequency(db):
    await _insert_window(db, 5, -20.0, center=2.437e9)
    await _insert_window(db, 65, -10.0, center=5.8e9)
    rows = []
    async for chunk in db.iter_rollup_windows(since=T0, until=T0 + timedelta(minutes=5)):
        rows.extend(chunk)
    await db.upsert_avg_minutes(fold_windows(rows))

    peaks = await db.query_avg_minute_peaks(
        since=T0, until=T0 + timedelta(minutes=5), metric="pwr_max", sdr_center_freq=2.437e9
    )
    assert len(peaks) == 1
    assert round(peaks[0][1], 1) == -20.0


async def test_peaks_reject_an_unknown_metric(db):
    # The metric name is interpolated into ORDER BY, so it must be whitelisted.
    with pytest.raises(ValueError):
        await db.query_avg_minute_peaks(
            since=T0, until=T0 + timedelta(minutes=5), metric="1; DROP TABLE avg_windows"
        )


async def test_peaks_respect_the_limit(db):
    for i in range(5):
        await _insert_window(db, i * 60 + 5, -30.0 - i)
    rows = []
    async for chunk in db.iter_rollup_windows(since=T0, until=T0 + timedelta(minutes=10)):
        rows.extend(chunk)
    await db.upsert_avg_minutes(fold_windows(rows))
    peaks = await db.query_avg_minute_peaks(
        since=T0, until=T0 + timedelta(minutes=10), metric="pwr_max", limit=2
    )
    assert len(peaks) == 2


async def test_oldest_avg_window_time_is_none_on_an_empty_database(db):
    assert await db.oldest_avg_window_time() is None


async def test_oldest_avg_window_time_returns_the_first_window(db):
    await _insert_window(db, 300, -30.0)
    await _insert_window(db, 5, -30.0)
    oldest = await db.oldest_avg_window_time()
    assert oldest is not None
    assert oldest == T0 + timedelta(seconds=5)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_avg_minutes.py -x -q`

Expected: `AttributeError: 'SensorDatabase' object has no attribute 'iter_rollup_windows'`.

- [ ] **Step 3: Add the table to SCHEMA**

In `src/rfobserver/storage/database.py`, insert this after the `iq_captures`
table (which ends at line 180) and before the `CREATE INDEX` block at line 182,
still inside the triple-quoted `SCHEMA` string:

```sql
-- One row per minute per centre frequency, summarising avg_windows so the
-- Dashboard's peak finder can rank a month of history without scanning
-- millions of rows (43k rows a month here against 5M there). Each metric
-- carries the start_time of the window that achieved it, so a chosen peak
-- opens on the real event rather than on a minute boundary. A minute is
-- always folded from all of its windows at once, so INSERT OR REPLACE is
-- exact rather than lossy. The primary key's implicit index serves the
-- range scan; no separate index is needed at this row count.
CREATE TABLE IF NOT EXISTS avg_minutes (
    minute_start TEXT NOT NULL,
    sdr_center_freq_hz REAL NOT NULL,
    n INTEGER NOT NULL,
    sample_rate_hz REAL,
    gain_db REAL,
    pwr_max REAL,
    pwr_snr REAL,
    pwr_avg REAL,
    peak_max_time TEXT,
    peak_snr_time TEXT,
    peak_avg_time TEXT,
    PRIMARY KEY (minute_start, sdr_center_freq_hz)
);
```

- [ ] **Step 4: Add the module-level upsert SQL**

Next to `_INSERT_DETECTION_SQL` (line 43):

```python
_UPSERT_AVG_MINUTE_SQL = """INSERT OR REPLACE INTO avg_minutes
    (minute_start, sdr_center_freq_hz, n, sample_rate_hz, gain_db,
     pwr_max, pwr_snr, pwr_avg, peak_max_time, peak_snr_time, peak_avg_time)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
```

The column order matches the `MinuteSummary` field order exactly, so a summary
can be passed straight through as a tuple.

Add the import at the top of the file:

```python
from rfobserver.storage.rollup import METRICS, PEAK_TIME_COLUMN, MinuteSummary, WindowRow
```

- [ ] **Step 5: Add the four methods**

Place these next to `query_avg_stats` (line 1071) in `SensorDatabase`:

```python
    @_guarded_write
    async def upsert_avg_minutes(self, summaries: Sequence[MinuteSummary]) -> int:
        """Write minute summaries, replacing any existing row for the same key."""
        if not summaries:
            return 0
        assert self._db is not None
        await self._db.executemany(_UPSERT_AVG_MINUTE_SQL, [tuple(s) for s in summaries])
        await self._db.commit()
        return len(summaries)

    async def iter_rollup_windows(
        self, *, since: datetime, until: datetime, chunk: int = 5000
    ) -> AsyncIterator[list[WindowRow]]:
        """Yield the light columns the rollup folds, in chunks."""
        columns = (
            "start_time, sdr_center_freq_hz, sample_rate_hz, gain_db, "
            "pwr_max, pwr_median, pwr_avg"
        )
        where = "WHERE start_time >= ? AND start_time < ?"
        params: list[Any] = [since.isoformat(), until.isoformat()]
        async for rows in self._scan_avg_windows(columns, where, params, chunk=chunk):
            # _scan_avg_windows appends its keyset columns (start_time, id).
            yield [WindowRow(*r[:7]) for r in rows]

    async def query_avg_minute_peaks(
        self,
        *,
        since: datetime,
        until: datetime,
        metric: str,
        sdr_center_freq: float | None = None,
        sample_rate: float | None = None,
        gain: float | None = None,
        limit: int = 2000,
    ) -> list[tuple[str, float, float | None, float | None, float | None]]:
        """Top rollup minutes for a metric, strongest first.

        `metric` is interpolated into the ORDER BY, so it is whitelisted rather
        than parameterised. The minute bounds are widened to whole minutes and
        the caller re-checks each peak timestamp against the true range, since
        a minute at either edge may straddle it.
        """
        if metric not in METRICS:
            raise ValueError(f"unknown metric {metric!r}, expected one of {METRICS}")
        assert self._db is not None
        peak_col = PEAK_TIME_COLUMN[metric]
        conditions, params = self._sdr_conditions(sdr_center_freq, sample_rate, gain)
        where = [
            "minute_start >= ?",
            "minute_start <= ?",
            f"{metric} IS NOT NULL",
            f"{peak_col} IS NOT NULL",
            *conditions,
        ]
        args: list[Any] = [
            since.isoformat()[:16],
            until.isoformat()[:16],
            *params,
            limit,
        ]
        sql = (
            f"SELECT {peak_col}, {metric}, pwr_max, pwr_snr, pwr_avg FROM avg_minutes "
            f"WHERE {' AND '.join(where)} ORDER BY {metric} DESC LIMIT ?"
        )
        rows = await self._db.execute_fetchall(sql, args)
        return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]

    async def oldest_avg_window_time(self) -> datetime | None:
        """Start time of the earliest averaged window, or None if there are none."""
        assert self._db is not None
        async with self._db.execute("SELECT MIN(start_time) FROM avg_windows") as cursor:
            row = await cursor.fetchone()
        if row is None or row[0] is None:
            return None
        return datetime.fromisoformat(row[0])
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_avg_minutes.py -x -q`

Expected: 8 passed. If `insert_avg_window`'s signature differs from the test
helper, read it at `database.py:527` and correct the test call, not the method.

- [ ] **Step 7: Run the full check set**

```bash
ruff check src/ tests/
ruff format --check src/ tests/
PYTHONPATH= .venv/bin/mypy src/rfobserver/
PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q
```

- [ ] **Step 8: Commit**

```bash
git add src/rfobserver/storage/database.py tests/unit/test_avg_minutes.py
git commit -m "feat(db): avg_minutes rollup table with peak queries"
```

---

### Task 3: Rollup loop with newest-first backfill

**Files:**
- Modify: `src/rfobserver/config.py` (next to `DB_CLEANUP_INTERVAL_SEC`, line 118)
- Modify: `src/rfobserver/pipeline/app.py` (task creation near line 285, loop next to `_cleanup_loop` at line 433)
- Test: `tests/unit/test_rollup_loop.py`

**Interfaces:**
- Consumes: `upsert_avg_minutes`, `iter_rollup_windows`, `oldest_avg_window_time`, `get_config`, `set_config` from Task 2; `fold_windows` from Task 1.
- Produces:
  - `AppSettings.PEAKS_ROLLUP_INTERVAL_SEC: float = 60.0`
  - `async def _rollup_span(db, since: datetime, until: datetime) -> int`
  - `async def _rollup_forward(db, now: datetime) -> None`
  - `async def _rollup_backfill(db, now: datetime) -> None`
  - `async def _rollup_loop(settings, db) -> None`, argument order matching the sibling `_cleanup_loop(settings, db)`

**Why it is shaped this way:** both passes walk at most `_ROLLUP_SPAN` (one
hour, about 7,200 windows) per step so peak memory is bounded no matter how far
behind the rollup is, and each step advances a watermark in the `config` table
so a restart resumes instead of starting over. Each step runs until a small
wall-clock budget is spent, so a cold backfill of a month still completes in
minutes rather than in one blocking pass. `now` is a parameter, not a call to
the clock, so the tests do not need to sleep.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_rollup_loop.py`:

```python
"""Tests for the avg_minutes rollup loop and its watermarks."""

from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from rfobserver.pipeline.app import _rollup_backfill, _rollup_forward, _rollup_span
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.rollup import ROLLUP_NEWEST_KEY, ROLLUP_OLDEST_KEY

UTC = timezone.utc
NOW = datetime(2026, 9, 19, 6, 0, tzinfo=UTC)


@pytest.fixture
async def db(tmp_path):
    database = SensorDatabase(str(tmp_path / "test.db"))
    await database.connect()
    yield database
    await database.close()


async def _insert_window(db, when: datetime, pwr_max: float):
    await db.insert_avg_window(
        start_time=when,
        duration_sec=0.5,
        sdr_center_freq_hz=2.437e9,
        sample_rate_hz=56e6,
        gain_db=40.0,
        num_bins=4,
        freq_start_hz=2.409e9,
        freq_step_hz=1e6,
        powers=np.array([pwr_max, -60.0, -60.0, -60.0], dtype="<f4"),
        pwr_avg=-50.0,
        pwr_max=pwr_max,
        pwr_median=-60.0,
        pwr_std=1.0,
        kurtosis=2.0,
        interference=0,
    )


async def test_span_folds_and_writes(db):
    await _insert_window(db, NOW - timedelta(minutes=30), -25.0)
    written = await _rollup_span(db, NOW - timedelta(hours=1), NOW)
    assert written == 1
    peaks = await db.query_avg_minute_peaks(
        since=NOW - timedelta(hours=1), until=NOW, metric="pwr_max"
    )
    assert round(peaks[0][1], 1) == -25.0


async def test_first_forward_run_seeds_the_watermark_without_scanning(db):
    await _insert_window(db, NOW - timedelta(days=5), -25.0)
    await _rollup_forward(db, NOW)
    assert await db.get_config(_ROLLUP_NEWEST_KEY) == "2026-09-19T06:00"
    # Nothing was rolled up yet; that is the backfill's job.
    assert await db.query_avg_minute_peaks(
        since=NOW - timedelta(days=7), until=NOW, metric="pwr_max"
    ) == []


async def test_forward_rolls_minutes_that_have_closed(db):
    await db.set_config(_ROLLUP_NEWEST_KEY, "2026-09-19T05:00")
    await _insert_window(db, NOW - timedelta(minutes=30), -25.0)
    await _rollup_forward(db, NOW)
    assert await db.get_config(_ROLLUP_NEWEST_KEY) == "2026-09-19T06:00"
    peaks = await db.query_avg_minute_peaks(
        since=NOW - timedelta(hours=2), until=NOW, metric="pwr_max"
    )
    assert len(peaks) == 1


async def test_forward_never_rolls_the_open_minute(db):
    await db.set_config(_ROLLUP_NEWEST_KEY, "2026-09-19T05:59")
    # A window inside the minute that is still in progress.
    await _insert_window(db, NOW + timedelta(seconds=10), -25.0)
    await _rollup_forward(db, NOW + timedelta(seconds=30))
    peaks = await db.query_avg_minute_peaks(
        since=NOW, until=NOW + timedelta(minutes=5), metric="pwr_max"
    )
    assert peaks == []


async def test_backfill_walks_backwards_and_records_how_far_it_reached(db):
    await db.set_config(_ROLLUP_NEWEST_KEY, "2026-09-19T06:00")
    await _insert_window(db, NOW - timedelta(minutes=90), -25.0)
    await _rollup_backfill(db, NOW)
    oldest = await db.get_config(_ROLLUP_OLDEST_KEY)
    assert oldest is not None
    assert oldest <= "2026-09-19T04:30"
    peaks = await db.query_avg_minute_peaks(
        since=NOW - timedelta(hours=3), until=NOW, metric="pwr_max"
    )
    assert len(peaks) == 1


async def test_backfill_stops_at_the_oldest_window(db):
    await db.set_config(_ROLLUP_NEWEST_KEY, "2026-09-19T06:00")
    await _insert_window(db, NOW - timedelta(minutes=10), -25.0)
    await _rollup_backfill(db, NOW)
    first = await db.get_config(_ROLLUP_OLDEST_KEY)
    await _rollup_backfill(db, NOW)
    # Already at the bottom: the watermark must not keep walking into empty time.
    assert await db.get_config(_ROLLUP_OLDEST_KEY) == first


async def test_backfill_does_nothing_on_an_empty_database(db):
    await db.set_config(_ROLLUP_NEWEST_KEY, "2026-09-19T06:00")
    await _rollup_backfill(db, NOW)
    assert await db.get_config(_ROLLUP_OLDEST_KEY) in (None, "2026-09-19T06:00")


async def test_rollup_is_idempotent(db):
    await db.set_config(_ROLLUP_NEWEST_KEY, "2026-09-19T05:00")
    await _insert_window(db, NOW - timedelta(minutes=30), -25.0)
    await _rollup_forward(db, NOW)
    await db.set_config(_ROLLUP_NEWEST_KEY, "2026-09-19T05:00")
    await _rollup_forward(db, NOW)
    peaks = await db.query_avg_minute_peaks(
        since=NOW - timedelta(hours=2), until=NOW, metric="pwr_max"
    )
    assert len(peaks) == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_rollup_loop.py -x -q`

Expected: `ImportError: cannot import name '_rollup_span' from 'rfobserver.pipeline.app'`.

- [ ] **Step 3: Add the config setting**

In `src/rfobserver/config.py`, directly after `DB_CLEANUP_INTERVAL_SEC` (line 118):

```python
    # How often the avg_minutes rollup folds newly closed minutes and advances
    # its backfill of older history (0 disables the rollup, which disables the
    # Dashboard's peak finder). The work per tick is bounded by a span and a
    # time budget, so this is a latency knob, not a load knob.
    PEAKS_ROLLUP_INTERVAL_SEC: float = 60.0
```

- [ ] **Step 4: Add the loop to `pipeline/app.py`**

Module-level constants next to the other loop constants:

```python
# One hour of windows is about 7,200 rows, which bounds peak memory per step no
# matter how far behind the rollup has fallen.
_ROLLUP_SPAN = timedelta(hours=1)
# Wall-clock budget per pass, so a cold backfill of a month finishes in minutes
# without any single pass blocking the loop.
_ROLLUP_BUDGET_SEC = 5.0
```

Functions, next to `_cleanup_loop` (line 433):

```python
def _minute_str(when: datetime) -> str:
    """Minute-resolution key, matching avg_minutes.minute_start."""
    return when.strftime("%Y-%m-%dT%H:%M")


def _parse_minute(key: str) -> datetime:
    return datetime.fromisoformat(key + ":00+00:00")


async def _rollup_span(db: SensorDatabase, since: datetime, until: datetime) -> int:
    """Fold one bounded span of windows into avg_minutes."""
    rows: list[WindowRow] = []
    async for chunk in db.iter_rollup_windows(since=since, until=until):
        rows.extend(chunk)
    if not rows:
        return 0
    return await db.upsert_avg_minutes(fold_windows(rows))


async def _rollup_forward(db: SensorDatabase, now: datetime) -> None:
    """Fold every minute that has closed since the last run."""
    closed = now.replace(second=0, microsecond=0)
    key = await db.get_config(_ROLLUP_NEWEST_KEY)
    if key is None:
        # First run: anchor at the current minute and let the backfill reach
        # back, so a fresh start does not scan the whole table up front.
        await db.set_config(_ROLLUP_NEWEST_KEY, _minute_str(closed))
        return
    since = _parse_minute(key)
    deadline = time.monotonic() + _ROLLUP_BUDGET_SEC
    while since < closed and time.monotonic() < deadline:
        until = min(since + _ROLLUP_SPAN, closed)
        await _rollup_span(db, since, until)
        since = until
        await db.set_config(_ROLLUP_NEWEST_KEY, _minute_str(since))


async def _rollup_backfill(db: SensorDatabase, now: datetime) -> None:
    """Extend the rollup backwards, newest history first."""
    oldest_window = await db.oldest_avg_window_time()
    if oldest_window is None:
        return
    key = await db.get_config(_ROLLUP_OLDEST_KEY)
    if key is None:
        key = await db.get_config(_ROLLUP_NEWEST_KEY)
        if key is None:
            return
        await db.set_config(_ROLLUP_OLDEST_KEY, key)
    until = _parse_minute(key)
    floor = oldest_window.replace(second=0, microsecond=0)
    deadline = time.monotonic() + _ROLLUP_BUDGET_SEC
    while until > floor and time.monotonic() < deadline:
        since = max(until - _ROLLUP_SPAN, floor)
        await _rollup_span(db, since, until)
        until = since
        await db.set_config(_ROLLUP_OLDEST_KEY, _minute_str(until))


async def _rollup_loop(settings: AppSettings, db: SensorDatabase) -> None:
    """Keep avg_minutes in step with avg_windows.

    The forward pass folds minutes that have just closed (about 120 windows).
    The backfill pass deepens history newest-first, so the peak finder works on
    recent data immediately instead of waiting for a full pass over the table.
    """
    while True:
        try:
            now = datetime.now(timezone.utc)
            await _rollup_forward(db, now)
            await _rollup_backfill(db, now)
        except Exception:
            logger.exception("avg_minutes rollup pass failed")
        await asyncio.sleep(settings.PEAKS_ROLLUP_INTERVAL_SEC)
```

Add the imports this needs at the top of the file: `time`, `timedelta`,
`timezone` (check which are already imported), plus
`from rfobserver.storage.rollup import (ROLLUP_NEWEST_KEY, ROLLUP_OLDEST_KEY,
WindowRow, fold_windows)`.

- [ ] **Step 5: Start the task**

`pipeline/app.py:285` currently reads:

```python
    if settings.DB_RETENTION_DAYS > 0:
        workers.append(asyncio.create_task(_cleanup_loop(settings, db)))
```

Add directly beneath it:

```python
    if settings.PEAKS_ROLLUP_INTERVAL_SEC > 0:
        workers.append(asyncio.create_task(_rollup_loop(settings, db)))
```

`workers` is the set awaited and cancelled by the existing serve/teardown block
at lines 288-300, so appending is the whole registration. Do not add a second
teardown path.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_rollup_loop.py -x -q`

Expected: 8 passed.

- [ ] **Step 7: Run the full check set including integration**

```bash
ruff check src/ tests/
ruff format --check src/ tests/
PYTHONPATH= .venv/bin/mypy src/rfobserver/
PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q
PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q
```

The integration run needs NATS on localhost:4222. If it is not running:
`docker run -d --name rfobs-nats-test -p 4222:4222 nats:latest`.

- [ ] **Step 8: Commit**

```bash
git add src/rfobserver/config.py src/rfobserver/pipeline/app.py tests/unit/test_rollup_loop.py
git commit -m "feat(rollup): background loop folding minutes with newest-first backfill"
```

---

### Task 4: `GET /api/averaged/peaks`

**Files:**
- Modify: `src/rfobserver/web/app.py:63-68` (semaphore block)
- Modify: `src/rfobserver/web/routes/api.py` (cache constants near line 39, handler next to `averaged_stats` at line 991)
- Test: `tests/unit/test_peaks_route.py`

**Interfaces:**
- Consumes: `query_avg_minute_peaks`, `get_config` from Task 2; `_ROLLUP_OLDEST_KEY` value `"rollup_oldest"` from Task 3; `Candidate`, `METRICS`, `select_peaks` from Task 1.
- Produces: `GET /api/averaged/peaks` and `app.state.peaks_sem`.

**Two traps in this task:**

1. `_parse_range` may return naive datetimes while `datetime.fromisoformat` on a
   stored `+00:00` timestamp returns an aware one. Comparing the two raises
   `TypeError`. Normalise both through `_as_utc` below. Do not "fix" this by
   stripping tzinfo from the parsed peak times.
2. A rollup minute at either edge of the range can straddle it, so every
   candidate's real timestamp is re-checked against `since`/`until` before
   selection. Skipping this returns peaks from outside the requested range.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_peaks_route.py`:

```python
"""Tests for the Dashboard peak finder endpoint."""

from datetime import datetime, timedelta, timezone

import numpy as np
import pytest
from httpx import ASGITransport, AsyncClient

from rfobserver.config import AppSettings
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.rollup import ROLLUP_OLDEST_KEY, fold_windows
from rfobserver.web.app import create_app

UTC = timezone.utc
NOW = datetime.now(UTC).replace(second=0, microsecond=0)


@pytest.fixture
async def client(tmp_path):
    path = str(tmp_path / "t.db")
    settings = AppSettings(_env_file=None, DB_PATH=path)
    database = SensorDatabase(path)
    await database.connect()
    app = create_app(settings)
    app.state.database = database
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c, database
    await database.close()


async def _seed(db, minutes_ago: float, pwr_max: float):
    await db.insert_avg_window(
        start_time=NOW - timedelta(minutes=minutes_ago),
        duration_sec=0.5,
        sdr_center_freq_hz=2.437e9,
        sample_rate_hz=56e6,
        gain_db=40.0,
        num_bins=4,
        freq_start_hz=2.409e9,
        freq_step_hz=1e6,
        powers=np.array([pwr_max, -60.0, -60.0, -60.0], dtype="<f4"),
        pwr_avg=-50.0,
        pwr_max=pwr_max,
        pwr_median=-60.0,
        pwr_std=1.0,
        kurtosis=2.0,
        interference=0,
    )


async def _roll(db):
    rows = []
    async for chunk in db.iter_rollup_windows(
        since=NOW - timedelta(days=40), until=NOW + timedelta(minutes=1)
    ):
        rows.extend(chunk)
    await db.upsert_avg_minutes(fold_windows(rows))


def _range(days: int = 7) -> dict:
    return {
        "since": (NOW - timedelta(days=days)).isoformat(),
        "until": NOW.isoformat(),
    }


async def test_returns_separated_peaks_strongest_first(client):
    c, db = client
    await _seed(db, 100, -25.0)
    await _seed(db, 500, -18.0)
    await _seed(db, 900, -30.0)
    await _roll(db)
    r = await c.get("/api/averaged/peaks", params={**_range(), "window_sec": 1800, "count": 10})
    assert r.status_code == 200
    body = r.json()
    assert [p["rank"] for p in body["peaks"]] == [1, 2, 3]
    assert round(body["peaks"][0]["value"], 1) == -18.0
    assert body["truncated"] is False


async def test_window_bounds_are_centred_on_the_peak(client):
    c, db = client
    await _seed(db, 500, -18.0)
    await _roll(db)
    r = await c.get("/api/averaged/peaks", params={**_range(), "window_sec": 1800})
    peak = r.json()["peaks"][0]
    t = datetime.fromisoformat(peak["peak_time"])
    assert datetime.fromisoformat(peak["since"]) == t - timedelta(seconds=900)
    assert datetime.fromisoformat(peak["until"]) == t + timedelta(seconds=900)


async def test_peaks_closer_than_the_window_collapse_to_one(client):
    c, db = client
    for i in range(6):
        await _seed(db, 500 + i, -20.0 - i)
    await _roll(db)
    r = await c.get("/api/averaged/peaks", params={**_range(), "window_sec": 1800})
    assert len(r.json()["peaks"]) == 1


async def test_old_peaks_are_flagged_when_their_psd_is_gone(client):
    c, db = client
    # DB_RETENTION_DAYS defaults to 7, so a 10-day-old peak has no blob left.
    await _seed(db, 60 * 24 * 10, -18.0)
    await _seed(db, 60, -19.0)
    await _roll(db)
    r = await c.get("/api/averaged/peaks", params={**_range(days=30), "window_sec": 1800})
    flags = {p["rank"]: p["psd_available"] for p in r.json()["peaks"]}
    assert flags[1] is False
    assert flags[2] is True


async def test_empty_range_returns_an_empty_list_not_a_wider_search(client):
    c, db = client
    await _seed(db, 60 * 24 * 20, -18.0)
    await _roll(db)
    r = await c.get("/api/averaged/peaks", params={**_range(days=3), "window_sec": 1800})
    assert r.status_code == 200
    assert r.json()["peaks"] == []


@pytest.mark.parametrize(
    "params",
    [
        {"window_sec": 1234},
        {"count": 0},
        {"count": 21},
        {"metric": "nonsense"},
    ],
)
async def test_invalid_parameters_are_rejected(client, params):
    c, _ = client
    r = await c.get("/api/averaged/peaks", params={**_range(), **params})
    assert r.status_code == 400


async def test_inverted_range_is_rejected(client):
    c, _ = client
    r = await c.get(
        "/api/averaged/peaks",
        params={"since": NOW.isoformat(), "until": (NOW - timedelta(days=1)).isoformat()},
    )
    assert r.status_code == 400


async def test_covered_since_reports_the_backfill_depth(client):
    c, db = client
    await _seed(db, 60, -18.0)
    await _roll(db)
    await db.set_config(ROLLUP_OLDEST_KEY, (NOW - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M"))
    r = await c.get("/api/averaged/peaks", params=_range(days=30))
    covered = datetime.fromisoformat(r.json()["covered_since"])
    assert covered > NOW - timedelta(days=3)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_peaks_route.py -x -q`

Expected: 404 responses, since the route does not exist.

- [ ] **Step 3: Add the semaphore**

In `src/rfobserver/web/app.py`, after line 68 (`app.state.stats_sem = ...`):

```python
    # The peak search reads the rollup, not the blobs, so it is far lighter than
    # the two above; it still gets its own gate so a burst of panel opens cannot
    # queue behind a waterfall or starve the pipeline.
    app.state.peaks_sem = asyncio.Semaphore(1)
```

- [ ] **Step 4: Add the endpoint**

In `src/rfobserver/web/routes/api.py`, constants next to `_WATERFALL_CACHE` (line 39):

```python
_PEAKS_CACHE: OrderedDict[tuple[Any, ...], dict[str, Any]] = OrderedDict()
_PEAKS_CACHE_MAX = 8
# At minute resolution this covers 33 hours of one event dominating the range
# before the separation rule can run out of candidates.
_PEAKS_CANDIDATE_LIMIT = 2000
_PEAK_WINDOW_SEC = (900, 1800, 3600, 10800)
_PEAK_COUNT_MAX = 20
```

Imports to add: `from rfobserver.storage.rollup import (METRICS, ROLLUP_OLDEST_KEY,
Candidate, select_peaks)` and `timezone` from `datetime`.

Handler, next to `averaged_stats` (line 991):

```python
def _as_utc(value: datetime) -> datetime:
    """Treat a naive datetime as UTC.

    Stored timestamps are timezone-aware, query parameters may not be, and
    comparing the two raises TypeError.
    """
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


@router.get("/averaged/peaks", response_model=None)
async def averaged_peaks(
    request: Request,
    since: str,
    until: str,
    window_sec: str | None = None,
    count: str | None = None,
    metric: str | None = None,
    sdr_center: str | None = None,
    sample_rate: str | None = None,
    gain: str | None = None,
) -> dict[str, Any] | Response:
    """Strongest separated events in a range, for the Dashboard's peak finder.

    Reads the avg_minutes rollup, so cost depends on the number of minutes in
    the range rather than on the number of windows.
    """
    db = _get_db(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    since_dt, until_dt = _parse_range(since, until)
    since_dt, until_dt = _as_utc(since_dt), _as_utc(until_dt)

    window = int(window_sec) if window_sec else 1800
    if window not in _PEAK_WINDOW_SEC:
        raise HTTPException(
            status_code=400, detail=f"window_sec must be one of {list(_PEAK_WINDOW_SEC)}"
        )
    n = int(count) if count else 10
    if not 1 <= n <= _PEAK_COUNT_MAX:
        raise HTTPException(status_code=400, detail=f"count must be 1 to {_PEAK_COUNT_MAX}")
    metric_name = metric or "pwr_max"
    if metric_name not in METRICS:
        raise HTTPException(status_code=400, detail=f"metric must be one of {list(METRICS)}")

    key = (since, until, window, n, metric_name, sdr_center, sample_rate, gain)
    hit = _PEAKS_CACHE.get(key)
    if hit is not None:
        return hit

    async with request.app.state.peaks_sem:
        if await request.is_disconnected():
            return Response(status_code=499)
        hit = _PEAKS_CACHE.get(key)
        if hit is not None:
            return hit
        rows = await db.query_avg_minute_peaks(
            since=since_dt,
            until=until_dt,
            metric=metric_name,
            sdr_center_freq=_opt_float(sdr_center),
            sample_rate=_opt_float(sample_rate),
            gain=_opt_float(gain),
            limit=_PEAKS_CANDIDATE_LIMIT,
        )
        covered_key = await db.get_config(ROLLUP_OLDEST_KEY)

    candidates: list[Candidate] = []
    for peak_time, value, pwr_max, pwr_snr, pwr_avg in rows:
        when = _as_utc(datetime.fromisoformat(peak_time))
        # An edge minute can straddle the requested range.
        if when < since_dt or when >= until_dt:
            continue
        candidates.append(
            Candidate(
                peak_time=when, value=value, pwr_max=pwr_max, pwr_snr=pwr_snr, pwr_avg=pwr_avg
            )
        )

    now = datetime.now(timezone.utc)
    peaks = select_peaks(candidates, window_sec=window, count=n, now=now)
    settings = request.app.state.settings
    psd_cutoff = now - timedelta(days=settings.DB_RETENTION_DAYS)
    covered = since_dt
    if covered_key:
        covered = max(covered, _as_utc(datetime.fromisoformat(covered_key + ":00")))

    payload: dict[str, Any] = {
        "metric": metric_name,
        "window_sec": window,
        "peaks": [
            {
                "rank": p.rank,
                "peak_time": p.peak_time.isoformat(),
                "since": p.since.isoformat(),
                "until": p.until.isoformat(),
                "value": p.value,
                "pwr_max": p.pwr_max,
                "pwr_snr": p.pwr_snr,
                "pwr_avg": p.pwr_avg,
                "psd_available": p.peak_time >= psd_cutoff,
            }
            for p in peaks
        ],
        "covered_since": covered.isoformat(),
        "psd_cutoff": psd_cutoff.isoformat(),
        "truncated": len(peaks) < n and len(rows) >= _PEAKS_CANDIDATE_LIMIT,
    }
    _PEAKS_CACHE[key] = payload
    _PEAKS_CACHE.move_to_end(key)
    while len(_PEAKS_CACHE) > _PEAKS_CACHE_MAX:
        _PEAKS_CACHE.popitem(last=False)
    return payload
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_peaks_route.py -x -q`

Expected: 11 passed (the parametrised case counts as four).

- [ ] **Step 6: Full check set including integration, then commit**

```bash
ruff check src/ tests/ && ruff format --check src/ tests/
PYTHONPATH= .venv/bin/mypy src/rfobserver/
PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q
PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q
git add src/rfobserver/web/app.py src/rfobserver/web/routes/api.py tests/unit/test_peaks_route.py
git commit -m "feat(api): peak finder endpoint over the minute rollup"
```

---

### Task 5: Peaks panel markup, styling and search

Deliverable: opening the panel lists the peaks. Picking one is Task 6.

**Files:**
- Modify: `src/rfobserver/web/templates/averaged.html:15-36` (toolbar) and after line 64 (popover)
- Modify: `src/rfobserver/web/static/averaged.js` (state at line 85, controls at line 1273)
- Modify: `src/rfobserver/web/static/style.css` (next to `.avg-picker` at line 1439)
- Test: `tests/ui/puppeteer_avg_history.js`

**Interfaces:**
- Consumes: `GET /api/averaged/peaks` from Task 4.
- Produces, in `averaged.js`: `state.peaks` object, `PEAKS_LOOKBACK_MS`, `openPeaks()`, `closePeaks()`, `loadPeaks()`, `renderPeaks()`. Task 6 adds `pickPeak`, `stepPeak`, `clearPeakMode`.

- [ ] **Step 1: Add the toolbar markup**

In `averaged.html`, insert before `#avg-back` (line 16), so the control sits at
the left of the toolbar as the spec requires:

```html
                <button type="button" id="avg-peaks-prev" class="btn-preset avg-nav"
                        title="Previous peak" hidden>&#8249;</button>
                <button type="button" id="avg-peaks-btn" class="btn-preset avg-picker-btn"
                        title="Find the strongest events in recent history">
                    <svg width="13" height="13" viewBox="0 0 24 24" fill="none"
                         stroke="currentColor" stroke-width="2" stroke-linecap="round">
                        <path d="M3 18l5-9 4 6 3-4 6 7z"/>
                    </svg>
                    <span id="avg-peaks-label">Peaks</span>
                    <span class="avg-caret">&#9662;</span>
                </button>
                <button type="button" id="avg-peaks-next" class="btn-preset avg-nav"
                        title="Next peak" hidden>&#8250;</button>
```

- [ ] **Step 2: Add the popover markup**

In `averaged.html`, immediately after the `#avg-picker` div closes (line 64):

```html
        <div id="avg-peaks-panel" class="avg-picker avg-peaks-panel" hidden>
            <div class="avg-picker-title">Find peaks</div>
            <div class="avg-peaks-row" role="group" aria-label="Look back">
                <span class="avg-peaks-key">Look back</span>
                <button type="button" data-peaks-lookback="3day">3 days</button>
                <button type="button" data-peaks-lookback="week">7 days</button>
                <button type="button" data-peaks-lookback="2week">2 weeks</button>
                <button type="button" data-peaks-lookback="month">1 month</button>
            </div>
            <div class="avg-peaks-row" role="group" aria-label="Window">
                <span class="avg-peaks-key">Window</span>
                <button type="button" data-peaks-window="900">15 min</button>
                <button type="button" data-peaks-window="1800">30 min</button>
                <button type="button" data-peaks-window="3600">1 hour</button>
                <button type="button" data-peaks-window="10800">3 hours</button>
            </div>
            <div class="avg-peaks-row" role="group" aria-label="Show top">
                <span class="avg-peaks-key">Show top</span>
                <button type="button" data-peaks-count="5">5</button>
                <button type="button" data-peaks-count="10">10</button>
                <button type="button" data-peaks-count="15">15</button>
                <button type="button" data-peaks-count="20">20</button>
            </div>
            <div class="avg-peaks-row" role="group" aria-label="Rank by">
                <span class="avg-peaks-key">Rank by</span>
                <button type="button" data-peaks-metric="pwr_max">Peak power</button>
                <button type="button" data-peaks-metric="pwr_snr">Above noise</button>
                <button type="button" data-peaks-metric="pwr_avg">Band average</button>
            </div>
            <div id="avg-peaks-list" class="avg-peaks-list"></div>
            <div class="avg-hint" id="avg-peaks-foot"></div>
        </div>
```

- [ ] **Step 3: Add the state and the search**

In `averaged.js`, add to `state` (after line 99, next to the range stacks):

```javascript
        peaks: {
            lookback: "week",
            windowSec: 1800,
            count: 10,
            metric: "pwr_max",
            items: [],     // the peaks from the last successful search
            index: -1,     // which peak is currently open, -1 when none
            open: false,
            seq: 0,        // only the newest search may render, as with loadAll
        },
```

Constants next to `PRESET_MS` (line 57):

```javascript
    const PEAKS_LOOKBACK_MS = {
        "3day": 3 * DAY_MS,
        week: 7 * DAY_MS,
        "2week": 14 * DAY_MS,
        month: 30 * DAY_MS,
    };
    const PEAKS_METRIC_LABELS = {
        pwr_max: "peak power",
        pwr_snr: "above noise",
        pwr_avg: "band average",
    };
```

Functions, next to `openPicker`/`closePicker` (line 263):

```javascript
    function openPeaks() {
        state.peaks.open = true;
        $("avg-peaks-panel").hidden = false;
        markPeaksButtons();
        loadPeaks();
    }

    function closePeaks() {
        state.peaks.open = false;
        $("avg-peaks-panel").hidden = true;
    }

    function markPeaksButtons() {
        const p = state.peaks;
        const pairs = [
            ["peaksLookback", String(p.lookback)],
            ["peaksWindow", String(p.windowSec)],
            ["peaksCount", String(p.count)],
            ["peaksMetric", String(p.metric)],
        ];
        for (const [key, value] of pairs) {
            const nodes = $("avg-peaks-panel").querySelectorAll("[data-" + key.replace(
                /[A-Z]/g, function (m) { return "-" + m.toLowerCase(); }) + "]");
            for (const node of nodes) {
                node.classList.toggle("active", node.dataset[key] === value);
            }
        }
    }

    async function loadPeaks() {
        const p = state.peaks;
        const seq = ++p.seq;
        const until = Date.now();
        const since = until - PEAKS_LOOKBACK_MS[p.lookback];
        const params = new URLSearchParams({
            since: new Date(since).toISOString(),
            until: new Date(until).toISOString(),
            window_sec: String(p.windowSec),
            count: String(p.count),
            metric: p.metric,
        });
        const center = $("avg-center").value;
        const rate = $("avg-samplerate").value;
        const gain = $("avg-gain").value;
        if (center) params.set("sdr_center", center);
        if (rate) params.set("sample_rate", rate);
        if (gain) params.set("gain", gain);

        $("avg-peaks-list").innerHTML = '<div class="avg-peaks-empty">Searching...</div>';
        $("avg-peaks-foot").textContent = "";
        const started = Date.now();
        try {
            const res = await fetch("/api/averaged/peaks?" + params.toString());
            if (!res.ok) throw new Error("HTTP " + res.status);
            const body = await res.json();
            if (seq !== p.seq) return;   // a newer search has taken over
            p.items = body.peaks;
            renderPeaks(body, Date.now() - started);
        } catch (err) {
            if (seq !== p.seq) return;
            $("avg-peaks-list").innerHTML = "";
            $("avg-peaks-foot").textContent = "Peak search failed: " + err.message;
        }
    }

    function renderPeaks(body, elapsedMs) {
        const list = $("avg-peaks-list");
        list.innerHTML = "";
        if (!body.peaks.length) {
            list.innerHTML =
                '<div class="avg-peaks-empty">No data in the last ' +
                peaksLookbackLabel() + "</div>";
            return;
        }
        const top = Math.max.apply(null, body.peaks.map(function (x) { return x.value; }));
        const bottom = Math.min.apply(null, body.peaks.map(function (x) { return x.value; }));
        const span = top - bottom || 1;
        for (const peak of body.peaks) {
            const row = document.createElement("button");
            row.type = "button";
            row.className = "avg-peaks-item";
            row.dataset.peakRank = String(peak.rank);
            const bars = Math.max(1, Math.round(((peak.value - bottom) / span) * 7) + 1);
            row.innerHTML =
                '<span class="avg-peaks-rank">' + peak.rank + "</span>" +
                '<span class="avg-peaks-when">' + fmtShort(Date.parse(peak.peak_time)) + "</span>" +
                '<span class="avg-peaks-value">' + peak.value.toFixed(1) + " dB</span>" +
                '<span class="avg-peaks-bar">' + "#".repeat(bars) + "</span>" +
                (peak.psd_available ? "" : '<span class="avg-peaks-tag">stats only</span>');
            list.appendChild(row);
        }
        const foot = ["searched " + peaksLookbackLabel() + " in " + (elapsedMs / 1000).toFixed(2) + " s"];
        const covered = Date.parse(body.covered_since);
        if (covered > Date.now() - PEAKS_LOOKBACK_MS[state.peaks.lookback] + 60000) {
            foot.push("history only reaches back to " + fmtShort(covered));
        }
        if (body.truncated) foot.push("one long event dominates this range");
        $("avg-peaks-foot").textContent = foot.join(" - ");
    }

    function peaksLookbackLabel() {
        return { "3day": "3 days", week: "7 days", "2week": "2 weeks", month: "1 month" }[
            state.peaks.lookback];
    }
```

- [ ] **Step 4: Wire the controls**

Inside `setupControls()` (line 1273), next to the picker wiring at line 1305:

```javascript
        $("avg-peaks-btn").addEventListener("click", function (e) {
            e.stopPropagation();
            if (state.peaks.open) closePeaks();
            else { closePicker(); openPeaks(); }
        });
        $("avg-peaks-panel").addEventListener("click", function (e) { e.stopPropagation(); });
        $("avg-peaks-panel").addEventListener("click", function (e) {
            const b = e.target.closest("button[data-peaks-lookback], button[data-peaks-window]," +
                " button[data-peaks-count], button[data-peaks-metric]");
            if (!b) return;
            const d = b.dataset;
            if (d.peaksLookback) state.peaks.lookback = d.peaksLookback;
            if (d.peaksWindow) state.peaks.windowSec = Number(d.peaksWindow);
            if (d.peaksCount) state.peaks.count = Number(d.peaksCount);
            if (d.peaksMetric) state.peaks.metric = d.peaksMetric;
            markPeaksButtons();
            loadPeaks();
        });
```

Extend the two existing global handlers at lines 1314-1320 so the panel closes
the same way the picker does:

```javascript
        document.addEventListener("click", function () {
            if (state.pickerOpen) closePicker();
            if (state.peaks.open) closePeaks();
        });
        document.addEventListener("keydown", function (e) {
            if (e.key !== "Escape") return;
            if (state.pickerOpen) closePicker();
            if (state.peaks.open) closePeaks();
        });
```

Replace the existing handlers rather than adding a second pair, or the picker
will get two close calls per click.

- [ ] **Step 5: Add the styling**

In `style.css`, next to `.avg-picker` (line 1439). Match the surrounding file's
custom-property names for colours instead of hardcoding hex values; read lines
1428-1480 first and reuse what is there.

`.avg-picker` sets `display: flex` and relies on an explicit
`.avg-picker[hidden] { display: none; }` rule to stay hidden. The peaks panel
reuses that class, so it inherits both; it only needs to become a column and to
sit under its own button on the left.

```css
.avg-peaks-panel {
    flex-direction: column;
    left: 16px;
    right: auto;
    padding: 14px;
    min-width: 380px;
}
.avg-peaks-row { display: flex; align-items: center; gap: 4px; margin-bottom: 6px; }
.avg-peaks-key { width: 74px; font-size: 11px; opacity: 0.7; }
.avg-peaks-row button { font-size: 11px; padding: 3px 8px; }
.avg-peaks-row button.active { font-weight: 600; }
.avg-peaks-list { max-height: 260px; overflow-y: auto; margin-top: 8px; }
.avg-peaks-item {
    display: flex; align-items: center; gap: 8px; width: 100%;
    padding: 4px 6px; font-size: 11px; text-align: left; background: none;
    border: none; border-radius: var(--radius); cursor: pointer; color: inherit;
}
.avg-peaks-item:hover { background: var(--border); }
.avg-peaks-list { border-top: 1px solid var(--border); }
.avg-peaks-rank { width: 18px; opacity: 0.6; }
.avg-peaks-when { width: 120px; }
.avg-peaks-value { width: 64px; text-align: right; }
.avg-peaks-bar { flex: 1; font-family: monospace; opacity: 0.5; letter-spacing: -1px; }
.avg-peaks-tag { font-size: 10px; opacity: 0.6; }
.avg-peaks-empty { font-size: 11px; opacity: 0.7; padding: 8px 6px; }
```

- [ ] **Step 6: Add the UI test**

Append to `tests/ui/puppeteer_avg_history.js`, following the file's existing
structure (read it first; it needs a running instance and
`NODE_PATH=./node_modules` with Chrome at `/usr/bin/google-chrome-stable`):

```javascript
    // Peak finder: the panel opens, searches, and lists what it found.
    await page.click("#avg-peaks-btn");
    await page.waitForSelector("#avg-peaks-panel:not([hidden])");
    await page.waitForFunction(function () {
        const el = document.getElementById("avg-peaks-list");
        return el && !el.textContent.includes("Searching...");
    }, { timeout: 15000 });
    const foot = await page.$eval("#avg-peaks-foot", function (e) { return e.textContent; });
    assert(/searched .* in \d/.test(foot), "peaks footer should report the search: " + foot);

    // Changing a control re-runs the search against the new parameters.
    await page.click('#avg-peaks-panel button[data-peaks-window="3600"]');
    await page.waitForFunction(function () {
        const b = document.querySelector('#avg-peaks-panel button[data-peaks-window="3600"]');
        return b && b.classList.contains("active");
    });

    // Escape closes it, like the range picker.
    await page.keyboard.press("Escape");
    await page.waitForSelector("#avg-peaks-panel[hidden]");
```

- [ ] **Step 7: Run the checks and commit**

```bash
ruff check src/ tests/ && ruff format --check src/ tests/
PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q
# Start an instance first, then run the UI test:
#   PYTHONPATH= RFOBS_MOCK_RECEIVER=true RFOBS_WEB_PORT=8888 .venv/bin/rfobserver run
NODE_PATH=./node_modules node tests/ui/puppeteer_avg_history.js
git add src/rfobserver/web/templates/averaged.html src/rfobserver/web/static/averaged.js \
        src/rfobserver/web/static/style.css tests/ui/puppeteer_avg_history.js
git commit -m "feat(dashboard): peaks panel lists the strongest recent events"
```

---

### Task 6: Picking a peak, stepping between peaks, and clearing peak mode

Deliverable: clicking a peak navigates the Dashboard to it, and the arrows move
between peaks without another search.

**Files:**
- Modify: `src/rfobserver/web/static/averaged.js` (functions next to `renderPeaks`, wiring in `setupControls`)
- Test: `tests/ui/puppeteer_avg_history.js`

**Interfaces:**
- Consumes: `state.peaks`, `loadPeaks`, `renderPeaks`, `closePeaks` from Task 5; the existing `pushRangeHistory()`, `markPresetButtons()`, `setLive(bool)`, `setStale(bool)`, `loadAll(bool)`, `updateRangeLabel()`.
- Produces: `applyPeak(idx)`, `pickPeak(rank)`, `stepPeak(delta)`, `updatePeakNav()`, `clearPeakMode()`.

**The rule that matters:** picking a peak must follow the existing absolute-range
Apply path at `averaged.js:1274-1291` exactly, including `pushRangeHistory()`
before mutating the range. Any other route into `state.sinceMs`/`state.untilMs`
breaks the back button.

- [ ] **Step 1: Add the navigation functions**

Next to `renderPeaks` in `averaged.js`:

```javascript
    function applyPeak(idx) {
        const p = state.peaks;
        const peak = p.items[idx];
        if (!peak) return;
        // Same path as the absolute-range Apply button, so back/forward works.
        pushRangeHistory();
        state.sinceMs = Date.parse(peak.since);
        state.untilMs = Date.parse(peak.until);
        state.spanMs = state.untilMs - state.sinceMs;
        state.activePreset = null;
        p.index = idx;
        markPresetButtons();
        updatePeakNav();
        setLive(false);
        setStale(true);
        loadAll(false);
    }

    function pickPeak(rank) {
        const idx = state.peaks.items.findIndex(function (x) { return x.rank === rank; });
        if (idx < 0) return;
        applyPeak(idx);
        closePeaks();
    }

    function stepPeak(delta) {
        const next = state.peaks.index + delta;
        if (next < 0 || next >= state.peaks.items.length) return;
        applyPeak(next);   // from the cached list; no refetch
    }

    function updatePeakNav() {
        const p = state.peaks;
        const active = p.index >= 0 && p.index < p.items.length;
        $("avg-peaks-label").textContent = active
            ? "Peak " + (p.index + 1) + "/" + p.items.length
            : "Peaks";
        $("avg-peaks-prev").hidden = !active;
        $("avg-peaks-next").hidden = !active;
        $("avg-peaks-prev").disabled = !active || p.index === 0;
        $("avg-peaks-next").disabled = !active || p.index === p.items.length - 1;
    }

    // The range no longer corresponds to a peak, so stop claiming it does.
    function clearPeakMode() {
        if (state.peaks.index < 0) return;
        state.peaks.index = -1;
        updatePeakNav();
    }
```

- [ ] **Step 2: Wire the list and the arrows**

In `setupControls()`, next to the Task 5 panel wiring:

```javascript
        $("avg-peaks-list").addEventListener("click", function (e) {
            const row = e.target.closest(".avg-peaks-item");
            if (row) pickPeak(Number(row.dataset.peakRank));
        });
        $("avg-peaks-prev").addEventListener("click", function (e) {
            e.stopPropagation();
            stepPeak(-1);
        });
        $("avg-peaks-next").addEventListener("click", function (e) {
            e.stopPropagation();
            stepPeak(1);
        });
```

- [ ] **Step 3: Clear peak mode everywhere the range stops being a peak**

Add a `clearPeakMode();` call as the first line of each of these existing
handlers:

- the preset button click handler (`averaged.js:1321-1334`)
- the `#avg-apply` absolute-range handler (`averaged.js:1274`)
- the `#avg-now` handler that turns live mode on
- the back and forward range-history restore functions (`averaged.js:278-320`),
  because a restored snapshot carries no peak index

- [ ] **Step 4: Extend the UI test**

Append to the peaks block in `tests/ui/puppeteer_avg_history.js`:

```javascript
    // Picking a peak closes the panel and navigates the Dashboard to it.
    await page.click("#avg-peaks-btn");
    await page.waitForFunction(function () {
        return document.querySelectorAll("#avg-peaks-list .avg-peaks-item").length > 0;
    }, { timeout: 15000 });
    const firstWhen = await page.$eval(".avg-peaks-item .avg-peaks-when",
        function (e) { return e.textContent; });
    await page.click(".avg-peaks-item");
    await page.waitForSelector("#avg-peaks-panel[hidden]");
    await page.waitForFunction(function () {
        return document.getElementById("avg-peaks-label").textContent.startsWith("Peak 1/");
    });
    assert(firstWhen.length > 0, "peak row should show a timestamp");

    // The arrows step without reopening the panel.
    await page.click("#avg-peaks-next");
    await page.waitForFunction(function () {
        return document.getElementById("avg-peaks-label").textContent.startsWith("Peak 2/");
    });

    // Choosing a preset means the range is no longer a peak, so the label resets.
    await page.click("#avg-picker-btn");
    await page.click('#avg-picker button[data-preset="15m"]');
    await page.waitForFunction(function () {
        return document.getElementById("avg-peaks-label").textContent === "Peaks";
    });
```

- [ ] **Step 5: Run the checks and commit**

```bash
ruff check src/ tests/ && ruff format --check src/ tests/
PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q
NODE_PATH=./node_modules node tests/ui/puppeteer_avg_history.js
git add src/rfobserver/web/static/averaged.js tests/ui/puppeteer_avg_history.js
git commit -m "feat(dashboard): jump to a peak and step between peaks"
```

---

### Task 7: Measure the backfill on a field-size database

The one number in the spec that is estimated rather than measured. It decides
whether `_ROLLUP_SPAN` and `_ROLLUP_BUDGET_SEC` are right, and what to tell the
operator to expect on the first start after upgrading.

**Files:**
- Create: `docs/debugging/2026-09-21_peak-finder-backfill-cost.md`
- Possibly modify: `src/rfobserver/pipeline/app.py` (the two constants)

- [ ] **Step 1: Build a field-scale database**

Seven days at the field rate with real 8 KB blobs is about 10 GB, which is
tractable and extrapolates linearly to the 30-day case. Write the generator to
the scratch directory, not the repo.

```python
# seed_backfill_db.py
import sqlite3, os, random, datetime as dt
p = "backfill_bench.db"
c = sqlite3.connect(p)
c.executescript("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;")
# Copy the CREATE TABLE avg_windows statement verbatim from
# src/rfobserver/storage/database.py so the row layout matches.
N = 7 * 24 * 3600 * 2          # 7 days at 2 windows/sec
t0 = dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc)
blob = os.urandom(8192)         # 2048 bins * 4 bytes, the real size
def rows():
    for i in range(N):
        ts = (t0 + dt.timedelta(seconds=i * 0.5)).isoformat()
        m = random.gauss(-60, 8)
        yield (ts, 0.5, 2.437e9, 56e6, 40.0, 2048, 2.409e9, 27343.75,
               m - 20, m, m - 12, 3.0, 2.0, 0, blob, None, ts)
c.executemany("INSERT INTO avg_windows(...) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows())
c.commit()
c.execute("CREATE INDEX idx_avg_windows_time ON avg_windows(start_time)")
c.execute("CREATE INDEX idx_avg_windows_center_time ON avg_windows(sdr_center_freq_hz, start_time)")
c.commit()
```

- [ ] **Step 2: Time a full backfill**

Run `_rollup_backfill` in a loop against that database until
`rollup_oldest` stops moving, recording wall-clock time, the number of
`avg_minutes` rows produced, and peak RSS. Report seconds per day of history.

- [ ] **Step 3: Decide whether the constants need changing**

Judgement, not a fixed threshold: the backfill runs in the background beside a
live pipeline, so what matters is that no single `_rollup_span` call blocks the
event loop long enough to matter (tens of milliseconds is fine, seconds is not),
and that a month completes in minutes rather than hours. If one hour of windows
takes too long to fold, reduce `_ROLLUP_SPAN`; if the whole backfill is too slow,
raise `_ROLLUP_BUDGET_SEC`. Change the constants and re-measure rather than
guessing.

- [ ] **Step 4: Write the debugging document**

Create `docs/debugging/2026-09-21_peak-finder-backfill-cost.md` with the
sections this repo uses: the question with the exact hardware and scale, the
answer in one or two sentences up front, the procedure, the raw measured output,
anything measured and rejected marked "do not retry", measurement traps, and an
explicit list of what was not determined. State plainly that the numbers come
from a synthetic database on the workstation and that the Jetson is slower.

- [ ] **Step 5: Update the spec's open item**

In `docs/superpowers/specs/2026-09-21-dashboard-peak-finder-design.md`, replace
the "Open, not yet answered" entry about backfill cost with the measured result
and a link to the new document.

- [ ] **Step 6: Commit**

```bash
git add docs/debugging/2026-09-21_peak-finder-backfill-cost.md \
        docs/superpowers/specs/2026-09-21-dashboard-peak-finder-design.md
# add src/rfobserver/pipeline/app.py too if the constants changed
git commit -m "docs: measured backfill cost for the peak finder rollup"
```

---

## Done when

- The Peaks control lists the strongest separated events for 3 days, 7 days,
  2 weeks and 1 month, at 15 min, 30 min, 1 hour and 3 hour windows, for 5, 10,
  15 or 20 peaks, ranked by peak power, above-noise or band average.
- Picking one opens that window through the normal render path; the arrows step
  between peaks; the back button returns to where you were.
- Peaks whose PSD blobs have aged out are tagged before they are clicked.
- An empty lookback says so and does not silently widen the search.
- The rollup keeps itself current and backfills history newest-first, resuming
  after a restart.
- Full check set green, including the Puppeteer test and the integration suite.
