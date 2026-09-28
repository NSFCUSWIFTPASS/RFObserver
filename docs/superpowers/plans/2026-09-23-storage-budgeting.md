# Storage Budgeting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** RFObserver defends a free-space floor on its storage volume, gives up the most re-acquirable data first, never records a failed write as a good capture, and keeps stats rows for two years.

**Architecture:** A pure `StorageGovernor` (`storage/governor.py`) turns a sampled `StorageSample` into a ladder step (0 to 4) with hysteresis, plus actions. `_storage_loop` in `pipeline/app.py` samples every `STORAGE_CHECK_SEC`, ticks the governor and carries out its actions (evict `auto/`, wake retention). The processor, retention loop, health endpoint, heartbeat and UI read the governor's published `StorageState`; the file writer runs its own faster disk check. Write failures end the recording promptly, metadata is derived from the file on disk, and every failure sets a sticky degraded flag persisted in the DB `config` table.

**Tech Stack:** Python 3.10+ (runs on the Jetson's 3.10), asyncio, aiosqlite/SQLite (WAL, `auto_vacuum=0`), FastAPI/Starlette, Jinja2 templates with inline JS, pytest + pytest-asyncio.

**Spec:** `docs/superpowers/specs/2026-09-23-storage-budgeting-design.md`

## Global Constraints

- Branch: `feat/storage-budgeting`. Commit only the files a task names, by explicit path. Never `git add -A` / `git add .`.
- Never add a `Co-Authored-By: Claude` (or any Claude co-author) line to commits.
- No emojis and no em-dashes anywhere: code, comments, UI text, docs.
- UI follows the existing Apple-style CSS variables in `style.css` (`--accent`, `--text-secondary`, `--radius`, ...).
- Python must stay 3.10-clean: no `asyncio.timeout`, no `except*`, `datetime.UTC` not allowed (use `timezone.utc`).
- Every command is prefixed `PYTHONPATH=`; ruff is global (`ruff`, not `.venv/bin/ruff`).
- Before every commit run all five CI checks: `ruff check src/ tests/`, `ruff format --check src/ tests/`, `PYTHONPATH= .venv/bin/mypy src/rfobserver/`, `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`, `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q` (NATS on `localhost:4222`).
- Never `pkill -f "rfobserver run"`; stop a server with `fuser -k <port>/tcp`. Web port is 8888.
- Field sensor `rfnano` is hands-off. Hardware validation happens only on nano-super (`ocollaco@192.168.97.153`), which must be returned to branch `feat/averaged-window-store` with `stash@{0}` intact.
- Settings (exact): `DISK_MIN_FREE_GB: float = 0.0` (0 = auto: 5% of volume, min 2 GB), `STATS_RETENTION_DAYS: int = 730`, `STORAGE_CHECK_SEC: float = 10.0`.
- Constants (exact): recovery margin `1.15`, recovery ticks `3`, step-4 fraction `0.5`, pressure cutoffs PSD `7` days and detections `90` days.
- Never touched by the governor: `manual/` captures, the capture being recorded, stats rows (outside `STATS_RETENTION_DAYS`), `tone_checks`, `iq_captures`.
- `stopped_reason` values: `manual`, `max_duration`, `trigger_end`, `disk_floor`, `write_error`, plus `shutdown` (a real stop cause the spec's list omits; see Task 5).
- Sticky flag config key: `storage_degraded` (value: ISO timestamp, empty string = cleared).

## Review Focus

1. **A capture file vanishes or is renamed between listing and stat** (eviction racing `enforce_cap`, the drop-rename at finalize, a user deleting over SFTP): sampling and eviction must skip it, not crash the storage loop. Test: `test_sample_tolerates_a_capture_deleted_mid_walk` (Task 2).
2. **The active capture after a drop-rename** (`X.sc16` becomes `X_drop3.sc16` during finalize): it must still be excluded from eviction. Test: `test_evict_never_takes_the_active_capture_or_its_drop_renamed_name` (Task 2).
3. **A write error that surfaces only at close** (buffered writer, ENOSPC raised by the final flush after the stop sentinel): the writer must not then block forever waiting for a second sentinel, and the capture must still be marked `write_failed`. Test: `test_error_at_close_after_stop_does_not_hang_and_is_recorded` (Task 6).
4. **Space comes back mid-ladder but only partly** (step 4, then free rises to between floor/2 and floor): the step must not drop until the full recovery condition holds for 3 ticks, and must not flap at the floor. Test: `test_partial_recovery_does_not_leave_step_4` and `test_no_flapping_at_the_floor` (Task 1).
5. **A DB on a different volume filling while the IQ volume is fine**: IQ must not be evicted (it frees nothing there), but pruning and the blob stop still apply. Test: `test_db_volume_short_prunes_but_never_evicts` (Task 1).

---

## File Structure

| File | Responsibility |
|---|---|
| `src/rfobserver/storage/governor.py` (create) | Pure ladder logic, `StorageSample`/`StorageState`/`StorageActions`, floor resolution, error classification helpers. No filesystem or DB access. |
| `src/rfobserver/storage/local.py` (modify) | `sample()` (filesystem half of a `StorageSample`), `evict_until_free()`, manual usage, race-tolerant sizes. |
| `src/rfobserver/storage/database.py` (modify) | `file_stats()`, chunked `delete_older_than()`, chunked keyset `prune_avg_psd_blobs()`, `insert_avg_window(powers=None)`. |
| `src/rfobserver/config.py` (modify) | The three new settings. |
| `src/rfobserver/pipeline/app.py` (modify) | Governor construction and restore, `_storage_loop`, retention rework in `_cleanup_loop`, wiring into processor, web and heartbeat. |
| `src/rfobserver/pipeline/streaming.py` (modify) | Recording refusal, stop reasons, writer error handling, mid-recording guard, file-derived metadata, RAM `tofile` guard, write-error reporting, step-4 blob skip. |
| `src/rfobserver/web/app.py` (modify) | Health `storage` block and degraded status. |
| `src/rfobserver/web/routes/api.py` (modify) | `POST /api/storage/clear-degraded`; 409 on refused recording starts. |
| `src/rfobserver/web/routes/config.py` (modify) | Field map for the three settings. |
| `src/rfobserver/web/templates/dashboard.html`, `config.html`, `static/style.css` (modify) | Storage banner, record-refusal notice, config storage bar and fields. |
| Tests (create) | `tests/unit/test_storage_governor.py`, `tests/unit/test_storage_local_budget.py`, `tests/unit/test_retention.py`, `tests/unit/test_storage_loop.py`, `tests/unit/test_recording_storage.py`, `tests/unit/test_storage_health.py` |
| `README.md` (modify) | "Storage" section. |

---

### Task 1: Settings and the pure StorageGovernor

**Files:**
- Create: `src/rfobserver/storage/governor.py`
- Modify: `src/rfobserver/config.py` (after `DB_CLEANUP_INTERVAL_SEC`, line ~123)
- Test: `tests/unit/test_storage_governor.py`

**Interfaces:**
- Consumes: nothing.
- Produces (later tasks rely on these exact names):
  - `GB: int`, `RECOVERY_MARGIN = 1.15`, `RECOVERY_TICKS = 3`, `HARD_FLOOR_FRACTION = 0.5`, `PRESSURE_PSD_DAYS = 7`, `PRESSURE_DETECTION_DAYS = 90`, `DEGRADED_CONFIG_KEY = "storage_degraded"`
  - `resolve_floor(min_free_gb: float, total_bytes: int) -> int`
  - `VolumeSample(free_bytes: int, total_bytes: int)` (frozen dataclass)
  - `StorageSample(data: VolumeSample, db_volume: VolumeSample | None, db_file_bytes: int, db_reusable_bytes: int, auto_bytes: int, manual_bytes: int, evictable_auto: bool)`
  - `StorageActions(evict_to_free_bytes: int | None = None, start_pressure_prune: bool = False)`
  - `StorageState` with fields `step, step_since, floor_bytes, db_floor_bytes, sample, last_write_error, degraded_since` and properties `pressure` (step>=2), `refuse_recording` (step>=3), `skip_psd_blobs` (step>=4), `degraded`, method `to_health() -> dict[str, Any]`
  - `StorageGovernor()` with `tick(sample, *, min_free_gb: float, now: datetime) -> StorageActions`, `state` (property, thread-safe snapshot), `report_write_error(message: str, now: datetime | None = None) -> None`, `clear_degraded() -> None`, `restore_degraded(raw: str | None) -> None`, `take_degraded_change() -> tuple[bool, str]`
  - `describe_write_error(exc: BaseException) -> str`, `is_disk_full_error(exc: BaseException) -> bool`

- [ ] **Step 1: Add the settings**

In `src/rfobserver/config.py`, directly after `DB_CLEANUP_INTERVAL_SEC: float = 3600.0`:

```python
    # Storage budgeting (docs/superpowers/specs/2026-09-23-storage-budgeting-design.md).
    # Free space RFObserver defends on STORAGE_PATH's volume (and on DB_PATH's,
    # if that is a different device). 0 = auto: 5% of the volume, at least 2 GB.
    # Below it, automatic captures are evicted oldest first, then PSD history and
    # detections are pruned harder, then recordings are refused, then PSD blobs
    # stop being written. Manual captures are never deleted.
    DISK_MIN_FREE_GB: float = 0.0
    # Rows of avg_windows (stats), detections and avg_minutes older than this
    # are deleted (0 disables). DB_RETENTION_DAYS still governs the PSD blobs.
    STATS_RETENTION_DAYS: int = 730
    # How often the storage governor samples the disk.
    STORAGE_CHECK_SEC: float = 10.0
```

- [ ] **Step 2: Write the failing governor tests**

Create `tests/unit/test_storage_governor.py`:

```python
"""StorageGovernor: the free-space ladder, as pure decisions over samples."""

from __future__ import annotations

import errno
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from rfobserver.storage.governor import (
    GB,
    StorageGovernor,
    StorageSample,
    VolumeSample,
    describe_write_error,
    is_disk_full_error,
    resolve_floor,
)

T0 = datetime(2026, 9, 23, 10, 0, tzinfo=timezone.utc)
VOL = 1000 * GB  # auto floor = 50 GB


def _s(free_gb: float, *, evictable: bool = True, db_free_gb: float | None = None) -> StorageSample:
    return StorageSample(
        data=VolumeSample(int(free_gb * GB), VOL),
        db_volume=None if db_free_gb is None else VolumeSample(int(db_free_gb * GB), 100 * GB),
        db_file_bytes=40 * GB,
        db_reusable_bytes=3 * GB,
        auto_bytes=480 * GB,
        manual_bytes=118 * GB,
        evictable_auto=evictable,
    )


def _run(gov: StorageGovernor, *samples: StorageSample, min_free_gb: float = 0.0):
    out = []
    for i, s in enumerate(samples):
        out.append(gov.tick(s, min_free_gb=min_free_gb, now=T0 + timedelta(seconds=10 * i)))
    return out


def test_auto_floor_is_5_percent_with_a_2_gb_minimum():
    assert resolve_floor(0, 916 * GB) == int(916 * GB * 0.05)
    assert resolve_floor(0, 20 * GB) == 2 * GB
    assert resolve_floor(12.5, 916 * GB) == int(12.5 * GB)
    assert resolve_floor(-1, 916 * GB) == int(916 * GB * 0.05)  # negative = auto


def test_healthy_is_step_0_with_no_actions():
    gov = StorageGovernor()
    (a,) = _run(gov, _s(200))
    assert gov.state.step == 0
    assert a.evict_to_free_bytes is None and not a.start_pressure_prune


def test_below_floor_with_evictable_iq_is_step_1_and_evicts_to_the_margin():
    gov = StorageGovernor()
    (a,) = _run(gov, _s(40))
    assert gov.state.step == 1
    assert a.evict_to_free_bytes == int(50 * GB * 1.15)
    assert not gov.state.refuse_recording


def test_below_floor_without_evictable_iq_is_step_2_then_3():
    gov = StorageGovernor()
    a1, a2 = _run(gov, _s(40, evictable=False), _s(40, evictable=False))
    assert a1.start_pressure_prune  # entering step 2 wakes retention now
    assert not a2.start_pressure_prune
    assert gov.state.step == 3  # pruning returned no disk
    assert gov.state.refuse_recording
    assert gov.state.degraded_since == T0 + timedelta(seconds=10)


def test_step_2_is_not_treated_as_reclaiming():
    """Pruning frees pages inside the DB file, not disk: a tick at step 2 that is
    still short must escalate, never sit at 2 waiting for space."""
    gov = StorageGovernor()
    _run(gov, *[_s(40, evictable=False)] * 5)
    assert gov.state.step == 3


def test_below_half_floor_is_step_4_even_with_evictable_iq():
    gov = StorageGovernor()
    (a,) = _run(gov, _s(20))
    assert gov.state.step == 4
    assert gov.state.skip_psd_blobs and gov.state.refuse_recording
    assert a.evict_to_free_bytes == int(50 * GB * 1.15)  # cumulative: still evicts


def test_recovery_needs_the_margin_for_3_consecutive_ticks():
    gov = StorageGovernor()
    _run(gov, _s(40))
    assert gov.state.step == 1
    _run(gov, _s(58), _s(58))  # 58 >= 57.5 for 2 ticks
    assert gov.state.step == 1
    _run(gov, _s(58))
    assert gov.state.step == 0


def test_no_flapping_at_the_floor():
    gov = StorageGovernor()
    steps = []
    for free in [49, 51, 49.5, 52, 49.9, 55, 50.1, 56]:
        _run(gov, _s(free))
        steps.append(gov.state.step)
    assert steps == [1] * 8  # never reached 57.5, so never left step 1


def test_recovery_streak_resets_on_a_short_tick():
    gov = StorageGovernor()
    _run(gov, _s(40), _s(60), _s(60), _s(56), _s(60), _s(60))
    assert gov.state.step == 1
    _run(gov, _s(60))
    assert gov.state.step == 0


def test_partial_recovery_does_not_leave_step_4():
    gov = StorageGovernor()
    _run(gov, _s(20, evictable=False))
    assert gov.state.step == 4
    _run(gov, *[_s(40, evictable=False)] * 5)  # above floor/2, below floor
    assert gov.state.step == 4
    _run(gov, *[_s(60, evictable=False)] * 3)
    assert gov.state.step == 0


def test_evicts_whenever_short_and_evictable_even_above_step_1():
    gov = StorageGovernor()
    _run(gov, _s(40, evictable=False), _s(40, evictable=False))
    assert gov.state.step == 3
    (a,) = _run(gov, _s(40, evictable=True))  # a capture finished meanwhile
    assert gov.state.step == 3
    assert a.evict_to_free_bytes == int(50 * GB * 1.15)


def test_db_volume_short_prunes_but_never_evicts():
    gov = StorageGovernor()
    (a,) = _run(gov, _s(500, db_free_gb=4))  # db floor = 5 GB (5% of 100)
    assert gov.state.step == 2
    assert a.evict_to_free_bytes is None
    assert a.start_pressure_prune
    assert gov.state.db_floor_bytes == 5 * GB


def test_explicit_floor_overrides_auto():
    gov = StorageGovernor()
    _run(gov, _s(40), min_free_gb=30)
    assert gov.state.step == 0


def test_step_since_moves_only_on_a_change():
    gov = StorageGovernor()
    _run(gov, _s(40), _s(41))
    assert gov.state.step_since == T0


def test_write_error_sets_the_sticky_flag_and_it_survives_recovery():
    gov = StorageGovernor()
    _run(gov, _s(200))
    gov.report_write_error("ENOSPC: No space left on device", now=T0)
    _run(gov, *[_s(200)] * 5)
    st = gov.state
    assert st.degraded and st.step == 0
    assert st.last_write_error == {"at": T0.isoformat(), "error": "ENOSPC: No space left on device"}
    gov.clear_degraded()
    assert not gov.state.degraded and gov.state.last_write_error is None


def test_clearing_at_step_3_does_not_reset_the_flag_until_step_3_is_entered_again():
    gov = StorageGovernor()
    _run(gov, _s(40, evictable=False), _s(40, evictable=False))
    gov.clear_degraded()
    _run(gov, _s(40, evictable=False))
    assert gov.state.degraded_since is None
    assert gov.state.degraded  # still step 3


def test_degraded_change_is_reported_once_for_persistence():
    gov = StorageGovernor()
    assert gov.take_degraded_change() == (False, "")
    gov.report_write_error("x", now=T0)
    assert gov.take_degraded_change() == (True, T0.isoformat())
    assert gov.take_degraded_change() == (False, "")
    gov.clear_degraded()
    assert gov.take_degraded_change() == (True, "")


def test_restore_degraded_parses_the_persisted_value():
    gov = StorageGovernor()
    gov.restore_degraded(T0.isoformat())
    assert gov.state.degraded_since == T0
    assert gov.take_degraded_change() == (False, "")  # already persisted
    gov2 = StorageGovernor()
    gov2.restore_degraded("")
    gov2.restore_degraded(None)
    gov2.restore_degraded("not a timestamp")
    assert gov2.state.degraded_since is None


def test_health_block_shape():
    gov = StorageGovernor()
    _run(gov, _s(40))
    h = gov.state.to_health()
    assert h["free_gb"] == 40.0 and h["floor_gb"] == 50.0 and h["volume_gb"] == 1000.0
    assert h["db_gb"] == 40.0 and h["db_reusable_gb"] == 3.0
    assert h["auto_gb"] == 480.0 and h["manual_gb"] == 118.0
    assert h["step"] == 1 and h["step_text"]
    assert h["step_since"] == T0.isoformat()
    assert h["last_write_error"] is None and h["degraded_since"] is None
    assert h["db_volume"] is None
    gov2 = StorageGovernor()
    _run(gov2, _s(500, db_free_gb=4))
    assert gov2.state.to_health()["db_volume"] == {"free_gb": 4.0, "floor_gb": 5.0}


def test_health_before_the_first_tick():
    h = StorageGovernor().state.to_health()
    assert h["step"] == 0 and h["free_gb"] is None


@pytest.mark.parametrize(
    "exc,expected",
    [
        (OSError(errno.ENOSPC, "No space left on device"), True),
        (OSError(errno.EDQUOT, "Disk quota exceeded"), True),
        (sqlite3.OperationalError("database or disk is full"), True),
        (OSError(errno.EIO, "Input/output error"), False),
        (sqlite3.OperationalError("database is locked"), False),
        (ValueError("x"), False),
    ],
)
def test_is_disk_full_error(exc, expected):
    assert is_disk_full_error(exc) is expected


def test_describe_write_error():
    assert describe_write_error(OSError(errno.ENOSPC, "No space left on device")) == (
        "ENOSPC: No space left on device"
    )
    assert describe_write_error(ValueError("bad")) == "ValueError: bad"
```

- [ ] **Step 3: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_storage_governor.py -q`
Expected: collection error, `ModuleNotFoundError: No module named 'rfobserver.storage.governor'`.

- [ ] **Step 4: Implement `storage/governor.py`**

```python
"""Storage governor: keeps the storage volume above a free-space floor.

Pure decisions over a sampled StorageSample: nothing here touches the
filesystem or the DB. pipeline/app.py:_storage_loop samples, ticks, and
carries out the returned actions; consumers read the published state.
Design: docs/superpowers/specs/2026-09-23-storage-budgeting-design.md

The ladder (cumulative; leaving any step needs free >= floor x 1.15 for
RECOVERY_TICKS consecutive ticks):
  0 healthy
  1 free < floor, an evictable auto/ capture exists: evict oldest first
  2 free < floor, nothing evictable: prune PSD blobs and detections harder
  3 still short a tick after step 2 (pruning returns no disk): refuse recordings
  4 free < floor / 2: stop writing PSD blobs
"""

from __future__ import annotations

import errno
import sqlite3
import threading
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

GB = 1024**3
AUTO_FLOOR_FRACTION = 0.05
AUTO_FLOOR_MIN_BYTES = 2 * GB
RECOVERY_MARGIN = 1.15
RECOVERY_TICKS = 3
HARD_FLOOR_FRACTION = 0.5
PRESSURE_PSD_DAYS = 7
PRESSURE_DETECTION_DAYS = 90
DEGRADED_CONFIG_KEY = "storage_degraded"

STEP_TEXT = {
    0: "healthy",
    1: "evicting the oldest automatic captures",
    2: "pruning PSD history and detections",
    3: "recordings refused",
    4: "PSD history writes stopped",
}


def resolve_floor(min_free_gb: float, total_bytes: int) -> int:
    """The floor in bytes: an explicit DISK_MIN_FREE_GB, or 5% of the volume
    with a 2 GB minimum when it is 0 (or negative)."""
    if min_free_gb > 0:
        return int(min_free_gb * GB)
    return max(int(total_bytes * AUTO_FLOOR_FRACTION), AUTO_FLOOR_MIN_BYTES)


@dataclass(frozen=True)
class VolumeSample:
    free_bytes: int
    total_bytes: int


@dataclass(frozen=True)
class StorageSample:
    data: VolumeSample
    # Only when DB_PATH is on a different device from STORAGE_PATH.
    db_volume: VolumeSample | None
    db_file_bytes: int
    db_reusable_bytes: int
    auto_bytes: int
    manual_bytes: int
    # Any auto/ capture other than the one being recorded.
    evictable_auto: bool


@dataclass(frozen=True)
class StorageActions:
    # Evict oldest auto/ captures until the data volume has this much free.
    evict_to_free_bytes: int | None = None
    # Step 2 was just entered: run a retention pass now, not at the next hour.
    start_pressure_prune: bool = False


def _gb(n: int | None) -> float | None:
    return None if n is None else round(n / GB, 1)


@dataclass(frozen=True)
class StorageState:
    step: int
    step_since: datetime
    floor_bytes: int
    db_floor_bytes: int | None
    sample: StorageSample | None
    last_write_error: dict[str, str] | None
    degraded_since: datetime | None

    @property
    def pressure(self) -> bool:
        return self.step >= 2

    @property
    def refuse_recording(self) -> bool:
        return self.step >= 3

    @property
    def skip_psd_blobs(self) -> bool:
        return self.step >= 4

    @property
    def degraded(self) -> bool:
        return self.step >= 3 or self.degraded_since is not None

    def to_health(self) -> dict[str, Any]:
        s = self.sample
        db_volume = None
        if s is not None and s.db_volume is not None:
            db_volume = {
                "free_gb": _gb(s.db_volume.free_bytes),
                "floor_gb": _gb(self.db_floor_bytes),
            }
        return {
            "free_gb": _gb(s.data.free_bytes) if s else None,
            "floor_gb": _gb(self.floor_bytes) if s else None,
            "volume_gb": _gb(s.data.total_bytes) if s else None,
            "db_gb": _gb(s.db_file_bytes) if s else None,
            "db_reusable_gb": _gb(s.db_reusable_bytes) if s else None,
            "auto_gb": _gb(s.auto_bytes) if s else None,
            "manual_gb": _gb(s.manual_bytes) if s else None,
            "step": self.step,
            "step_text": STEP_TEXT[self.step],
            "step_since": self.step_since.isoformat(),
            "last_write_error": self.last_write_error,
            "degraded_since": self.degraded_since.isoformat() if self.degraded_since else None,
            "db_volume": db_volume,
        }


def _volume_step(free: int, floor: int, evictable: bool, current: int) -> int:
    """The step one volume asks for right now, before hysteresis."""
    if free < floor * HARD_FLOOR_FRACTION:
        return 4
    if free >= floor:
        return 0
    if evictable:
        return 1
    # Short with nothing to evict. Step 2 prunes, which returns no disk, so a
    # tick that finds the volume still short after step 2 escalates to 3.
    return 3 if current >= 2 else 2


class StorageGovernor:
    """Holds the ladder step and the sticky degraded flag.

    ``tick`` runs on the event loop; ``report_write_error`` is called from the
    writer and recording-control threads; ``state`` from anywhere. A lock
    keeps each snapshot consistent.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state = StorageState(
            step=0,
            step_since=datetime.now(timezone.utc),
            floor_bytes=0,
            db_floor_bytes=None,
            sample=None,
            last_write_error=None,
            degraded_since=None,
        )
        self._good_ticks = 0
        self._degraded_dirty = False

    @property
    def state(self) -> StorageState:
        with self._lock:
            return self._state

    def tick(self, sample: StorageSample, *, min_free_gb: float, now: datetime) -> StorageActions:
        with self._lock:
            st = self._state
            floor = resolve_floor(min_free_gb, sample.data.total_bytes)
            data = sample.data
            raw = _volume_step(data.free_bytes, floor, sample.evictable_auto, st.step)
            recovered = data.free_bytes >= floor * RECOVERY_MARGIN
            db_floor = None
            if sample.db_volume is not None:
                db_floor = resolve_floor(min_free_gb, sample.db_volume.total_bytes)
                # Evicting IQ frees nothing on the DB's volume: never evictable.
                raw = max(raw, _volume_step(sample.db_volume.free_bytes, db_floor, False, st.step))
                recovered = (
                    recovered and sample.db_volume.free_bytes >= db_floor * RECOVERY_MARGIN
                )

            step = st.step
            if raw > step:
                step = raw
                self._good_ticks = 0
            elif raw < step and recovered:
                self._good_ticks += 1
                if self._good_ticks >= RECOVERY_TICKS:
                    step = raw
                    self._good_ticks = 0
            else:
                self._good_ticks = 0

            degraded_since = st.degraded_since
            if step >= 3 and st.step < 3 and degraded_since is None:
                degraded_since = now
                self._degraded_dirty = True
            self._state = replace(
                st,
                step=step,
                step_since=now if step != st.step else st.step_since,
                floor_bytes=floor,
                db_floor_bytes=db_floor,
                sample=sample,
                degraded_since=degraded_since,
            )

            target = int(floor * RECOVERY_MARGIN)
            evict = step >= 1 and sample.evictable_auto and data.free_bytes < target
            return StorageActions(
                evict_to_free_bytes=target if evict else None,
                start_pressure_prune=st.step < 2 <= step,
            )

    def report_write_error(self, message: str, now: datetime | None = None) -> None:
        when = now or datetime.now(timezone.utc)
        with self._lock:
            since = self._state.degraded_since
            if since is None:
                since = when
                self._degraded_dirty = True
            self._state = replace(
                self._state,
                last_write_error={"at": when.isoformat(), "error": message},
                degraded_since=since,
            )

    def clear_degraded(self) -> None:
        with self._lock:
            self._state = replace(self._state, last_write_error=None, degraded_since=None)
            self._degraded_dirty = True

    def restore_degraded(self, raw: str | None) -> None:
        """Load the persisted flag at startup (not marked dirty: it is on disk)."""
        if not raw:
            return
        try:
            since = datetime.fromisoformat(raw)
        except ValueError:
            return
        with self._lock:
            self._state = replace(self._state, degraded_since=since)

    def take_degraded_change(self) -> tuple[bool, str]:
        """(changed since last call, value to persist: ISO time or "")."""
        with self._lock:
            changed = self._degraded_dirty
            self._degraded_dirty = False
            since = self._state.degraded_since
            return changed, since.isoformat() if since else ""


def describe_write_error(exc: BaseException) -> str:
    """``ENOSPC: No space left on device`` for OS errors, else type and text."""
    if isinstance(exc, OSError) and exc.errno is not None:
        name = errno.errorcode.get(exc.errno, str(exc.errno))
        return f"{name}: {exc.strerror or exc}"
    return f"{type(exc).__name__}: {exc}"


def is_disk_full_error(exc: BaseException) -> bool:
    if isinstance(exc, OSError) and exc.errno in (errno.ENOSPC, errno.EDQUOT):
        return True
    return isinstance(exc, sqlite3.Error) and "disk is full" in str(exc)
```

- [ ] **Step 5: Run the tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_storage_governor.py -q`
Expected: all pass. If `test_clearing_at_step_3...` fails, check that the flag is only set on the transition into step >= 3 (`st.step < 3`).

- [ ] **Step 6: Full checks and commit**

Run the five Global Constraints checks, then:

```bash
git add src/rfobserver/storage/governor.py src/rfobserver/config.py tests/unit/test_storage_governor.py
git commit -m "feat(storage): free-space governor ladder and settings"
```

---

### Task 2: LocalStorage sampling and eviction to a free-space target

**Files:**
- Modify: `src/rfobserver/storage/local.py`
- Test: `tests/unit/test_storage_local_budget.py`

**Interfaces:**
- Consumes: `VolumeSample`, `StorageSample` from Task 1.
- Produces:
  - `LocalStorage.sample(*, db_path: Path, active_names: Collection[str], db_file_bytes: int, db_reusable_bytes: int) -> StorageSample`
  - `LocalStorage.evict_until_free(target_free_bytes: int, *, exclude: Collection[str] = (), free_bytes: Callable[[], int] | None = None) -> int` (bytes freed)
  - `LocalStorage.get_manual_usage_bytes() -> int`
  - module function `is_active_capture(name: str, active_names: Collection[str]) -> bool`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_storage_local_budget.py`:

```python
"""LocalStorage under the governor: sampling and eviction to a free target."""

from __future__ import annotations

import os
from pathlib import Path

from rfobserver.storage.local import LocalStorage, is_active_capture


def _cap(d: Path, name: str, size: int, mtime: float) -> Path:
    sc16 = d / f"{name}.sc16"
    sc16.write_bytes(b"\0" * size)
    (d / f"{name}.json").write_text("{}")
    (d / f"{name}.psd").write_bytes(b"\0" * 8)
    for p in (sc16, d / f"{name}.json", d / f"{name}.psd"):
        os.utime(p, (mtime, mtime))
    return sc16


class _Disk:
    """Free space that grows as captures are deleted."""

    def __init__(self, ls: LocalStorage, free: int) -> None:
        self.ls, self.base = ls, free
        self.start = self._used()

    def _used(self) -> int:
        return sum(p.stat().st_size for p in self.ls.storage_path.rglob("*") if p.is_file())

    def __call__(self) -> int:
        return self.base + (self.start - self._used())


def test_evicts_oldest_first_until_the_target(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    a = _cap(ls.auto_dir, "A", 1000, 1)
    b = _cap(ls.auto_dir, "B", 1000, 2)
    c = _cap(ls.auto_dir, "C", 1000, 3)
    disk = _Disk(ls, free=100)
    freed = ls.evict_until_free(1500, free_bytes=disk)
    assert not a.exists() and not b.exists() and c.exists()
    assert not (ls.auto_dir / "A.json").exists()  # companions go too
    assert freed == 2 * (1000 + 2 + 8)


def test_stops_when_nothing_is_left_to_evict(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    _cap(ls.auto_dir, "A", 1000, 1)
    freed = ls.evict_until_free(10**12, free_bytes=lambda: 0)
    assert freed > 0 and list(ls.auto_dir.glob("*.sc16")) == []


def test_manual_captures_are_never_evicted(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    m = _cap(ls.manual_dir, "M", 1000, 0)
    ls.evict_until_free(10**12, free_bytes=lambda: 0)
    assert m.exists()


def test_evict_never_takes_the_active_capture_or_its_drop_renamed_name(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    active = _cap(ls.auto_dir, "S-host-20260923T100000", 1000, 1)
    renamed = _cap(ls.auto_dir, "S-host-20260923T090000_drop3", 1000, 0)
    other = _cap(ls.auto_dir, "S-host-20260923T080000", 1000, 0)
    ls.evict_until_free(
        10**12,
        exclude={"S-host-20260923T100000.sc16", "S-host-20260923T090000.sc16"},
        free_bytes=lambda: 0,
    )
    assert active.exists() and renamed.exists() and not other.exists()


def test_is_active_capture():
    act = {"X-h-1.sc16"}
    assert is_active_capture("X-h-1.sc16", act)
    assert is_active_capture("X-h-1_drop12.sc16", act)
    assert not is_active_capture("X-h-10.sc16", act)
    assert not is_active_capture("X-h-1.sc16", set())


def test_sample_counts_auto_and_manual_and_evictability(tmp_path):
    ls = LocalStorage(str(tmp_path / "s"), max_gb=100)
    _cap(ls.auto_dir, "A", 1000, 1)
    _cap(ls.manual_dir, "M", 500, 1)
    s = ls.sample(
        db_path=tmp_path / "s" / "db.sqlite",
        active_names=set(),
        db_file_bytes=7,
        db_reusable_bytes=3,
    )
    assert s.auto_bytes == 1000 + 2 + 8
    assert s.manual_bytes == 500 + 2 + 8
    assert s.evictable_auto is True
    assert s.db_volume is None  # same device
    assert s.db_file_bytes == 7 and s.db_reusable_bytes == 3
    assert 0 < s.data.free_bytes <= s.data.total_bytes
    only_active = ls.sample(
        db_path=tmp_path / "s" / "db.sqlite",
        active_names={"A.sc16"},
        db_file_bytes=0,
        db_reusable_bytes=0,
    )
    assert only_active.evictable_auto is False


def test_sample_reports_a_db_on_another_device(tmp_path, monkeypatch):
    ls = LocalStorage(str(tmp_path / "s"), max_gb=100)
    real_stat = os.stat
    db_dir = tmp_path / "dbvol"
    db_dir.mkdir()

    class _S:
        def __init__(self, st, dev):
            self._st, self.st_dev = st, dev

        def __getattr__(self, k):
            return getattr(self._st, k)

    def fake_stat(p, *a, **k):
        st = real_stat(p, *a, **k)
        return _S(st, 999) if Path(p) == db_dir else st

    monkeypatch.setattr("rfobserver.storage.local.os.stat", fake_stat)
    s = ls.sample(db_path=db_dir / "x.db", active_names=(), db_file_bytes=0, db_reusable_bytes=0)
    assert s.db_volume is not None and s.db_volume.total_bytes > 0


def test_sample_tolerates_a_capture_deleted_mid_walk(tmp_path, monkeypatch):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    gone = _cap(ls.auto_dir, "GONE", 1000, 1)
    _cap(ls.auto_dir, "KEEP", 1000, 2)
    real = LocalStorage._capture_size

    def racing(self, p):
        if p.name == "GONE.sc16":
            gone.unlink()
            p.stat()  # raises FileNotFoundError, as a real race would
        return real(self, p)

    monkeypatch.setattr(LocalStorage, "_capture_size", racing)
    s = ls.sample(db_path=tmp_path / "db", active_names=(), db_file_bytes=0, db_reusable_bytes=0)
    assert s.auto_bytes == 1000 + 2 + 8


def test_manual_usage(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    _cap(ls.manual_dir, "M", 500, 1)
    assert ls.get_manual_usage_bytes() == 500 + 2 + 8
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_storage_local_budget.py -q`
Expected: `ImportError: cannot import name 'is_active_capture'`.

- [ ] **Step 3: Implement in `storage/local.py`**

Add imports at the top (keep `from __future__ import annotations`):

```python
import logging
import os
import shutil
from collections.abc import Callable, Collection
from pathlib import Path

from rfobserver.storage.governor import StorageSample, VolumeSample
```

Add the module function after `_COMPANION_SUFFIXES`:

```python
def is_active_capture(name: str, active_names: Collection[str]) -> bool:
    """True for a capture being recorded, including the ``_drop<N>`` name the
    finalize step may rename it to while the governor is looking."""
    for active in active_names:
        stem = active[: -len(".sc16")] if active.endswith(".sc16") else active
        if name == active or (name.startswith(f"{stem}_drop") and name.endswith(".sc16")):
            return True
    return False
```

Add these methods to `LocalStorage` (after `get_usage_gb`):

```python
    def _size_or_zero(self, sc16_path: Path) -> int:
        """_capture_size tolerant of a capture deleted or renamed mid-walk
        (eviction, the drop-rename at finalize, a person deleting over SFTP)."""
        try:
            return self._capture_size(sc16_path)
        except OSError:
            return 0

    @staticmethod
    def _mtime_or_zero(p: Path) -> float:
        try:
            return p.stat().st_mtime
        except OSError:
            return 0.0

    def get_manual_usage_bytes(self) -> int:
        """Bytes used by manual/ captures. Reported only; never evicted."""
        return sum(self._size_or_zero(c) for c in self.manual_dir.glob("*.sc16"))

    def evict_until_free(
        self,
        target_free_bytes: int,
        *,
        exclude: Collection[str] = (),
        free_bytes: Callable[[], int] | None = None,
    ) -> int:
        """Delete the oldest auto/ captures until the volume has
        ``target_free_bytes`` free or none is left to delete. Returns bytes freed.

        The governor's step 1. Unlike enforce_cap this may take the newest
        finished capture; it never takes one named in ``exclude`` (the capture
        being recorded) or anything in manual/.
        """
        free = free_bytes or (lambda: shutil.disk_usage(self.storage_path).free)
        captures = sorted(
            (c for c in self.auto_dir.glob("*.sc16") if not is_active_capture(c.name, exclude)),
            key=self._mtime_or_zero,
        )
        freed = 0
        while captures and free() < target_free_bytes:
            freed += self._delete_capture(captures.pop(0))
        if freed:
            logger.warning(
                "Storage floor: evicted %.1f GB of automatic captures", freed / 1024**3
            )
        return freed

    def sample(
        self,
        *,
        db_path: Path,
        active_names: Collection[str],
        db_file_bytes: int,
        db_reusable_bytes: int,
    ) -> StorageSample:
        """The filesystem half of a governor sample (blocking: call in a thread)."""
        du = shutil.disk_usage(self.storage_path)
        db_volume = None
        db_dir = Path(db_path).resolve().parent
        try:
            if os.stat(db_dir).st_dev != os.stat(self.storage_path).st_dev:
                ddu = shutil.disk_usage(db_dir)
                db_volume = VolumeSample(ddu.free, ddu.total)
        except OSError:
            db_volume = None
        autos = list(self.auto_dir.glob("*.sc16"))
        return StorageSample(
            data=VolumeSample(du.free, du.total),
            db_volume=db_volume,
            db_file_bytes=db_file_bytes,
            db_reusable_bytes=db_reusable_bytes,
            auto_bytes=sum(self._size_or_zero(c) for c in autos),
            manual_bytes=self.get_manual_usage_bytes(),
            evictable_auto=any(not is_active_capture(c.name, active_names) for c in autos),
        )
```

Also make `enforce_cap` and `get_usage_bytes` use the tolerant helpers (same race): in `enforce_cap` replace `key=lambda f: f.stat().st_mtime` with `key=self._mtime_or_zero` and `self._capture_size(c)` with `self._size_or_zero(c)`; in `get_usage_bytes` use `self._size_or_zero(c)`.

Check the import direction: `governor.py` imports nothing from `local.py`, so there is no cycle.

- [ ] **Step 4: Run the tests (new and existing LocalStorage tests)**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_storage_local_budget.py tests/unit/test_local_storage.py tests/unit/test_local_storage_resilience.py tests/unit/test_captures_dirs.py -q`
Expected: all pass.

- [ ] **Step 5: Full checks and commit**

```bash
git add src/rfobserver/storage/local.py tests/unit/test_storage_local_budget.py
git commit -m "feat(storage): sample volume usage and evict auto captures to a free target"
```

---

### Task 3: Chunked retention, DB file stats, and blob-less avg windows

**Files:**
- Modify: `src/rfobserver/storage/database.py` (`insert_avg_window` ~558, `prune_avg_psd_blobs` ~1512, new methods beside it)
- Modify: `src/rfobserver/pipeline/app.py` (`_cleanup_loop` ~451, the worker start ~304)
- Modify: `tests/unit/test_app_cleanup.py`
- Test: `tests/unit/test_retention.py`

**Interfaces:**
- Consumes: `PRESSURE_PSD_DAYS`, `PRESSURE_DETECTION_DAYS` (Task 1); `StorageGovernor.state.pressure` (Task 1).
- Produces:
  - `RETENTION_CHUNK_ROWS: int` (module constant in `database.py`, default 5000; Task 9 sets it from measurement)
  - `SensorDatabase.file_stats() -> tuple[int, int]` (DB + WAL bytes, reusable freelist bytes)
  - `SensorDatabase.delete_older_than(table: str, days: int, *, chunk: int = RETENTION_CHUNK_ROWS, pause_sec: float = 0.05) -> int`
  - `SensorDatabase.prune_avg_psd_blobs(days: int = 7, *, chunk: int = RETENTION_CHUNK_ROWS, pause_sec: float = 0.05) -> int` (same name and return as today; now chunked)
  - `SensorDatabase.insert_avg_window(..., powers: list[float] | None, ...)`: `None` stores `psd_powers = NULL`
  - `pipeline.app._retention_days(configured: int, pressure_cap: int, pressure: bool) -> int`
  - `pipeline.app._run_retention(settings, db, *, pressure: bool) -> None`
  - `pipeline.app._cleanup_loop(settings, db, governor=None, wake: asyncio.Event | None = None) -> None`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_retention.py`:

```python
"""Row retention (STATS_RETENTION_DAYS), chunked so the writer is never starved."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta

import pytest

from rfobserver.config import AppSettings
from rfobserver.pipeline.app import _cleanup_loop, _retention_days, _run_retention
from rfobserver.storage.database import SensorDatabase


@pytest.fixture
async def db(tmp_path):
    d = SensorDatabase(str(tmp_path / "r.db"))
    await d.connect()
    yield d
    await d.close()


async def _window(db, when: datetime, powers=(1.0, 2.0)):
    await db.insert_avg_window(
        start_time=when,
        duration_sec=1.0,
        sdr_center_freq_hz=915e6,
        sample_rate_hz=2e6,
        gain_db=30.0,
        num_bins=len(powers) if powers is not None else 2,
        freq_start_hz=914e6,
        freq_step_hz=1e6,
        pwr_avg=-50.0,
        pwr_max=-40.0,
        pwr_median=-50.0,
        pwr_std=1.0,
        kurtosis=3.0,
        powers=None if powers is None else list(powers),
    )


async def _detection(db, bid: str, when: datetime):
    await db.insert_detection(
        burst_id=bid,
        start_time=when,
        stop_time=when,
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1.0,
        detection_timestamp=when,
    )


async def _count(db, sql: str) -> int:
    async with db._db.execute(sql) as cur:
        return (await cur.fetchone())[0]


async def test_delete_older_than_removes_only_old_rows_across_chunks(db):
    now = datetime.utcnow()
    for i in range(23):
        await _detection(db, f"old{i}", now - timedelta(days=800, minutes=i))
    for i in range(4):
        await _detection(db, f"new{i}", now - timedelta(days=10, minutes=i))
    removed = await db.delete_older_than("detections", 730, chunk=5, pause_sec=0)
    assert removed == 23
    left = {r["burst_id"] for r in await db.query_detections()}
    assert left == {f"new{i}" for i in range(4)}


async def test_delete_older_than_handles_windows_and_minutes(db):
    now = datetime.utcnow()
    await _window(db, now - timedelta(days=800))
    await _window(db, now - timedelta(days=1))
    await db._db.execute(
        "INSERT INTO avg_minutes (minute_start, sdr_center_freq_hz, n) VALUES (?, ?, 1), (?, ?, 1)",
        (
            (now - timedelta(days=800)).strftime("%Y-%m-%dT%H:%M"),
            915e6,
            (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M"),
            915e6,
        ),
    )
    await db._db.commit()
    assert await db.delete_older_than("avg_windows", 730, chunk=1, pause_sec=0) == 1
    assert await db.delete_older_than("avg_minutes", 730, chunk=1, pause_sec=0) == 1
    assert await _count(db, "SELECT COUNT(*) FROM avg_windows") == 1
    assert await _count(db, "SELECT COUNT(*) FROM avg_minutes") == 1


async def test_delete_older_than_rejects_unknown_tables(db):
    with pytest.raises(KeyError):
        await db.delete_older_than("tone_checks", 1)


async def test_chunked_blob_prune_nulls_only_old_blobs_and_resumes(db):
    now = datetime.utcnow()
    for i in range(12):
        await _window(db, now - timedelta(days=40, minutes=i))
    await _window(db, now - timedelta(days=1))
    assert await db.prune_avg_psd_blobs(30, chunk=5, pause_sec=0) == 12
    assert await _count(db, "SELECT COUNT(*) FROM avg_windows WHERE psd_powers IS NULL") == 12
    assert await _count(db, "SELECT COUNT(*) FROM avg_windows") == 13  # rows kept
    assert await db.prune_avg_psd_blobs(30, chunk=5, pause_sec=0) == 0
    # A tighter cutoff later continues past the watermark.
    await _window(db, now - timedelta(days=10))
    assert await db.prune_avg_psd_blobs(7, chunk=5, pause_sec=0) == 1


async def test_blob_prune_handles_rows_sharing_a_timestamp(db):
    t = datetime.utcnow() - timedelta(days=40)
    for _ in range(7):
        await _window(db, t)
    assert await db.prune_avg_psd_blobs(30, chunk=3, pause_sec=0) == 7


async def test_insert_avg_window_without_powers_stores_a_null_blob(db):
    await _window(db, datetime.utcnow(), powers=None)
    assert await _count(db, "SELECT COUNT(*) FROM avg_windows WHERE psd_powers IS NULL") == 1


async def test_file_stats_reports_file_and_reusable_bytes(db):
    size, reusable = await db.file_stats()
    assert size > 0 and reusable >= 0


def test_retention_days_under_pressure():
    assert _retention_days(30, 7, False) == 30
    assert _retention_days(30, 7, True) == 7
    assert _retention_days(3, 7, True) == 3
    assert _retention_days(0, 7, False) == 0  # disabled
    assert _retention_days(0, 7, True) == 7  # pressure prunes even when disabled


class _RecDB:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    async def prune_avg_psd_blobs(self, days: int) -> int:
        self.calls.append(("blobs", days))
        return 0

    async def delete_older_than(self, table: str, days: int) -> int:
        self.calls.append((table, days))
        return 0


async def test_run_retention_normal_and_pressure():
    s = AppSettings(_env_file=None, DB_RETENTION_DAYS=30, STATS_RETENTION_DAYS=730)
    d = _RecDB()
    await _run_retention(s, d, pressure=False)
    assert d.calls == [
        ("blobs", 30),
        ("detections", 730),
        ("avg_windows", 730),
        ("avg_minutes", 730),
    ]
    d.calls.clear()
    await _run_retention(s, d, pressure=True)
    assert d.calls == [
        ("blobs", 7),
        ("detections", 90),
        ("avg_windows", 730),  # stats rows are never cut by pressure
        ("avg_minutes", 730),
    ]


async def test_run_retention_skips_disabled_parts_and_survives_errors():
    s = AppSettings(_env_file=None, DB_RETENTION_DAYS=0, STATS_RETENTION_DAYS=0)
    d = _RecDB()
    await _run_retention(s, d, pressure=False)
    assert d.calls == []

    class _Boom(_RecDB):
        async def prune_avg_psd_blobs(self, days: int) -> int:
            raise RuntimeError("x")

    b = _Boom()
    s2 = AppSettings(_env_file=None, DB_RETENTION_DAYS=30, STATS_RETENTION_DAYS=730)
    await _run_retention(s2, b, pressure=False)
    assert ("detections", 730) in b.calls  # one failure does not stop the rest


async def test_cleanup_loop_wakes_early_on_the_event():
    s = AppSettings(_env_file=None, DB_RETENTION_DAYS=30, DB_CLEANUP_INTERVAL_SEC=3600)
    d = _RecDB()
    wake = asyncio.Event()
    task = asyncio.create_task(_cleanup_loop(s, d, wake=wake))
    for _ in range(5):
        await asyncio.sleep(0)
    first = len(d.calls)
    wake.set()
    for _ in range(10):
        await asyncio.sleep(0)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert first >= 1 and len(d.calls) > first
```

In `tests/unit/test_app_cleanup.py`, give `_FakeDB` the new method so the existing test keeps asserting the blob call:

```python
    async def delete_older_than(self, table: str, days: int) -> int:
        return 0
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_retention.py -q`
Expected: `ImportError: cannot import name '_retention_days'`.

- [ ] **Step 3: Implement the DB side**

In `database.py`, near the other module constants:

```python
# Rows per retention statement. Statement size on the writer connection is what
# starves the pipeline (the peak-finder rollup, 2026-09-21): keep each one well
# under the ~300 ms at which chunks begin to drop. Set from a nano-super
# measurement (docs/debugging/..., Task 9 of the storage budgeting plan).
RETENTION_CHUNK_ROWS = 5000
# Tables row retention may delete from, and their time column.
_RETENTION_TABLES = {
    "avg_windows": "start_time",
    "detections": "start_time",
    "avg_minutes": "minute_start",
}


def _retention_cutoff(days: int) -> str:
    """Same naive-UTC ISO form prune_avg_psd_blobs has always compared with."""
    return (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=days)).isoformat()
```

(`timezone` must be imported from `datetime` if it is not already.)

In `SensorDatabase.__init__` add:

```python
        # (start_time, rowid) up to which every avg_windows blob is known to be
        # pruned, so each hourly pass resumes instead of rescanning history.
        self._blob_prune_mark: tuple[str, int] = ("", 0)
```

Change `insert_avg_window`'s signature to `powers: list[float] | None,` and the blob line to:

```python
        # None (storage step 4) keeps the stats row but stores no PSD blob.
        psd_blob = None if powers is None else np.asarray(powers, dtype="<f4").tobytes()
```

Replace `prune_avg_psd_blobs` and add the new methods:

```python
    @_guarded_write
    async def _prune_blob_chunk(self, cutoff: str, limit: int) -> tuple[int, int]:
        """Null one chunk of blobs after the watermark. Returns (rows scanned, nulled)."""
        assert self._db is not None
        after_time, after_rowid = self._blob_prune_mark
        async with self._db.execute(
            "SELECT rowid, start_time FROM avg_windows "
            "WHERE start_time < ? AND (start_time, rowid) > (?, ?) "
            "ORDER BY start_time, rowid LIMIT ?",
            (cutoff, after_time, after_rowid, limit),
        ) as cur:
            rows = await cur.fetchall()
        if not rows:
            return 0, 0
        ids = [r[0] for r in rows]
        marks = ",".join("?" * len(ids))
        cursor = await self._db.execute(
            "UPDATE avg_windows SET psd_powers = NULL, violations = NULL "
            f"WHERE rowid IN ({marks}) AND psd_powers IS NOT NULL",
            ids,
        )
        await self._db.commit()
        self._blob_prune_mark = (rows[-1][1], rows[-1][0])
        return len(rows), cursor.rowcount

    async def prune_avg_psd_blobs(
        self, days: int = 7, *, chunk: int = RETENTION_CHUNK_ROWS, pause_sec: float = 0.05
    ) -> int:
        """Evict the PSD blobs of averaged windows older than ``days`` days.

        Only the heavy ``psd_powers``/``violations`` blobs are nulled out; the
        cheap stats row is kept (row retention is delete_older_than). Walks the
        start_time index in chunks from a watermark, so each statement is small
        and a pass after the first touches only the newly expired rows. The DB
        file does not shrink (auto_vacuum=0): freed pages are reused by later
        inserts. Returns how many blobs were nulled this pass.
        """
        cutoff = _retention_cutoff(days)
        pruned = 0
        while True:
            scanned, nulled = await self._prune_blob_chunk(cutoff, chunk)
            pruned += nulled
            if scanned < chunk:
                break
            await asyncio.sleep(pause_sec)
        if pruned > 0:
            logger.info("Pruned PSD blobs for %d avg windows (cutoff: %s)", pruned, cutoff)
        return pruned

    @_guarded_write
    async def _delete_older_chunk(self, table: str, cutoff: str, limit: int) -> int:
        assert self._db is not None
        col = _RETENTION_TABLES[table]
        cursor = await self._db.execute(
            f"DELETE FROM {table} WHERE rowid IN "
            f"(SELECT rowid FROM {table} WHERE {col} < ? ORDER BY {col} LIMIT ?)",
            (cutoff, limit),
        )
        await self._db.commit()
        return int(cursor.rowcount)

    async def delete_older_than(
        self,
        table: str,
        days: int,
        *,
        chunk: int = RETENTION_CHUNK_ROWS,
        pause_sec: float = 0.05,
    ) -> int:
        """Delete rows of ``table`` older than ``days`` days, ``chunk`` rows per
        statement with a pause between, so the pipeline's inserts interleave.
        Raises KeyError for a table outside _RETENTION_TABLES."""
        _RETENTION_TABLES[table]  # validate before building any SQL
        cutoff = _retention_cutoff(days)
        total = 0
        while True:
            n = await self._delete_older_chunk(table, cutoff, chunk)
            total += n
            if n < chunk:
                break
            await asyncio.sleep(pause_sec)
        if total:
            logger.info("Retention: deleted %d %s rows older than %d days", total, table, days)
        return total

    async def file_stats(self) -> tuple[int, int]:
        """(DB file plus WAL bytes, bytes of free pages reusable without growth)."""
        assert self._db is not None
        async with self._db.execute("PRAGMA page_size") as cur:
            page_size = int((await cur.fetchone())[0])
        async with self._db.execute("PRAGMA freelist_count") as cur:
            free_pages = int((await cur.fetchone())[0])
        size = 0
        for suffix in ("", "-wal"):
            with contextlib.suppress(OSError):
                size += os.stat(self._db_path + suffix).st_size
        return size, page_size * free_pages
```

(Import `contextlib` and `os` in `database.py` if not already imported.)

`avg_minutes` has a composite `PRIMARY KEY` but is a rowid table (no `WITHOUT ROWID`), so `rowid` works; the `ORDER BY minute_start` walks the primary-key index.

- [ ] **Step 4: Implement the app side**

In `pipeline/app.py`, replace `_cleanup_loop` with:

```python
def _retention_days(configured: int, pressure_cap: int, pressure: bool) -> int:
    """Retention in days for one class of data: the configured value, cut to
    the pressure cap at storage step >= 2 (which applies even when the
    configured retention is disabled). 0 = do not prune."""
    if not pressure:
        return configured
    return pressure_cap if configured <= 0 else min(configured, pressure_cap)


async def _run_retention(settings: AppSettings, db: Any, *, pressure: bool) -> None:
    """One retention pass. Each part has its own try so one failure does not
    stop the rest, and the pipeline keeps running regardless."""
    from rfobserver.storage.governor import PRESSURE_DETECTION_DAYS, PRESSURE_PSD_DAYS

    parts: list[tuple[str, int]] = [
        ("blobs", _retention_days(settings.DB_RETENTION_DAYS, PRESSURE_PSD_DAYS, pressure)),
        (
            "detections",
            _retention_days(settings.STATS_RETENTION_DAYS, PRESSURE_DETECTION_DAYS, pressure),
        ),
        ("avg_windows", settings.STATS_RETENTION_DAYS),
        ("avg_minutes", settings.STATS_RETENTION_DAYS),
    ]
    for what, days in parts:
        if days <= 0:
            continue
        try:
            if what == "blobs":
                await db.prune_avg_psd_blobs(days)
            else:
                await db.delete_older_than(what, days)
        except Exception:
            logger.exception("Retention of %s failed; continuing", what)


async def _cleanup_loop(
    settings: AppSettings,
    db: Any,
    governor: Any = None,
    wake: asyncio.Event | None = None,
) -> None:
    """Scheduled DB retention.

    PSD blobs expire after DB_RETENTION_DAYS; stats rows, detections and
    minute rollups after STATS_RETENTION_DAYS. At storage step >= 2 the blob
    and detection cutoffs tighten to the pressure caps. Runs one pass
    immediately, then every DB_CLEANUP_INTERVAL_SEC, or at once when ``wake``
    is set (the storage loop sets it on entering step 2).
    """
    while True:
        pressure = governor is not None and governor.state.pressure
        await _run_retention(settings, db, pressure=pressure)
        if wake is None:
            await asyncio.sleep(settings.DB_CLEANUP_INTERVAL_SEC)
            continue
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(wake.wait(), timeout=settings.DB_CLEANUP_INTERVAL_SEC)
        wake.clear()
```

(Import `contextlib` in `app.py` if not already.) The worker start in `run_pipeline` stays as-is in this task; Task 4 rewires it.

- [ ] **Step 5: Run the tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_retention.py tests/unit/test_app_cleanup.py tests/unit/test_database.py tests/unit/test_streaming_avg_window.py -q`
Expected: all pass. `test_database.py`'s existing prune tests use `days=7` and assert counts; they must still pass unchanged.

- [ ] **Step 6: Full checks and commit**

```bash
git add src/rfobserver/storage/database.py src/rfobserver/pipeline/app.py tests/unit/test_retention.py tests/unit/test_app_cleanup.py
git commit -m "feat(storage): chunked two-year row retention with pressure cutoffs"
```

---

### Task 4: The storage loop, sticky-flag persistence, and wiring

**Files:**
- Modify: `src/rfobserver/pipeline/app.py` (`run_pipeline` ~160-310, `_heartbeat_loop` ~379, `_run_web_server` ~550, new `_storage_loop`)
- Modify: `src/rfobserver/pipeline/streaming.py` (`__init__` signature only: `storage_governor` param)
- Modify: `src/rfobserver/web/app.py` (`app.state.storage_governor = None` default in `create_app`)
- Test: `tests/unit/test_storage_loop.py`

**Interfaces:**
- Consumes: Task 1 governor, Task 2 `LocalStorage.sample/evict_until_free`, Task 3 `SensorDatabase.file_stats`, `_cleanup_loop(..., governor, wake)`.
- Produces:
  - `pipeline.app._active_capture_names(supervisor) -> set[str]`
  - `pipeline.app._storage_tick(settings, governor, db, local_storage, supervisor, retention_wake) -> None`
  - `pipeline.app._storage_loop(settings, governor, db, local_storage, supervisor, retention_wake) -> None`
  - `StreamingProcessor(..., storage_governor: StorageGovernor | None = None)` stored as `self._governor`
  - `app.state.storage_governor` (web)
  - heartbeat payload key `"storage"`: `governor.state.to_health()` or `None`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_storage_loop.py`:

```python
"""_storage_loop: sample, tick, evict, wake retention, persist the sticky flag."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from rfobserver.config import AppSettings
from rfobserver.pipeline.app import _active_capture_names, _storage_tick
from rfobserver.storage.governor import (
    DEGRADED_CONFIG_KEY,
    GB,
    StorageGovernor,
    StorageSample,
    VolumeSample,
)


def _sample(free_gb: float, evictable: bool) -> StorageSample:
    return StorageSample(
        data=VolumeSample(int(free_gb * GB), 1000 * GB),
        db_volume=None,
        db_file_bytes=0,
        db_reusable_bytes=0,
        auto_bytes=0,
        manual_bytes=0,
        evictable_auto=evictable,
    )


class _LS:
    def __init__(self, samples: list[StorageSample]) -> None:
        self.samples = samples
        self.evictions: list[tuple[int, set[str]]] = []
        self.active_seen: list[set[str]] = []

    def sample(self, *, db_path, active_names, db_file_bytes, db_reusable_bytes):
        self.active_seen.append(set(active_names))
        return self.samples.pop(0)

    def evict_until_free(self, target, *, exclude=()):
        self.evictions.append((target, set(exclude)))
        return 0


class _DB:
    def __init__(self) -> None:
        self.config: dict[str, str] = {}

    async def file_stats(self):
        return 10, 2

    async def set_config(self, k: str, v: str) -> None:
        self.config[k] = v


def _sup(state: str = "idle", file: str | None = None):
    proc = SimpleNamespace(recording_status=lambda: {"state": state, "file": file})
    return SimpleNamespace(processor=proc)


def test_active_capture_names():
    assert _active_capture_names(_sup("recording", "A.sc16")) == {"A.sc16"}
    assert _active_capture_names(_sup("finalizing", "A.sc16")) == {"A.sc16"}
    assert _active_capture_names(_sup("idle", "A.sc16")) == set()
    assert _active_capture_names(SimpleNamespace(processor=None)) == set()


async def test_tick_evicts_excluding_the_active_capture():
    s = AppSettings(_env_file=None)
    gov, ls, db, wake = StorageGovernor(), _LS([_sample(40, True)]), _DB(), asyncio.Event()
    await _storage_tick(s, gov, db, ls, _sup("recording", "A.sc16"), wake)
    assert gov.state.step == 1
    assert ls.evictions == [(int(50 * GB * 1.15), {"A.sc16"})]
    assert not wake.is_set()


async def test_entering_step_2_wakes_retention_and_step_3_persists_the_flag():
    s = AppSettings(_env_file=None)
    gov, db, wake = StorageGovernor(), _DB(), asyncio.Event()
    ls = _LS([_sample(40, False), _sample(40, False)])
    await _storage_tick(s, gov, db, ls, _sup(), wake)
    assert wake.is_set() and gov.state.step == 2
    assert DEGRADED_CONFIG_KEY not in db.config
    await _storage_tick(s, gov, db, ls, _sup(), wake)
    assert gov.state.step == 3
    assert db.config[DEGRADED_CONFIG_KEY] == gov.state.degraded_since.isoformat()


async def test_write_error_from_a_thread_is_persisted_on_the_next_tick():
    s = AppSettings(_env_file=None)
    gov, db, wake = StorageGovernor(), _DB(), asyncio.Event()
    gov.report_write_error("ENOSPC: No space left on device")
    await _storage_tick(s, gov, db, _LS([_sample(200, True)]), _sup(), wake)
    assert db.config[DEGRADED_CONFIG_KEY]


async def test_tick_uses_the_live_floor_setting():
    s = AppSettings(_env_file=None, DISK_MIN_FREE_GB=30)
    gov = StorageGovernor()
    await _storage_tick(s, gov, _DB(), _LS([_sample(40, True)]), _sup(), asyncio.Event())
    assert gov.state.step == 0
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_storage_loop.py -q`
Expected: `ImportError: cannot import name '_active_capture_names'`.

- [ ] **Step 3: Implement the loop in `pipeline/app.py`**

Add after `_cleanup_loop`:

```python
def _active_capture_names(supervisor: Any) -> set[str]:
    """The capture being recorded or finalized, which eviction must not take."""
    proc = getattr(supervisor, "processor", None)
    if proc is None or not hasattr(proc, "recording_status"):
        return set()
    st = proc.recording_status()
    name = st.get("file")
    if st.get("state") in ("recording", "finalizing") and name:
        return {str(name)}
    return set()


async def _storage_tick(
    settings: AppSettings,
    governor: Any,
    db: Any,
    local_storage: Any,
    supervisor: Any,
    retention_wake: asyncio.Event,
) -> None:
    """One governor tick: sample, decide, act, persist the sticky flag."""
    from rfobserver.storage.governor import DEGRADED_CONFIG_KEY

    active = _active_capture_names(supervisor)
    db_file, db_reusable = await db.file_stats()
    sample = await asyncio.to_thread(
        local_storage.sample,
        db_path=Path(settings.DB_PATH),
        active_names=active,
        db_file_bytes=db_file,
        db_reusable_bytes=db_reusable,
    )
    prev_step = governor.state.step
    actions = governor.tick(
        sample, min_free_gb=settings.DISK_MIN_FREE_GB, now=datetime.now(timezone.utc)
    )
    st = governor.state
    if st.step != prev_step:
        log = logger.warning if st.step > prev_step else logger.info
        log(
            "Storage step %d -> %d (%s): %.1f GB free, floor %.1f GB",
            prev_step,
            st.step,
            st.to_health()["step_text"],
            sample.data.free_bytes / 1024**3,
            st.floor_bytes / 1024**3,
        )
    if actions.evict_to_free_bytes is not None:
        await asyncio.to_thread(
            local_storage.evict_until_free, actions.evict_to_free_bytes, exclude=active
        )
    if actions.start_pressure_prune:
        retention_wake.set()
    changed, value = governor.take_degraded_change()
    if changed:
        try:
            await db.set_config(DEGRADED_CONFIG_KEY, value)
        except Exception:
            logger.exception("Could not persist the storage degraded flag")


async def _storage_loop(
    settings: AppSettings,
    governor: Any,
    db: Any,
    local_storage: Any,
    supervisor: Any,
    retention_wake: asyncio.Event,
) -> None:
    """Every STORAGE_CHECK_SEC: one governor tick. A failed tick is logged and
    the loop continues; the published state keeps its last value."""
    while True:
        try:
            await _storage_tick(settings, governor, db, local_storage, supervisor, retention_wake)
        except Exception:
            logger.exception("Storage check failed; continuing")
        await asyncio.sleep(max(1.0, float(settings.STORAGE_CHECK_SEC)))
```

Ensure `Path`, `datetime`, `timezone` are imported at module top (`from pathlib import Path`, `from datetime import datetime, timezone`); add what is missing.

- [ ] **Step 4: Wire it in `run_pipeline`**

After `local_storage = LocalStorage(...)`:

```python
    from rfobserver.storage.governor import DEGRADED_CONFIG_KEY, StorageGovernor

    storage_governor = StorageGovernor()
    try:
        storage_governor.restore_degraded(await db.get_config(DEGRADED_CONFIG_KEY))
    except Exception:
        logger.exception("Could not read the persisted storage degraded flag")
    retention_wake = asyncio.Event()
```

In `build_processor`, pass `storage_governor=storage_governor,` to `StreamingProcessor(...)` (not to `ContinuousProcessor`, which does not record).

Pass the governor to the web server and heartbeat:

```python
        web_task = asyncio.create_task(
            _run_web_server(
                settings, supervisor, read_db, db, broadcast, beacon, stop,
                storage_governor=storage_governor,
            )
        )
        ...
                _heartbeat_loop(
                    settings, supervisor, read_db, local_storage, broadcast,
                    governor=storage_governor,
                )
```

Replace the retention worker start:

```python
    # Retention always runs: even with DB_RETENTION_DAYS=0 the storage governor
    # may need the step 2 pressure cutoffs.
    workers.append(
        asyncio.create_task(_cleanup_loop(settings, db, storage_governor, retention_wake))
    )
    workers.append(
        asyncio.create_task(
            _storage_loop(
                settings, storage_governor, db, local_storage, supervisor, retention_wake
            )
        )
    )
```

In `_run_web_server`, add the keyword parameter `storage_governor: Any = None` and after `app.state.write_database = write_database`: `app.state.storage_governor = storage_governor`.

In `_heartbeat_loop`, add parameter `governor: Any = None` (after `interval_sec`) and in the published dict add:

```python
                    "storage": governor.state.to_health() if governor is not None else None,
```

In `web/app.py` `create_app`, beside `app.state.write_database = None`: `app.state.storage_governor = None`.

In `streaming.py` `StreamingProcessor.__init__`, add the parameter `storage_governor: StorageGovernor | None = None` (last), store `self._governor = storage_governor`, and import under `TYPE_CHECKING`: `from rfobserver.storage.governor import StorageGovernor`.

- [ ] **Step 5: Run the tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_storage_loop.py tests/unit/test_app_db_lifecycle.py tests/unit/test_app_shutdown.py tests/unit/test_pipeline_wiring.py tests/unit/test_websocket.py -q`
Expected: all pass. If `test_pipeline_wiring` fakes `_run_web_server` or `_heartbeat_loop` with a fixed signature, add the new keyword to the fake.

- [ ] **Step 6: Full checks and commit**

```bash
git add src/rfobserver/pipeline/app.py src/rfobserver/pipeline/streaming.py src/rfobserver/web/app.py tests/unit/test_storage_loop.py
git commit -m "feat(storage): storage loop evicts, wakes retention, persists the degraded flag"
```

(Add any wiring test file touched in Step 5 to the `git add` by path.)

---

### Task 5: Refusing recordings at step 3 and recording why a capture stopped

**Files:**
- Modify: `src/rfobserver/pipeline/streaming.py` (`start_recording` ~598, `arm_trigger` ~610, `stop_recording` ~621, `_request_end_recording` ~656, `recording_status` ~678, `_check_trigger_and_record` ~857, shutdown ~556, `_begin_recording` ~1074, `_write_recording_metadata` ~1466)
- Modify: `src/rfobserver/web/routes/api.py` (`/trigger`, `/recording/start`, `/recording/arm`, `/replay/record`)
- Test: `tests/unit/test_recording_storage.py`

**Interfaces:**
- Consumes: `self._governor` (Task 4), `StorageState.refuse_recording`, `to_health()`.
- Produces:
  - `StreamingProcessor._recording_refusal() -> str | None`
  - `StreamingProcessor._request_end_recording(wait: bool, reason: str = "manual") -> None`
  - `recording_status()` gains `"refused": str | None`
  - capture `.json` gains `"stopped_reason"` (Task 6 adds `write_failed`/`write_error`)
  - API returns HTTP 409 with `detail` = the refusal text

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_recording_storage.py` (Task 6 appends to it):

```python
"""Recording under storage pressure: refusal, stop reasons, write failures."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from fastapi.testclient import TestClient

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import StreamingProcessor
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.governor import GB, StorageGovernor, StorageSample, VolumeSample
from rfobserver.storage.local import LocalStorage
from rfobserver.web.app import create_app

T0 = datetime(2026, 9, 23, tzinfo=timezone.utc)


def _governor_at(step: int) -> StorageGovernor:
    gov = StorageGovernor()
    free = {0: 200, 1: 40, 3: 40, 4: 20}[step]
    evictable = step == 1
    for _ in range(2 if step == 3 else 1):
        gov.tick(
            StorageSample(
                data=VolumeSample(int(free * GB), 1000 * GB),
                db_volume=None,
                db_file_bytes=0,
                db_reusable_bytes=0,
                auto_bytes=0,
                manual_bytes=0,
                evictable_auto=evictable,
            ),
            min_free_gb=0,
            now=T0,
        )
    assert gov.state.step == step
    return gov


def _proc(tmp_path: Path, governor: StorageGovernor | None = None, **overrides) -> StreamingProcessor:
    storage = tmp_path / "storage"
    storage.mkdir(exist_ok=True)
    base = dict(
        FREQUENCY_START=915_000_000,
        FREQUENCY_END=915_000_000,
        BANDWIDTH=1_000_000,
        DURATION_SEC=0.5,
        GAIN=35,
        NUM_FFT_BINS=64,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage),
        DB_PATH=str(tmp_path / "t.db"),
        ARCHIVE_MAX_GB=1.0,
        TRIGGER_PRE_SEC=0.001,
        _env_file=None,
    )
    base.update(overrides)
    s = AppSettings(**base)
    rx = MockReceiver(
        receiver_config=ReceiverConfig(
            gain_db=s.GAIN, bandwidth_hz=s.BANDWIDTH, duration_sec=s.DURATION_SEC
        )
    )
    rx.initialize()
    return StreamingProcessor(
        receiver=rx,
        database=SensorDatabase(s.DB_PATH),
        local_storage=LocalStorage(s.STORAGE_PATH, max_gb=s.ARCHIVE_MAX_GB),
        settings=s,
        storage_governor=governor,
    )


def _only_json(proc: StreamingProcessor) -> dict:
    (p,) = [
        q
        for d in (proc._storage.auto_dir, proc._storage.manual_dir)
        for q in d.glob("*.json")
        if not q.name.endswith((".psd.json", ".detections.json"))
    ]
    return json.loads(p.read_text())


@pytest.mark.parametrize("ram", [True, False])
def test_manual_start_is_refused_at_step_3(tmp_path, ram):
    proc = _proc(tmp_path, _governor_at(3), RECORDING_RAM_BUFFER=ram, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    st = proc.recording_status()
    assert st["state"] == "idle"
    assert "below" in st["refused"] and "floor" in st["refused"]


def test_arm_is_refused_at_step_3(tmp_path):
    proc = _proc(tmp_path, _governor_at(3))
    proc.arm_trigger()
    assert proc.recording_status()["state"] == "idle"
    assert proc.recording_status()["refused"]


def test_armed_trigger_does_not_fire_once_step_3_is_reached(tmp_path):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, TRIGGER_THRESHOLD_DB=-200.0)
    proc.arm_trigger()
    assert proc.recording_status()["state"] == "armed"
    for _ in range(2):
        gov.tick(
            StorageSample(
                data=VolumeSample(40 * GB, 1000 * GB),
                db_volume=None,
                db_file_bytes=0,
                db_reusable_bytes=0,
                auto_bytes=0,
                manual_bytes=0,
                evictable_auto=False,
            ),
            min_free_gb=0,
            now=T0,
        )
    proc._check_trigger_and_record(np.full(4096, 1 << 20, dtype=np.int32), (), 0)
    assert proc.recording_status()["state"] == "armed"  # still waiting, not recording
    assert proc.recording_status()["refused"]


@pytest.mark.parametrize("step", [0, 1])
def test_steps_below_3_allow_recording(tmp_path, step):
    proc = _proc(
        tmp_path, _governor_at(step), RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0
    )
    proc.start_recording()
    try:
        assert proc.recording_status()["state"] == "recording"
        assert proc.recording_status()["refused"] is None
    finally:
        proc.stop_recording()


def test_no_governor_never_refuses(tmp_path):
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    assert proc.recording_status()["state"] == "recording"
    proc.stop_recording()


def test_manual_stop_is_recorded_as_manual(tmp_path):
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    proc.stop_recording()
    assert _only_json(proc)["stopped_reason"] == "manual"


def test_max_duration_stop_is_recorded(tmp_path):
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    proc._effective_max_sec = 0.0
    proc._check_trigger_and_record(np.zeros(1000, dtype=np.int32), (), None)
    assert _only_json(proc)["stopped_reason"] == "max_duration"


def test_trigger_end_stop_is_recorded(tmp_path):
    proc = _proc(
        tmp_path,
        None,
        RECORDING_RAM_BUFFER=True,
        RECORDING_MAX_SEC=5.0,
        TRIGGER_THRESHOLD_DB=0.0,
        TRIGGER_HYSTERESIS=1,
    )
    proc._trigger_initiated = True
    with proc._rec_lock:
        proc._begin_recording()
    proc._check_trigger_and_record(np.zeros(1000, dtype=np.int32), (), None)
    assert _only_json(proc)["stopped_reason"] == "trigger_end"


def _client_with(proc_status: dict) -> TestClient:
    app = create_app(AppSettings(_env_file=None))
    app.state.processor = SimpleNamespace(
        start_recording=lambda: None,
        arm_trigger=lambda: None,
        manual_trigger=lambda: None,
        recording_status=lambda: proc_status,
    )
    return TestClient(app)


@pytest.mark.parametrize("path", ["/api/recording/start", "/api/recording/arm", "/api/trigger"])
def test_api_answers_409_with_the_reason_when_refused(path):
    c = _client_with({"state": "idle", "file": None, "refused": "Recording refused: x"})
    r = c.post(path)
    assert r.status_code == 409
    assert r.json()["detail"] == "Recording refused: x"


def test_api_start_succeeds_when_not_refused():
    c = _client_with({"state": "recording", "file": "a.sc16", "refused": None})
    assert c.post("/api/recording/start").status_code == 200
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_recording_storage.py -q`
Expected: failures on `refused` KeyError / `stopped_reason` KeyError / 200 instead of 409.

- [ ] **Step 3: Implement refusal and stop reasons in `streaming.py`**

In `__init__`, beside the other recording state:

```python
        # Why the current (or last) recording ended; written to the .json.
        self._stop_reason: str | None = None
        # The last refusal to start a recording (storage step >= 3), shown by
        # the API and UI. Cleared by the next start that is allowed.
        self._last_refusal: str | None = None
```

Add the helper (near `start_recording`):

```python
    def _recording_refusal(self) -> str | None:
        """Why a recording may not start now, or None. Storage step >= 3:
        free space is below the floor and nothing RFObserver can delete
        would raise it."""
        g = self._governor
        if g is None:
            return None
        st = g.state
        if not st.refuse_recording:
            return None
        h = st.to_health()
        return (
            f"Recording refused: free space {h['free_gb']} GB is below the "
            f"{h['floor_gb']} GB floor (storage step {st.step}, {h['step_text']})"
        )

    def _note_refusal(self, reason: str) -> None:
        if reason != self._last_refusal:
            logger.warning(reason)
        self._last_refusal = reason
```

`start_recording` becomes:

```python
    def start_recording(self) -> None:
        """Start recording IQ data immediately (manual mode)."""
        if self._replay_mode and not self._replay_record:
            return
        reason = self._recording_refusal()
        if reason is not None:
            self._note_refusal(reason)
            return
        self._last_refusal = None
        with self._rec_lock:
            if self._recording_state in ("recording", "finalizing"):
                return
            self._trigger_initiated = False
            self._begin_recording()
```

`arm_trigger`: after the replay gate, the same three lines (`reason = ...; if reason is not None: self._note_refusal(reason); return` and `self._last_refusal = None`).

In `_check_trigger_and_record`, the fire site becomes:

```python
            if state == "armed" and self._check_power_above_threshold(sc16_buf):
                reason = self._recording_refusal()
                if reason is not None:
                    # Stay armed: once space is back the next crossing fires.
                    self._note_refusal(reason)
                    return
                self._last_refusal = None
                self._trigger_initiated = True
                self._begin_recording()
```

`_request_end_recording(self, wait: bool, reason: str = "manual")`: inside the lock, right after the `!= "recording"` early return, add `self._stop_reason = reason`. Call sites:
- `stop_recording`: `self._request_end_recording(wait=True, reason="manual")`
- shutdown in `run()`: `self._request_end_recording(wait=True, reason="shutdown")`
- max duration: `self._request_end_recording(wait=False, reason="max_duration")`
- trigger hysteresis: `self._request_end_recording(wait=False, reason="trigger_end")`

In `_begin_recording`, with the other resets: `self._stop_reason = None`.

`recording_status` adds `"refused": self._last_refusal,`.

In `_write_recording_metadata`'s `meta` dict, after `"trigger_initiated"`: `"stopped_reason": self._stop_reason or "manual",`.

- [ ] **Step 4: Implement the API 409s in `routes/api.py`**

Add a helper after `_rec_status`:

```python
def _raise_if_refused(proc: Any) -> dict[str, Any]:
    """The recording status, or 409 when storage refused the start/arm."""
    st = _rec_status(proc)
    refused = st.get("refused")
    # isinstance: a MagicMock processor (web route tests) returns a truthy mock.
    if isinstance(refused, str) and st.get("state") not in ("recording", "finalizing", "armed"):
        raise HTTPException(status_code=409, detail=refused)
    return st
```

Use it: in `recording_start` return `_raise_if_refused(proc)` instead of `_rec_status(proc)`; same in `recording_arm`; in `trigger_capture` call `_raise_if_refused(proc)` before `return {"status": "triggered"}`; in `replay_record` for `on` return `_raise_if_refused(proc)`.

- [ ] **Step 5: Run the tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_recording_storage.py tests/unit/test_trigger.py tests/unit/test_trigger_continuous.py tests/unit/test_recording_gaps.py tests/unit/test_recording_cap.py tests/unit/test_web_routes.py tests/unit/test_replay_routes.py -q`
Expected: all pass.

- [ ] **Step 6: Full checks and commit**

```bash
git add src/rfobserver/pipeline/streaming.py src/rfobserver/web/routes/api.py tests/unit/test_recording_storage.py
git commit -m "feat(recording): refuse recordings at storage step 3 and record why each stopped"
```

---

### Task 6: Write failures, file-derived metadata, and the mid-recording guard

**Files:**
- Modify: `src/rfobserver/pipeline/streaming.py` (`__init__`, `_check_trigger_and_record`, `_begin_recording`, `_finalize_recording` ~1293, `_write_recording_metadata` ~1466, `_file_writer_loop` ~1564, `_persist_avg_window` ~2217, detection insert ~2349)
- Test: append to `tests/unit/test_recording_storage.py`

**Interfaces:**
- Consumes: `describe_write_error`, `is_disk_full_error`, `resolve_floor`, `HARD_FLOOR_FRACTION` (Task 1); `self._governor` (Task 4); `_request_end_recording(reason=...)` (Task 5).
- Produces:
  - `self._writer_error: str | None`, `self._disk_floor_hit: bool`, `self._disk_usage` (injectable, default `shutil.disk_usage`)
  - `StreamingProcessor._report_write_error(message: str) -> None`
  - `StreamingProcessor._check_disk_floor() -> None`
  - `StreamingProcessor._bytes_from_file(path: Path) -> int`
  - capture `.json` gains `"write_failed": bool` always, `"write_error": str` on failure
  - `_persist_avg_window` writes `powers=None` at step 4

- [ ] **Step 1: Write the failing tests (append)**

```python
# --- write failures ---------------------------------------------------------

import errno  # noqa: E402
import time  # noqa: E402


class _FailingFile:
    """Writes the first `ok` bytes, then raises ENOSPC (on write or on close)."""

    def __init__(self, real, ok: int, on_close: bool = False) -> None:
        self.real, self.left, self.on_close = real, ok, on_close

    def write(self, data) -> int:
        b = memoryview(data).cast("B")
        if not self.on_close and len(b) > self.left:
            self.real.write(b[: self.left])
            self.left = 0
            raise OSError(errno.ENOSPC, "No space left on device")
        self.left -= len(b)
        return self.real.write(b)

    def close(self) -> None:
        self.real.close()
        if self.on_close:
            raise OSError(errno.ENOSPC, "No space left on device")

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _patch_sc16_open(monkeypatch, ok: int, on_close: bool = False) -> None:
    import builtins

    real_open = builtins.open

    def fake_open(path, mode="r", *a, **k):
        f = real_open(path, mode, *a, **k)
        if str(path).endswith(".sc16") and "w" in mode:
            return _FailingFile(f, ok, on_close)
        return f

    monkeypatch.setattr("rfobserver.pipeline.streaming.open", fake_open, raising=False)


def _run_disk_recording(proc: StreamingProcessor, chunks: int, n: int = 1000) -> None:
    """Drive a disk-mode recording the way the receiver thread does."""
    proc.start_recording()
    pos = proc._pre_trigger_buf.total_written
    for _ in range(chunks):
        if proc.recording_status()["state"] != "recording":
            break
        proc._check_trigger_and_record(np.ones(n, dtype=np.int32), (), pos)
        pos += n
        time.sleep(0.01)  # let the writer thread drain
    if proc.recording_status()["state"] == "recording":
        proc.stop_recording()
    else:
        proc._end_done.wait(timeout=15)


def test_writer_enospc_ends_the_recording_promptly_and_is_recorded(tmp_path, monkeypatch):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, RECORDING_RAM_BUFFER=False)
    _patch_sc16_open(monkeypatch, ok=6000)  # 1500 samples reach disk
    _run_disk_recording(proc, chunks=50)
    meta = _only_json(proc)
    assert meta["write_failed"] is True
    assert meta["write_error"].startswith("ENOSPC")
    assert meta["stopped_reason"] == "write_error"
    (sc16,) = proc._storage.manual_dir.glob("*.sc16")
    assert sc16.stat().st_size == meta["total_bytes"] == meta["total_samples"] * 4
    assert meta["total_samples"] == 1500
    assert meta["dropped_chunks"] < 5  # ended promptly, not 50 chunks of gaps
    assert all(g[0] <= meta["total_samples"] for g in meta["gaps"])
    assert gov.state.degraded and "ENOSPC" in gov.state.last_write_error["error"]


def test_error_at_close_after_stop_does_not_hang_and_is_recorded(tmp_path, monkeypatch):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, RECORDING_RAM_BUFFER=False)
    _patch_sc16_open(monkeypatch, ok=10**9, on_close=True)
    t0 = time.monotonic()
    _run_disk_recording(proc, chunks=3)
    assert time.monotonic() - t0 < 5
    meta = _only_json(proc)
    assert meta["write_failed"] is True
    assert meta["stopped_reason"] == "manual"
    assert gov.state.degraded


def test_partial_trailing_sample_is_truncated(tmp_path, monkeypatch):
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=False)
    _patch_sc16_open(monkeypatch, ok=6002)  # half a sample past 1500
    _run_disk_recording(proc, chunks=50)
    (sc16,) = proc._storage.manual_dir.glob("*.sc16")
    assert sc16.stat().st_size == 6000
    assert _only_json(proc)["total_samples"] == 1500


def test_clean_recording_reports_no_failure(tmp_path):
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=False)
    _run_disk_recording(proc, chunks=3)
    meta = _only_json(proc)
    assert meta["write_failed"] is False and "write_error" not in meta
    (sc16,) = proc._storage.manual_dir.glob("*.sc16")
    assert sc16.stat().st_size == meta["total_bytes"]


def test_ram_tofile_failure_keeps_what_reached_disk(tmp_path, monkeypatch):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()
    proc._write_recording_chunk(np.ones(4000, dtype=np.int32))

    class _FailingArray(np.ndarray):
        """ndarray.tofile cannot be monkeypatched (immutable type); a view of
        this subclass survives the slice finalize takes."""

        def tofile(self, path, *a, **k):
            with open(path, "wb") as f:
                f.write(np.asarray(self[:1000]).tobytes())
            raise OSError(errno.ENOSPC, "No space left on device")

    proc._recording_buf = proc._recording_buf.view(_FailingArray)
    proc.stop_recording()
    meta = _only_json(proc)  # metadata still written
    assert meta["write_failed"] is True and meta["total_samples"] == 1000
    assert gov.state.degraded


def test_disk_floor_guard_ends_the_recording_before_enospc(tmp_path):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, RECORDING_RAM_BUFFER=False, DISK_MIN_FREE_GB=10)
    proc._disk_usage = lambda p: SimpleNamespace(total=1000 * GB, used=996 * GB, free=4 * GB)
    _run_disk_recording(proc, chunks=40, n=100_000)  # 4 s of IQ at 1 Msps: >= 1 check
    meta = _only_json(proc)
    assert meta["stopped_reason"] == "disk_floor"
    assert meta["write_failed"] is False


def test_disk_floor_guard_is_quiet_above_half_the_floor(tmp_path):
    proc = _proc(tmp_path, None, RECORDING_RAM_BUFFER=False, DISK_MIN_FREE_GB=10)
    proc._disk_usage = lambda p: SimpleNamespace(total=1000 * GB, used=994 * GB, free=6 * GB)
    _run_disk_recording(proc, chunks=15, n=100_000)  # 1.5 s of IQ: one check, above floor/2
    assert _only_json(proc)["stopped_reason"] == "manual"


def test_sidecar_write_failure_reports_to_the_governor(tmp_path, monkeypatch):
    gov = _governor_at(0)
    proc = _proc(tmp_path, gov, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0)
    proc.start_recording()

    def boom(self, *a, **k):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(Path, "write_text", boom)
    proc.stop_recording()
    assert proc.recording_status()["state"] == "idle"  # finalize did not wedge
    assert "json" in gov.state.last_write_error["error"]


async def test_step_4_writes_the_stats_row_without_a_blob(tmp_path):
    proc = _proc(tmp_path, _governor_at(4))
    captured = {}

    async def fake_insert(**kw):
        captured.update(kw)

    proc._db.insert_avg_window = fake_insert
    result = SimpleNamespace(
        summary_psd=SimpleNamespace(frequencies=[1.0, 2.0], num_bins=2),
        center_freq_hz=915e6,
        capture_num=1,
    )
    stats = SimpleNamespace(average=0.0, max=0.0, median=0.0, std=0.0, kurtosis=0.0)
    await proc._persist_avg_window([1.0, 2.0], result, stats)
    assert captured["powers"] is None
    assert captured["pwr_avg"] == 0.0


async def test_sqlite_disk_full_reports_to_the_governor(tmp_path):
    import sqlite3

    gov = _governor_at(0)
    proc = _proc(tmp_path, gov)

    async def full(**kw):
        raise sqlite3.OperationalError("database or disk is full")

    proc._db.insert_avg_window = full
    result = SimpleNamespace(
        summary_psd=SimpleNamespace(frequencies=[1.0, 2.0], num_bins=2),
        center_freq_hz=915e6,
        capture_num=1,
    )
    stats = SimpleNamespace(average=0.0, max=0.0, median=0.0, std=0.0, kurtosis=0.0)
    await proc._persist_avg_window([1.0, 2.0], result, stats)
    assert "disk is full" in gov.state.last_write_error["error"]
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_recording_storage.py -q -k "write or enospc or tofile or floor or sidecar or step_4 or sqlite or trailing or clean"`
Expected: failures (`write_failed` KeyError, the writer hanging until the 10 s put timeout, `_disk_usage` attribute unused).

- [ ] **Step 3: Implement**

Imports in `streaming.py`: `import shutil`, and
`from rfobserver.storage.governor import HARD_FLOOR_FRACTION, describe_write_error, is_disk_full_error, resolve_floor`.

`__init__`, beside `_stop_reason`:

```python
        # Set by the writer thread (or the RAM flush) on a failed write; the
        # receiver thread ends the recording on seeing it.
        self._writer_error: str | None = None
        # Set by the writer's own disk check when free space drops below half
        # the floor mid-recording (the governor's 10 s tick is too slow).
        self._disk_floor_hit = False
        self._disk_usage: Callable[[Any], Any] = shutil.disk_usage
```

`_begin_recording`, with the other resets: `self._writer_error = None` and `self._disk_floor_hit = False`.

`_check_trigger_and_record`, at the top of the `if state == "recording":` branch, before `_write_recording_chunk`:

```python
            if self._writer_error is not None:
                self._request_end_recording(wait=False, reason="write_error")
                return
            if self._disk_floor_hit:
                self._request_end_recording(wait=False, reason="disk_floor")
                return
```

New helpers:

```python
    def _report_write_error(self, message: str) -> None:
        """Log a failed write and set the governor's sticky flag."""
        logger.error("Write failed: %s", message)
        if self._governor is not None:
            self._governor.report_write_error(message)

    def _check_disk_floor(self) -> None:
        """Writer thread, about once per second of IQ: end the recording
        cleanly if free space is below half the floor, before ENOSPC."""
        try:
            du = self._disk_usage(self._recording_dir)
        except OSError:
            return
        floor = resolve_floor(self._settings.DISK_MIN_FREE_GB, du.total)
        if du.free < floor * HARD_FLOOR_FRACTION and not self._disk_floor_hit:
            logger.warning(
                "Free space %.2f GB below half the %.2f GB floor: ending the recording",
                du.free / 1024**3,
                floor / 1024**3,
            )
            self._disk_floor_hit = True

    def _bytes_from_file(self, path: Path) -> int:
        """The capture's true size, from the closed file: a partial trailing
        sample (a short write) is truncated away. The enqueue counter can
        claim bytes that never reached the disk."""
        try:
            size = path.stat().st_size
        except OSError:
            return 0
        whole = size - size % 4
        if whole != size:
            with contextlib.suppress(OSError):
                os.truncate(path, whole)
        if whole != self._recording_bytes and self._writer_error is None:
            logger.warning(
                "IQ bytes on disk (%d) differ from bytes queued (%d); reporting the file",
                whole,
                self._recording_bytes,
            )
        return whole
```

`_file_writer_loop` body after the pinning block becomes:

```python
        filepath = self._recording_dir / (self._recording_file or "recording.sc16")
        rate = float(self._settings.BANDWIDTH) or 1.0
        check_every = max(1, int(rate * 4))  # about one second of IQ
        since_check = 0
        stopped = False
        try:
            with (
                open(filepath, "wb", buffering=8 * 1024 * 1024) as f,
                open(self._grid_raw_path, "wb") as gf,
            ):
                while True:
                    item = self._recording_queue.get()
                    if item is None:
                        stopped = True
                        break
                    kind, data = item
                    if kind == "iq":
                        f.write(data)
                        since_check += memoryview(data).nbytes
                        if since_check >= check_every:
                            since_check = 0
                            self._check_disk_floor()
                    else:
                        gf.write(data)
                    # No flush: let the OS buffer writes for throughput. The
                    # close at loop exit flushes, and can itself fail (ENOSPC).
        except Exception as exc:
            self._writer_error = describe_write_error(exc)
            logger.error("Recording write failed (%s); ending the recording", self._writer_error)
            if not stopped:
                # Keep consuming so finalize's stop sentinel is never blocked
                # and the receiver thread never sees a full queue.
                while self._recording_queue.get() is not None:
                    pass
```

`_finalize_recording`:
- RAM branch: wrap `used.tofile(str(filepath))` in `try: ... except OSError as exc: self._writer_error = describe_write_error(exc)` with the comment "whatever reached disk is kept; metadata, DB insert and eviction still run, flagged as failed". Drop the `self._recording_bytes = self._recording_buf_pos * 4` line (the file-derived step below replaces it).
- After the disk branch's drop-rename block (so after both branches), add:

```python
        # Metadata comes from the file on disk, never from the enqueue counter.
        self._recording_bytes = self._bytes_from_file(self._recording_dir / base_name)
        if self._writer_error is not None:
            self._report_write_error(f"{base_name}: {self._writer_error}")
```

- Wrap the RAM `.psd` flush (`with open(raw_path, "wb") as fh: ...`) in `try/except OSError as exc: self._report_write_error(f"{raw_path.name}: {describe_write_error(exc)}")`.
- Wrap the `psd_grid.write_meta(...)` call the same way with `meta_path.name`.
- Wrap `self._write_recording_metadata(base_name, duration)` the same way with `f"{base_name} metadata .json: ..."`. (Inside it, the `.json` write is the part that can fail; the DB insert is scheduled after it, so move the `json_path.write_text(...)` into its own `try/except OSError` that reports and continues to the DB insert, instead of wrapping the whole call.)

`_write_recording_metadata`:

```python
        write_error = self._writer_error
        gaps = self._recording_gaps
        if write_error is not None:
            # The file ends early: gaps queued past its end describe nothing.
            gaps = [g for g in gaps if g[0] <= total_samples]
            if not self._recording_gaps_truncated:
                lost = sum(g[1] for g in gaps)
```

(`lost` is assigned from `self._recording_lost` just above this; place the block after it and before `time_span` is computed.) Use `gaps` for `"gaps"` in `meta`, and add:

```python
            "write_failed": write_error is not None,
```

then after the dict: `if write_error is not None: meta["write_error"] = write_error`.

`_persist_avg_window`: pass

```python
                powers=(
                    None
                    if self._governor is not None and self._governor.state.skip_psd_blobs
                    else avg_powers
                ),
```

and change its `except Exception:` to:

```python
        except Exception as exc:
            if is_disk_full_error(exc):
                self._report_write_error(f"database: {exc}")
            logger.exception("avg-window persist failed (chunk #%d)", result.capture_num)
```

Same change around `await self._db.insert_detections(rows)` (~2349), keeping its existing log text.

- [ ] **Step 4: Run the tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_recording_storage.py tests/unit/test_recording_gaps.py tests/unit/test_recording_cap.py tests/unit/test_trigger.py tests/unit/test_trigger_continuous.py tests/unit/test_streaming.py tests/unit/test_streaming_avg_window.py tests/unit/test_grid_prebuffer.py -q`
Expected: all pass. `test_dropped_disk_chunk_is_one_gap_at_the_file_position` still asserts `_recording_bytes == 4000` during recording; that is the enqueue position, which this task leaves untouched until finalize.

Then the integration suite, which drives real disk recordings: `PYTHONPATH= .venv/bin/pytest tests/integration/test_recording_grids.py tests/integration/test_recording_gaps.py -q`.

- [ ] **Step 5: Full checks and commit**

```bash
git add src/rfobserver/pipeline/streaming.py tests/unit/test_recording_storage.py
git commit -m "feat(recording): end on write failure, derive metadata from the file, guard the floor"
```

---

### Task 7: Health storage block and clearing the sticky flag

**Files:**
- Modify: `src/rfobserver/web/app.py` (`health` ~92)
- Modify: `src/rfobserver/web/routes/api.py` (new route)
- Test: `tests/unit/test_storage_health.py`

**Interfaces:**
- Consumes: `app.state.storage_governor`, `app.state.write_database` (Task 4); `DEGRADED_CONFIG_KEY`, `StorageGovernor` (Task 1).
- Produces: `GET /api/health` `storage` key (the `to_health()` dict or absent when no governor); `POST /api/storage/clear-degraded` returning the new storage block.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_storage_health.py`:

```python
"""Storage state on /api/health, and clearing the sticky flag."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from rfobserver.config import AppSettings
from rfobserver.storage.governor import (
    DEGRADED_CONFIG_KEY,
    GB,
    StorageGovernor,
    StorageSample,
    VolumeSample,
)
from rfobserver.web.app import create_app

T0 = datetime(2026, 9, 23, tzinfo=timezone.utc)


def _tick(gov: StorageGovernor, free_gb: float, evictable: bool = True) -> None:
    gov.tick(
        StorageSample(
            data=VolumeSample(int(free_gb * GB), 1000 * GB),
            db_volume=None,
            db_file_bytes=GB,
            db_reusable_bytes=0,
            auto_bytes=0,
            manual_bytes=0,
            evictable_auto=evictable,
        ),
        min_free_gb=0,
        now=T0,
    )


class _WDB:
    def __init__(self) -> None:
        self.config: dict[str, str] = {}

    async def set_config(self, k: str, v: str) -> None:
        self.config[k] = v


def _client(gov: StorageGovernor | None, wdb: _WDB | None = None) -> TestClient:
    app = create_app(AppSettings(_env_file=None))
    app.state.storage_governor = gov
    app.state.write_database = wdb
    return TestClient(app)


def test_health_without_a_governor_has_no_storage_block():
    body = _client(None).get("/api/health").json()
    assert "storage" not in body and body["status"] == "ok"


def test_steps_1_and_2_are_reported_but_not_degraded():
    gov = StorageGovernor()
    _tick(gov, 40)
    body = _client(gov).get("/api/health").json()
    assert body["storage"]["step"] == 1
    assert body["status"] == "ok"


def test_step_3_is_degraded():
    gov = StorageGovernor()
    _tick(gov, 40, evictable=False)
    _tick(gov, 40, evictable=False)
    assert _client(gov).get("/api/health").json()["status"] == "degraded"


def test_sticky_flag_is_degraded_after_recovery_until_cleared():
    gov = StorageGovernor()
    _tick(gov, 200)
    gov.report_write_error("ENOSPC: No space left on device", now=T0)
    wdb = _WDB()
    c = _client(gov, wdb)
    body = c.get("/api/health").json()
    assert body["status"] == "degraded"
    assert body["storage"]["last_write_error"]["error"].startswith("ENOSPC")
    r = c.post("/api/storage/clear-degraded")
    assert r.status_code == 200 and r.json()["degraded_since"] is None
    assert wdb.config[DEGRADED_CONFIG_KEY] == ""
    assert c.get("/api/health").json()["status"] == "ok"


def test_clear_without_a_governor_is_409():
    assert _client(None).post("/api/storage/clear-degraded").status_code == 409
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_storage_health.py -q`
Expected: `storage` missing from health; 404 on the clear route.

- [ ] **Step 3: Implement**

In `web/app.py` `health()`, before `return body`:

```python
        gov = getattr(app.state, "storage_governor", None)
        if gov is not None:
            st = gov.state
            body["storage"] = st.to_health()
            # Steps 1-2 are the system working as designed: reported, not
            # degraded. Step >= 3 or the sticky flag is degraded.
            if st.degraded:
                body["status"] = "degraded"
```

In `routes/api.py`, next to the other `/storage/...` route:

```python
@router.post("/storage/clear-degraded")
async def storage_clear_degraded(request: Request) -> dict[str, Any]:
    """Acknowledge a storage failure: clears the sticky degraded flag (and the
    last write error). The flag stays set after space recovers until this is
    called, so the evidence survives until someone has seen it."""
    from rfobserver.storage.governor import DEGRADED_CONFIG_KEY

    gov = getattr(request.app.state, "storage_governor", None)
    if gov is None:
        raise HTTPException(status_code=409, detail="Storage governor not running")
    gov.clear_degraded()
    wdb = getattr(request.app.state, "write_database", None)
    if wdb is not None:
        await wdb.set_config(DEGRADED_CONFIG_KEY, "")
    result: dict[str, Any] = gov.state.to_health()
    return result
```

- [ ] **Step 4: Run the tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_storage_health.py tests/unit/test_health_uptime.py tests/unit/test_web_routes.py -q`
Expected: all pass.

- [ ] **Step 5: Full checks and commit**

```bash
git add src/rfobserver/web/app.py src/rfobserver/web/routes/api.py tests/unit/test_storage_health.py
git commit -m "feat(web): storage block on /api/health and a clear-degraded endpoint"
```

---

### Task 8: Dashboard banner, record-refusal notice, config page fields and storage bar

**Files:**
- Modify: `src/rfobserver/web/templates/dashboard.html` (banner markup at the top of `{% block content %}`; `handleHeartbeat` ~1233; record buttons ~1267 and markup ~24)
- Modify: `src/rfobserver/web/templates/config.html` (Storage card ~245-270)
- Modify: `src/rfobserver/web/routes/config.py` (`field_map` ~106)
- Modify: `src/rfobserver/web/static/style.css` (append)
- Test: append to `tests/unit/test_storage_health.py`

**Interfaces:**
- Consumes: heartbeat `d.storage` (Task 4), `/api/health` `storage` (Task 7), `POST /api/storage/clear-degraded` (Task 7), 409 `detail` from record/arm (Task 5).
- Produces: DOM ids `storage-banner`, `storage-banner-text`, `storage-banner-clear`, `rec-notice`, `storage-bar`, `storage-legend`; form names `disk_min_free_gb`, `stats_retention_days`, `storage_check_sec`.

- [ ] **Step 1: Write the failing tests (append to `test_storage_health.py`)**

```python
def test_dashboard_has_the_storage_banner_and_record_notice():
    html = _client(None).get("/").text
    for needle in ('id="storage-banner"', 'id="storage-banner-clear"', 'id="rec-notice"'):
        assert needle in html


def test_config_page_has_the_storage_bar_and_fields():
    html = _client(None).get("/config").text
    for needle in (
        'id="storage-bar"',
        'name="disk_min_free_gb"',
        'name="stats_retention_days"',
        'name="storage_check_sec"',
    ):
        assert needle in html


def test_config_apply_accepts_the_storage_settings():
    app = create_app(AppSettings(_env_file=None))
    c = TestClient(app)
    r = c.post(
        "/config/apply",
        json={"disk_min_free_gb": "12.5", "stats_retention_days": "365", "storage_check_sec": "5"},
    )
    assert r.status_code == 200, r.text
    s = app.state.settings
    assert s.DISK_MIN_FREE_GB == 12.5 and s.STATS_RETENTION_DAYS == 365
    assert s.STORAGE_CHECK_SEC == 5.0
```

`/config/apply` reads `request.app.state.settings`; `tests/unit/test_web_routes.py:291` posts to it the same way. If that file redirects `.env` persistence (a fixture or monkeypatch around the apply call), copy that setup into this test so it cannot write the repo's `.env`.

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_storage_health.py -q -k "dashboard or config"`
Expected: 3 failures.

- [ ] **Step 3: Config route and page**

`routes/config.py` `field_map`, after `"history_days"`:

```python
        "disk_min_free_gb": ("DISK_MIN_FREE_GB", float),
        "stats_retention_days": ("STATS_RETENTION_DAYS", int),
        "storage_check_sec": ("STORAGE_CHECK_SEC", float),
```

`config.html`, Storage card: change the PSD History help text to `Spectra older than this lose their PSD data. Stats and detections follow Stats History.`, then add after the Archive Max / PSD History `form-row`:

```html
            <div class="form-row">
                <div class="form-group">
                    <label for="disk-min-free">Minimum Free Space (GB)</label>
                    <input type="number" id="disk-min-free" name="disk_min_free_gb" value="{{ settings.DISK_MIN_FREE_GB }}" step="1" min="0">
                    <span style="font-size: 10px; opacity: 0.6; margin-top: 2px; display: block;">0 = automatic: 5% of the volume, at least 2 GB. Below it the oldest automatic captures are deleted, then history is pruned harder, then recordings are refused. Manual captures are never deleted.</span>
                </div>
                <div class="form-group">
                    <label for="stats-retention">Stats History (days)</label>
                    <input type="number" id="stats-retention" name="stats_retention_days" value="{{ settings.STATS_RETENTION_DAYS }}" min="0">
                    <span style="font-size: 10px; opacity: 0.6; margin-top: 2px; display: block;">Power statistics, detections and minute summaries older than this are deleted. 0 keeps them forever.</span>
                </div>
                <div class="form-group">
                    <label for="storage-check">Storage Check (s)</label>
                    <input type="number" id="storage-check" name="storage_check_sec" value="{{ settings.STORAGE_CHECK_SEC }}" step="1" min="1">
                    <span style="font-size: 10px; opacity: 0.6; margin-top: 2px; display: block;">How often free space is checked. A recording checks its own disk about every second.</span>
                </div>
            </div>
            <div class="form-row">
                <div class="form-group" style="flex: 1;">
                    <label>Volume</label>
                    <div id="storage-bar" class="storage-bar" role="img" aria-label="Storage volume usage">
                        <span class="sb-seg sb-manual"></span><span class="sb-seg sb-auto"></span><span class="sb-seg sb-db"></span><span class="sb-seg sb-other"></span>
                        <span class="sb-floor" title="Minimum free space"></span>
                    </div>
                    <div id="storage-legend" class="storage-legend">--</div>
                </div>
            </div>
```

At the end of the page's script block:

```js
    // Storage bar: manual, auto, DB and everything else, left to right; the
    // remainder is free. The red line marks the minimum free space.
    async function refreshStorageBar() {
        let s;
        try {
            const r = await fetch('/api/health', {cache: 'no-store'});
            s = (await r.json()).storage;
        } catch (e) { return; }
        const bar = document.getElementById('storage-bar');
        const legend = document.getElementById('storage-legend');
        if (!bar || !s || !s.volume_gb) { if (legend) legend.textContent = 'Not available'; return; }
        const vol = s.volume_gb;
        const dbHere = s.db_volume ? 0 : s.db_gb;
        const used = vol - s.free_gb;
        const other = Math.max(0, used - s.manual_gb - s.auto_gb - dbHere);
        const pct = v => (100 * Math.max(0, v) / vol).toFixed(2) + '%';
        bar.querySelector('.sb-manual').style.width = pct(s.manual_gb);
        bar.querySelector('.sb-auto').style.width = pct(s.auto_gb);
        bar.querySelector('.sb-db').style.width = pct(dbHere);
        bar.querySelector('.sb-other').style.width = pct(other);
        bar.querySelector('.sb-floor').style.left = pct(vol - s.floor_gb);
        const parts = [
            `Manual ${s.manual_gb} GB`, `Automatic ${s.auto_gb} GB`,
            s.db_volume ? `Database ${s.db_gb} GB (other volume, ${s.db_volume.free_gb} GB free)` : `Database ${s.db_gb} GB`,
            `Other ${other.toFixed(1)} GB`, `Free ${s.free_gb} of ${vol} GB`, `Minimum free ${s.floor_gb} GB`,
        ];
        if (s.step > 0) parts.push(`Step ${s.step}: ${s.step_text}`);
        legend.textContent = parts.join('  ·  ');
    }
    refreshStorageBar();
    setInterval(refreshStorageBar, 30000);
```

- [ ] **Step 4: Dashboard banner and notice**

Top of `{% block content %}` in `dashboard.html`, directly after the `shared-charts.js` script tag:

```html
<div id="storage-banner" class="storage-banner" role="status" style="display: none;">
    <span class="storage-banner-label">Storage</span>
    <span id="storage-banner-text" class="storage-banner-text"></span>
    <button type="button" id="storage-banner-clear" class="storage-banner-clear" style="display: none;">Clear Warning</button>
</div>
```

After the `stop-btn` button (line ~26): `<span id="rec-notice" class="rec-notice" style="display:none;"></span>`

In the script, beside `handleHeartbeat`:

```js
    const storageBanner = document.getElementById("storage-banner");
    const storageBannerText = document.getElementById("storage-banner-text");
    const storageBannerClear = document.getElementById("storage-banner-clear");

    function updateStorageBanner(s) {
        const show = s && (s.step > 0 || s.degraded_since);
        storageBanner.style.display = show ? "flex" : "none";
        if (!show) return;
        const parts = [];
        if (s.step > 0) {
            parts.push(`Step ${s.step}: ${s.step_text}. ${s.free_gb} GB free, minimum ${s.floor_gb} GB.`);
        }
        if (s.last_write_error) {
            parts.push(`Last write error: ${s.last_write_error.error}`);
        } else if (s.degraded_since) {
            parts.push(`A storage problem occurred at ${new Date(s.degraded_since).toLocaleString()}.`);
        }
        storageBannerText.textContent = parts.join(" ");
        storageBanner.classList.toggle("storage-banner-critical", s.step >= 3 || !!s.degraded_since);
        storageBannerClear.style.display = s.degraded_since ? "" : "none";
    }

    storageBannerClear.addEventListener("click", async function() {
        const r = await fetch("/api/storage/clear-degraded", {method: "POST"});
        if (r.ok) updateStorageBanner(await r.json());
    });

    const recNotice = document.getElementById("rec-notice");
    let recNoticeTimer = null;
    async function recordRequest(url) {
        const r = await fetch(url, {method: "POST"});
        if (r.ok) return;
        let detail = "Request failed";
        try { detail = (await r.json()).detail || detail; } catch (e) { /* keep default */ }
        recNotice.textContent = detail;
        recNotice.style.display = "";
        clearTimeout(recNoticeTimer);
        recNoticeTimer = setTimeout(() => { recNotice.style.display = "none"; }, 10000);
    }
```

In `handleHeartbeat(d)` add: `if (d.storage !== undefined) updateStorageBanner(d.storage);`

Replace the rec and arm click handlers' bodies with `await recordRequest("/api/recording/start");` and `await recordRequest("/api/recording/arm");`.

- [ ] **Step 5: Styles (append to `style.css`)**

```css
/* Storage governor banner (dashboard): amber while RFObserver is freeing
   space on its own (steps 1-2), red once recordings are refused or a write
   failed. */
.storage-banner {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 12px;
    padding: 8px 14px;
    border-radius: var(--radius);
    background: rgba(255, 159, 10, 0.12);
    color: var(--text-primary);
    font-size: 13px;
}
.storage-banner-critical { background: rgba(255, 59, 48, 0.12); }
.storage-banner-label { font-weight: 600; }
.storage-banner-text { flex: 1; }
.storage-banner-clear {
    border: none;
    border-radius: 8px;
    padding: 4px 12px;
    background: var(--accent);
    color: #fff;
    font-size: 12px;
    cursor: pointer;
}
.rec-notice { margin-left: 8px; font-size: 11px; color: #ff3b30; }

/* Config page storage bar */
.storage-bar {
    position: relative;
    display: flex;
    height: 14px;
    border-radius: 7px;
    overflow: hidden;
    background: var(--divider);
    border: 1px solid var(--border);
}
.sb-seg { height: 100%; }
.sb-manual { background: #0a84ff; }
.sb-auto { background: #5e5ce6; }
.sb-db { background: #ff9f0a; }
.sb-other { background: #8e8e93; }
.sb-floor {
    position: absolute;
    top: 0;
    bottom: 0;
    width: 2px;
    background: #ff3b30;
}
.storage-legend { margin-top: 4px; font-size: 10px; color: var(--text-secondary); }
```

- [ ] **Step 6: Run the tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_storage_health.py tests/unit/test_web_routes.py tests/unit/test_static_cache.py -q`
Expected: all pass.

- [ ] **Step 7: Headless check in mock mode (workstation)**

```bash
cd /home/orencollaco/GitHub/RFObserver
PYTHONPATH= RFOBS_MOCK_RECEIVER=true RFOBS_WEB_PORT=8888 RFOBS_DISK_MIN_FREE_GB=100000 \
  RFOBS_STORAGE_PATH=$PWD/.scratch-storage RFOBS_DB_PATH=$PWD/.scratch-storage/t.db \
  sh -c 'echo $$ > /tmp/claude-1000/rfobs-mock.pid; exec .venv/bin/rfobserver run' &
```

A floor of 100000 GB forces step 4 on any disk. With puppeteer (`launchOptions {"headless": true}`), load `http://localhost:8888/` and confirm `#storage-banner` is visible with "Step 4"; click REC and confirm `#rec-notice` shows "Recording refused ..."; load `/config` and confirm the bar and legend render; screenshot both. Stop with `fuser -k 8888/tcp`; remove `.scratch-storage`.

- [ ] **Step 8: Full checks and commit**

```bash
git add src/rfobserver/web/templates/dashboard.html src/rfobserver/web/templates/config.html src/rfobserver/web/routes/config.py src/rfobserver/web/static/style.css tests/unit/test_storage_health.py
git commit -m "feat(web): storage banner, refusal notice, and config storage bar"
```

---

### Task 9: Validation on nano-super, chunk size, and docs

**Files:**
- Modify: `src/rfobserver/storage/database.py` (`RETENTION_CHUNK_ROWS` from measurement)
- Modify: `README.md` (new "Storage" section after "Downloading captures")
- Create: `docs/debugging/2026-09-23_storage-budgeting-validation.md` (validation record, the global doc format)

**Interfaces:**
- Consumes: everything above.
- Produces: the measured chunk size and a validation record.

nano-super is `ocollaco@192.168.97.153`; its checkout is on `feat/averaged-window-store` with `stash@{0}: wip-before-peak-finder-validation`. Record the branch and `git stash list` first, validate from a separate worktree (`git worktree add ~/rfobs-storage feat/storage-budgeting` after `git fetch`), run with `PYTHONPATH=$HOME/rfobs-storage/src` so the editable install does not shadow it, and confirm `rfobserver.__file__` points into the worktree. Remove the worktree at the end and confirm the original branch and stash are unchanged.

- [ ] **Step 1: Retention chunk timing on a real-size DB**

Seed a copy DB with two years of `avg_windows` (no blobs), 90 days of blobs, and 10 M detections (the noisy-threshold rate) with a script in the scratchpad. Run `delete_older_than` / `prune_avg_psd_blobs` with chunk sizes 1000, 2000, 5000, 10000, timing each statement (wrap `_delete_older_chunk` and `_prune_blob_chunk`). Pick the largest chunk whose p99 statement time is under 100 ms (a third of the ~300 ms drop line). Then run one full pass while the mock pipeline runs against the same DB and confirm no dropped chunks in the log. Set `RETENTION_CHUNK_ROWS` to the chosen value and cite the numbers in its comment.

- [ ] **Step 2: Real ENOSPC on a tmpfs**

```bash
sudo mkdir -p /mnt/rfobs-tmpfs && sudo mount -t tmpfs -o size=256m tmpfs /mnt/rfobs-tmpfs
sudo chown ocollaco /mnt/rfobs-tmpfs
```

Run with the real B200mini at 2 Msps, `RFOBS_STORAGE_PATH=/mnt/rfobs-tmpfs`, `RFOBS_DB_PATH` on the NVMe, `RFOBS_DISK_MIN_FREE_GB=0.001` (so the floor guard does not pre-empt ENOSPC; this test exists to exercise the real error path) and `RFOBS_RECORDING_MAX_SEC=0`. Start a manual recording via `POST /api/recording/start`; it fills 256 MB in about 30 s. Expect: the recording ends by itself; the `.json` has `write_failed: true`, `write_error: "ENOSPC: ..."`, `stopped_reason: "write_error"`, `total_bytes` equal to the `.sc16` size and divisible by 4; `/api/health` is `degraded` with `last_write_error`; the pipeline keeps running (beacon age small, PSD still updating). Restart the process and confirm `degraded_since` survived; clear it via the endpoint. Then repeat with the default floor (`DISK_MIN_FREE_GB` unset, i.e. 2 GB auto on a 256 MB volume) and confirm recordings are refused at step 3/4 instead. Unmount the tmpfs afterwards.

- [ ] **Step 3: The floor ladder with a filler file**

On the NVMe, set `RFOBS_DISK_MIN_FREE_GB` to current free minus 20 GB, enable continuous triggering with a low threshold so `auto/` captures accumulate, then `fallocate` a filler file to push free below the floor. Watch `/api/health` every 10 s (python3 one-liner; the box has no curl/jq): step 1 evicts `auto/` captures until free >= floor x 1.15, the active one untouched. Delete the auto captures' headroom by growing the filler until nothing is evictable: step 2 (retention wakes; log shows the pressure pass), step 3 on the next tick (recordings refused, health degraded, banner red), step 4 after pushing free below floor / 2 (new `avg_windows` rows have `psd_powers IS NULL`). Remove the filler and confirm the step stays until three ticks at >= floor x 1.15, then drops to 0 while the sticky flag remains. Screenshot the dashboard banner and config bar at step 3 with headless puppeteer.

- [ ] **Step 4: README section**

After "Downloading captures", add a "Storage" section: the floor and its auto default, the four steps in one table, that manual captures are never deleted, that the DB file does not shrink (pages are reused), how to read `/api/health`'s `storage` block, and how to clear the warning (`curl -X POST $SENSOR/api/storage/clear-degraded`). No em-dashes.

- [ ] **Step 5: Validation record**

Write `docs/debugging/2026-09-23_storage-budgeting-validation.md` with the measured chunk timings (raw table), the tmpfs and floor runs (verbatim health JSON at each step, `.json` excerpts), anything that did not behave as designed under "Measured and REJECTED", measurement traps hit, and an "Open" list (for example the field `DB_PATH` placement, the field burst threshold).

- [ ] **Step 6: Full checks and commit**

```bash
git add src/rfobserver/storage/database.py README.md docs/debugging/2026-09-23_storage-budgeting-validation.md
git commit -m "docs(storage): validation on nano-super; retention chunk size from measurement"
```

Then ask the user (AskUserQuestion) whether to merge and push.
