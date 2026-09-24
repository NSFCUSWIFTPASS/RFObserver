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
import json
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
# The reason that goes with the flag: JSON {"at": ISO, "error": text}, or "".
LAST_WRITE_ERROR_CONFIG_KEY = "storage_last_write_error"

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
        self._ticks = 0

    @property
    def state(self) -> StorageState:
        with self._lock:
            return self._state

    @property
    def ticks(self) -> int:
        """Completed ticks since start; only ever increases. The recorder uses
        it to wait for a fresh storage check after a disk_floor/write_error stop."""
        with self._lock:
            return self._ticks

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
                recovered = recovered and sample.db_volume.free_bytes >= db_floor * RECOVERY_MARGIN

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
            self._ticks += 1
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
            # Dirty on every report: the persisted reason must follow the latest.
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

    def restore_degraded(self, raw: str | None, raw_error: str | None = None) -> None:
        """Load the persisted flag and its last write error at startup (not
        marked dirty: they are on disk). Missing or garbled values are ignored."""
        since: datetime | None = None
        if raw:
            try:
                since = datetime.fromisoformat(raw)
            except ValueError:
                since = None
        error: dict[str, str] | None = None
        if raw_error:
            try:
                value = json.loads(raw_error)
            except ValueError:
                value = None
            if (
                isinstance(value, dict)
                and isinstance(value.get("at"), str)
                and isinstance(value.get("error"), str)
            ):
                error = {"at": value["at"], "error": value["error"]}
        if since is None and error is None:
            return
        with self._lock:
            st = self._state
            self._state = replace(
                st,
                degraded_since=since if since is not None else st.degraded_since,
                last_write_error=error if error is not None else st.last_write_error,
            )

    def take_degraded_change(self) -> tuple[bool, dict[str, str]]:
        """(changed since last call, config values to persist). The values map
        DEGRADED_CONFIG_KEY to an ISO time or "" and LAST_WRITE_ERROR_CONFIG_KEY
        to JSON {"at", "error"} or ""; empty when nothing changed."""
        with self._lock:
            if not self._degraded_dirty:
                return (False, {})
            self._degraded_dirty = False
            st = self._state
            since = st.degraded_since
            err = st.last_write_error
            return True, {
                DEGRADED_CONFIG_KEY: since.isoformat() if since else "",
                LAST_WRITE_ERROR_CONFIG_KEY: json.dumps(err) if err else "",
            }


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
