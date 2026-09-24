"""Storage governor: keeps the storage volume above a free-space floor.

Pure decisions over a sampled StorageSample: nothing here touches the
filesystem or the DB, except persist_degraded_change, which writes the sticky
flag through the caller's DB handle. pipeline/app.py:_storage_loop samples,
ticks, and carries out the returned actions; consumers read the published
state.
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

import asyncio
import errno
import json
import logging
import sqlite3
import threading
from collections import deque
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

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
# A capture evicted this soon after its last write is "young" -- continuous
# triggering can otherwise sit just above the floor by deleting each new
# automatic capture about one storage check after it is saved, and health
# alone ("step 1") does not distinguish that from ordinary FIFO rotation.
YOUNG_CAPTURE_SEC = 600
# The evicting_young warning stays up this long after the last young eviction.
YOUNG_EVICTION_WINDOW_SEC = 1800
# Bound on the rolling window's entries (one per tick that evicted young
# captures). Past it, a new note merges into the newest entry, which can only
# keep evictions in the count a little longer, never drop them.
_YOUNG_WINDOW_MAX_ENTRIES = 4096

STEP_TEXT = {
    0: "healthy",
    1: "evicting the oldest automatic captures",
    2: "pruning PSD history and detections",
    3: "recordings refused",
    4: "PSD history writes stopped",
}

EVICTING_YOUNG_STEP_TEXT = "deleting automatic captures minutes after they are recorded"


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
    # Set by note_young_evictions; cleared by tick() once YOUNG_EVICTION_WINDOW_SEC
    # has lapsed since last_young_eviction. Does not affect step or degraded --
    # steps 1-2 stay "working as designed". young_evictions is the number of
    # young evictions within the last YOUNG_EVICTION_WINDOW_SEC (rolling).
    evicting_young: bool = False
    last_young_eviction: datetime | None = None
    young_evictions: int = 0
    youngest_evicted_age_sec: float | None = None

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
        # Only step 1's own text is replaced: at step >= 2 (or 0, after recovery)
        # the plain step text still matters and must not be hidden behind the
        # young-eviction wording for up to YOUNG_EVICTION_WINDOW_SEC. The
        # evicting_young flag and the dashboard banner still carry the warning.
        step_text = (
            EVICTING_YOUNG_STEP_TEXT
            if self.evicting_young and self.step == 1
            else STEP_TEXT[self.step]
        )
        return {
            "free_gb": _gb(s.data.free_bytes) if s else None,
            "floor_gb": _gb(self.floor_bytes) if s else None,
            "volume_gb": _gb(s.data.total_bytes) if s else None,
            "db_gb": _gb(s.db_file_bytes) if s else None,
            "db_reusable_gb": _gb(s.db_reusable_bytes) if s else None,
            "auto_gb": _gb(s.auto_bytes) if s else None,
            "manual_gb": _gb(s.manual_bytes) if s else None,
            "step": self.step,
            "step_text": step_text,
            "step_since": self.step_since.isoformat(),
            "last_write_error": self.last_write_error,
            "degraded_since": self.degraded_since.isoformat() if self.degraded_since else None,
            "db_volume": db_volume,
            "evicting_young": self.evicting_young,
            "young_evictions": self.young_evictions,
            "last_young_eviction": self.last_young_eviction.isoformat()
            if self.last_young_eviction
            else None,
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
        # Young evictions as (when, count), oldest first, within the window.
        self._young: deque[tuple[datetime, int]] = deque()
        # Serializes persisting the sticky flag (the storage loop and the
        # clear route), so an older write can never land after a newer one.
        self.persist_lock = asyncio.Lock()

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

            young_evictions = self._prune_young(now)
            evicting_young = bool(self._young)
            window_lapsed = st.evicting_young and not evicting_young

            self._ticks += 1
            self._state = replace(
                st,
                step=step,
                step_since=now if step != st.step else st.step_since,
                floor_bytes=floor,
                db_floor_bytes=db_floor,
                sample=sample,
                degraded_since=degraded_since,
                evicting_young=evicting_young,
                young_evictions=young_evictions,
            )

            target = int(floor * RECOVERY_MARGIN)
            evict = step >= 1 and sample.evictable_auto and data.free_bytes < target
            actions = StorageActions(
                evict_to_free_bytes=target if evict else None,
                start_pressure_prune=st.step < 2 <= step,
            )
        if window_lapsed:
            logger.info(
                "Storage: no young evictions in %d min; evicting_young warning cleared",
                YOUNG_EVICTION_WINDOW_SEC // 60,
            )
        return actions

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

    def note_young_evictions(self, count: int, youngest_age_sec: float, now: datetime) -> None:
        """Record that ``count`` automatic captures younger than
        YOUNG_CAPTURE_SEC were evicted this tick (``youngest_age_sec`` is the
        age of the youngest of them). Turns the ``evicting_young`` warning on;
        tick() clears it once YOUNG_EVICTION_WINDOW_SEC passes with no more.
        Does not touch step or the sticky degraded flag."""
        with self._lock:
            st = self._state
            young_evictions = self._prune_young(now) + count
            if len(self._young) >= _YOUNG_WINDOW_MAX_ENTRIES:
                _, merged = self._young.pop()
                self._young.append((now, merged + count))
            else:
                self._young.append((now, count))
            turning_on = not st.evicting_young
            self._state = replace(
                st,
                evicting_young=True,
                last_young_eviction=now,
                young_evictions=young_evictions,
                youngest_evicted_age_sec=youngest_age_sec,
            )
        if turning_on:
            logger.warning(
                "Storage floor: evicting automatic captures within %d s of recording "
                "(youngest %.0f s); this warning stays up for %d min",
                YOUNG_CAPTURE_SEC,
                youngest_age_sec,
                YOUNG_EVICTION_WINDOW_SEC // 60,
            )

    def _prune_young(self, now: datetime) -> int:
        """Drop young evictions older than the window; return the sum left.
        Caller holds ``_lock``."""
        while self._young and (now - self._young[0][0]).total_seconds() >= (
            YOUNG_EVICTION_WINDOW_SEC
        ):
            self._young.popleft()
        return sum(c for _, c in self._young)

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

    def mark_degraded_dirty(self) -> None:
        """Persisting the last change failed: the next take returns it again."""
        with self._lock:
            self._degraded_dirty = True

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


async def persist_degraded_change(governor: StorageGovernor, db: Any) -> None:
    """Write the sticky flag and its reason if they changed since the last
    write. The values are taken under ``persist_lock`` so writes land in
    order; a failed write marks the change dirty again (the next storage tick
    retries it) and re-raises."""
    async with governor.persist_lock:
        changed, values = governor.take_degraded_change()
        if not changed:
            return
        try:
            for key, value in values.items():
                await db.set_config(key, value)
        except BaseException:
            governor.mark_degraded_dirty()
            raise


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
