"""IQ collection statistics: raw IQ recorded, IQ deleted automatically and
live RF time, counted per UTC hour (``collection_hours``).

The recording-finalize path, the storage evictions and the receiver loop add
to one ``CollectionStats`` from their own threads; a pipeline loop flushes it
into the table, adding to each hour's row. Recorded figures never decrease:
deletions are counted separately, in the hour they happen. There is no
backfill: counting starts at the hour of the first flush
(``COLLECTION_SINCE_KEY``).
"""

from __future__ import annotations

import math
import threading
from collections import defaultdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rfobserver.storage.database import SensorDatabase

HOUR = 3600

FIELDS = (
    "auto_captures",
    "manual_captures",
    "auto_bytes",
    "manual_bytes",
    "auto_seconds",
    "manual_seconds",
    "deleted_cap_captures",
    "deleted_cap_bytes",
    "deleted_space_captures",
    "deleted_space_bytes",
    "active_seconds",
)

_ORIGINS = ("auto", "manual")
_REASONS = ("cap", "space")


def _hour(ts: float) -> int:
    return int(math.floor(ts / HOUR)) * HOUR


class CollectionStats:
    """Pending per-hour additions, safe to add to from any thread."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending: defaultdict[int, defaultdict[str, float]] = defaultdict(
            lambda: defaultdict(float)
        )

    def add_recorded(self, origin: str, start_ts: float, stop_ts: float, nbytes: int) -> None:
        """A finished capture of ``nbytes`` raw IQ spanning [start, stop].
        Bytes and seconds are split over the hours in proportion to time, the
        count goes to the hour it started in."""
        if origin not in _ORIGINS:
            return
        stop_ts = max(stop_ts, start_ts)
        span = stop_ts - start_ts
        parts: list[tuple[int, float]] = []  # (hour, seconds in it)
        t = start_ts
        while True:
            h = _hour(t)
            end = min(stop_ts, h + HOUR)
            parts.append((h, end - t))
            if end >= stop_ts:
                break
            t = end
        with self._lock:
            self._pending[_hour(start_ts)][f"{origin}_captures"] += 1
            given = 0
            for i, (h, secs) in enumerate(parts):
                if i == len(parts) - 1:
                    b = nbytes - given  # the rest, so the parts sum exactly
                else:
                    b = round(nbytes * secs / span) if span > 0 else 0
                given += b
                row = self._pending[h]
                if b:
                    row[f"{origin}_bytes"] += b
                if secs:
                    row[f"{origin}_seconds"] += secs

    def add_deleted(self, reason: str, nbytes: int, at_ts: float) -> None:
        """A capture of ``nbytes`` raw IQ deleted automatically: ``reason`` is
        "cap" (the storage cap) or "space" (the low-free-space guard)."""
        if reason not in _REASONS:
            return
        with self._lock:
            row = self._pending[_hour(at_ts)]
            row[f"deleted_{reason}_captures"] += 1
            row[f"deleted_{reason}_bytes"] += nbytes

    def add_active(self, seconds: float, at_ts: float) -> None:
        """Seconds of live RF received."""
        with self._lock:
            self._pending[_hour(at_ts)]["active_seconds"] += seconds

    def take(self) -> dict[int, dict[str, float]]:
        """The pending additions by hour, cleared."""
        with self._lock:
            out = {h: dict(v) for h, v in self._pending.items() if v}
            self._pending.clear()
        return out

    def _put_back(self, rows: dict[int, dict[str, float]]) -> None:
        with self._lock:
            for h, fields in rows.items():
                for k, v in fields.items():
                    self._pending[h][k] += v

    async def flush(self, db: SensorDatabase, now: float) -> None:
        """Add the pending figures to the table. The first flush ever marks
        the hour of ``now`` as the start of counting. On a database error the
        figures are kept for the next flush and the error is raised."""
        from rfobserver.storage.database import COLLECTION_SINCE_KEY

        if await db.get_config(COLLECTION_SINCE_KEY) is None:
            await db.set_config(COLLECTION_SINCE_KEY, str(_hour(now)))
        rows = self.take()
        if not rows:
            return
        try:
            await db.add_collection_hours(rows)
        except BaseException:
            self._put_back(rows)
            raise
