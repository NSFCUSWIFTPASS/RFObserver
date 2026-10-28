"""The sensor's schedules: persisted in SQLite, read by the receiver thread.

The API adds and deletes schedules on the event loop; the receiver thread
takes a ``snapshot()`` (cheap, under a lock) and only rebuilds its plan when
the version changes. Run outcomes are reported from the receiver and writer
threads with ``report_run``, which keeps the newest outcome per run in memory
at once and persists it on the loop.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time
from typing import TYPE_CHECKING, Any

from rfobserver.schedules.model import to_json

if TYPE_CHECKING:
    from collections.abc import Callable

    from rfobserver.schedules.model import Schedule
    from rfobserver.storage.database import SensorDatabase

logger = logging.getLogger(__name__)

RUNS_KEPT = 500
_PRUNE_EVERY = 50
_IDENTITY_IGNORES = ("created_ms", "source")


class ScheduleConflict(Exception):
    """The id is taken by a different schedule."""


def _same(a: Schedule, b: Schedule) -> bool:
    ja, jb = to_json(a), to_json(b)
    for k in _IDENTITY_IGNORES:
        ja.pop(k, None)
        jb.pop(k, None)
    return ja == jb


class ScheduleBook:
    def __init__(self, db: SensorDatabase, now: Callable[[], float] = time.time) -> None:
        self.db = db
        self._now = now
        self._lock = threading.Lock()
        self._active: dict[str, Schedule] = {}
        self._version = 0
        self._last: dict[str, int] = {}
        self._latest: dict[str, dict[int, dict[str, Any]]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    async def load(self) -> None:
        schedules = await self.db.list_schedules()
        last = {s.id: await self.db.last_run(s.id) for s in schedules}
        with self._lock:
            self._active = {s.id: s for s in schedules}
            self._last = {k: v for k, v in last.items() if v is not None}
            self._version += 1

    # --- API side (event loop) ---

    async def add(self, s: Schedule) -> tuple[Schedule, bool]:
        """(schedule, created). The same id and body again returns the stored
        one; a different body under a used id raises ScheduleConflict."""
        existing = await self.db.get_schedule(s.id)
        if existing is not None:
            with self._lock:
                live = s.id in self._active
            if live and _same(existing, s):
                return existing, False
            raise ScheduleConflict(f"id {s.id} is already used by another schedule")
        await self.db.insert_schedule(s)
        with self._lock:
            self._active[s.id] = s
            self._version += 1
        return s, True

    async def delete(self, schedule_id: str) -> bool:
        ok = bool(await self.db.delete_schedule(schedule_id, int(self._now() * 1000)))
        with self._lock:
            if self._active.pop(schedule_id, None) is not None:
                self._version += 1
        return ok

    def get(self, schedule_id: str) -> Schedule | None:
        with self._lock:
            return self._active.get(schedule_id)

    async def runs(self, schedule_id: str, limit: int = 50) -> list[dict[str, Any]]:
        """Newest first: the database's runs with the in-memory outcomes
        (runs still recording, or not yet written) laid over them."""
        rows = {r["run"]: r for r in await self.db.list_runs(schedule_id, limit)}
        with self._lock:
            for run, v in self._latest.get(schedule_id, {}).items():
                rows[run] = {"run": run, **v}
        return [rows[k] for k in sorted(rows, reverse=True)[:limit]]

    # --- receiver / writer threads ---

    def snapshot(self) -> tuple[int, list[Schedule]]:
        with self._lock:
            return self._version, list(self._active.values())

    def last_run(self, schedule_id: str) -> int | None:
        with self._lock:
            return self._last.get(schedule_id)

    def latest(self, schedule_id: str, run: int) -> dict[str, Any] | None:
        with self._lock:
            return self._latest.get(schedule_id, {}).get(run)

    def latest_runs(self, schedule_id: str) -> dict[int, dict[str, Any]]:
        with self._lock:
            return dict(self._latest.get(schedule_id, {}))

    def report_run(
        self,
        schedule_id: str,
        run: int,
        status: str,
        *,
        start_ms: int,
        filename: str | None = None,
        reason: str | None = None,
        info: dict[str, Any] | None = None,
    ) -> None:
        entry = {
            "status": status,
            "reason": reason,
            "start_ms": start_ms,
            "filename": filename,
            "info": info,
        }
        with self._lock:
            runs = self._latest.setdefault(schedule_id, {})
            runs[run] = entry
            if len(runs) > RUNS_KEPT:
                for old in sorted(runs)[: len(runs) - RUNS_KEPT]:
                    del runs[old]
            if run > self._last.get(schedule_id, -1):
                self._last[schedule_id] = run
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        with contextlib.suppress(RuntimeError):  # loop shutting down
            loop.call_soon_threadsafe(self._persist, schedule_id, run, entry)

    def _persist(self, schedule_id: str, run: int, entry: dict[str, Any]) -> None:
        async def write() -> None:
            try:
                await self.db.upsert_run(
                    schedule_id,
                    run,
                    entry["status"],
                    start_ms=entry["start_ms"],
                    filename=entry["filename"],
                    reason=entry["reason"],
                    info=entry["info"],
                )
                if run % _PRUNE_EVERY == 0:
                    await self.db.prune_runs(schedule_id, RUNS_KEPT)
            except Exception:
                logger.exception("Schedule %s run %d: could not save its outcome", schedule_id, run)

        asyncio.ensure_future(write())
