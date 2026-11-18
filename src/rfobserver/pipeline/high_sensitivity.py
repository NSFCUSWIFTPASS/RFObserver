"""High-sensitivity mode: a run of at most HS_MAX_SEC during which the burst
threshold is lowered, the tone detector runs and the isolation budget rises.

``HighSensitivity`` owns the run and is the overlay the pipeline reads
(``burst_threshold_db``, ``isolation_max_per_sec``); nothing is written to the
settings, so a restart always comes back in routine mode. Listeners hear
``"start"`` and ``"end"`` on whichever thread caused them. See
docs/superpowers/specs/2026-11-18-high-sensitivity-mode-design.md.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from rfobserver.schedules.model import HS_MAX_MS, HS_MAX_THRESHOLD_DB, HS_MIN_THRESHOLD_DB

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

HS_MAX_SEC = HS_MAX_MS / 1000
MIN_THRESHOLD_DB = HS_MIN_THRESHOLD_DB
MAX_THRESHOLD_DB = HS_MAX_THRESHOLD_DB


class HSError(ValueError):
    """A refused start or stop; ``status`` is the HTTP status it maps to."""

    def __init__(self, message: str, status: int) -> None:
        super().__init__(message)
        self.status = status


@dataclass
class HSRun:
    id: str
    source: str
    start: float
    end: float
    threshold_db: float
    ended: float | None = None
    end_reason: str | None = None


class HighSensitivity:
    def __init__(self, settings: Any, clock: Callable[[], float] = time.time) -> None:
        self._s = settings
        self._clock = clock
        self._lock = threading.Lock()
        self._run: HSRun | None = None
        self.listeners: list[Callable[[str, HSRun], None]] = []

    def start(
        self,
        seconds: float,
        *,
        run_id: str | None = None,
        threshold_db: float | None = None,
        source: str = "api",
    ) -> HSRun:
        """Start a run of ``seconds`` (1 to 300), or extend the running one when
        ``run_id`` is its id (never past its start + 300 s)."""
        try:
            seconds = float(seconds)
        except (TypeError, ValueError):
            raise HSError("seconds must be a number", 400) from None
        if not 1.0 <= seconds <= HS_MAX_SEC:
            raise HSError(f"seconds must be 1 to {HS_MAX_SEC:.0f}", 400)
        thr = float(self._s.HS_BURST_THRESHOLD_DB if threshold_db is None else threshold_db)
        if not MIN_THRESHOLD_DB <= thr <= MAX_THRESHOLD_DB:
            raise HSError(
                f"threshold_db must be {MIN_THRESHOLD_DB:.0f} to {MAX_THRESHOLD_DB:.0f}", 400
            )
        self._expire()
        now = self._clock()
        with self._lock:
            cur = self._run
            if cur is not None:
                if run_id != cur.id:
                    raise HSError(f"high-sensitivity run {cur.id} is on", 409)
                cur.end = min(cur.start + HS_MAX_SEC, max(cur.end, now + seconds))
                return cur
            run = HSRun(
                id=run_id or uuid.uuid4().hex[:12],
                source=source,
                start=now,
                end=now + seconds,
                threshold_db=thr,
            )
            self._run = run
        self._tell("start", run)
        return run

    def stop(self, run_id: str, reason: str = "deleted") -> HSRun:
        self._expire()
        with self._lock:
            cur = self._run
            if cur is None or cur.id != run_id:
                raise HSError(f"high-sensitivity run {run_id} is not on", 404)
        ended = self.end_now(reason)
        assert ended is not None
        return ended

    def end_now(self, reason: str) -> HSRun | None:
        """End whatever run is on (a retune, a stop); None when none is."""
        with self._lock:
            run = self._run
            if run is None:
                return None
            self._run = None
            run.ended = min(self._clock(), run.end)
            run.end_reason = reason
        self._tell("end", run)
        return run

    def current(self) -> HSRun | None:
        self._expire()
        with self._lock:
            return self._run

    def now(self) -> float:
        return self._clock()

    def burst_threshold_db(self) -> float:
        run = self.current()
        return run.threshold_db if run is not None else float(self._s.BURST_THRESHOLD_HIGH_DB)

    def isolation_max_per_sec(self) -> int:
        on = self.current() is not None
        return int(self._s.HS_ISOLATION_MAX_PER_SEC if on else self._s.ISOLATION_MAX_PER_SEC)

    def _expire(self) -> None:
        with self._lock:
            run = self._run
            if run is None or self._clock() < run.end:
                return
        self.end_now("expired")

    def _tell(self, event: str, run: HSRun) -> None:
        for listener in list(self.listeners):
            try:
                listener(event, run)
            except Exception:
                logger.exception("High-sensitivity %s listener failed", event)
