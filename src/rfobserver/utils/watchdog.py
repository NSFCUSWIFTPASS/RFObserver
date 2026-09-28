"""Thread-based pipeline liveness watchdog.

Runs on its own daemon thread, so it survives both event-loop starvation and
the pipeline task dying - the two failure modes that make an asyncio-based
watchdog useless. Reads a ProgressBeacon; on a confirmed stall it asks the
supervisor to restart the pipeline on the main loop, and if the loop does not
complete that within a deadline (wedged), it exits the process so systemd's
Restart=on-failure brings it back fresh.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from rfobserver.pipeline.beacon import ProgressBeacon

logger = logging.getLogger(__name__)


class PipelineWatchdog:
    """Monitors a ProgressBeacon from a daemon thread and restarts on stall."""

    def __init__(
        self,
        beacon: ProgressBeacon,
        is_active: Callable[[], bool],
        restart: Callable[[], Coroutine[Any, Any, Any]],
        loop: asyncio.AbstractEventLoop,
        *,
        timeout_sec: float,
        restart_deadline_sec: float,
        check_interval_sec: float = 5.0,
        exit_fn: Callable[[int], object] = os._exit,
        exit_code: int = 90,
    ) -> None:
        self._beacon = beacon
        self._is_active = is_active
        self._restart = restart
        self._loop = loop
        self._timeout = timeout_sec
        self._restart_deadline = restart_deadline_sec
        self._interval = check_interval_sec
        self._exit_fn = exit_fn
        self._exit_code = exit_code
        self._stop = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="watchdog", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop = True

    def _run(self) -> None:
        while not self._stop:
            time.sleep(self._interval)
            if not self._stop:
                self._tick()

    def _tick(self) -> None:
        if not self._is_active():
            return
        age = self._beacon.age()
        if age <= self._timeout:
            return
        logger.error("Watchdog: pipeline stalled (%.1fs since last progress); restarting", age)
        if self._attempt_restart():
            self._beacon.mark()
            logger.error("Watchdog: pipeline restarted")
            return
        logger.error(
            "Watchdog: restart did not complete in %.1fs; exiting for systemd",
            self._restart_deadline,
        )
        self._exit_fn(self._exit_code)

    def _attempt_restart(self) -> bool:
        try:
            fut = asyncio.run_coroutine_threadsafe(self._restart(), self._loop)
            fut.result(timeout=self._restart_deadline)
            return True
        except Exception:
            logger.exception("Watchdog: in-process restart failed")
            return False
