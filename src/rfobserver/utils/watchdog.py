"""Watchdog for pipeline liveness monitoring."""

from __future__ import annotations

import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class Watchdog:
    """Monitors that pipeline components are active."""

    def __init__(self, timeout_sec: float = 30.0) -> None:
        self._timeout = timeout_sec
        self._sources: dict[str, float] = {}

    def pet(self, source: str) -> None:
        """Signal that a component is alive."""
        self._sources[source] = time.monotonic()

    def check(self) -> list[str]:
        """Return list of sources that have timed out."""
        now = time.monotonic()
        timed_out = []
        for source, last_pet in self._sources.items():
            if now - last_pet > self._timeout:
                timed_out.append(source)
        return timed_out

    async def monitor(self) -> None:
        """Periodically check for timed-out sources."""
        while True:
            await asyncio.sleep(self._timeout / 2)
            timed_out = self.check()
            if timed_out:
                logger.warning("Watchdog: timed out sources: %s", timed_out)
