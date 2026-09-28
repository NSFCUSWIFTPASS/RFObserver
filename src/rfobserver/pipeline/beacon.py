"""A thread-safe monotonic liveness heartbeat.

The pipeline calls mark() on every unit of forward progress (each processed
result). A watchdog on another thread reads age() to detect a stall. Kept
trivially small and lock-guarded so it is safe to touch from the asyncio loop
and a daemon thread at once.
"""

from __future__ import annotations

import threading
import time


class ProgressBeacon:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last = time.monotonic()

    def mark(self) -> None:
        with self._lock:
            self._last = time.monotonic()

    def reset(self) -> None:
        self.mark()

    def age(self) -> float:
        with self._lock:
            return time.monotonic() - self._last
