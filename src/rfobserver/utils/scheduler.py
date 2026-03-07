"""Scheduling utilities for periodic operations."""

from __future__ import annotations

import time


def calculate_wait_time(interval_sec: int) -> float:
    """Calculate seconds until the next aligned interval boundary."""
    now = time.time()
    next_boundary = (int(now / interval_sec) + 1) * interval_sec
    return max(0.0, next_boundary - now)
