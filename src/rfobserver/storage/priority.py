"""Capture priority: which triggered captures are kept longest.

A triggered capture is priority when it is long (PRIORITY_MIN_SEC) or strong
(mean power PRIORITY_EXCESS_DB above the background). On the HCRO field sensor
96% of captures are 0.1-0.3 s bursts from a nearby meter, while an RFI event
shows up as captures of 2-30 s, averaging 25-30 dB over the background; plain
FIFO eviction lost such an event within a day. Captures recorded within
PRIORITY_SPAN_SEC of a priority capture are promoted with it, since an event
is often cut into many short captures between the long ones.

The mark is a zero-byte ``<base>.priority`` file next to the capture, so the
startup scan finds it without reading anything and eviction deletes it with
the other companions.
"""

from __future__ import annotations

import math
import statistics
import threading
from collections import deque

PRIORITY_SUFFIX = ".priority"

# Window averages kept for the background: 10 minutes at the default 1 s
# DURATION_SEC. A median, so an event filling part of it does not lift it.
_BACKGROUND_WINDOWS = 600
# Fewer windows than this (just after start) and there is no background yet.
_BACKGROUND_MIN_WINDOWS = 30


def classify(
    *,
    duration_sec: float,
    mean_power_db: float | None,
    background_db: float | None,
    min_sec: float,
    excess_db: float,
) -> str | None:
    """Why a capture is priority ("duration" or "power"), or None if it is not.

    A rule set to 0 (or less) is off. The power rule needs both the capture's
    mean power and a background to compare it with.
    """
    if min_sec > 0 and duration_sec >= min_sec:
        return "duration"
    if (
        excess_db > 0
        and mean_power_db is not None
        and background_db is not None
        and mean_power_db - background_db >= excess_db
    ):
        return "power"
    return None


class BackgroundPower:
    """Median of the recent averaged-window powers (dB).

    Fed from the event loop once per averaged window, read by the
    recording-control thread at finalize; a lock keeps the two apart.
    """

    def __init__(self, maxlen: int = _BACKGROUND_WINDOWS) -> None:
        self._values: deque[float] = deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def add(self, power_db: float) -> None:
        if math.isfinite(power_db):
            with self._lock:
                self._values.append(power_db)

    def median(self) -> float | None:
        with self._lock:
            if len(self._values) < _BACKGROUND_MIN_WINDOWS:
                return None
            values = list(self._values)
        return float(statistics.median(values))


class MeanPower:
    """Mean of linear power over a capture's chunks, reported in dB."""

    def __init__(self) -> None:
        self._sum = 0.0
        self._n = 0

    def reset(self) -> None:
        self._sum = 0.0
        self._n = 0

    def add_db(self, power_db: float) -> None:
        if math.isfinite(power_db):
            self._sum += 10.0 ** (power_db / 10.0)
            self._n += 1

    def db(self) -> float | None:
        if self._n == 0 or self._sum <= 0:
            return None
        return 10.0 * math.log10(self._sum / self._n)
