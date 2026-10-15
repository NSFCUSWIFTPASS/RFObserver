"""Capture time of each chunk on the host's (NTP) clock.

The USRP stamps every packet with its device time, which is exact from
sample to sample but only as good as where it started: the receiver sets it
from the host clock once at startup (or, with an external reference, leaves
it wherever the device counts from), and the radio's crystal then drifts
from NTP by a few ppm, about 0.1-0.2 s a day. Different sensors would
disagree by that much.

So the device time is kept only for its spacing, and its offset to the host
clock is measured on every chunk: when recv_chunk returns, the chunk's last
sample has already happened, so ``host_now - device_end`` is the true offset
plus the transport and scheduling delay, which is never negative. The
smallest value over the last WINDOW_SEC is the best estimate (the same
minimum filter NTP uses on round trips). A device time that jumps forward
just gives a new minimum. One that jumps back (the receiver was reset to
host time) shows as every chunk arriving JUMP_SEC late; a stalled host looks
the same for a moment, so the estimate starts over only once that has lasted
JUMP_HOLD_SEC.

Without device time (the mock and replay receivers), a chunk's start is the
host time at return minus its duration.
"""

from __future__ import annotations

import collections

WINDOW_SEC = 60.0
JUMP_SEC = 1.0
JUMP_HOLD_SEC = 5.0


class CaptureClock:
    def __init__(self) -> None:
        # (host time, offset) of recent chunks; offsets increase front to back.
        self._mins: collections.deque[tuple[float, float]] = collections.deque()
        self._late_since: float | None = None

    @property
    def offset(self) -> float | None:
        """Host time minus device time, or None before the first chunk."""
        return self._mins[0][1] if self._mins else None

    def reset(self) -> None:
        self._mins.clear()
        self._late_since = None

    def chunk_start(
        self, device_start: float | None, samples: int, rate: float, host_now: float
    ) -> float:
        """Host-clock time (epoch seconds) of a chunk's first sample.

        device_start: the device time of that sample (None if unknown);
        host_now: time.time() when the chunk finished arriving.
        """
        duration = samples / rate if rate > 0 else 0.0
        if device_start is None:
            return host_now - duration
        sample = host_now - (device_start + duration)
        best = self.offset
        if best is not None and sample - best > JUMP_SEC:
            if self._late_since is None:
                self._late_since = host_now
            elif host_now - self._late_since >= JUMP_HOLD_SEC:
                self.reset()
        else:
            self._late_since = None
        while self._mins and self._mins[-1][1] >= sample:
            self._mins.pop()
        self._mins.append((host_now, sample))
        while self._mins[0][0] < host_now - WINDOW_SEC:
            self._mins.popleft()
        return device_start + self._mins[0][1]
