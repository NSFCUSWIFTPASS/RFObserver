"""Circular pre-trigger buffer for RAM-based IQ capture.

Maintains a fixed-size circular buffer of recent IQ samples so that
pre-trigger data is available when a trigger fires.

Thread-safe: the receiver thread writes while the recording fire site (not
always the same thread — manual starts come from a web worker) reads, so all
access is serialized on a lock. Read holds the lock for its concatenate
(~100 ms at wide bandwidths); that is rare (capture starts only) and bounded,
and the stream buffering absorbs it.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass
class GridPreRoll:
    """Drained pre-trigger PSD grids plus the metadata to persist them.

    ``grids`` are the retained per-chunk grids in chronological order (each
    ``(rows, num_bins)`` float32); ``rows`` is their combined row count.
    """

    grids: list[np.ndarray[Any, np.dtype[Any]]]
    freq_axis: np.ndarray[Any, np.dtype[Any]]
    time_res: float
    rows: int
    grid_min: float
    grid_max: float


class GridPreBuffer:
    """Rolling buffer of recent PSD-grid chunks for pre-trigger PSD.

    Mirrors ``CircularBuffer`` (the IQ pre-trigger buffer) but for computed
    PSD grids: keeps the most recent grids whose combined time span is at most
    ``max_seconds`` so, when a recording fires, the pre-roll IQ that gets
    prepended to the ``.sc16`` has matching PSD rows for the ``.psd`` companion.

    Thread-safe: written from the dispatch thread as each chunk's grid is
    handled, drained from the recording fire site (receiver thread for
    triggers, a web worker for manual starts), so all access is serialized on
    a lock. Grid widths (``num_bins``) are assumed consistent for the buffer's
    lifetime; the owner recreates the buffer on reconfiguration, so a config
    change with a different bin count can never mix widths here.
    """

    def __init__(self, max_seconds: float) -> None:
        self._max_seconds = max(0.0, float(max_seconds))
        self._grids: deque[tuple[np.ndarray[Any, np.dtype[Any]], float]] = deque()
        self._freq_axis: np.ndarray[Any, np.dtype[Any]] | None = None
        self._rows = 0
        self._span = 0.0
        self._lock = threading.Lock()

    @property
    def rows(self) -> int:
        """Total buffered grid rows (thread-safe snapshot)."""
        with self._lock:
            return self._rows

    def write(
        self,
        grid: np.ndarray[Any, np.dtype[Any]],
        freq_axis: np.ndarray[Any, np.dtype[Any]],
        time_res: float,
    ) -> None:
        """Append one chunk's grid, dropping oldest grids past ``max_seconds``.

        Empty grids and non-positive ``time_res`` are ignored (they carry no
        usable time span). A copy is stored so a later mutation of the source
        array (e.g. buffer-pool reuse) cannot corrupt the pre-roll.
        """
        if grid.size == 0 or grid.shape[0] == 0 or time_res <= 0:
            return
        rows = int(grid.shape[0])
        span = rows * float(time_res)
        stored = np.ascontiguousarray(grid, dtype=np.float32).copy()
        with self._lock:
            self._grids.append((stored, float(time_res)))
            self._freq_axis = np.asarray(freq_axis).copy()
            self._rows += rows
            self._span += span
            # Trim oldest while over budget, but always keep the last grid.
            while self._span > self._max_seconds and len(self._grids) > 1:
                g, tr = self._grids.popleft()
                self._rows -= int(g.shape[0])
                self._span -= int(g.shape[0]) * tr

    def drain(self) -> GridPreRoll | None:
        """Return the buffered pre-roll (chronological) and clear, or None if empty."""
        with self._lock:
            if not self._grids or self._freq_axis is None:
                self._reset_locked()
                return None
            grids = [g for (g, _) in self._grids]
            time_res = self._grids[-1][1]
            freq_axis = self._freq_axis
            rows = self._rows
            gmin = min(float(g.min()) for g in grids)
            gmax = max(float(g.max()) for g in grids)
            self._reset_locked()
            return GridPreRoll(
                grids=grids,
                freq_axis=freq_axis,
                time_res=float(time_res),
                rows=int(rows),
                grid_min=gmin,
                grid_max=gmax,
            )

    def clear(self) -> None:
        """Reset the buffer."""
        with self._lock:
            self._reset_locked()

    def _reset_locked(self) -> None:
        self._grids.clear()
        self._freq_axis = None
        self._rows = 0
        self._span = 0.0


class CircularBuffer:
    """Fixed-size circular buffer for IQ samples.

    Supports any numpy dtype — use ``np.complex64`` for complex samples
    or ``np.int32`` for raw SC16 data (halves memory usage).
    """

    def __init__(self, max_samples: int, dtype: np.dtype | type = np.complex64) -> None:
        self._buffer = np.zeros(max_samples, dtype=dtype)
        self._max_samples = max_samples
        self._write_pos = 0
        self._total_written = 0
        self._lock = threading.Lock()

    @property
    def capacity(self) -> int:
        return self._max_samples

    @property
    def filled(self) -> int:
        return min(self._total_written, self._max_samples)

    def write(self, data: np.ndarray) -> None:
        """Append samples to the circular buffer, overwriting oldest data."""
        n = len(data)
        with self._lock:
            if n >= self._max_samples:
                # Data larger than buffer -- keep only the last max_samples
                self._buffer[:] = data[-self._max_samples :]
                self._write_pos = 0
                self._total_written += n
                return

            end_pos = self._write_pos + n
            if end_pos <= self._max_samples:
                self._buffer[self._write_pos : end_pos] = data
            else:
                first_chunk = self._max_samples - self._write_pos
                self._buffer[self._write_pos :] = data[:first_chunk]
                remaining = n - first_chunk
                self._buffer[:remaining] = data[first_chunk:]

            self._write_pos = end_pos % self._max_samples
            self._total_written += n

    def read(self) -> np.ndarray:
        """Read all available samples in chronological order."""
        with self._lock:
            if self._total_written <= self._max_samples:
                return self._buffer[: self._write_pos].copy()

            # Buffer wrapped -- read from write_pos to end, then start to write_pos
            return np.concatenate(
                [
                    self._buffer[self._write_pos :],
                    self._buffer[: self._write_pos],
                ]
            )

    def clear(self) -> None:
        """Reset the buffer."""
        with self._lock:
            self._buffer[:] = 0
            self._write_pos = 0
            self._total_written = 0
