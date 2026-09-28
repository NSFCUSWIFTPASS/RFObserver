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


def trim_grid_rows(
    grid: np.ndarray[Any, np.dtype[Any]],
    chunk_start: int,
    slice_samples: int,
    start_sample: int,
    end_sample: int,
) -> tuple[np.ndarray[Any, np.dtype[Any]], int]:
    """Return the rows of ``grid`` lying wholly inside ``[start_sample, end_sample)``.

    Row ``k`` of a chunk beginning at ``chunk_start`` covers stream samples
    ``[chunk_start + k*slice_samples, chunk_start + (k+1)*slice_samples)``. A
    row straddling either boundary is dropped rather than misplaced, so the
    returned position is always a true row boundary. The second element is the
    absolute sample position of the first kept row, or -1 when no row
    qualifies.
    """
    rows = int(grid.shape[0]) if grid.ndim == 2 else 0
    if rows == 0 or slice_samples <= 0 or end_sample <= start_sample:
        return grid[:0], -1
    first = 0
    if start_sample > chunk_start:
        # Ceiling division: a row straddling the start is dropped.
        first = (start_sample - chunk_start + slice_samples - 1) // slice_samples
    # Last row whose END is still inside the recording.
    last = (end_sample - chunk_start) // slice_samples
    last = min(last, rows)
    if first >= last:
        return grid[:0], -1
    return grid[first:last], chunk_start + first * slice_samples


@dataclass
class GridPreRoll:
    """Drained pre-trigger PSD grids plus the metadata to persist them.

    ``grids`` are the retained per-chunk grids in chronological order (each
    ``(rows, num_bins)`` float32); ``rows`` is their combined row count.
    ``start_sample`` is the absolute stream position of the first row.
    """

    grids: list[np.ndarray[Any, np.dtype[Any]]]
    freq_axis: np.ndarray[Any, np.dtype[Any]]
    time_res: float
    rows: int
    grid_min: float
    grid_max: float
    start_sample: int


class GridPreBuffer:
    """Rolling buffer of recent PSD-grid chunks, tagged by stream position.

    Mirrors ``CircularBuffer`` (the IQ pre-trigger buffer) but for computed
    PSD grids: keeps the most recent grids whose combined time span is at most
    ``max_seconds``, each tagged with the absolute stream sample position it
    was computed from, so a recording can take the rows that actually cover
    its IQ via ``drain(from_sample=...)``.

    Position tagging is load-bearing, not bookkeeping. Grids reach this buffer
    only after passing through the chunk queue and the worker pool, so they
    lag the IQ by the pipeline latency. Selecting "the most recent
    ``TRIGGER_PRE_SEC`` of grids" therefore yields rows from an earlier
    interval than the IQ pre-roll whenever that latency exceeds
    ``TRIGGER_PRE_SEC``. Selecting by position instead yields nothing in that
    case, which is correct: the rows covering the pre-roll simply have not
    been computed yet, and arrive shortly after as live grids. See
    docs/debugging/2026-09-22_trigger-psd-iq-misalignment.md.

    Thread-safe: written from the dispatch thread as each chunk's grid is
    handled, drained from the recording fire site (receiver thread for
    triggers, a web worker for manual starts), so all access is serialized on
    a lock. Grid widths (``num_bins``) are assumed consistent for the buffer's
    lifetime; the owner recreates the buffer on reconfiguration, so a config
    change with a different bin count can never mix widths here.
    """

    def __init__(self, max_seconds: float) -> None:
        self._max_seconds = max(0.0, float(max_seconds))
        # (grid, time_res, chunk_start, slice_samples)
        self._grids: deque[tuple[np.ndarray[Any, np.dtype[Any]], float, int, int]] = deque()
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
        chunk_start: int,
        slice_samples: int,
    ) -> None:
        """Append one chunk's grid, dropping oldest grids past ``max_seconds``.

        ``chunk_start`` is the position of the chunk's first sample in the
        receiver's sample stream and ``slice_samples`` the samples per grid
        row, so a later ``drain(from_sample=...)`` can select rows by position
        rather than by arrival order.

        Empty grids, non-positive ``time_res`` and non-positive
        ``slice_samples`` are ignored (they carry no usable span or position).
        A copy is stored so a later mutation of the source array (e.g.
        buffer-pool reuse) cannot corrupt the pre-roll.
        """
        if grid.size == 0 or grid.shape[0] == 0 or time_res <= 0 or slice_samples <= 0:
            return
        rows = int(grid.shape[0])
        span = rows * float(time_res)
        stored = np.ascontiguousarray(grid, dtype=np.float32).copy()
        with self._lock:
            self._grids.append((stored, float(time_res), int(chunk_start), int(slice_samples)))
            self._freq_axis = np.asarray(freq_axis).copy()
            self._rows += rows
            self._span += span
            # Trim oldest while over budget, but always keep the last grid.
            while self._span > self._max_seconds and len(self._grids) > 1:
                g, tr, _cs, _ss = self._grids.popleft()
                self._rows -= int(g.shape[0])
                self._span -= int(g.shape[0]) * tr

    def drain(self, from_sample: int | None = None) -> GridPreRoll | None:
        """Return the buffered pre-roll (chronological) and clear.

        With ``from_sample`` set, only rows whose first sample is at or after
        that stream position are returned; a row straddling the boundary is
        dropped rather than misplaced, so ``start_sample`` is always a true row
        boundary. Returns None when nothing qualifies, which is the normal case
        whenever the PSD pipeline latency exceeds the pre-roll window: every
        buffered grid predates the recording.
        """
        with self._lock:
            if not self._grids or self._freq_axis is None:
                self._reset_locked()
                return None
            kept: list[np.ndarray[Any, np.dtype[Any]]] = []
            start_sample = -1
            for g, _tr, chunk_start, slice_samples in self._grids:
                rows = int(g.shape[0])
                first = 0
                if from_sample is not None and from_sample > chunk_start:
                    first = (from_sample - chunk_start + slice_samples - 1) // slice_samples
                if first >= rows:
                    continue
                if start_sample < 0:
                    start_sample = chunk_start + first * slice_samples
                kept.append(g[first:] if first else g)
            time_res = self._grids[-1][1]
            freq_axis = self._freq_axis
            self._reset_locked()
            if not kept:
                return None
            return GridPreRoll(
                grids=kept,
                freq_axis=freq_axis,
                time_res=float(time_res),
                rows=int(sum(int(g.shape[0]) for g in kept)),
                grid_min=min(float(g.min()) for g in kept),
                grid_max=max(float(g.max()) for g in kept),
                start_sample=int(start_sample),
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

    @property
    def total_written(self) -> int:
        """Samples written since creation: the stream position of the next sample.

        Only the writer (the receiver thread) changes it, so the writer can read
        it without the lock.
        """
        return self._total_written

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
        return self.read_with_position()[0]

    def read_with_position(self) -> tuple[np.ndarray, int]:
        """``read()`` plus ``total_written`` at that instant (one lock hold).

        The returned samples are stream positions
        ``[total_written - len(data), total_written)``.
        """
        with self._lock:
            # Strictly less: at exactly one capacity written, _write_pos has
            # wrapped to 0 and the whole buffer is valid.
            if self._total_written < self._max_samples:
                return self._buffer[: self._write_pos].copy(), self._total_written
            # Wrapped: read from write_pos to the end, then start to write_pos.
            data = np.concatenate(
                [self._buffer[self._write_pos :], self._buffer[: self._write_pos]]
            )
            return data, self._total_written

    def clear(self) -> None:
        """Reset the buffer."""
        with self._lock:
            self._buffer[:] = 0
            self._write_pos = 0
            self._total_written = 0
