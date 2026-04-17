"""Circular pre-trigger buffer for RAM-based IQ capture.

Maintains a fixed-size circular buffer of recent IQ samples so that
pre-trigger data is available when a trigger fires.
"""

from __future__ import annotations

import numpy as np


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

    @property
    def capacity(self) -> int:
        return self._max_samples

    @property
    def filled(self) -> int:
        return min(self._total_written, self._max_samples)

    def write(self, data: np.ndarray) -> None:
        """Append samples to the circular buffer, overwriting oldest data."""
        n = len(data)
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
        if self._total_written <= self._max_samples:
            return self._buffer[: self._write_pos].copy()

        # Buffer has wrapped -- read from write_pos to end, then start to write_pos
        return np.concatenate(
            [
                self._buffer[self._write_pos :],
                self._buffer[: self._write_pos],
            ]
        )

    def clear(self) -> None:
        """Reset the buffer."""
        self._buffer[:] = 0
        self._write_pos = 0
        self._total_written = 0
