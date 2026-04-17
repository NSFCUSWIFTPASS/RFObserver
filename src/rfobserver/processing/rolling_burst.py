"""Rolling-window burst detection for streaming pipeline.

Wraps the existing ``detect_bursts()`` function with a sliding window so
that PSD grid rows can be fed incrementally (chunk at a time) while still
producing correct burst fingerprints.

Bursts that touch the trailing edge of the window are held as "pending"
and merged with continuations in the next evaluation pass.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import numpy as np

from rfobserver.processing.burst import BurstDetectionConfig, detect_bursts
from rfobserver.processing.spectral import PSDGridResult

if TYPE_CHECKING:
    from rfobserver.models import BurstFingerprint
    from rfobserver.processing.burst import BurstDetectionResult


class RollingBurstDetector:
    """Sliding-window burst detection over incrementally arriving PSD rows."""

    def __init__(
        self,
        window_rows: int,
        eval_interval_rows: int,
        num_bins: int,
        burst_config: BurstDetectionConfig,
        center_freq_hz: float,
        freq_axis: np.ndarray,
        time_resolution_s: float,
    ) -> None:
        self._window_rows = window_rows
        self._eval_interval = eval_interval_rows
        self._num_bins = num_bins
        self._burst_config = burst_config
        self._center_freq_hz = center_freq_hz
        self._freq_axis = freq_axis.copy()
        self._time_resolution_s = time_resolution_s

        # Rolling window (circular, overwritten when full)
        self._window = np.full((window_rows, num_bins), -200.0, dtype=np.float32)
        self._write_pos = 0  # next row to write in the circular buffer
        self._rows_filled = 0  # total rows written (capped at window_rows)
        self._rows_since_eval = 0
        self._pending_bursts: list[BurstFingerprint] = []
        self._last_detection: BurstDetectionResult | None = None

    @property
    def last_detection(self) -> BurstDetectionResult | None:
        return self._last_detection

    def feed(self, psd_grid: PSDGridResult) -> list[BurstFingerprint]:
        """Append rows from *psd_grid* and return any completed bursts."""
        new_rows = psd_grid.grid
        n_new = new_rows.shape[0]

        # Write rows into the circular window (vectorized, max 2 slices)
        end = self._write_pos + n_new
        if end <= self._window_rows:
            self._window[self._write_pos : end] = new_rows
        else:
            first = self._window_rows - self._write_pos
            self._window[self._write_pos :] = new_rows[:first]
            self._window[: n_new - first] = new_rows[first:]
        self._write_pos = end % self._window_rows
        self._rows_filled = min(self._rows_filled + n_new, self._window_rows)

        self._rows_since_eval += n_new

        ready = (
            self._rows_since_eval >= self._eval_interval
            and self._rows_filled >= self._eval_interval
        )
        if ready:
            self._rows_since_eval = 0
            return self._evaluate()

        return []

    def _evaluate(self) -> list[BurstFingerprint]:
        """Run burst detection on the current window contents."""
        # Reconstruct ordered window from circular buffer
        if self._rows_filled < self._window_rows:
            grid = self._window[: self._rows_filled].copy()
        else:
            # Circular: [write_pos:] + [:write_pos] gives chronological order
            grid = np.concatenate(
                [
                    self._window[self._write_pos :],
                    self._window[: self._write_pos],
                ]
            )

        n_rows = grid.shape[0]
        time_axis = np.arange(n_rows) * self._time_resolution_s

        psd_grid = PSDGridResult(
            grid=grid,
            time_axis=time_axis,
            freq_axis=self._freq_axis,
            ffts_per_slice=1,
            total_ffts=n_rows,
        )

        result = detect_bursts(
            psd_grid,
            config=self._burst_config,
            center_freq_hz=self._center_freq_hz,
            capture_time=datetime.now(timezone.utc),
        )
        self._last_detection = result

        # Separate completed bursts from those touching the trailing edge
        margin_rows = 3  # bursts within this many rows of the end are pending
        if n_rows > margin_rows:
            trailing_time = time_axis[-1] - margin_rows * self._time_resolution_s
        else:
            trailing_time = 0.0

        completed: list[BurstFingerprint] = []
        new_pending: list[BurstFingerprint] = []

        for burst in result.bursts:
            burst_end_offset = (burst.stop_time - burst.detection_timestamp).total_seconds()
            if burst_end_offset >= trailing_time:
                new_pending.append(burst)
            else:
                completed.append(burst)

        # De-duplicate: remove completed bursts that overlap with previous pending
        completed = self._deduplicate(completed, self._pending_bursts)
        self._pending_bursts = new_pending

        return completed

    @staticmethod
    def _deduplicate(
        new_bursts: list[BurstFingerprint],
        prev_pending: list[BurstFingerprint],
    ) -> list[BurstFingerprint]:
        """Remove bursts from *new_bursts* that significantly overlap with *prev_pending*."""
        if not prev_pending:
            return new_bursts

        unique: list[BurstFingerprint] = []
        for nb in new_bursts:
            is_dup = False
            for pb in prev_pending:
                # Check time and frequency overlap
                time_overlap = nb.start_time <= pb.stop_time and nb.stop_time >= pb.start_time
                freq_diff = abs(nb.center_freq_hz - pb.center_freq_hz)
                bw_sum = (nb.bandwidth_hz + pb.bandwidth_hz) / 2
                freq_close = freq_diff < bw_sum if bw_sum > 0 else freq_diff < 1e3
                if time_overlap and freq_close:
                    is_dup = True
                    break
            if not is_dup:
                unique.append(nb)
        return unique

    def reset(self) -> None:
        """Clear window and pending state. Called on frequency retune."""
        self._window[:] = -200.0
        self._write_pos = 0
        self._rows_filled = 0
        self._rows_since_eval = 0
        self._pending_bursts.clear()
        self._last_detection = None
