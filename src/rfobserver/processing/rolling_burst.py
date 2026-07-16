"""Rolling-window burst detection for streaming pipeline.

Wraps the existing ``detect_bursts()`` function with a sliding window so
that PSD grid rows can be fed incrementally (chunk at a time) while still
producing correct burst fingerprints.

Each evaluation re-detects bursts in the current window. Because a burst
persists across many evaluations while it sits in the window, and because a
burst that is still arriving touches the window's trailing (newest) edge, the
detector *tracks* each burst across evaluations by frequency and absolute-time
continuity, accumulating its full extent, and emits it EXACTLY ONCE -- when it
has stopped growing (no longer touches the trailing edge) or has scrolled out
of the window. This is what lets a burst be reported at its full duration even
when it first appears at the trailing edge, or is longer than a single
chunk/evaluation, without being dropped or re-emitted every pass.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import numpy as np

from rfobserver.models import BurstFingerprint
from rfobserver.processing.burst import BurstDetectionConfig, detect_bursts
from rfobserver.processing.spectral import PSDGridResult

if TYPE_CHECKING:
    from rfobserver.processing.burst import BurstDetectionResult


@dataclass
class _TrackedBurst:
    """A burst being tracked across evaluations (absolute row coordinates)."""

    abs_start: int
    abs_end: int
    f_lo_hz: float
    f_hi_hz: float
    center_freq_hz: float
    peak_power_db: float
    last_eval: int
    still_growing: bool
    emitted: bool = False


class RollingBurstDetector:
    """Sliding-window burst detection over incrementally arriving PSD rows."""

    def __init__(
        self,
        window_rows: int,
        eval_interval_rows: int,
        num_bins: int,
        burst_config: BurstDetectionConfig,
        center_freq_hz: float,
        freq_axis: np.ndarray[Any, np.dtype[Any]],
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
        self._total_rows_written = 0  # absolute row counter (for cross-eval identity)
        self._eval_count = 0
        self._tracked: list[_TrackedBurst] = []
        self._last_detection: BurstDetectionResult | None = None

        bin_hz = abs(float(freq_axis[1] - freq_axis[0])) if len(freq_axis) > 1 else 0.0
        # Frequency slop when matching a re-detected burst to a tracked one.
        self._match_freq_tol_hz = max(bin_hz * 2.0, 1_000.0)

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
        self._total_rows_written += n_new

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

        self._eval_count += 1
        window_base_abs = self._total_rows_written - n_rows
        # A burst whose newest row is within margin_rows of the window's trailing
        # (newest) edge may still be arriving, so it is not yet "done".
        margin_rows = 3

        for burst in result.bursts:
            t_start = (burst.start_time - burst.detection_timestamp).total_seconds()
            t_end = (burst.stop_time - burst.detection_timestamp).total_seconds()
            start_row = int(round(t_start / self._time_resolution_s))
            end_row = int(round(t_end / self._time_resolution_s))
            abs_start = window_base_abs + start_row
            abs_end = window_base_abs + end_row
            still_growing = end_row >= n_rows - margin_rows
            f_lo = burst.center_freq_hz - burst.bandwidth_hz / 2
            f_hi = burst.center_freq_hz + burst.bandwidth_hz / 2
            self._absorb(burst, abs_start, abs_end, f_lo, f_hi, still_growing)

        return self._collect_finished(window_base_abs)

    def _absorb(
        self,
        burst: BurstFingerprint,
        abs_start: int,
        abs_end: int,
        f_lo: float,
        f_hi: float,
        still_growing: bool,
    ) -> None:
        """Match *burst* to a tracked burst (freq + time continuity) or start a new one.

        Matching keeps a burst's identity stable across evaluations so its full
        extent accumulates and it is emitted once.
        """
        for t in self._tracked:
            freq_close = abs(burst.center_freq_hz - t.center_freq_hz) <= (
                self._match_freq_tol_hz + (f_hi - f_lo + t.f_hi_hz - t.f_lo_hz) / 2
            )
            # Absolute-time overlap (allow a few rows of gap for edge jitter).
            time_close = abs_start <= t.abs_end + 3 and abs_end >= t.abs_start - 3
            if freq_close and time_close:
                t.abs_start = min(t.abs_start, abs_start)
                t.abs_end = max(t.abs_end, abs_end)
                t.f_lo_hz = min(t.f_lo_hz, f_lo)
                t.f_hi_hz = max(t.f_hi_hz, f_hi)
                t.center_freq_hz = (t.f_lo_hz + t.f_hi_hz) / 2
                t.peak_power_db = max(t.peak_power_db, burst.peak_power_db)
                t.last_eval = self._eval_count
                t.still_growing = still_growing
                return

        self._tracked.append(
            _TrackedBurst(
                abs_start=abs_start,
                abs_end=abs_end,
                f_lo_hz=f_lo,
                f_hi_hz=f_hi,
                center_freq_hz=burst.center_freq_hz,
                peak_power_db=burst.peak_power_db,
                last_eval=self._eval_count,
                still_growing=still_growing,
            )
        )

    def _collect_finished(self, window_base_abs: int) -> list[BurstFingerprint]:
        """Emit each tracked burst once it stops growing or scrolls out.

        Emitted bursts are kept (marked) so that continued re-detections in
        later evaluations match them instead of spawning a fresh track and
        re-emitting. They are dropped only once they scroll out of the window.
        """
        finished: list[BurstFingerprint] = []
        keep: list[_TrackedBurst] = []
        for t in self._tracked:
            scrolled_out = t.abs_end < window_base_abs
            if not t.emitted:
                seen_this_eval = t.last_eval == self._eval_count
                # While a burst is in the window detect_bursts re-finds it every
                # eval; it is "done" once its trailing edge is no longer at the
                # window's newest rows (stopped growing), or it has scrolled out.
                if (seen_this_eval and not t.still_growing) or scrolled_out:
                    finished.append(self._to_fingerprint(t))
                    t.emitted = True
            if not scrolled_out:
                keep.append(t)
        self._tracked = keep
        return finished

    def _to_fingerprint(self, t: _TrackedBurst) -> BurstFingerprint:
        # abs_end is derived from detect_bursts' exclusive t_end (time_axis of
        # end_row + 1), so the span is abs_end - abs_start (no +1) -- matching
        # the duration detect_bursts itself reports, with no one-row over-count.
        n_rows = t.abs_end - t.abs_start
        duration_sec = n_rows * self._time_resolution_s
        now = datetime.now(timezone.utc)
        return BurstFingerprint(
            start_time=now - timedelta(seconds=duration_sec),
            stop_time=now,
            center_freq_hz=t.center_freq_hz,
            bandwidth_hz=max(t.f_hi_hz - t.f_lo_hz, 0.0),
            peak_power_db=t.peak_power_db,
            duration_ms=duration_sec * 1000.0,
            detection_timestamp=now,
        )

    def reset(self) -> None:
        """Clear window and pending state. Called on frequency retune."""
        self._window[:] = -200.0
        self._write_pos = 0
        self._rows_filled = 0
        self._rows_since_eval = 0
        self._total_rows_written = 0
        self._eval_count = 0
        self._tracked.clear()
        self._last_detection = None
