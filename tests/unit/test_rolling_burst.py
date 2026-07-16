"""Unit tests for RollingBurstDetector emit-once / full-duration behavior.

These pin down two failure modes that the sliding-window wrapper had:

1. A burst that first appears touching the window's trailing edge (held as
   "pending") and then, a later evaluation, sits fully inside the window must be
   emitted once at its FULL duration -- not dropped by the pending/completed
   de-duplication and later re-emitted as a truncated fragment as it scrolls
   out of the window.
2. A burst that stays inside the window across several evaluations must be
   emitted exactly ONCE, not re-emitted every evaluation.
"""

from __future__ import annotations

import numpy as np

from rfobserver.processing.burst import BurstDetectionConfig
from rfobserver.processing.rolling_burst import RollingBurstDetector
from rfobserver.processing.spectral import PSDGridResult


def _detector(window: int, eval_iv: int, num_bins: int, tres_s: float) -> RollingBurstDetector:
    freq = np.fft.fftshift(np.fft.fftfreq(num_bins, 1.0 / 1_000_000))
    return RollingBurstDetector(
        window_rows=window,
        eval_interval_rows=eval_iv,
        num_bins=num_bins,
        burst_config=BurstDetectionConfig(threshold_high_db=20.0),
        center_freq_hz=915_000_000,
        freq_axis=freq,
        time_resolution_s=tres_s,
    )


def _feed_grid(det: RollingBurstDetector, grid: np.ndarray, chunk: int, tres_s: float) -> list:
    freq = det._freq_axis
    emitted = []
    for i in range(0, grid.shape[0], chunk):
        sub = grid[i : i + chunk]
        pg = PSDGridResult(
            grid=sub,
            time_axis=np.arange(sub.shape[0]) * tres_s,
            freq_axis=freq,
            ffts_per_slice=1,
            total_ffts=sub.shape[0],
        )
        emitted.extend(det.feed(pg))
    return emitted


def test_burst_pending_then_complete_emitted_once_full_duration() -> None:
    """A burst caught first at the trailing edge must still be emitted whole.

    Geometry (window=100, eval=50, chunk=50, burst 40 rows at abs rows 78-118)
    reproduces the field case: eval@100 sees the burst at the trailing edge
    (pending), eval@150 sees it fully interior (the real 40-row burst). It must
    be emitted exactly once at ~40 rows, never as a truncated fragment.
    """
    tres = 0.001
    num_bins = 64
    grid = np.full((250, num_bins), -120.0, dtype=np.float32)
    grid[78:118, 25:40] = 0.0  # 40-row burst, ~15 bins wide

    det = _detector(window=100, eval_iv=50, num_bins=num_bins, tres_s=tres)
    emitted = _feed_grid(det, grid, chunk=50, tres_s=tres)

    durations_ms = sorted(b.duration_ms for b in emitted)
    assert len(emitted) == 1, f"expected exactly one emission, got {durations_ms}"
    # 40 rows * 1 ms = 40 ms; allow a couple rows of slack for edge handling.
    assert abs(emitted[0].duration_ms - 40.0) <= 2.0, (
        f"burst truncated: got {emitted[0].duration_ms:.1f} ms, expected ~40 ms"
    )


def test_interior_burst_not_re_emitted_each_eval() -> None:
    """A burst sitting inside the window for many evals is emitted only once."""
    tres = 0.001
    num_bins = 64
    grid = np.full((400, num_bins), -120.0, dtype=np.float32)
    grid[120:160, 25:40] = 0.0  # 40-row burst

    det = _detector(window=300, eval_iv=20, num_bins=num_bins, tres_s=tres)
    emitted = _feed_grid(det, grid, chunk=10, tres_s=tres)

    assert len(emitted) == 1, (
        f"burst should be emitted once, got {len(emitted)}: "
        f"{sorted(round(b.duration_ms, 1) for b in emitted)}"
    )
    assert abs(emitted[0].duration_ms - 40.0) <= 2.0
