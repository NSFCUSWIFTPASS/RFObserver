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


def test_emitted_times_anchored_to_absolute_row_not_emission_time() -> None:
    """start_time/stop_time must lag emission by the abs-row gap, not be ~now.

    Geometry (window=100, eval=50, chunk=50, burst 40 rows at abs rows
    78-118) reproduces the same "pending then complete" case used above: the
    burst stops growing and is emitted on the THIRD chunk (abs rows 100-150),
    well after its true abs_end (~118). At that point total_rows_written is
    150, so the emitted stop_time should trail detection_timestamp by
    roughly (150 - abs_end) * tres =~ 32 ms -- not ~0 ms, which is what the
    old "now - duration" / "now" stamping produced.
    """
    tres = 0.001
    num_bins = 64
    grid = np.full((250, num_bins), -120.0, dtype=np.float32)
    grid[78:118, 25:40] = 0.0  # 40-row burst, abs rows 78-118

    det = _detector(window=100, eval_iv=50, num_bins=num_bins, tres_s=tres)
    emitted = _feed_grid(det, grid, chunk=50, tres_s=tres)

    assert len(emitted) == 1
    b = emitted[0]

    lag_stop_ms = (b.detection_timestamp - b.stop_time).total_seconds() * 1000.0
    lag_start_ms = (b.detection_timestamp - b.start_time).total_seconds() * 1000.0

    # Old code stamped stop_time == detection_timestamp (lag ~= 0) and
    # start_time == detection_timestamp - duration. The new code must lag
    # meaningfully behind emission because the burst finished growing well
    # before the eval that emitted it (abs_end ~118, emitted with
    # total_rows_written = 150).
    assert lag_stop_ms > 15.0, (
        f"stop_time should lag detection_timestamp by ~32ms (abs-row gap), "
        f"got {lag_stop_ms:.1f} ms -- looks like emission-time stamping"
    )
    assert lag_stop_ms < 50.0, f"stop_time lag implausibly large: {lag_stop_ms:.1f} ms"
    assert lag_start_ms > lag_stop_ms, "start_time must lag more than stop_time"

    # start/stop lag difference must equal the reported duration (consistency).
    assert abs((lag_start_ms - lag_stop_ms) - b.duration_ms) < 3.0


def test_emitted_burst_carries_peak_freq_hz_of_strongest_constituent() -> None:
    """peak_freq_hz must track the peak-power bin, not the band midpoint.

    The occupied band spans cols 25..39 (midpoint ~col 32), but the strongest
    power is at col 25 (asymmetric, off-center). The emitted fingerprint's
    peak_freq_hz must land at the peak bin's frequency, not the midpoint.
    """
    tres = 0.001
    num_bins = 64
    grid = np.full((250, num_bins), -120.0, dtype=np.float32)
    grid[78:118, 25:40] = -50.0  # occupied plateau -> midpoint around col 32
    grid[78:118, 25] = 0.0  # strong peak at col 25 (off-center)

    det = _detector(window=100, eval_iv=50, num_bins=num_bins, tres_s=tres)
    freq = det._freq_axis
    emitted = _feed_grid(det, grid, chunk=50, tres_s=tres)

    assert len(emitted) == 1
    b = emitted[0]
    peak_bin_freq = det._center_freq_hz + float(freq[25])
    midpoint_freq = b.center_freq_hz
    assert abs(b.peak_freq_hz - peak_bin_freq) < abs(b.peak_freq_hz - midpoint_freq), (
        f"peak_freq_hz={b.peak_freq_hz} should be nearer the peak bin "
        f"({peak_bin_freq}) than the midpoint ({midpoint_freq})"
    )
    assert abs(b.peak_freq_hz - peak_bin_freq) < 1.0
    assert abs(midpoint_freq - peak_bin_freq) > 1.0  # sanity: peak != midpoint
