"""Tests for rfobserver.processing.burst -- CCL on PSD grids."""

from datetime import datetime

import numpy as np

from rfobserver.processing.burst import BurstDetectionConfig, detect_bursts
from rfobserver.processing.spectral import PSDGridResult


def _make_grid(
    n_rows: int = 100,
    n_bins: int = 64,
    noise_db: float = -60.0,
    sample_rate: int = 1_000_000,
    time_resolution_ms: float = 1.0,
) -> PSDGridResult:
    """Create a synthetic PSD grid filled with constant noise."""
    grid = np.full((n_rows, n_bins), noise_db, dtype=np.float32)
    slice_duration = time_resolution_ms / 1000.0
    time_axis = np.arange(n_rows) * slice_duration + slice_duration / 2
    freq_axis = np.fft.fftshift(np.fft.fftfreq(n_bins, 1.0 / sample_rate))
    return PSDGridResult(
        grid=grid,
        time_axis=time_axis,
        freq_axis=freq_axis,
        ffts_per_slice=10,
        total_ffts=n_rows * 10,
    )


def _inject_burst(
    grid_result: PSDGridResult,
    row_start: int,
    row_end: int,
    col_start: int,
    col_end: int,
    power_db: float,
) -> None:
    """Inject a rectangular burst into the PSD grid."""
    grid_result.grid[row_start:row_end, col_start:col_end] = power_db


def test_no_bursts_in_flat_noise():
    grid = _make_grid()
    config = BurstDetectionConfig(threshold_high_db=10.0)
    result = detect_bursts(grid, config, capture_time=datetime(2026, 1, 1))
    assert len(result.bursts) == 0


def test_single_burst_detected():
    grid = _make_grid(n_rows=200, n_bins=64, noise_db=-60.0)
    # Inject burst: rows 50-70, cols 20-30, at -30 dB (30 dB above noise)
    _inject_burst(grid, 50, 70, 20, 30, -30.0)

    config = BurstDetectionConfig(threshold_high_db=10.0, min_duration_sec=0.0)
    result = detect_bursts(grid, config, center_freq_hz=915e6, capture_time=datetime(2026, 1, 1))

    assert len(result.bursts) == 1
    burst = result.bursts[0]
    assert burst.peak_power_db == -30.0
    assert burst.duration_ms > 0


def test_two_separate_bursts():
    grid = _make_grid(n_rows=200, n_bins=64, noise_db=-60.0)
    _inject_burst(grid, 10, 20, 5, 10, -25.0)   # burst 1
    _inject_burst(grid, 150, 170, 40, 50, -30.0)  # burst 2

    config = BurstDetectionConfig(threshold_high_db=10.0, min_duration_sec=0.0)
    result = detect_bursts(grid, config, capture_time=datetime(2026, 1, 1))

    assert len(result.bursts) == 2


def test_burst_below_threshold_not_detected():
    grid = _make_grid(n_rows=100, n_bins=64, noise_db=-60.0)
    # Inject burst only 5 dB above noise (below 10 dB threshold)
    _inject_burst(grid, 30, 40, 10, 15, -55.0)

    config = BurstDetectionConfig(threshold_high_db=10.0)
    result = detect_bursts(grid, config, capture_time=datetime(2026, 1, 1))

    assert len(result.bursts) == 0


def test_dual_threshold_hysteresis():
    """A burst with a high-power core and low-power halo should be detected as one component."""
    grid = _make_grid(n_rows=100, n_bins=64, noise_db=-60.0)

    # Low-power halo (above T_L=6dB but below T_H=10dB above noise)
    _inject_burst(grid, 30, 50, 15, 35, -52.0)  # 8 dB above noise
    # High-power core (above T_H)
    _inject_burst(grid, 35, 45, 20, 30, -40.0)  # 20 dB above noise

    config = BurstDetectionConfig(threshold_high_db=10.0, threshold_low_ratio=0.6, min_duration_sec=0.0)
    result = detect_bursts(grid, config, capture_time=datetime(2026, 1, 1))

    assert len(result.bursts) == 1
    burst = result.bursts[0]
    # The burst should span the halo, not just the core
    assert burst.bandwidth_hz > 0


def test_min_duration_filter():
    grid = _make_grid(n_rows=100, n_bins=64, noise_db=-60.0, time_resolution_ms=1.0)
    # Very short burst: 2 rows at 1ms resolution = 2ms
    _inject_burst(grid, 50, 52, 20, 25, -30.0)

    # Require at least 5ms
    config = BurstDetectionConfig(threshold_high_db=10.0, min_duration_sec=0.005)
    result = detect_bursts(grid, config, capture_time=datetime(2026, 1, 1))

    assert len(result.bursts) == 0


def test_burst_merging():
    grid = _make_grid(n_rows=200, n_bins=64, noise_db=-60.0, time_resolution_ms=1.0)
    # Two nearby bursts at same frequency, small time gap
    _inject_burst(grid, 50, 55, 20, 25, -30.0)
    _inject_burst(grid, 58, 63, 20, 25, -30.0)  # 3ms gap

    config = BurstDetectionConfig(
        threshold_high_db=10.0,
        min_duration_sec=0.0,
        merge_freq_bins=5,
        merge_time_sec=0.005,
    )
    result = detect_bursts(grid, config, capture_time=datetime(2026, 1, 1))

    # Should merge into one burst
    assert result.num_merged > 0 or len(result.bursts) == 1


def test_burst_fingerprint_fields():
    grid = _make_grid(n_rows=100, n_bins=64, noise_db=-60.0)
    _inject_burst(grid, 30, 50, 20, 30, -25.0)

    capture_time = datetime(2026, 3, 6, 12, 0, 0)
    config = BurstDetectionConfig(threshold_high_db=10.0, min_duration_sec=0.0)
    result = detect_bursts(grid, config, center_freq_hz=915e6, capture_time=capture_time)

    assert len(result.bursts) == 1
    burst = result.bursts[0]
    assert burst.burst_id  # auto-generated UUID
    assert burst.start_time >= capture_time
    assert burst.stop_time > burst.start_time
    assert burst.center_freq_hz > 0
    assert burst.bandwidth_hz >= 0
    assert burst.peak_power_db == -25.0
    assert burst.duration_ms > 0


def test_noise_floor_reported():
    grid = _make_grid(noise_db=-60.0)
    result = detect_bursts(grid, capture_time=datetime(2026, 1, 1))
    np.testing.assert_allclose(result.noise_floor_db, -60.0, atol=1.0)
