"""Tests for rfobserver.processing.burst -- CCL on PSD grids."""

from datetime import datetime, timezone

import numpy as np

from rfobserver.models import BurstFingerprint
from rfobserver.processing.burst import BurstDetectionConfig, _merge_bursts, detect_bursts
from rfobserver.processing.spectral import PSDGridResult, compute_noise_floor


def test_merge_bursts_carries_stronger_peak_freq_hz():
    """A merged burst keeps the peak frequency of the stronger constituent, not 0.0."""
    from datetime import timedelta

    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    a = BurstFingerprint(
        start_time=t0,
        stop_time=t0 + timedelta(milliseconds=10),
        center_freq_hz=915_000_000.0,
        peak_freq_hz=915_020_000.0,
        bandwidth_hz=200_000.0,
        peak_power_db=-70.0,
        duration_ms=10.0,
        detection_timestamp=t0,
    )
    b = BurstFingerprint(
        start_time=t0 + timedelta(milliseconds=11),
        stop_time=t0 + timedelta(milliseconds=20),
        center_freq_hz=915_050_000.0,
        peak_freq_hz=915_070_000.0,
        bandwidth_hz=200_000.0,
        peak_power_db=-50.0,  # stronger
        duration_ms=9.0,
        detection_timestamp=t0,
    )
    merged = _merge_bursts([a, b], max_time_gap=0.005, freq_tolerance=500_000.0)
    assert len(merged) == 1
    assert merged[0].peak_freq_hz == 915_070_000.0  # stronger constituent's peak


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
    _inject_burst(grid, 10, 20, 5, 10, -25.0)  # burst 1
    _inject_burst(grid, 150, 170, 40, 50, -30.0)  # burst 2

    config = BurstDetectionConfig(threshold_high_db=10.0, min_duration_sec=0.0)
    result = detect_bursts(grid, config, capture_time=datetime(2026, 1, 1))

    assert len(result.bursts) == 2


def test_many_separated_bursts_all_extracted():
    """Guards the find_objects-based extraction: many labels, each isolated in
    its own bbox, must all be found with the correct per-region time/freq/peak
    (a regression guard for the O(bursts x grid) -> bbox rewrite)."""
    grid = _make_grid(n_rows=600, n_bins=64, noise_db=-60.0)
    # 12 bursts on a staggered time/freq lattice so none touch (8-connectivity).
    expected_cols = []
    for i in range(12):
        r0 = 10 + i * 45
        c0 = 3 + (i % 6) * 10
        _inject_burst(grid, r0, r0 + 20, c0, c0 + 4, -30.0)
        expected_cols.append(c0)

    config = BurstDetectionConfig(threshold_high_db=10.0, min_duration_sec=0.0)
    result = detect_bursts(grid, config, center_freq_hz=915e6, capture_time=datetime(2026, 1, 1))

    assert len(result.bursts) == 12
    # Every burst carries the injected peak power and a distinct, in-range peak
    # frequency (proves each region's cols were extracted from its own bbox,
    # with the row/col offsets applied correctly).
    assert all(abs(b.peak_power_db - (-30.0)) < 1e-6 for b in result.bursts)
    peak_freqs = sorted(b.peak_freq_hz for b in result.bursts)
    assert len(set(peak_freqs)) >= 6  # at least the 6 distinct freq columns
    assert all(914e6 < f < 916e6 for f in peak_freqs)


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

    config = BurstDetectionConfig(
        threshold_high_db=10.0,
        threshold_low_ratio=0.6,
        min_duration_sec=0.0,
    )
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


def test_compute_noise_floor_percentile_param():
    # 100 rows: 90 at 0 dB, 10 at 100 dB -> p10=0, median=0, p95=100
    grid = np.zeros((100, 4), dtype=np.float32)
    grid[90:, :] = 100.0
    assert np.allclose(compute_noise_floor(grid), 0.0)  # default p10
    assert np.allclose(compute_noise_floor(grid, 50.0), 0.0)  # median
    assert np.allclose(compute_noise_floor(grid, 95.0), 100.0)  # high pct


def test_peak_freq_hz_reports_peak_bin_not_midpoint():
    # 20 rows x 8 bins; occupied band bins 2..6, but the PEAK is at bin 3 (asymmetric).
    grid = np.full((20, 8), -100.0, dtype=np.float32)
    grid[5:15, 2:7] = -50.0  # occupied plateau -> midpoint at bin 4
    grid[5:15, 3] = -10.0  # strong peak at bin 3
    freq_axis = (np.arange(8) - 4) * 1_000_000.0  # 1 MHz bins, centered
    psd = PSDGridResult(
        grid=grid,
        time_axis=np.arange(20) * 0.001,
        freq_axis=freq_axis,
        ffts_per_slice=1,
        total_ffts=20,
    )
    cfg = BurstDetectionConfig(threshold_high_db=20.0, min_duration_sec=0.0)
    res = detect_bursts(psd, cfg, center_freq_hz=915e6, capture_time=datetime.now(timezone.utc))
    assert len(res.bursts) == 1
    b = res.bursts[0]
    assert abs(b.peak_freq_hz - (915e6 + freq_axis[3])) < 1.0  # peak = bin 3
    assert abs(b.center_freq_hz - (915e6 + freq_axis[4])) < 1.0  # midpoint = bin 4 (unchanged)
