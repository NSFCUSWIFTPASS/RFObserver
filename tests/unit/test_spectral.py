"""Tests for rfobserver.processing.spectral -- dual PSD design."""

import numpy as np

from rfobserver.processing.spectral import (
    PSDGridConfig,
    compute_noise_floor,
    compute_psd_grid,
    compute_summary_psd,
)


def _make_noise(n: int, seed: int = 42) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return (rng.standard_normal(n) + 1j * rng.standard_normal(n)).astype(np.complex64)


def test_psd_grid_output_shape():
    data = _make_noise(50_000)
    config = PSDGridConfig(num_bins=256, time_resolution_ms=1.0)
    result = compute_psd_grid(data, sampling_rate=1_000_000, config=config)

    # 50k samples at 1 MHz = 50ms, at 1ms resolution = ~50 slices
    assert result.grid.shape[1] == 256
    assert result.grid.shape[0] > 0
    assert len(result.time_axis) == result.grid.shape[0]
    assert len(result.freq_axis) == 256


def test_psd_grid_time_resolution():
    """Time axis spacing should match configured resolution."""
    sr = 1_000_000
    data = _make_noise(100_000)  # 100ms
    config = PSDGridConfig(num_bins=256, time_resolution_ms=1.0)
    result = compute_psd_grid(data, sampling_rate=sr, config=config)

    if len(result.time_axis) > 1:
        dt = result.time_axis[1] - result.time_axis[0]
        # Should be close to 1.0ms (quantized by FFT hop size)
        np.testing.assert_allclose(dt, 0.001, rtol=0.15)


def test_psd_grid_ffts_per_slice():
    """Check that ffts_per_slice is derived from time resolution."""
    sr = 26_000_000
    data = _make_noise(26_000)  # 1ms at 26 Msps
    config = PSDGridConfig(num_bins=256, time_resolution_ms=0.2)
    result = compute_psd_grid(data, sampling_rate=sr, config=config)

    # At 26 Msps, 0.2ms = 5200 samples. With 256 bins, 128 hop:
    # ffts = (5200 - 256) / 128 + 1 = ~39
    assert result.ffts_per_slice > 1
    assert result.total_ffts > 0


def test_psd_grid_short_data():
    """Should handle data shorter than one full time slice."""
    data = _make_noise(500)
    config = PSDGridConfig(num_bins=256, time_resolution_ms=1.0)
    result = compute_psd_grid(data, sampling_rate=1_000_000, config=config)

    # Should fall back to a single slice
    assert result.grid.shape[0] >= 1
    assert result.grid.shape[1] == 256


def test_psd_grid_freq_axis_centered():
    """Frequency axis should be centered around zero (DC)."""
    data = _make_noise(10_000)
    result = compute_psd_grid(data, sampling_rate=1_000_000)
    freqs = result.freq_axis
    # DC should be near the middle
    dc_idx = np.argmin(np.abs(freqs))
    assert abs(dc_idx - len(freqs) // 2) <= 1


def test_summary_psd_shape():
    data = _make_noise(50_000)
    grid_result = compute_psd_grid(data, sampling_rate=1_000_000)
    psd = compute_summary_psd(grid_result, center_freq=915_000_000, sampling_rate=1_000_000)

    assert len(psd.powers) == 256
    assert len(psd.frequencies) == 256
    assert psd.center_freq == 915_000_000
    assert psd.sample_rate == 1_000_000


def test_summary_psd_frequencies_offset():
    """Summary PSD frequencies should be offset by center_freq."""
    data = _make_noise(50_000)
    grid_result = compute_psd_grid(data, sampling_rate=1_000_000)
    psd = compute_summary_psd(grid_result, center_freq=915_000_000, sampling_rate=1_000_000)

    freqs = np.array(psd.frequencies)
    assert freqs.min() > 914_000_000
    assert freqs.max() < 916_000_000


def test_summary_psd_is_average_of_grid():
    """Summary PSD should equal the column-wise mean of the grid."""
    data = _make_noise(50_000)
    grid_result = compute_psd_grid(data, sampling_rate=1_000_000)
    psd = compute_summary_psd(grid_result, center_freq=0, sampling_rate=1_000_000)

    expected = np.mean(grid_result.grid, axis=0)
    np.testing.assert_allclose(psd.powers, expected, atol=1e-4)


def test_noise_floor_estimate():
    data = _make_noise(50_000)
    grid_result = compute_psd_grid(data, sampling_rate=1_000_000)
    nf = compute_noise_floor(grid_result.grid)

    assert nf.shape == (grid_result.grid.shape[1],)
    # 10th percentile should be below the mean
    mean_power = np.mean(grid_result.grid, axis=0)
    assert np.all(nf <= mean_power + 1.0)  # allow small float tolerance


def test_psd_grid_tone_visible():
    """A strong tone should produce elevated power in the correct frequency bin."""
    sr = 1_000_000
    n = 100_000
    t = np.arange(n) / sr
    # Tone at 100 kHz
    tone = np.exp(2j * np.pi * 100_000 * t).astype(np.complex64)
    noise = 0.01 * _make_noise(n)
    data = tone + noise

    config = PSDGridConfig(num_bins=256, time_resolution_ms=1.0)
    result = compute_psd_grid(data, sampling_rate=sr, config=config)

    # Average across time to find the tone
    avg_psd = np.mean(result.grid, axis=0)
    peak_idx = np.argmax(avg_psd)
    peak_freq = result.freq_axis[peak_idx]
    assert abs(peak_freq - 100_000) < sr / 256 * 2
