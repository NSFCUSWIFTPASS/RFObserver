"""Fast tests for the synthetic-waveform helpers (no NATS, no pipeline)."""

from __future__ import annotations

import numpy as np

from ._synth import GridParams, derive_grid_params, make_iq_with_wideband_burst


def test_grid_params_window_holds_long_burst() -> None:
    """A 393.1 ms burst must fit inside the derived window with margin."""
    p: GridParams = derive_grid_params(
        fs_hz=56_000_000, occupied_bw_hz=2_000_000, duration_ms=393.1
    )
    burst_rows = 393.1 / p.time_resolution_ms
    assert p.window_rows > burst_rows, "window must span the whole burst"
    assert p.eval_interval_rows <= p.window_rows
    assert p.chunk_slices >= 1


def test_grid_params_fft_resolves_narrow_burst() -> None:
    """A 50 kHz burst in a 28 MHz span must occupy at least a couple of bins."""
    p: GridParams = derive_grid_params(fs_hz=28_000_000, occupied_bw_hz=50_000, duration_ms=10.24)
    bin_spacing = 28_000_000 / p.num_bins
    occupied_bins = 50_000 / bin_spacing
    assert occupied_bins >= 2.0
    assert 256 <= p.num_bins <= 8192
    assert (p.num_bins & (p.num_bins - 1)) == 0, "num_bins must be a power of two"


def test_grid_params_slice_has_enough_samples_for_fft() -> None:
    """time_resolution must give at least num_bins samples per slice."""
    for fs in (28_000_000, 56_000_000):
        for b in (50_000, 150_000, 500_000, 2_000_000, 20_000_000):
            for d in (1.3, 2.7, 10.24, 83.2, 393.1):
                p: GridParams = derive_grid_params(fs_hz=fs, occupied_bw_hz=b, duration_ms=d)
                slice_samples = fs * p.time_resolution_ms / 1000.0
                assert slice_samples >= p.num_bins, (fs, b, d, slice_samples, p.num_bins)


def test_wideband_burst_occupies_expected_band() -> None:
    """The generated burst's PSD shows raised power across ~[offset +/- bw/2]."""
    from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid

    fs = 28_000_000
    bw = 2_000_000
    offset = 3_000_000
    iq = make_iq_with_wideband_burst(
        duration_sec=0.02,
        sample_rate_hz=fs,
        burst_start_sec=0.005,
        burst_duration_sec=0.010,
        burst_bw_hz=bw,
        burst_offset_hz=offset,
        burst_amplitude=0.5,
    )
    assert iq.dtype == np.complex64
    assert iq.shape == (int(0.02 * fs),)

    grid_res = compute_psd_grid(iq, fs, PSDGridConfig(num_bins=1024, time_resolution_ms=0.2))
    # Average PSD over the burst time slices (middle of the buffer).
    n = grid_res.grid.shape[0]
    burst_psd = grid_res.grid[n // 3 : 2 * n // 3].mean(axis=0)
    freqs = grid_res.freq_axis

    in_band = (freqs >= offset - bw / 2) & (freqs <= offset + bw / 2)
    # A guard band well away from the burst, used as the noise reference.
    out_band = (freqs >= -fs / 2 + 1_000_000) & (freqs <= -fs / 2 + 3_000_000)
    assert burst_psd[in_band].mean() - burst_psd[out_band].mean() > 15.0, (
        "occupied band must sit >15 dB above the out-of-band noise"
    )
