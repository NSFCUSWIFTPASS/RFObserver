"""Fast tests for the synthetic-waveform helpers (no NATS, no pipeline)."""

from __future__ import annotations

from ._synth import derive_grid_params


def test_grid_params_window_holds_long_burst() -> None:
    """A 393.1 ms burst must fit inside the derived window with margin."""
    p = derive_grid_params(fs_hz=56_000_000, occupied_bw_hz=2_000_000, duration_ms=393.1)
    burst_rows = 393.1 / p.time_resolution_ms
    assert p.window_rows > burst_rows, "window must span the whole burst"
    assert p.eval_interval_rows <= p.window_rows
    assert p.chunk_slices >= 1


def test_grid_params_fft_resolves_narrow_burst() -> None:
    """A 50 kHz burst in a 28 MHz span must occupy at least a couple of bins."""
    p = derive_grid_params(fs_hz=28_000_000, occupied_bw_hz=50_000, duration_ms=10.24)
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
                p = derive_grid_params(fs_hz=fs, occupied_bw_hz=b, duration_ms=d)
                slice_samples = fs * p.time_resolution_ms / 1000.0
                assert slice_samples >= p.num_bins, (fs, b, d, slice_samples, p.num_bins)
