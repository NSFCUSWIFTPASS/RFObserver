"""Fast tests for the synthetic-waveform helpers (no NATS, no pipeline)."""

from __future__ import annotations

import numpy as np

from ._synth import make_iq_with_wideband_burst


def test_wideband_burst_occupies_expected_band() -> None:
    """The generated comb's PSD shows raised power across ~[offset +/- bw/2]."""
    from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid

    fs = 28_000_000
    bw = 2_000_000
    offset = 3_000_000
    num_bins = 1024
    iq = make_iq_with_wideband_burst(
        duration_sec=0.02,
        sample_rate_hz=fs,
        burst_start_sec=0.005,
        burst_duration_sec=0.010,
        burst_bw_hz=bw,
        burst_offset_hz=offset,
        num_bins=num_bins,
    )
    assert iq.dtype == np.complex64
    assert iq.shape == (int(0.02 * fs),)
    # Low crest factor (Schroeder phases) keeps the burst inside the SC16 range.
    assert float(np.max(np.abs(iq))) < 1.0, "comb must not clip the [-1, 1] range"

    grid_res = compute_psd_grid(iq, fs, PSDGridConfig(num_bins=num_bins, time_resolution_ms=0.2))
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
