"""Dual-PSD computation: high-resolution PSD grid + full-duration summary PSD.

The PSD grid is a 2D time-frequency array where each row is a short-duration
averaged Welch PSD. The summary PSD averages the entire grid into a single
vector for outbound reporting.

The PSD grid replaces the separate waterfall/spectrogram computation --
it IS the spectrogram, with better per-cell SNR from averaging.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import scipy.signal

from rfobserver.models import PSDData


@dataclass
class PSDGridConfig:
    """Configuration for PSD grid computation."""

    num_bins: int = 256
    time_resolution_ms: float = 0.2
    overlap: float = 0.5  # FFT overlap ratio


@dataclass
class PSDGridResult:
    """High-resolution PSD grid output."""

    grid: np.ndarray  # shape: (n_time_slices, num_bins), power in dB
    time_axis: np.ndarray  # center time of each slice in seconds
    freq_axis: np.ndarray  # frequency axis in Hz (relative to baseband)
    ffts_per_slice: int
    total_ffts: int


def compute_psd_grid(
    data: np.ndarray,
    sampling_rate: int,
    config: PSDGridConfig | None = None,
) -> PSDGridResult:
    """Compute a high-resolution PSD grid from complex IQ data.

    Divides the capture into time slices of `config.time_resolution_ms` duration.
    Each slice gets its own averaged Welch PSD. The result is a 2D grid that
    serves as both the internal spectrogram and the basis for burst detection.

    Args:
        data: Complex numpy array of IQ samples.
        sampling_rate: Sample rate in Hz.
        config: Grid configuration (bins, time resolution, overlap).

    Returns:
        PSDGridResult with the 2D grid, time/freq axes, and metadata.
    """
    if config is None:
        config = PSDGridConfig()

    nperseg = config.num_bins
    hop = int(nperseg * (1 - config.overlap))
    n_samples = len(data)

    # How many samples per time slice
    slice_samples = int(sampling_rate * config.time_resolution_ms / 1000.0)
    if slice_samples < nperseg:
        slice_samples = nperseg

    # How many FFTs fit in one time slice
    ffts_per_slice = max(1, (slice_samples - nperseg) // hop + 1)

    # Actual samples consumed per slice (may differ slightly from slice_samples)
    actual_slice_samples = nperseg + (ffts_per_slice - 1) * hop

    # Number of non-overlapping time slices
    n_slices = n_samples // actual_slice_samples
    if n_slices == 0:
        # Fall back to a single slice using all data
        n_slices = 1
        actual_slice_samples = n_samples
        ffts_per_slice = max(1, (actual_slice_samples - nperseg) // hop + 1)

    # Compute the frequency axis (same for all slices)
    freq_axis = np.fft.fftshift(np.fft.fftfreq(nperseg, 1.0 / sampling_rate))

    # Build the grid: one averaged PSD per time slice
    grid = np.empty((n_slices, nperseg), dtype=np.float32)
    total_ffts = 0

    for i in range(n_slices):
        start = i * actual_slice_samples
        end = start + actual_slice_samples
        slice_data = data[start:end]

        # Welch PSD for this slice
        _, psd_linear = scipy.signal.welch(
            slice_data,
            sampling_rate,
            window="hann",
            nperseg=nperseg,
            noverlap=nperseg - hop,
            return_onesided=False,
        )

        psd_db = np.nan_to_num(10.0 * np.log10(psd_linear).astype(np.float32))
        grid[i, :] = np.fft.fftshift(psd_db)
        total_ffts += ffts_per_slice

    # Time axis: center of each slice
    slice_duration = actual_slice_samples / sampling_rate
    time_axis = np.arange(n_slices) * slice_duration + slice_duration / 2

    return PSDGridResult(
        grid=grid,
        time_axis=time_axis,
        freq_axis=freq_axis,
        ffts_per_slice=ffts_per_slice,
        total_ffts=total_ffts,
    )


def compute_summary_psd(
    psd_grid: PSDGridResult,
    center_freq: int,
    sampling_rate: int,
) -> PSDData:
    """Average the entire PSD grid into a single summary PSD vector.

    This is the PSD published to NATS and used for champion selection.
    Averaging in dB domain (the grid is already in dB).
    """
    # Average across all time slices (axis=0)
    summary_db = np.mean(psd_grid.grid, axis=0)
    frequencies = psd_grid.freq_axis + center_freq

    return PSDData(
        powers=summary_db.tolist(),
        frequencies=frequencies.tolist(),
        center_freq=float(center_freq),
        sample_rate=sampling_rate,
        num_bins=len(summary_db),
    )


def compute_noise_floor(grid: np.ndarray) -> np.ndarray:
    """Estimate per-bin noise floor as 10th percentile across time slices."""
    result: np.ndarray = np.percentile(grid, 10, axis=0).astype(np.float32)
    return result
