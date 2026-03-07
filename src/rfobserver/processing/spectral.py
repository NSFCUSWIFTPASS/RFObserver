"""Dual-PSD computation: high-resolution PSD grid + full-duration summary PSD.

The PSD grid is a 2D time-frequency array where each row is a short-duration
averaged Welch PSD. The summary PSD averages the entire grid into a single
vector for outbound reporting.

All FFT windows are extracted using stride tricks and processed as a single
batch FFT via scipy.fft with explicit multi-threading (workers=-1).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import scipy.fft

from rfobserver.models import PSDData


@dataclass
class PSDGridConfig:
    """Configuration for PSD grid computation."""

    num_bins: int = 256
    time_resolution_ms: float = 0.2
    overlap: float = 0.5  # FFT overlap ratio
    num_workers: int = -1  # -1 = all cores, passed to scipy.fft


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

    Fully vectorized: extracts all overlapping FFT windows at once using
    stride tricks, applies Hann window, computes batch FFT via scipy.fft
    with explicit multi-threading, then reshapes and averages per time slice.
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

    # Actual samples consumed per slice
    actual_slice_samples = nperseg + (ffts_per_slice - 1) * hop

    # Number of non-overlapping time slices
    n_slices = n_samples // actual_slice_samples
    if n_slices == 0:
        n_slices = 1
        actual_slice_samples = n_samples
        ffts_per_slice = max(1, (actual_slice_samples - nperseg) // hop + 1)

    total_ffts = n_slices * ffts_per_slice

    # Pre-compute window and normalization
    hann = np.hanning(nperseg).astype(data.dtype)
    window_norm = float(1.0 / (sampling_rate * np.sum(np.abs(hann) ** 2)))

    # --- Extract all FFT windows using global stride tricks ---
    # Simple 2D strided view: (total_ffts, nperseg) with hop stride
    # This avoids the expensive 3D reshape + copy approach
    usable_samples = n_slices * actual_slice_samples
    d = data[:usable_samples]

    # Reshape into slices, then stride within each slice
    slices = d.reshape(n_slices, actual_slice_samples)
    stride_row = slices.strides[0]
    stride_col = slices.strides[1]

    windows_3d = np.lib.stride_tricks.as_strided(
        slices,
        shape=(n_slices, ffts_per_slice, nperseg),
        strides=(stride_row, hop * stride_col, stride_col),
    )

    # Flatten and copy to contiguous memory
    flat = windows_3d.reshape(total_ffts, nperseg).copy()

    # Apply Hann window in-place
    flat *= hann

    # Batch FFT with explicit multi-threading
    workers = config.num_workers
    spectra = scipy.fft.fft(flat, axis=1, workers=workers)

    # PSD: |X|^2 * norm
    psd_linear = np.abs(spectra)
    np.square(psd_linear, out=psd_linear)
    psd_linear *= window_norm

    # Reshape to (n_slices, ffts_per_slice, nperseg), average per slice
    psd_per_slice = psd_linear.reshape(n_slices, ffts_per_slice, nperseg)
    psd_avg = np.mean(psd_per_slice, axis=1)

    # Convert to dB + fftshift
    np.log10(psd_avg, out=psd_avg)
    psd_avg *= 10.0
    np.nan_to_num(psd_avg, copy=False, nan=-200.0, posinf=0.0, neginf=-200.0)
    grid = np.fft.fftshift(psd_avg.astype(np.float32), axes=1)

    # Axes
    freq_axis = np.fft.fftshift(np.fft.fftfreq(nperseg, 1.0 / sampling_rate))
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
    """Average the entire PSD grid into a single summary PSD vector."""
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
