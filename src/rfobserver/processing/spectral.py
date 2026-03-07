"""Dual-PSD computation: high-resolution PSD grid + full-duration summary PSD.

The PSD grid is a 2D time-frequency array where each row is a short-duration
averaged Welch PSD. The summary PSD averages the entire grid into a single
vector for outbound reporting.

The PSD grid replaces the separate waterfall/spectrogram computation --
it IS the spectrogram, with better per-cell SNR from averaging.

All FFT windows are extracted and processed as a single vectorized numpy
operation, using pocketfft's internal multi-threading for full CPU utilization.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from rfobserver.models import PSDData


@dataclass
class PSDGridConfig:
    """Configuration for PSD grid computation."""

    num_bins: int = 256
    time_resolution_ms: float = 0.2
    overlap: float = 0.5  # FFT overlap ratio
    num_workers: int = 1  # kept for API compat; numpy handles threading internally


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

    Fully vectorized: extracts all FFT windows at once using stride tricks,
    applies Hann window via broadcasting, computes all FFTs in a single
    np.fft.fft call (pocketfft multi-threaded), then reshapes and averages
    per time slice.

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

    # Actual samples consumed per slice
    actual_slice_samples = nperseg + (ffts_per_slice - 1) * hop

    # Number of non-overlapping time slices
    n_slices = n_samples // actual_slice_samples
    if n_slices == 0:
        n_slices = 1
        actual_slice_samples = n_samples
        ffts_per_slice = max(1, (actual_slice_samples - nperseg) // hop + 1)

    total_ffts = n_slices * ffts_per_slice

    # Truncate data to exact multiple
    usable_samples = n_slices * actual_slice_samples
    data = data[:usable_samples]

    # --- Vectorized window extraction using stride tricks ---
    # Reshape into (n_slices, actual_slice_samples)
    slices = data.reshape(n_slices, actual_slice_samples)

    # Extract overlapping FFT windows from each slice: (n_slices, ffts_per_slice, nperseg)
    # Use stride_tricks to create views without copying
    stride_slice = slices.strides[1]  # bytes per sample
    stride_row = slices.strides[0]  # bytes per slice row
    windows = np.lib.stride_tricks.as_strided(
        slices,
        shape=(n_slices, ffts_per_slice, nperseg),
        strides=(stride_row, hop * stride_slice, stride_slice),
    )

    # Flatten to (total_ffts, nperseg) for batch FFT
    windows_flat = windows.reshape(total_ffts, nperseg).copy()  # copy to make contiguous

    # --- Hann window + FFT + power (all vectorized) ---
    hann = np.hanning(nperseg).astype(np.complex64)
    window_power = np.sum(np.abs(hann) ** 2)

    # Apply window
    windows_flat *= hann

    # Batch FFT across all windows at once (pocketfft uses all cores)
    spectra = np.fft.fft(windows_flat, axis=1)

    # Power spectral density: |X|^2 / (fs * window_power)
    psd_linear = (np.abs(spectra) ** 2) / (sampling_rate * window_power)

    # Reshape back to (n_slices, ffts_per_slice, nperseg) and average per slice
    psd_per_slice = psd_linear.reshape(n_slices, ffts_per_slice, nperseg)
    psd_avg = np.mean(psd_per_slice, axis=1)  # (n_slices, nperseg)

    # Convert to dB and fftshift
    psd_db = np.fft.fftshift(
        np.nan_to_num(10.0 * np.log10(psd_avg)).astype(np.float32),
        axes=1,
    )

    # Frequency axis
    freq_axis = np.fft.fftshift(np.fft.fftfreq(nperseg, 1.0 / sampling_rate))

    # Time axis: center of each slice
    slice_duration = actual_slice_samples / sampling_rate
    time_axis = np.arange(n_slices) * slice_duration + slice_duration / 2

    return PSDGridResult(
        grid=psd_db,
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
