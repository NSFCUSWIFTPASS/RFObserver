"""Dual-PSD computation: high-resolution PSD grid + full-duration summary PSD.

The PSD grid is a 2D time-frequency array where each row is a short-duration
averaged Welch PSD. The summary PSD averages the entire grid into a single
vector for outbound reporting.

FFT windows are extracted via stride tricks and processed in cache-friendly
chunks (~200 slices at a time) to avoid thrashing main memory with a single
giant copy. Each chunk's copy fits in L3 cache for efficient processing.

With PSDGridConfig.backend == "cuda" the grid is computed on the GPU instead
(rfobserver.processing.psd_cuda), falling back to the CPU when it cannot run.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import numpy as np
import scipy.fft

from rfobserver.models import PSDData

logger = logging.getLogger(__name__)

PSD_BACKENDS = ("cpu", "cuda")


@dataclass
class PSDGridConfig:
    """Configuration for PSD grid computation."""

    num_bins: int = 256
    time_resolution_ms: float = 0.2
    overlap: float = 0.5  # FFT overlap ratio
    num_workers: int = -1  # -1 = all cores, passed to scipy.fft
    # "cpu", or "cuda" to compute on the GPU. A cuda request falls back to the
    # CPU (logged once) when the GPU library or a CUDA device is missing.
    backend: str = "cpu"

    def __post_init__(self) -> None:
        if self.backend not in PSD_BACKENDS:
            raise ValueError(f"backend must be one of {PSD_BACKENDS}, got {self.backend!r}")


@dataclass
class PSDGridResult:
    """High-resolution PSD grid output."""

    grid: np.ndarray  # shape: (n_time_slices, num_bins), power in dB
    time_axis: np.ndarray  # center time of each slice in seconds
    freq_axis: np.ndarray  # frequency axis in Hz (relative to baseband)
    ffts_per_slice: int
    total_ffts: int


@dataclass(frozen=True)
class GridGeometry:
    """How compute_psd_grid tiles its input into FFT segments and time slices.
    Shared by the CPU and GPU paths so they always cut the input the same way."""

    nperseg: int
    hop: int
    ffts_per_slice: int
    actual_slice_samples: int  # input samples consumed per time slice
    n_slices: int
    short_input: bool  # input shorter than one slice: a single slice spans it

    @property
    def usable_samples(self) -> int:
        return self.n_slices * self.actual_slice_samples


def grid_geometry(n_samples: int, sampling_rate: int, config: PSDGridConfig) -> GridGeometry:
    """Segment and slice layout of a grid over n_samples of input."""
    nperseg = config.num_bins
    hop = int(nperseg * (1 - config.overlap))

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
    short_input = n_slices == 0
    if short_input:
        n_slices = 1
        actual_slice_samples = n_samples
        ffts_per_slice = max(1, (actual_slice_samples - nperseg) // hop + 1)

    return GridGeometry(nperseg, hop, ffts_per_slice, actual_slice_samples, n_slices, short_input)


def hann_window(nperseg: int, dtype: Any, sampling_rate: int) -> tuple[np.ndarray, float]:
    """The Hann window (as dtype) and the PSD normalization that goes with it."""
    hann = np.hanning(nperseg).astype(dtype)
    window_norm = float(1.0 / (sampling_rate * np.sum(np.abs(hann) ** 2)))
    return hann, window_norm


def grid_axes(geometry: GridGeometry, sampling_rate: int) -> tuple[np.ndarray, np.ndarray]:
    """(time_axis, freq_axis) of a grid with this geometry."""
    freq_axis = np.fft.fftshift(np.fft.fftfreq(geometry.nperseg, 1.0 / sampling_rate))
    slice_duration = geometry.actual_slice_samples / sampling_rate
    time_axis = np.arange(geometry.n_slices) * slice_duration + slice_duration / 2
    return time_axis, freq_axis


def compute_psd_grid(
    data: np.ndarray,
    sampling_rate: int,
    config: PSDGridConfig | None = None,
) -> PSDGridResult:
    """Compute a high-resolution PSD grid from complex IQ data.

    Runs on the GPU when config.backend is "cuda" and it can (the result
    matches the CPU path to float32 rounding), otherwise on the CPU.
    """
    if config is None:
        config = PSDGridConfig()
    if config.backend == "cuda":
        result = _compute_psd_grid_gpu(data, sampling_rate, config)
        if result is not None:
            return result
    return _compute_psd_grid_cpu(data, sampling_rate, config)


# GPU fallback notices already logged, so a fallback is reported once per
# process rather than once per chunk.
_gpu_notes: set[str] = set()


def _gpu_note_once(key: str, msg: str, *args: Any, exc_info: bool = False) -> None:
    if key in _gpu_notes:
        return
    _gpu_notes.add(key)
    logger.warning(msg, *args, exc_info=exc_info)


def _compute_psd_grid_gpu(
    data: np.ndarray, sampling_rate: int, config: PSDGridConfig
) -> PSDGridResult | None:
    """The GPU grid, or None to fall back to the CPU."""
    # Imported here so the CPU path never loads ctypes/CUDA machinery.
    from rfobserver.processing import psd_cuda

    try:
        result = psd_cuda.compute_psd_grid_cuda(data, sampling_rate, config)
    except Exception:
        # psd_cuda has disabled itself, so later calls go straight to the CPU.
        _gpu_note_once("error", "GPU PSD failed; using the CPU from now on", exc_info=True)
        return None
    if result is None:
        reason = psd_cuda.unavailable_reason()
        if reason is not None:
            _gpu_note_once(
                "unavailable", "PSD backend 'cuda' is unavailable (%s); using the CPU", reason
            )
    return result


def _compute_psd_grid_cpu(
    data: np.ndarray, sampling_rate: int, config: PSDGridConfig
) -> PSDGridResult:
    """Fully vectorized: extracts all overlapping FFT windows at once using
    stride tricks, applies Hann window, computes batch FFT via scipy.fft
    with explicit multi-threading, then reshapes and averages per time slice.
    """
    geometry = grid_geometry(len(data), sampling_rate, config)
    nperseg = geometry.nperseg
    hop = geometry.hop
    ffts_per_slice = geometry.ffts_per_slice
    n_slices = geometry.n_slices
    total_ffts = n_slices * ffts_per_slice

    # Pre-compute window and normalization
    hann, window_norm = hann_window(nperseg, data.dtype, sampling_rate)

    d = data[: geometry.usable_samples]
    slices = d.reshape(n_slices, geometry.actual_slice_samples)
    stride_row = slices.strides[0]
    stride_col = slices.strides[1]

    workers = config.num_workers

    # --- Chunked processing to keep copies in L3 cache ---
    # Processing all 216K windows at once copies ~443MB (at 56 MHz BW).
    # Instead, process ~200 slices at a time so each copy is ~15MB.
    chunk_sz = 50
    grid_f64 = np.empty((n_slices, nperseg), dtype=np.float64)

    for ci in range(0, n_slices, chunk_sz):
        ce = min(ci + chunk_sz, n_slices)
        ns = ce - ci
        chunk = slices[ci:ce]

        w3d = np.lib.stride_tricks.as_strided(
            chunk,
            shape=(ns, ffts_per_slice, nperseg),
            strides=(stride_row, hop * stride_col, stride_col),
        )
        flat = w3d.reshape(ns * ffts_per_slice, nperseg).copy()
        flat *= hann

        spectra = scipy.fft.fft(flat, axis=1, workers=workers)

        psd_linear = np.abs(spectra)
        np.square(psd_linear, out=psd_linear)

        psd_rs = psd_linear.reshape(ns, ffts_per_slice, nperseg)
        grid_f64[ci:ce] = np.mean(psd_rs, axis=1)

    grid_f64 *= window_norm

    # Convert to dB + fftshift
    np.log10(grid_f64, out=grid_f64)
    grid_f64 *= 10.0
    np.nan_to_num(grid_f64, copy=False, nan=-200.0, posinf=0.0, neginf=-200.0)
    grid = np.fft.fftshift(grid_f64.astype(np.float32), axes=1)

    time_axis, freq_axis = grid_axes(geometry, sampling_rate)

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


def compute_noise_floor(grid: np.ndarray, percentile: float = 10.0) -> np.ndarray:
    """Estimate per-bin noise floor as the given percentile across time slices.

    The default 10.0 is the historical value; burst detection passes its own
    configured percentile (median by default, see BurstDetectionConfig).
    """
    result: np.ndarray = np.percentile(grid, percentile, axis=0).astype(np.float32)
    return result
