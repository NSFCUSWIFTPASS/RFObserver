"""Fused numexpr compute_psd_grid vs the current one. Equivalence + timing."""
from __future__ import annotations
import time, numpy as np, scipy.fft, numexpr as ne
from rfobserver.processing.spectral import PSDGridConfig, PSDGridResult, compute_psd_grid

ne.set_num_threads(ne.detect_number_of_cores())

def compute_psd_grid_fused(data, sampling_rate, config=None):
    if config is None: config = PSDGridConfig()
    nperseg = config.num_bins
    hop = int(nperseg * (1 - config.overlap))
    n_samples = len(data)
    slice_samples = int(sampling_rate * config.time_resolution_ms / 1000.0)
    if slice_samples < nperseg: slice_samples = nperseg
    ffts_per_slice = max(1, (slice_samples - nperseg) // hop + 1)
    actual_slice_samples = nperseg + (ffts_per_slice - 1) * hop
    n_slices = n_samples // actual_slice_samples
    if n_slices == 0:
        n_slices = 1; actual_slice_samples = n_samples
        ffts_per_slice = max(1, (actual_slice_samples - nperseg) // hop + 1)
    total_ffts = n_slices * ffts_per_slice
    hann = np.hanning(nperseg).astype(data.dtype)
    window_norm = float(1.0 / (sampling_rate * np.sum(np.abs(hann) ** 2)))
    usable = n_slices * actual_slice_samples
    slices = data[:usable].reshape(n_slices, actual_slice_samples)
    sr, sc = slices.strides
    workers = config.num_workers
    chunk_sz = 50
    grid = np.empty((n_slices, nperseg), dtype=np.float32)  # f32 accumulator (half the traffic)
    for ci in range(0, n_slices, chunk_sz):
        ce = min(ci + chunk_sz, n_slices); ns = ce - ci
        w3d = np.lib.stride_tricks.as_strided(
            slices[ci:ce], shape=(ns, ffts_per_slice, nperseg),
            strides=(sr, hop * sc, sc))
        # fuse windowed copy + hann into one multi-threaded pass
        flat = ne.evaluate("w3d * hann").reshape(ns * ffts_per_slice, nperseg)
        spectra = scipy.fft.fft(flat, axis=1, workers=workers)
        # |z|^2 directly (no sqrt), multi-threaded, one pass -> float32
        psd = ne.evaluate("real(spectra)**2 + imag(spectra)**2")
        grid[ci:ce] = psd.reshape(ns, ffts_per_slice, nperseg).mean(axis=1)
    grid *= np.float32(window_norm)
    # dB in one fused multi-threaded pass, with the -200 floor
    ne.evaluate("where(grid > 0, 10.0 * log10(grid), -200.0)", out=grid, casting="unsafe")
    grid = np.fft.fftshift(grid, axes=1)
    freq_axis = np.fft.fftshift(np.fft.fftfreq(nperseg, 1.0 / sampling_rate))
    sd = actual_slice_samples / sampling_rate
    return PSDGridResult(grid=grid, time_axis=np.arange(n_slices) * sd + sd / 2,
                         freq_axis=freq_axis, ffts_per_slice=ffts_per_slice, total_ffts=total_ffts)

SR = 26_000_000
rng = np.random.default_rng(0)
n = int(SR * 0.5)
data = (rng.standard_normal(n) + 1j * rng.standard_normal(n)).astype(np.complex64)
# inject a tone so it is not pure noise
t = np.arange(n); data += (0.5 * np.exp(2j * np.pi * 0.13 * t)).astype(np.complex64)
cfg = PSDGridConfig(num_bins=256, time_resolution_ms=0.2)

a = compute_psd_grid(data, SR, cfg); b = compute_psd_grid_fused(data, SR, cfg)
d = np.abs(a.grid - b.grid)
print(f"shapes {a.grid.shape} vs {b.grid.shape}; max|dB diff|={d.max():.4f}  mean={d.mean():.5f}")

def bench(fn, iters=10):
    fn(); ts=[]
    for _ in range(iters):
        t0=time.perf_counter(); fn(); ts.append((time.perf_counter()-t0)*1000)
    return np.mean(ts), np.min(ts)
m0,mn0 = bench(lambda: compute_psd_grid(data, SR, cfg))
m1,mn1 = bench(lambda: compute_psd_grid_fused(data, SR, cfg))
print(f"original : mean {m0:7.1f} ms  min {mn0:7.1f} ms")
print(f"fused    : mean {m1:7.1f} ms  min {mn1:7.1f} ms   speedup {m0/m1:.2f}x")
