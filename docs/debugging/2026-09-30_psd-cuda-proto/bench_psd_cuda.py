"""Native CUDA compute_psd_grid vs the current CPU one. Equivalence + timing."""
from __future__ import annotations
import ctypes as C, time, os, numpy as np
from rfobserver.processing.spectral import PSDGridConfig, PSDGridResult, compute_psd_grid

lib = C.CDLL(os.path.join(os.path.dirname(__file__) or ".", "libpsdcuda.so"))
lib.psd_init.restype = C.c_void_p
lib.psd_init.argtypes = [C.c_int]*6 + [C.c_float, C.POINTER(C.c_float)]
lib.psd_run.restype = C.c_int
lib.psd_run.argtypes = [C.c_void_p, C.POINTER(C.c_float), C.POINTER(C.c_float)]
lib.psd_free.argtypes = [C.c_void_p]


class CudaPsd:
    def __init__(self, sampling_rate, config):
        self.sr = sampling_rate; self.cfg = config
        nperseg = config.num_bins; hop = int(nperseg * (1 - config.overlap))
        self.nperseg = nperseg; self.hop = hop
        # geometry identical to the CPU version
        ss = int(sampling_rate * config.time_resolution_ms / 1000.0)
        if ss < nperseg: ss = nperseg
        self.fps = max(1, (ss - nperseg) // hop + 1)
        self.ass = nperseg + (self.fps - 1) * hop
        self._n_probe = None  # set on first run when we know n

    def _ensure(self, n_samples):
        n_slices = n_samples // self.ass
        if n_slices == 0:
            raise ValueError("input too short for one slice")
        self.n_slices = n_slices
        self.usable = n_slices * self.ass
        hann = np.hanning(self.nperseg).astype(np.float32)
        self.window_norm = float(1.0 / (self.sr * np.sum(hann.astype(np.float64) ** 2)))
        self.ctx = lib.psd_init(self.nperseg, self.hop, self.fps, n_slices, self.ass,
                                self.usable, C.c_float(self.window_norm),
                                hann.ctypes.data_as(C.POINTER(C.c_float)))
        if not self.ctx:
            raise RuntimeError("psd_init failed")
        self.out = np.empty((n_slices, self.nperseg), dtype=np.float32)
        self._n_probe = n_samples

    def run(self, data):
        if self._n_probe is None:
            self._ensure(len(data))
        d = np.ascontiguousarray(data[:self.usable], dtype=np.complex64)
        rc = lib.psd_run(self.ctx, d.ctypes.data_as(C.POINTER(C.c_float)),
                         self.out.ctypes.data_as(C.POINTER(C.c_float)))
        if rc != 0:
            raise RuntimeError(f"psd_run rc={rc}")
        freq = np.fft.fftshift(np.fft.fftfreq(self.nperseg, 1.0 / self.sr))
        sd = self.ass / self.sr
        return PSDGridResult(grid=self.out.copy(),
                             time_axis=np.arange(self.n_slices) * sd + sd / 2,
                             freq_axis=freq, ffts_per_slice=self.fps,
                             total_ffts=self.n_slices * self.fps)


SR = 26_000_000
rng = np.random.default_rng(0)
n = int(SR * 0.5)
data = (rng.standard_normal(n) + 1j * rng.standard_normal(n)).astype(np.complex64)
t = np.arange(n)
data += (0.5 * np.exp(2j * np.pi * 0.13 * t)).astype(np.complex64)
cfg = PSDGridConfig(num_bins=256, time_resolution_ms=0.2)

a = compute_psd_grid(data, SR, cfg)
gpu = CudaPsd(SR, cfg)
b = gpu.run(data)
d = np.abs(a.grid - b.grid)
print(f"shapes {a.grid.shape} vs {b.grid.shape}")
print(f"max|dB diff|={d.max():.4f}  mean={d.mean():.6f}  p99={np.percentile(d,99):.4f}")


def bench(fn, iters=20):
    fn()
    ts = []
    for _ in range(iters):
        t0 = time.perf_counter(); fn(); ts.append((time.perf_counter() - t0) * 1000)
    return float(np.mean(ts)), float(np.min(ts))


m0, mn0 = bench(lambda: compute_psd_grid(data, SR, cfg), 10)
m1, mn1 = bench(lambda: gpu.run(data), 20)
print(f"CPU (current) : mean {m0:7.1f} ms  min {mn0:7.1f} ms")
print(f"CUDA native   : mean {m1:7.1f} ms  min {mn1:7.1f} ms   speedup {m0/m1:.1f}x")
