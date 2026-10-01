"""Per-chunk order as the pipeline runs it: convert -> psd -> moments, same buffer."""
import time, numpy as np
from rfobserver.processing import psd_cuda
from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid
from rfobserver.processing.iq_utils import convert_sc16_to_complex, moments_from_iq
SR = 26_000_000; n = 1_024_000
cfg = PSDGridConfig(num_bins=2048, time_resolution_ms=0.2, backend="cuda")
sc16 = np.random.default_rng(0).integers(-(2**31), 2**31 - 1, n, dtype=np.int32)
plain = np.empty(n, np.complex64)
with psd_cuda.pinned_complex64(n) as pin:
    for label, buf in (("numpy (copy path)", plain), ("pinned (zero-copy)", pin)):
        acc = np.zeros(3); N = 200
        for i in range(N + 5):
            a = time.perf_counter(); convert_sc16_to_complex(sc16, out=buf)
            b = time.perf_counter(); compute_psd_grid(buf, SR, cfg)
            c = time.perf_counter(); moments_from_iq(buf)
            d = time.perf_counter()
            if i >= 5: acc += (b - a, c - b, d - c)
        acc = acc / N * 1000
        print(f"{label:20s} convert {acc[0]:5.1f}  psd {acc[1]:5.1f}  moments {acc[2]:5.1f}  sum {acc.sum():5.1f} ms (mean)")
