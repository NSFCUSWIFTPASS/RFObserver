"""Per-chunk worker cost: zero-copy (convert into pinned, GPU reads in place,
moments read pinned) vs copy path (normal numpy everywhere, GPU copies in)."""
import time, numpy as np
from rfobserver.processing import psd_cuda
from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid
from rfobserver.processing.iq_utils import convert_sc16_to_complex, moments_from_iq
SR = 26_000_000; n = 1_024_000
cfg = PSDGridConfig(num_bins=2048, time_resolution_ms=0.2, backend="cuda")
sc16 = np.random.default_rng(0).integers(-(2**31), 2**31 - 1, n, dtype=np.int32)
def t(fn, it=200):
    fn(); ts = []
    for _ in range(it):
        a = time.perf_counter(); fn(); ts.append((time.perf_counter() - a) * 1000)
    return np.median(ts)
plain = np.empty(n, np.complex64)
with psd_cuda.pinned_complex64(n) as pin:
    for label, buf in (("numpy (copy path)", plain), ("pinned (zero-copy)", pin)):
        convert_sc16_to_complex(sc16, out=buf)
        c = t(lambda: convert_sc16_to_complex(sc16, out=buf))
        p = t(lambda: compute_psd_grid(buf, SR, cfg))
        m = t(lambda: moments_from_iq(buf))
        print(f"{label:20s} convert {c:5.1f}  psd {p:5.1f}  moments {m:5.1f}  sum {c+p+m:5.1f} ms (median)")
