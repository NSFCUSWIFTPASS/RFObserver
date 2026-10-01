"""Production geometry: 26 Msps, 2048 bins, 0.2 ms, 200 slices/chunk, CPU FFT workers=1."""
import ctypes as C, os, time, numpy as np
from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid
from rfobserver.processing.iq_utils import convert_sc16_to_complex, moments_from_iq
here = os.path.dirname(os.path.abspath(__file__))
lib = C.CDLL(os.path.join(here, "libpsdcuda2.so"))
lib.psd_init.restype = C.c_void_p
lib.psd_init.argtypes = [C.c_int]*6 + [C.c_float, C.POINTER(C.c_float)]
lib.psd_buffer.restype = C.c_void_p; lib.psd_buffer.argtypes = [C.c_void_p, C.c_int]
lib.psd_run2.restype = C.c_int
lib.psd_run2.argtypes = [C.c_void_p, C.c_int, C.c_int] + [C.POINTER(C.c_float)]*3
FP = C.POINTER(C.c_float)
SR = 26_000_000; NB = 2048
cfg = PSDGridConfig(num_bins=NB, time_resolution_ms=0.2, num_workers=1)
hop = NB // 2; ss = max(int(SR*0.2/1000), NB); fps = max(1, (ss-NB)//hop+1); ass = NB+(fps-1)*hop
n = 200 * ass
rng = np.random.default_rng(0)
sc16 = rng.integers(-3000, 3000, 2*n, dtype=np.int16).view(np.int32)
data = convert_sc16_to_complex(sc16)
print(f"chunk n={n:,} ({n/SR*1000:.1f} ms of signal), fps={fps}, ffts/chunk={200*fps}")
def bench(fn, it=100):
    fn(); ts=[]
    for _ in range(it):
        t0=time.perf_counter(); fn(); ts.append((time.perf_counter()-t0)*1000)
    return np.mean(ts), np.percentile(ts, 99)
hann = np.hanning(NB).astype(np.float32)
wn = float(1.0/(SR*np.sum(np.abs(np.hanning(NB).astype(np.complex64))**2)))
ctx = lib.psd_init(NB, hop, fps, 200, ass, n, C.c_float(wn), hann.ctypes.data_as(FP)); assert ctx
pin = np.ctypeslib.as_array((C.c_byte*(n*8)).from_address(lib.psd_buffer(ctx,1))).view(np.complex64)
pout = np.ctypeslib.as_array((C.c_byte*(200*NB*4)).from_address(lib.psd_buffer(ctx,2))).view(np.float32)
np.copyto(pin, data); out = np.empty(200*NB, np.float32); t = np.zeros(6, np.float32)
ref = compute_psd_grid(data, SR, cfg).grid
def gpu(mode):
    lib.psd_run2(ctx, mode, 1, (data if mode==0 else pin).ctypes.data_as(FP), out.ctypes.data_as(FP), t.ctypes.data_as(FP))
    return (out if mode==0 else pout).reshape(200, NB).copy()
for mode in (0, 2):
    print(f"  gpu mode {mode} max|diff| = {np.max(np.abs(gpu(mode)-ref)):.4f} dB")
print("per-chunk mean / p99 (ms):")
for lbl, fn in (("convert_sc16_to_complex", lambda: convert_sc16_to_complex(sc16)),
                ("CPU compute_psd_grid (1 thread)", lambda: compute_psd_grid(data, SR, cfg)),
                ("GPU pageable+memcpy", lambda: gpu(0)),
                ("GPU zero-copy", lambda: gpu(2)),
                ("moments_from_iq", lambda: moments_from_iq(data))):
    m, p = bench(fn); print(f"  {lbl:32s} {m:7.2f}  {p:7.2f}")
