"""CUDA PSD v2: memory modes x kernel sets, equivalence, stage timing, and the
cost of the CPU producer writing straight into GPU-visible memory."""
from __future__ import annotations
import ctypes as C, os, time, numpy as np
from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid
from rfobserver.processing.iq_utils import convert_sc16_to_complex

here = os.path.dirname(os.path.abspath(__file__))
lib = C.CDLL(os.path.join(here, "libpsdcuda2.so"))
lib.psd_init.restype = C.c_void_p
lib.psd_init.argtypes = [C.c_int]*6 + [C.c_float, C.POINTER(C.c_float)]
lib.psd_buffer.restype = C.c_void_p
lib.psd_buffer.argtypes = [C.c_void_p, C.c_int]
lib.psd_run2.restype = C.c_int
lib.psd_run2.argtypes = [C.c_void_p, C.c_int, C.c_int, C.POINTER(C.c_float),
                         C.POINTER(C.c_float), C.POINTER(C.c_float)]
FP = C.POINTER(C.c_float)

SR = 26_000_000
cfg = PSDGridConfig(num_bins=256, time_resolution_ms=0.2)
nperseg = cfg.num_bins; hop = int(nperseg * (1 - cfg.overlap))
ss = max(int(SR * cfg.time_resolution_ms / 1000.0), nperseg)
fps = max(1, (ss - nperseg) // hop + 1); ass = nperseg + (fps - 1) * hop

rng = np.random.default_rng(0)
n = int(SR * 0.5)
sc16 = np.empty(n * 2, dtype=np.int16)
sc16[0::2] = rng.integers(-3000, 3000, n, dtype=np.int16)
sc16[1::2] = rng.integers(-3000, 3000, n, dtype=np.int16)
t_ = np.arange(n)
tone = (np.round(8000 * np.cos(2*np.pi*0.13*t_)).astype(np.int16),
        np.round(8000 * np.sin(2*np.pi*0.13*t_)).astype(np.int16))
sc16[0::2] += tone[0]; sc16[1::2] += tone[1]
sc16 = sc16.view(np.int32)
data = convert_sc16_to_complex(sc16)

n_slices = n // ass; usable = n_slices * ass
hann = np.hanning(nperseg).astype(np.float32)
wn = float(1.0 / (SR * np.sum(hann.astype(np.float64) ** 2)))
ctx = lib.psd_init(nperseg, hop, fps, n_slices, ass, usable, C.c_float(wn), hann.ctypes.data_as(FP))
assert ctx, "psd_init failed"

def view(which, dtype, count):
    p = lib.psd_buffer(ctx, which)
    return np.ctypeslib.as_array((C.c_byte * (count * np.dtype(dtype).itemsize)).from_address(p)).view(dtype)

pin_in = view(1, np.complex64, usable); pin_out = view(2, np.float32, n_slices * nperseg)
man_in = view(3, np.complex64, usable); man_out = view(4, np.float32, n_slices * nperseg)
np.copyto(pin_in, data[:usable]); np.copyto(man_in, data[:usable])
d_pageable = np.ascontiguousarray(data[:usable])
h_out = np.empty(n_slices * nperseg, dtype=np.float32)

ref = compute_psd_grid(data, SR, cfg).grid

names = {0: "pageable+memcpy", 1: "pinned+memcpy", 2: "zero-copy mapped", 3: "managed"}
t = np.zeros(6, dtype=np.float32)
print(f"{'mode':18s} {'kern':4s} {'maxdiff':>8s} {'wall ms':>8s} | {'in':>6s} {'extr':>6s} {'fft':>6s} {'pm':>6s} {'db':>5s} {'out':>5s}")
for mode in (0, 1, 2, 3):
    for opt in (0, 1):
        src = d_pageable if mode == 0 else pin_in
        outbuf = {0: h_out, 1: h_out, 2: pin_out, 3: man_out}[mode]
        def call():
            rc = lib.psd_run2(ctx, mode, opt, src.ctypes.data_as(FP), h_out.ctypes.data_as(FP),
                              t.ctypes.data_as(FP))
            assert rc == 0, rc
            return outbuf.reshape(n_slices, nperseg).copy()   # result handed to the caller
        g = call()
        diff = float(np.max(np.abs(g - ref)))
        acc = np.zeros(6); walls = []
        for _ in range(30):
            t0 = time.perf_counter(); call(); walls.append((time.perf_counter() - t0) * 1000); acc += t
        acc /= 30
        print(f"{names[mode]:18s} {'v2' if opt else 'v1':4s} {diff:8.4f} {np.mean(walls):8.1f} | "
              + " ".join(f"{v:6.2f}" for v in acc[:4]) + f" {acc[4]:5.2f} {acc[5]:5.2f}")

print("\nCPU producer: convert_sc16_to_complex(sc16, out=...) into each memory type")
plain = np.empty(usable, dtype=np.complex64)
for label, buf in (("numpy (pageable)", plain), ("pinned mapped", pin_in), ("managed", man_in)):
    convert_sc16_to_complex(sc16[:usable], out=buf)
    ts = []
    for _ in range(10):
        t0 = time.perf_counter(); convert_sc16_to_complex(sc16[:usable], out=buf); ts.append((time.perf_counter()-t0)*1000)
    print(f"  {label:18s} {np.mean(ts):7.1f} ms")

t0 = time.perf_counter()
for _ in range(10): compute_psd_grid(data, SR, cfg)
print(f"\nCPU compute_psd_grid baseline: {(time.perf_counter()-t0)/10*1000:.1f} ms")
