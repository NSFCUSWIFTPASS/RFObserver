"""Corrected hot-path profile.

Measurement trap fixed: cProfile 'tottime' on a Python frame INCLUDES the time
of inline numpy operators (arr*arr, arr.real=..., arr[::k]) and indexing, because
operator dispatch is not a separate profiled call. So a frame showing high
'python' self-time is usually running native array kernels, not interpreter code.

To separate the two we report, per stage:
  - ms/call (wall)
  - python-level CALLS per invocation  (a direct proxy for interpreter dispatch;
    if this is tiny while ms/call is large, the interpreter is NOT the cost)
  - whether the work is fundamentally memory-bandwidth bound (bytes touched)
"""
from __future__ import annotations

import cProfile
import pstats
import numpy as np

from rfobserver.processing.iq_utils import calculate_iq_statistics, convert_bytes_to_complex
from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid
from rfobserver.processing.burst import BurstDetectionConfig, detect_bursts
from rfobserver.processing.channelize import channelize_to_cs16

SR = 26_000_000
rng = np.random.default_rng(0)

win_n = int(SR * 0.5)
iq16 = np.empty(win_n * 2, dtype=np.int16)
iq16[0::2] = rng.integers(-500, 500, win_n, dtype=np.int16)
iq16[1::2] = rng.integers(-500, 500, win_n, dtype=np.int16)
win_bytes = iq16.tobytes()
win_c = convert_bytes_to_complex(win_bytes)

burst_n = int(SR * 0.010)
burst = (rng.standard_normal(burst_n) + 1j * rng.standard_normal(burst_n)).astype(np.complex64)

# trigger path: replicate _check_power_above_threshold exactly on a 0.1s chunk
chunk_n = int(SR * 0.1)
chunk_sc16 = np.empty(chunk_n, dtype=np.int32)
chunk_sc16[:] = rng.integers(-(1 << 20), 1 << 20, chunk_n, dtype=np.int32)


def trigger_check(sc16_buf):
    raw16 = sc16_buf.view(np.int16).reshape(-1, 2)
    step = max(1, len(raw16) // 4096)
    sub = raw16[::step].astype(np.float32) / 32768.0
    power_sq = sub[:, 0] ** 2 + sub[:, 1] ** 2
    return float(10.0 * np.log10(np.mean(power_sq) / 50.0 + 1e-30))


def measure(label, fn, iters, bytes_touched):
    fn()  # warm
    pr = cProfile.Profile()
    pr.enable()
    for _ in range(iters):
        fn()
    pr.disable()
    st = pstats.Stats(pr)
    py_calls = 0
    ms = 0.0
    top = []
    for func, (cc, nc, tt, ct, callers) in st.stats.items():
        name = func[2]
        # count only genuine python frames (have a real filename+lineno), not
        # the ~:0(<built-in ...>) native entries
        if func[0] != "~":
            py_calls += nc
        top.append((tt, func[0] != "~", f"{func[0].split('/')[-1]}:{func[1]}({name})", nc))
    ms = st.total_tt / iters * 1000
    top.sort(reverse=True)
    bw = bytes_touched / (ms / 1000) / 1e9  # GB/s implied if memory-bound
    print(f"\n=== {label} ===")
    print(f"  {ms:7.2f} ms/call   python-fn calls/invocation: {py_calls/iters:6.0f}"
          f"   >={bytes_touched/1e6:.0f} MB touched  (~{bw:.1f} GB/s)")
    for tt, is_py, nm, nc in top[:5]:
        kind = "py-frame" if is_py else "native  "
        print(f"     {tt/iters*1000:7.2f} ms  {kind} x{nc//iters:<4d} {nm}")


cfg = PSDGridConfig(num_bins=256, time_resolution_ms=0.2)
bcfg = BurstDetectionConfig()
grid = compute_psd_grid(win_c, SR, config=cfg)

# bytes_touched = rough lower bound of memory the op must read+write
measure("IQ trigger check (0.1s chunk, subsampled)", lambda: trigger_check(chunk_sc16), 200,
        chunk_n * 4)  # reads the int32 buffer view
measure("convert_bytes_to_complex (0.5s win)", lambda: convert_bytes_to_complex(win_bytes), 20,
        win_n * 4 + win_n * 8)  # read i16 pair + write complex64
measure("calculate_iq_statistics (0.5s win)", lambda: calculate_iq_statistics(win_c), 20,
        win_n * 8 * 3)  # full-res max makes ~3 passes over complex64
measure("compute_psd_grid (0.5s win)", lambda: compute_psd_grid(win_c, SR, config=cfg), 10,
        win_n * 8 * 4)
measure("detect_bursts (grid)", lambda: detect_bursts(grid, bcfg, SR, 915e6), 20,
        grid.grid.nbytes)
measure("channelize_to_cs16 (10ms burst)",
        lambda: channelize_to_cs16(burst, SR, 3.0e6, 1_600_000), 50,
        burst_n * 8 * 4)
