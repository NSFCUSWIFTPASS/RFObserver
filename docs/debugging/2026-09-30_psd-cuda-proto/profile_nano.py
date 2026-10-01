"""Hot-path profile, import-tolerant (nano-super may lack the channelizer)."""
from __future__ import annotations
import cProfile, pstats, numpy as np

SR = 26_000_000
rng = np.random.default_rng(0)
win_n = int(SR * 0.5)
iq16 = np.empty(win_n * 2, dtype=np.int16)
iq16[0::2] = rng.integers(-500, 500, win_n, dtype=np.int16)
iq16[1::2] = rng.integers(-500, 500, win_n, dtype=np.int16)
win_bytes = iq16.tobytes()

from rfobserver.processing.iq_utils import calculate_iq_statistics, convert_bytes_to_complex
win_c = convert_bytes_to_complex(win_bytes)
chunk_n = int(SR * 0.1)
chunk_sc16 = rng.integers(-(1 << 20), 1 << 20, chunk_n, dtype=np.int32)

def trigger_check(b):
    r = b.view(np.int16).reshape(-1, 2); step = max(1, len(r)//4096)
    s = r[::step].astype(np.float32)/32768.0
    return float(10.0*np.log10(np.mean(s[:,0]**2+s[:,1]**2)/50.0+1e-30))

def measure(label, fn, iters, bytes_touched):
    try:
        fn()
    except Exception as e:
        print(f"\n=== {label} ===\n  SKIPPED: {e!r}"); return
    pr = cProfile.Profile(); pr.enable()
    for _ in range(iters): fn()
    pr.disable(); st = pstats.Stats(pr)
    py_calls = 0; top = []
    for func,(cc,nc,tt,ct,cr) in st.stats.items():
        if func[0] != "~": py_calls += nc
        top.append((tt, func[0] != "~", f"{func[0].split('/')[-1]}:{func[1]}({func[2]})", nc))
    ms = st.total_tt/iters*1000; bw = bytes_touched/(ms/1000)/1e9
    top.sort(reverse=True)
    print(f"\n=== {label} ===")
    print(f"  {ms:8.2f} ms/call   py-calls/inv: {py_calls/iters:6.0f}   >={bytes_touched/1e6:.0f} MB  (~{bw:.1f} GB/s)")
    for tt,is_py,nm,nc in top[:4]:
        print(f"     {tt/iters*1000:8.2f} ms  {'py' if is_py else 'C ':2s} x{nc//max(iters,1):<4d} {nm}")

try:
    from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid
    cfg = PSDGridConfig(num_bins=256, time_resolution_ms=0.2)
    grid = compute_psd_grid(win_c, SR, config=cfg); HAVE_PSD=True
except Exception as e:
    print("psd import failed:", repr(e)); HAVE_PSD=False
try:
    from rfobserver.processing.burst import BurstDetectionConfig, detect_bursts
    bcfg = BurstDetectionConfig(); HAVE_DET=True
except Exception as e:
    print("burst import failed:", repr(e)); HAVE_DET=False
try:
    from rfobserver.processing.channelize import channelize_to_cs16
    bn = int(SR*0.010)
    burst = (rng.standard_normal(bn)+1j*rng.standard_normal(bn)).astype(np.complex64)
    HAVE_CH=True
except Exception as e:
    print("channelize import failed (expected on this branch):", repr(e)); HAVE_CH=False

measure("IQ trigger check (0.1s chunk)", lambda: trigger_check(chunk_sc16), 200, chunk_n*4)
measure("convert_bytes_to_complex (0.5s)", lambda: convert_bytes_to_complex(win_bytes), 20, win_n*4+win_n*8)
measure("calculate_iq_statistics (0.5s)", lambda: calculate_iq_statistics(win_c), 20, win_n*8*3)
if HAVE_PSD: measure("compute_psd_grid (0.5s)", lambda: compute_psd_grid(win_c, SR, config=cfg), 10, win_n*8*4)
if HAVE_DET and HAVE_PSD: measure("detect_bursts (grid)", lambda: detect_bursts(grid, bcfg, SR, 915e6), 20, grid.grid.nbytes)
if HAVE_CH: measure("channelize_to_cs16 (10ms burst)", lambda: channelize_to_cs16(burst, SR, 3.0e6, 1_600_000), 50, bn*8*4)
