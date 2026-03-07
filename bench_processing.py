"""Benchmark each stage of the processing pipeline to find bottlenecks."""

import time
import numpy as np

from rfobserver.processing.iq_utils import calculate_iq_statistics, convert_bytes_to_complex
from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid, compute_summary_psd
from rfobserver.processing.burst import BurstDetectionConfig, detect_bursts
from datetime import datetime, timezone

SAMPLE_RATE = 56_000_000
DURATION_SEC = 0.5
NUM_SAMPLES = int(SAMPLE_RATE * DURATION_SEC)
NUM_FFT_BINS = 256
TIME_RES_MS = 0.2
RUNS = 20

print(f"Samples: {NUM_SAMPLES:,} ({DURATION_SEC}s @ {SAMPLE_RATE/1e6:.0f} Msps)")
print(f"FFT bins: {NUM_FFT_BINS}, time resolution: {TIME_RES_MS} ms")
print(f"Runs: {RUNS}")
print()

# Generate mock IQ data once
rng = np.random.default_rng(42)
noise_i = rng.integers(-500, 500, size=NUM_SAMPLES, dtype=np.int16)
noise_q = rng.integers(-500, 500, size=NUM_SAMPLES, dtype=np.int16)
iq = np.empty(NUM_SAMPLES * 2, dtype=np.int16)
iq[0::2] = noise_i
iq[1::2] = noise_q
iq_bytes = iq.tobytes()

print(f"IQ data size: {len(iq_bytes) / 1e6:.1f} MB")
print("-" * 60)

# Warm up
data = convert_bytes_to_complex(iq_bytes)
_ = calculate_iq_statistics(data)
config = PSDGridConfig(num_bins=NUM_FFT_BINS, time_resolution_ms=TIME_RES_MS)
grid = compute_psd_grid(data, SAMPLE_RATE, config=config)

times = {
    "convert_bytes_to_complex": [],
    "calculate_iq_statistics": [],
    "compute_psd_grid": [],
    "compute_summary_psd": [],
    "detect_bursts": [],
    "total": [],
}

for i in range(RUNS):
    t_total = time.monotonic()

    t0 = time.monotonic()
    data = convert_bytes_to_complex(iq_bytes)
    times["convert_bytes_to_complex"].append((time.monotonic() - t0) * 1000)

    t0 = time.monotonic()
    iq_stats = calculate_iq_statistics(data)
    times["calculate_iq_statistics"].append((time.monotonic() - t0) * 1000)

    t0 = time.monotonic()
    config = PSDGridConfig(num_bins=NUM_FFT_BINS, time_resolution_ms=TIME_RES_MS)
    psd_grid = compute_psd_grid(data, SAMPLE_RATE, config=config)
    times["compute_psd_grid"].append((time.monotonic() - t0) * 1000)

    t0 = time.monotonic()
    summary_psd = compute_summary_psd(psd_grid, 915_000_000, SAMPLE_RATE)
    times["compute_summary_psd"].append((time.monotonic() - t0) * 1000)

    t0 = time.monotonic()
    burst_config = BurstDetectionConfig(
        threshold_high_db=10.0,
        threshold_low_ratio=0.6,
        merge_freq_bins=5,
        merge_time_sec=0.003,
    )
    detection_result = detect_bursts(
        psd_grid,
        config=burst_config,
        center_freq_hz=915_000_000.0,
        capture_time=datetime.now(timezone.utc),
    )
    times["detect_bursts"].append((time.monotonic() - t0) * 1000)

    times["total"].append((time.monotonic() - t_total) * 1000)

# Print results
print(f"\n{'Stage':<30} {'Mean':>8} {'Min':>8} {'Max':>8} {'Std':>8}  {'%':>5}")
print("-" * 75)
total_mean = np.mean(times["total"])
for name, vals in times.items():
    arr = np.array(vals)
    pct = (np.mean(arr) / total_mean * 100) if name != "total" else 100.0
    print(f"{name:<30} {np.mean(arr):>7.1f}ms {np.min(arr):>7.1f}ms {np.max(arr):>7.1f}ms {np.std(arr):>7.1f}ms {pct:>5.1f}%")

print(f"\nTarget: < {DURATION_SEC * 1000:.0f} ms (capture duration)")
print(f"Headroom: {DURATION_SEC * 1000 - total_mean:.0f} ms" if total_mean < DURATION_SEC * 1000 else f"Excess: {total_mean - DURATION_SEC * 1000:.0f} ms")
