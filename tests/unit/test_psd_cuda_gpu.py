"""The GPU PSD path against the CPU one. Skipped unless the GPU library is built
(deploy/build_psd_cuda.sh) and a CUDA device is present, i.e. on a Jetson."""

from __future__ import annotations

import logging
import threading

import numpy as np
import pytest

from rfobserver.processing import psd_cuda, spectral
from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid
from tests.unit.test_recording_gaps import _proc

pytestmark = pytest.mark.skipif(
    not psd_cuda.cuda_available(), reason=f"GPU PSD unavailable: {psd_cuda.unavailable_reason()}"
)

# Both paths FFT in float32 (scipy keeps complex64 input in single precision;
# cuFFT C2C is float32), with different algorithms, so they agree to rounding
# near the top of each row. Bins far below the row's peak sit in the float32
# FFT's rounding noise and differ more: on an Orin Nano, a one-FFT-per-row grid
# of a strong tone differed by 0.02 dB at a bin 105 dB down, and by at most
# 0.0007 dB within 60 dB of the peak.
TOL_DB = 2e-3  # within NEAR_PEAK_DB of the row's peak
NEAR_PEAK_DB = 60.0
TOL_FAR_DB = 0.1  # anywhere


def _assert_close(gpu: np.ndarray, cpu: np.ndarray) -> None:
    below_peak = cpu.max(axis=1, keepdims=True) - cpu
    diff = np.abs(gpu - cpu)
    assert diff[below_peak <= NEAR_PEAK_DB].max() < TOL_DB
    assert diff.max() < TOL_FAR_DB


@pytest.fixture(autouse=True)
def _fresh_backend_state():
    psd_cuda.reset()
    spectral._gpu_notes.clear()
    yield
    psd_cuda.reset()


def _signal(n: int, seed: int = 0) -> np.ndarray:
    """Noise plus a strong tone, so the grid spans a wide dynamic range."""
    rng = np.random.default_rng(seed)
    t = np.arange(n)
    x = (rng.standard_normal(n) + 1j * rng.standard_normal(n)) * 0.01
    x += 0.5 * np.exp(2j * np.pi * 0.13 * t)
    return x.astype(np.complex64)


def _both(data, sr, **cfg):
    cpu = compute_psd_grid(data, sr, PSDGridConfig(backend="cpu", num_workers=1, **cfg))
    gpu = compute_psd_grid(data, sr, PSDGridConfig(backend="cuda", **cfg))
    return cpu, gpu


@pytest.mark.parametrize(
    ("n", "sr", "bins", "res_ms", "overlap"),
    [
        (1_024_000, 26_000_000, 2048, 0.2, 0.5),  # a production streaming chunk
        (500_000, 56_000_000, 256, 0.2, 0.5),
        (300_000, 10_000_000, 1024, 1.0, 0.75),
        (400_000, 26_000_000, 4096, 0.5, 0.5),  # more bins than threads per block
        (77_777, 2_000_000, 512, 0.5, 0.5),  # leftover samples past the last slice
    ],
)
def test_gpu_grid_matches_cpu(n, sr, bins, res_ms, overlap):
    cpu, gpu = _both(_signal(n), sr, num_bins=bins, time_resolution_ms=res_ms, overlap=overlap)
    assert gpu.grid.shape == cpu.grid.shape and gpu.grid.dtype == np.float32
    _assert_close(gpu.grid, cpu.grid)
    assert np.array_equal(gpu.time_axis, cpu.time_axis)
    assert np.array_equal(gpu.freq_axis, cpu.freq_axis)
    assert (gpu.ffts_per_slice, gpu.total_ffts) == (cpu.ffts_per_slice, cpu.total_ffts)


@pytest.mark.filterwarnings("ignore:divide by zero encountered in log10")  # the CPU's -inf
def test_silence_hits_the_same_floor():
    cpu, gpu = _both(np.zeros(100_000, np.complex64), 1_000_000, num_bins=256)
    assert np.all(cpu.grid == -200.0) and np.all(gpu.grid == -200.0)


def test_in_place_pinned_input_matches_the_copy_path():
    data = _signal(1_024_000)
    cfg = PSDGridConfig(num_bins=2048, time_resolution_ms=0.2, backend="cuda")
    copied = compute_psd_grid(data, 26_000_000, cfg)
    with psd_cuda.pinned_complex64(len(data)) as buf:
        assert buf is not None
        buf[:] = data
        assert psd_cuda._pool.owns(buf, len(data))
        in_place = compute_psd_grid(buf, 26_000_000, cfg)
    assert np.array_equal(in_place.grid, copied.grid)


@pytest.mark.parametrize("kind", ["complex128", "strided"])
def test_inputs_needing_conversion(kind):
    base = _signal(400_000)
    data = base.astype(np.complex128) if kind == "complex128" else np.repeat(base, 2)[::2]
    assert kind != "strided" or not data.flags.c_contiguous
    cpu = compute_psd_grid(base, 1_000_000, PSDGridConfig(num_bins=256, backend="cpu"))
    gpu = compute_psd_grid(data, 1_000_000, PSDGridConfig(num_bins=256, backend="cuda"))
    _assert_close(gpu.grid, cpu.grid)


def test_short_input_uses_the_cpu_quietly(caplog):
    data = _signal(3000)
    with caplog.at_level(logging.WARNING):
        cpu, gpu = _both(data, 26_000_000, num_bins=2048, time_resolution_ms=0.2)
    assert np.array_equal(gpu.grid, cpu.grid)
    assert not caplog.records


def test_concurrent_callers_get_their_own_grids():
    inputs = [_signal(1_024_000, seed=s) for s in range(4)]
    cfg = PSDGridConfig(num_bins=2048, time_resolution_ms=0.2, backend="cuda")
    # The GPU path is deterministic (same plan, no atomics), so each thread
    # must reproduce its serial result bit for bit; any cross-talk shows.
    expected = [compute_psd_grid(x, 26_000_000, cfg).grid for x in inputs]
    errors: list[str] = []

    def worker(i: int) -> None:
        for _ in range(5):
            got = compute_psd_grid(inputs[i], 26_000_000, cfg)
            if not np.array_equal(got.grid, expected[i]):
                errors.append(f"thread {i} got another grid")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []


def test_engine_cache_stays_bounded_and_correct():
    data = _signal(300_000)
    bins = [256, 512, 1024, 2048, 4096, 128]
    assert len(bins) > psd_cuda.MAX_ENGINES
    for b in bins + bins[:1]:  # the first geometry again, after it was evicted
        cpu, gpu = _both(data, 10_000_000, num_bins=b, time_resolution_ms=1.0)
        _assert_close(gpu.grid, cpu.grid)
    assert len(psd_cuda._engines) <= psd_cuda.MAX_ENGINES


def test_pinned_pool_runs_out_then_recovers():
    held = []
    try:
        for _ in range(psd_cuda.MAX_PINNED):
            cm = psd_cuda.pinned_complex64(4096)
            buf = cm.__enter__()
            assert buf is not None
            held.append(cm)
        with psd_cuda.pinned_complex64(4096) as extra:
            assert extra is None
    finally:
        for cm in held:
            cm.__exit__(None, None, None)
    with psd_cuda.pinned_complex64(4096) as again:
        assert again is not None


def test_streaming_chunk_on_the_gpu_matches_cpu(tmp_path):
    (tmp_path / "cpu").mkdir()
    (tmp_path / "cuda").mkdir()
    cpu_proc = _proc(tmp_path / "cpu")
    gpu_proc = _proc(tmp_path / "cuda", PSD_BACKEND="cuda")
    buf = np.random.default_rng(3).integers(
        -(2**31), 2**31 - 1, cpu_proc._chunk_samples, dtype=np.int32
    )
    a = cpu_proc._process_one_chunk(buf, 0.0, 0, 0, 915_000_000, cpu_proc._make_grid_config())
    b = gpu_proc._process_one_chunk(buf, 0.0, 0, 0, 915_000_000, gpu_proc._make_grid_config())
    _assert_close(b.psd_grid.grid, a.psd_grid.grid)
    assert a.iq_stats == b.iq_stats
