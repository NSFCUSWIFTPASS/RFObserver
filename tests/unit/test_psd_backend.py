"""PSD backend selection and the CPU fallback. Runs without CUDA: the GPU path
itself is tested in test_psd_cuda_gpu.py, on machines that have it."""

from __future__ import annotations

import logging

import numpy as np
import pytest

from rfobserver.processing import psd_cuda, spectral
from rfobserver.processing.spectral import PSDGridConfig, compute_psd_grid, grid_geometry
from tests.unit.test_recording_gaps import _proc


@pytest.fixture(autouse=True)
def _fresh_backend_state(monkeypatch, tmp_path):
    """Point the loader at a library that does not exist, and forget what an
    earlier test loaded or logged."""
    monkeypatch.setenv(psd_cuda.LIB_ENV, str(tmp_path / "missing" / "libpsdcuda.so"))
    psd_cuda.reset()
    spectral._gpu_notes.clear()
    yield
    psd_cuda.reset()
    spectral._gpu_notes.clear()


def _noise(n: int, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return (rng.standard_normal(n) + 1j * rng.standard_normal(n)).astype(np.complex64)


def _cfg(backend: str) -> PSDGridConfig:
    return PSDGridConfig(num_bins=256, time_resolution_ms=1.0, num_workers=1, backend=backend)


def test_default_backend_is_cpu():
    assert PSDGridConfig().backend == "cpu"


def test_unknown_backend_is_rejected():
    with pytest.raises(ValueError, match="backend"):
        PSDGridConfig(backend="opencl")


def test_geometry_of_a_production_chunk():
    # 26 Msps, 2048 bins, 0.2 ms rows, 200 rows per streaming chunk.
    cfg = PSDGridConfig(num_bins=2048, time_resolution_ms=0.2)
    g = grid_geometry(200 * 5120, 26_000_000, cfg)
    assert (g.nperseg, g.hop, g.ffts_per_slice, g.actual_slice_samples) == (2048, 1024, 4, 5120)
    assert g.n_slices == 200 and g.usable_samples == 1_024_000 and not g.short_input


def test_geometry_flags_input_shorter_than_one_slice():
    g = grid_geometry(3000, 26_000_000, PSDGridConfig(num_bins=2048, time_resolution_ms=0.2))
    assert g.short_input and g.n_slices == 1 and g.actual_slice_samples == 3000


def test_cuda_without_the_library_falls_back_to_the_identical_cpu_grid(caplog):
    data = _noise(200_000)
    cpu = compute_psd_grid(data, 1_000_000, _cfg("cpu"))
    with caplog.at_level(logging.WARNING, logger="rfobserver.processing.spectral"):
        first = compute_psd_grid(data, 1_000_000, _cfg("cuda"))
        second = compute_psd_grid(data, 1_000_000, _cfg("cuda"))
    for got in (first, second):
        assert np.array_equal(got.grid, cpu.grid)
        assert np.array_equal(got.time_axis, cpu.time_axis)
        assert np.array_equal(got.freq_axis, cpu.freq_axis)
    notes = [r for r in caplog.records if "unavailable" in r.getMessage()]
    assert len(notes) == 1  # once per process, not once per chunk
    assert "build_psd_cuda.sh" in notes[0].getMessage()


def test_unavailable_reason_names_the_missing_library():
    assert not psd_cuda.cuda_available()
    reason = psd_cuda.unavailable_reason()
    assert reason is not None and "not found" in reason


def test_a_gpu_failure_falls_back_to_the_cpu_and_is_logged_once(monkeypatch, caplog):
    calls = []

    def broken(*args, **kwargs):
        calls.append(1)
        raise RuntimeError("psd_run failed: an illegal memory access was encountered")

    monkeypatch.setattr(psd_cuda, "compute_psd_grid_cuda", broken)
    data = _noise(200_000)
    cpu = compute_psd_grid(data, 1_000_000, _cfg("cpu"))
    with caplog.at_level(logging.WARNING, logger="rfobserver.processing.spectral"):
        for _ in range(3):
            assert np.array_equal(compute_psd_grid(data, 1_000_000, _cfg("cuda")).grid, cpu.grid)
    assert len(calls) == 3
    failures = [r for r in caplog.records if "GPU PSD failed" in r.getMessage()]
    assert len(failures) == 1 and failures[0].exc_info is not None


def test_pinned_buffer_is_none_without_the_gpu():
    with psd_cuda.pinned_complex64(1024) as buf:
        assert buf is None


def test_streaming_chunk_with_cuda_requested_matches_cpu_without_a_gpu(tmp_path):
    (tmp_path / "cpu").mkdir()
    (tmp_path / "cuda").mkdir()
    cpu_proc = _proc(tmp_path / "cpu")
    cuda_proc = _proc(tmp_path / "cuda", PSD_BACKEND="cuda")
    assert cuda_proc._make_grid_config().backend == "cuda"
    buf = np.random.default_rng(7).integers(
        -(2**31), 2**31 - 1, cpu_proc._chunk_samples, dtype=np.int32
    )
    a = cpu_proc._process_one_chunk(buf, 0.0, 0, 0, 915_000_000, cpu_proc._make_grid_config())
    b = cuda_proc._process_one_chunk(buf, 0.0, 0, 0, 915_000_000, cuda_proc._make_grid_config())
    assert np.array_equal(a.psd_grid.grid, b.psd_grid.grid)
    assert a.iq_stats == b.iq_stats
