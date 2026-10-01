"""GPU backend for compute_psd_grid (PSDGridConfig.backend == "cuda").

The kernels are in cuda/psd_cuda.cu, built by deploy/build_psd_cuda.sh into
cuda/libpsdcuda.so and loaded here through ctypes. Importing this module needs
no CUDA: the library is loaded on first use, and when it, the CUDA runtime or a
GPU is missing, compute_psd_grid_cuda returns None and the caller uses the CPU.

Each grid geometry gets an engine (cuFFT plan and device buffers), created on
first use and reused; calls on one engine are serialized because the GPU-side
buffers are shared. Input is normally copied to the GPU. Input that lives in a
buffer from pinned_complex64() (mapped pinned memory) is read by the kernels in
place: on Jetson the CPU and GPU share DRAM, so this skips the copy, which was
the largest single cost. On an Orin Nano, a 1M-sample chunk (26 Msps, 2048
bins) took 3.0 ms in place, 5.6 ms with the copy and 46 ms on one CPU core
(docs/debugging/2026-09-30_rust-cpp-rewrite-hotpath-profile.md).
"""

from __future__ import annotations

import ctypes
import logging
import os
import threading
from collections import OrderedDict
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np

from rfobserver.processing.spectral import (
    GridGeometry,
    PSDGridResult,
    grid_axes,
    grid_geometry,
    hann_window,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from rfobserver.processing.spectral import PSDGridConfig

logger = logging.getLogger(__name__)

LIB_ENV = "RFOBS_PSD_CUDA_LIB"
LIB_DEFAULT = Path(__file__).parent / "cuda" / "libpsdcuda.so"
# Engines kept at once. There is one per grid geometry, which changes only with
# the settings, so this just bounds what repeated config changes leave behind.
MAX_ENGINES = 4
# Pinned input buffers kept at once (streaming borrows one per busy worker).
MAX_PINNED = 8

_state_lock = threading.Lock()  # guards the module state below
_lib: Any = None
_loaded = False
_reason: str | None = None


def library_path() -> Path:
    """Where the GPU library is loaded from (RFOBS_PSD_CUDA_LIB overrides)."""
    override = os.environ.get(LIB_ENV)
    return Path(override) if override else LIB_DEFAULT


def _declare(lib: Any) -> None:
    c = ctypes
    lib.psd_last_error.restype = c.c_char_p
    lib.psd_last_error.argtypes = []
    lib.psd_device_count.restype = c.c_int
    lib.psd_device_count.argtypes = []
    lib.psd_init.restype = c.c_void_p
    lib.psd_init.argtypes = [c.c_int] * 6 + [c.c_float, c.c_void_p]
    lib.psd_run.restype = c.c_int
    lib.psd_run.argtypes = [c.c_void_p, c.c_void_p, c.c_int, c.c_void_p]
    lib.psd_free.restype = None
    lib.psd_free.argtypes = [c.c_void_p]
    lib.psd_host_alloc.restype = c.c_void_p
    lib.psd_host_alloc.argtypes = [c.c_size_t]
    lib.psd_host_free.restype = None
    lib.psd_host_free.argtypes = [c.c_void_p]


def _last_error(lib: Any) -> str:
    raw = lib.psd_last_error()
    return raw.decode(errors="replace") if raw else "unknown error"


def _library() -> Any:
    """The loaded library, or None (the reason is in unavailable_reason())."""
    global _lib, _loaded, _reason
    with _state_lock:
        if not _loaded:
            _loaded = True
            path = library_path()
            if not path.exists():
                _reason = f"{path} not found (build it with deploy/build_psd_cuda.sh)"
                return None
            try:
                lib = ctypes.CDLL(str(path))
                _declare(lib)
            except (OSError, AttributeError) as exc:
                _reason = f"cannot load {path}: {exc}"
                return None
            if lib.psd_device_count() < 1:
                _reason = "no CUDA device"
                return None
            _lib = lib
            logger.info("GPU PSD backend loaded from %s", path)
        return _lib


def cuda_available() -> bool:
    """True when the GPU library loads and a CUDA device is present."""
    return _library() is not None


def unavailable_reason() -> str | None:
    """Why the GPU cannot be used, or None when it can."""
    _library()
    return _reason


def disable(reason: str) -> None:
    """Stop using the GPU for the rest of the process (after a runtime error)."""
    global _lib, _reason
    with _state_lock:
        _lib = None
        _reason = reason


class _Engine:
    """cuFFT plan and device buffers for one grid geometry."""

    def __init__(self, lib: Any, geometry: GridGeometry, sampling_rate: int) -> None:
        self._lib = lib
        self.geometry = geometry
        hann, window_norm = hann_window(geometry.nperseg, np.complex64, sampling_rate)
        # The kernels apply the real window; keep it alive for psd_init's copy.
        self._hann = np.ascontiguousarray(hann.real, dtype=np.float32)
        self._lock = threading.Lock()
        self._ctx = lib.psd_init(
            geometry.nperseg,
            geometry.hop,
            geometry.ffts_per_slice,
            geometry.n_slices,
            geometry.actual_slice_samples,
            geometry.usable_samples,
            ctypes.c_float(window_norm),
            self._hann.ctypes.data,
        )
        if not self._ctx:
            raise RuntimeError(f"psd_init failed: {_last_error(lib)}")

    def run(self, data: np.ndarray, zero_copy: bool) -> np.ndarray | None:
        """The dB grid, or None if the engine was closed in the meantime."""
        g = self.geometry
        out = np.empty((g.n_slices, g.nperseg), dtype=np.float32)
        with self._lock:
            if not self._ctx:
                return None
            # ctypes releases the GIL for the call, so other workers keep going.
            rc = self._lib.psd_run(self._ctx, data.ctypes.data, int(zero_copy), out.ctypes.data)
            if rc != 0:
                raise RuntimeError(f"psd_run failed: {_last_error(self._lib)}")
        return out

    def close(self) -> None:
        with self._lock:
            if self._ctx:
                self._lib.psd_free(self._ctx)
                self._ctx = None


_engines: OrderedDict[tuple[int, ...], _Engine] = OrderedDict()


def _engine(lib: Any, geometry: GridGeometry, sampling_rate: int) -> _Engine:
    key = (
        geometry.nperseg,
        geometry.hop,
        geometry.ffts_per_slice,
        geometry.actual_slice_samples,
        geometry.n_slices,
        int(sampling_rate),
    )
    with _state_lock:
        engine = _engines.get(key)
        if engine is not None:
            _engines.move_to_end(key)
            return engine
        engine = _Engine(lib, geometry, sampling_rate)
        _engines[key] = engine
        logger.info(
            "GPU PSD engine: %d bins, %d slices x %d FFTs per call",
            geometry.nperseg,
            geometry.n_slices,
            geometry.ffts_per_slice,
        )
        while len(_engines) > MAX_ENGINES:
            _, old = _engines.popitem(last=False)
            old.close()  # waits for a call in progress on it
        return engine


class _PinnedPool:
    """Reusable complex64 buffers in mapped pinned memory."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._free: list[np.ndarray] = []
        self._samples: dict[int, int] = {}  # buffer address -> length in samples
        self._lib: Any = None

    def acquire(self, lib: Any, n: int) -> np.ndarray | None:
        with self._lock:
            for i, buf in enumerate(self._free):
                if len(buf) == n:
                    return self._free.pop(i)
            # Buffers of another length are left over from a settings change.
            for buf in [b for b in self._free if len(b) != n]:
                self._free.remove(buf)
                self._release_memory(buf)
            if len(self._samples) >= MAX_PINNED:
                return None
            ptr = lib.psd_host_alloc(n * np.dtype(np.complex64).itemsize)
            if not ptr:
                logger.warning("Pinned buffer allocation failed: %s", _last_error(lib))
                return None
            self._lib = lib
            raw = (ctypes.c_byte * (n * np.dtype(np.complex64).itemsize)).from_address(ptr)
            buf = np.ctypeslib.as_array(raw).view(np.complex64)
            self._samples[ptr] = n
            return buf

    def release(self, buf: np.ndarray) -> None:
        with self._lock:
            if buf.ctypes.data in self._samples:
                self._free.append(buf)

    def owns(self, data: np.ndarray, n_needed: int) -> bool:
        """True when data starts a pool buffer holding at least n_needed samples."""
        if data.dtype != np.complex64 or not data.flags.c_contiguous:
            return False
        with self._lock:
            return self._samples.get(data.ctypes.data, 0) >= n_needed

    def _release_memory(self, buf: np.ndarray) -> None:
        ptr = buf.ctypes.data
        self._samples.pop(ptr, None)
        self._lib.psd_host_free(ptr)

    def clear(self) -> None:
        """Free the idle buffers and forget borrowed ones (for reset())."""
        with self._lock:
            for buf in self._free:
                self._release_memory(buf)
            self._free.clear()
            self._samples.clear()


_pool = _PinnedPool()


@contextmanager
def pinned_complex64(n: int) -> Iterator[np.ndarray | None]:
    """Borrow an n-sample complex64 buffer that compute_psd_grid's GPU path
    reads in place, or None when the GPU is unavailable or the pool is used
    up. Fill it (e.g. convert_sc16_to_complex(..., out=buf)) and finish with
    it inside the with block; it is reused afterwards."""
    lib = _library()
    buf = _pool.acquire(lib, n) if lib is not None else None
    try:
        yield buf
    finally:
        if buf is not None:
            _pool.release(buf)


def compute_psd_grid_cuda(
    data: np.ndarray, sampling_rate: int, config: PSDGridConfig
) -> PSDGridResult | None:
    """compute_psd_grid on the GPU, or None when it cannot run here: no library
    or device (see unavailable_reason()), or input shorter than one time slice.

    Raises RuntimeError on a GPU failure, after disabling the GPU path."""
    lib = _library()
    if lib is None:
        return None
    geometry = grid_geometry(len(data), sampling_rate, config)
    if geometry.short_input:
        return None
    zero_copy = _pool.owns(data, geometry.usable_samples)
    if not zero_copy and (data.dtype != np.complex64 or not data.flags.c_contiguous):
        data = np.ascontiguousarray(data[: geometry.usable_samples], dtype=np.complex64)
    grid = None
    try:
        # A second try covers an engine evicted between lookup and use.
        for _ in range(2):
            grid = _engine(lib, geometry, sampling_rate).run(data, zero_copy)
            if grid is not None:
                break
    except RuntimeError as exc:
        disable(str(exc))
        raise
    if grid is None:
        return None
    time_axis, freq_axis = grid_axes(geometry, sampling_rate)
    return PSDGridResult(
        grid=grid,
        time_axis=time_axis,
        freq_axis=freq_axis,
        ffts_per_slice=geometry.ffts_per_slice,
        total_ffts=geometry.n_slices * geometry.ffts_per_slice,
    )


def reset() -> None:
    """Forget the library, engines and pinned buffers, so the next call loads
    again (tests, or retrying after disable()). Buffers still borrowed through
    pinned_complex64 are not freed."""
    global _lib, _loaded, _reason
    with _state_lock:
        for engine in _engines.values():
            engine.close()
        _engines.clear()
        _lib = None
        _loaded = False
        _reason = None
    _pool.clear()
