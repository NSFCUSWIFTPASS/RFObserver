"""Upstream modules: thread-safe output hand-off, and the FM demod's
resampler and band check (the GPU path itself runs only where CuPy is)."""

from __future__ import annotations

import asyncio
import threading
from typing import Any

import numpy as np
import pytest

from rfobserver.modules.base import UpstreamModule
from rfobserver.modules.fm_demod import FMDemodModule


class _Echo(UpstreamModule):
    module_type = "echo"

    @classmethod
    def parameters(cls) -> list[Any]:
        return []

    def configure(self, params: dict[str, Any]) -> None:
        pass

    def feed(self, sc16_buf: np.ndarray, center_freq_hz: int, sample_rate: int) -> None:
        pass

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def status(self) -> dict[str, Any]:
        return {}


async def test_emit_from_a_thread_wakes_the_reader_at_once() -> None:
    """The reader awaits the queue; a thread's emit must wake it, not leave
    the data until some later loop activity (the 2 s audio gaps)."""
    m = _Echo()
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    threading.Thread(target=m.emit, args=(b"pcm",)).start()
    got = await asyncio.wait_for(m.output_queue.get(), timeout=1.0)
    assert got == b"pcm" and loop.time() - t0 < 0.5


async def test_full_queue_drops_the_oldest() -> None:
    m = _Echo()
    for i in range(m.output_queue.maxsize + 3):
        m.emit(bytes([i]))
    await asyncio.sleep(0.01)  # let the loop run the hand-offs
    assert m.output_dropped == 3
    assert m.output_queue.get_nowait() == bytes([3])  # the three oldest are gone


def _fm(rate_in: float, rate_out: int = 48_000) -> FMDemodModule:
    """An FM module without its GPU parts, for the CPU-side stages."""
    m = FMDemodModule.__new__(FMDemodModule)
    UpstreamModule.__init__(m, {})
    m._params = dict(FMDemodModule._DEFAULTS)
    m._rs_step = rate_in / rate_out
    m._rs_pos = 0.0
    m._rs_last = None
    m._error = None
    return m


@pytest.mark.parametrize("rate_in", [250_000.0, 251_803.27868852458, 48_000.0])
def test_resampler_is_continuous_and_exact_across_chunks(rate_in: float) -> None:
    """Chunks of any length come out as one unbroken 48 kHz signal: the same
    samples as resampling the whole stream at once."""
    m = _fm(rate_in)
    t = np.arange(int(rate_in * 0.5)) / rate_in
    x = np.sin(2 * np.pi * 1000.0 * t)
    sizes = [9142, 9143, 7, 9142, 1, 20000]
    out, i = [], 0
    while i < len(x):
        n = sizes[len(out) % len(sizes)]
        out.append(m._resample(x[i : i + n]))
        i += n
    y = np.concatenate(out)
    want = np.interp(np.arange(len(y)) * (rate_in / 48_000), np.arange(len(x)), x)
    assert len(y) == pytest.approx(0.5 * 48_000, abs=2)
    assert np.max(np.abs(y - want)) < 1e-6


def test_channel_outside_the_band_reports_and_returns_no_audio() -> None:
    m = _fm(250_000.0)
    m._params["channel_freq_hz"] = 101_900_000
    buf = np.zeros(1024, dtype=np.int32)
    assert m._demod_chunk(buf, 915_000_000, 56_000_000) is None
    assert m._error is not None and "outside the captured band" in m._error
    assert "887.0 to 943.0 MHz" in m._error
