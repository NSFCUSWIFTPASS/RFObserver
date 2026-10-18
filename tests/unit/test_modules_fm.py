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


def _numpy_as_cupy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run the GPU path on the CPU: numpy provides everything it uses from cupy."""
    from types import SimpleNamespace

    from rfobserver.modules import fm_demod

    shim = SimpleNamespace(**{k: getattr(np, k) for k in dir(np) if not k.startswith("__")})
    shim.asnumpy = np.asarray
    monkeypatch.setattr(fm_demod, "cp", shim, raising=False)
    monkeypatch.setattr(fm_demod, "HAS_CUPY", True)


def _station(n: int, c: int, fs: int, offset: float, tone: float, phase: float) -> Any:
    """Chunk c of an FM carrier at offset Hz, a tone at 37.5 kHz deviation."""
    t = (np.arange(n) + c * n) / fs
    f_inst = offset + 37_500.0 * np.sin(2 * np.pi * tone * t)
    ph = phase + 2 * np.pi * np.cumsum(f_inst) / fs
    return np.exp(1j * ph), ph[-1]


def _run(
    monkeypatch: pytest.MonkeyPatch, fs: int, n: int, neighbour: float = 0.0, chunks: int = 30
) -> tuple[Any, int]:
    """Demodulate a station 500 kHz off centre (1 kHz tone) in chunks of n,
    optionally with a neighbour 200 kHz above it, `neighbour` times as strong
    in amplitude (3.7 kHz tone). Returns the tone's residual after the
    filters' start-up, and the number of audio samples."""
    _numpy_as_cupy(monkeypatch)
    center, offset = 100_000_000, 500_000
    m = FMDemodModule({"channel_freq_hz": center + offset})
    rng = np.random.default_rng(1)
    ph1 = ph2 = 0.0
    out = []
    for c in range(chunks):
        s1, ph1 = _station(n, c, fs, offset, 1000.0, ph1)
        s2, ph2 = _station(n, c, fs, offset + 200_000, 3700.0, ph2)
        iq = 0.3 / (1 + neighbour) * (s1 + neighbour * s2)
        iq += 0.002 * (rng.standard_normal(n) + 1j * rng.standard_normal(n))
        sc = np.empty(2 * n, dtype=np.int16)
        sc[0::2] = np.round(iq.real * 32767)
        sc[1::2] = np.round(iq.imag * 32767)
        out.append(m._demod_chunk(sc.view(np.int32), center, fs))
    audio = np.concatenate(out[3:])
    k = np.arange(len(audio))
    w = 2 * np.pi * 1000.0 * k / 48_000
    basis = np.column_stack([np.sin(w), np.cos(w), np.ones(len(k))])
    res = audio - basis @ np.linalg.lstsq(basis, audio, rcond=None)[0]
    return res, len(np.concatenate(out))


@pytest.mark.parametrize("fs", [5_600_000, 2_400_000])
def test_demod_is_seamless_across_chunks(monkeypatch: pytest.MonkeyPatch, fs: int) -> None:
    """A synthetic station, in chunks that are not a multiple of either
    decimation stage: the audio is the tone with no extra error at chunk
    boundaries (dropping the box-car remainder clicked 27 times a second) and
    comes out at exactly 48 kHz. 5.6 MS/s takes a box-car /2 then the FIR /11;
    2.4 MS/s (RTL-SDR) the FIR /9 alone."""
    n = 204_819
    res, n_audio = _run(monkeypatch, fs, n)
    assert n_audio == pytest.approx(30 * n / fs * 48_000, abs=2)
    # The tone (amplitude 0.5) and nothing else: the old code's dropped
    # samples left an error of about 0.14, the fixed path about 0.0003.
    assert np.max(np.abs(res)) < 0.005


def test_a_strong_neighbour_stays_out_of_the_channel(monkeypatch: pytest.MonkeyPatch) -> None:
    """A station 200 kHz away and 10 dB stronger (an RTL-SDR on a city's FM
    band). A box-car down to 267 kHz rejected it by only ~13 dB and it folded
    into the audio; the channel filter keeps it out."""
    res, _ = _run(monkeypatch, 2_400_000, 204_819, neighbour=10 ** (10 / 20))
    assert np.sqrt(np.mean(res**2)) < 0.005
