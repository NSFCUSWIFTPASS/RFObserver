"""Synthetic IQ helpers for burst-detection integration tests.

Builds a deterministic 2-second-ish IQ buffer with planted bursts at known
(time, frequency, amplitude), packs it as int32 SC16 (the format the streaming
pipeline expects), and exposes a ``MockReceiver`` subclass that serves the
buffer slice-by-slice. Pacing is configurable: full-speed playback overwhelms
the dispatch chunk queue and drops most chunks (causing flaky detection), so
the default ``pacing_factor`` of 4 plays at 4× realtime — fast enough to keep
tests <1 s yet slow enough that dispatch keeps up.

The amplitude scale targets the same complex64 [-1, 1] range that
``rfobserver.processing.iq_utils.convert_sc16_to_complex`` produces, so a
``noise_stddev`` of 0.01 and a burst amplitude of 0.05 means the burst is
roughly 14 dB above the time-domain noise (the actual PSD-grid headroom is
larger thanks to FFT processing gain — calibrate per test).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any

import numpy as np

from rfobserver.capture.mock_receiver import MockReceiver


@dataclass(frozen=True)
class Burst:
    """A planted burst in a synthetic IQ buffer.

    Times are in seconds, frequency offset in Hz from baseband.
    ``amplitude`` is in normalized complex64 units (the same scale the
    streaming pipeline sees after SC16 conversion).
    """

    start_sec: float
    duration_sec: float
    freq_offset_hz: float
    amplitude: float


def make_iq_with_bursts(
    duration_sec: float,
    bandwidth_hz: int,
    bursts: list[Burst],
    *,
    noise_stddev: float = 0.01,
    seed: int = 42,
) -> np.ndarray:
    """Generate complex64 IQ samples with planted bursts.

    Returns a 1-D array shaped ``(int(duration_sec * bandwidth_hz),)``.
    Each burst is added as a tone at ``freq_offset_hz`` from baseband,
    windowed with a 5% raised-cosine ramp at each end so detection edges
    aren't dominated by spectral splatter.
    """
    n_samples = int(duration_sec * bandwidth_hz)
    rng = np.random.default_rng(seed)

    iq = (rng.standard_normal(n_samples) + 1j * rng.standard_normal(n_samples)).astype(np.complex64)
    iq *= np.float32(noise_stddev)

    t = np.arange(n_samples, dtype=np.float64) / bandwidth_hz

    for b in bursts:
        i0 = max(0, int(b.start_sec * bandwidth_hz))
        i1 = min(n_samples, int((b.start_sec + b.duration_sec) * bandwidth_hz))
        if i1 <= i0:
            continue
        seg = i1 - i0
        env = np.ones(seg, dtype=np.float32)
        ramp = max(1, seg // 20)
        env[:ramp] = (0.5 * (1 - np.cos(np.pi * np.arange(ramp) / ramp))).astype(np.float32)
        env[-ramp:] = env[:ramp][::-1]
        phase = 2 * np.pi * b.freq_offset_hz * t[i0:i1]
        tone = (np.cos(phase) + 1j * np.sin(phase)).astype(np.complex64)
        iq[i0:i1] += np.float32(b.amplitude) * env * tone

    return iq


def iq_to_sc16_int32(iq: np.ndarray) -> np.ndarray:
    """Pack complex64 IQ in [-1, 1] into the int32 SC16 format the pipeline expects.

    Mirrors ``MockReceiver.recv_chunk`` packing: interleaved int16 I/Q pairs
    viewed as int32. ``convert_sc16_to_complex`` reverses this (divides by 32768).
    """
    if iq.dtype != np.complex64:
        iq = iq.astype(np.complex64)
    scale = np.float32(32767.0)
    interleaved = np.empty(iq.size * 2, dtype=np.int16)
    interleaved[0::2] = np.clip(iq.real * scale, -32768, 32767).astype(np.int16)
    interleaved[1::2] = np.clip(iq.imag * scale, -32768, 32767).astype(np.int16)
    return interleaved.view(np.int32).copy()


class SyntheticBurstReceiver(MockReceiver):
    """``MockReceiver`` that serves a fixed synthetic IQ buffer chunk by chunk.

    Paces ``recv_chunk`` at ``pacing_factor`` × realtime (default 4) so the
    streaming dispatch keeps up — full-speed playback overruns
    ``_chunk_queue`` (maxsize=4) and drops most chunks, which makes
    detection flaky. After the buffer is exhausted, ``recv_chunk`` fills
    with low-amplitude noise (not zeros — all-zero IQ trips
    ``calculate_iq_statistics`` on log10(0)) and sets ``exhausted`` so
    callers can stop the pipeline cleanly.
    """

    def __init__(
        self,
        receiver_config: Any,
        iq_int32: np.ndarray,
        pacing_factor: float = 4.0,
    ) -> None:
        super().__init__(receiver_config=receiver_config)
        self._buffer = iq_int32
        self._pos = 0
        self._exhausted = threading.Event()
        self._drain_rng = np.random.default_rng(0)
        self._pacing_factor = max(0.0, pacing_factor)

    @property
    def exhausted(self) -> bool:
        return self._exhausted.is_set()

    def _fill_drain_noise(self, out_buf: np.ndarray, start: int = 0) -> None:
        """Fill ``out_buf[start:]`` with low-amplitude SC16 noise."""
        n = len(out_buf) - start
        if n <= 0:
            return
        noise_i = self._drain_rng.integers(-50, 50, size=n, dtype=np.int16)
        noise_q = self._drain_rng.integers(-50, 50, size=n, dtype=np.int16)
        packed = np.empty(n * 2, dtype=np.int16)
        packed[0::2] = noise_i
        packed[1::2] = noise_q
        out_buf[start:] = packed.view(np.int32)

    def recv_chunk(self, out_buf: np.ndarray) -> int:
        n = len(out_buf)
        if self._pacing_factor > 0:
            chunk_duration = n / self._config.bandwidth_hz
            time.sleep(chunk_duration / self._pacing_factor)

        remaining = len(self._buffer) - self._pos
        if remaining <= 0:
            self._exhausted.set()
            self._fill_drain_noise(out_buf)
            return n
        copy_n = min(n, remaining)
        out_buf[:copy_n] = self._buffer[self._pos : self._pos + copy_n]
        if copy_n < n:
            self._fill_drain_noise(out_buf, start=copy_n)
            self._exhausted.set()
        self._pos += copy_n
        return n
