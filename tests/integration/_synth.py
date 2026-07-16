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


def make_iq_with_wideband_burst(
    duration_sec: float,
    sample_rate_hz: int,
    *,
    burst_start_sec: float,
    burst_duration_sec: float,
    burst_bw_hz: float,
    burst_offset_hz: float,
    num_bins: int,
    per_tone_amp: float = 0.02,
    noise_stddev: float = 0.01,
    seed: int = 42,
) -> np.ndarray:
    """complex64 IQ with a single constant-envelope multitone-comb burst.

    The burst is a dense comb of equal-amplitude CW tones evenly filling
    ``[burst_offset_hz - bw/2, burst_offset_hz + bw/2]`` (~1.2 tones per FFT
    bin, hence the ``num_bins`` argument), shaped by a 5% raised-cosine time
    envelope. Two properties make it a clean measurement target where
    band-limited noise fails:

    - **Constant per-bin power over time** (CW tones, not noise) -> the PSD grid
      is a crisp duration x bandwidth rectangle, so the connected-component
      detector doesn't fragment it in time (accurate duration) and doesn't
      over-read the edges (accurate bandwidth).
    - **Constant per-tone amplitude** (``per_tone_amp``, NOT normalized to a
      fixed total power) -> per-bin SNR is roughly constant across occupied
      bandwidths, so a single detection threshold both finds a wide burst
      (power spread over many bins) and avoids inflating a narrow burst's
      measured bandwidth via an oversized leakage skirt.

    Schroeder phases give the multitone a low crest factor, so a strong per-bin
    SNR stays well within the [-1, 1] range the SC16 packing expects (no
    clipping, which would splatter the spectrum).
    """
    n_samples = int(duration_sec * sample_rate_hz)
    rng = np.random.default_rng(seed)

    iq = (rng.standard_normal(n_samples) + 1j * rng.standard_normal(n_samples)).astype(np.complex64)
    iq *= np.float32(noise_stddev)

    i0 = max(0, int(burst_start_sec * sample_rate_hz))
    i1 = min(n_samples, int((burst_start_sec + burst_duration_sec) * sample_rate_hz))
    seg = i1 - i0
    if seg <= 1:
        return iq

    bin_spacing = sample_rate_hz / num_bins
    occupied_bins = max(2, int(round(burst_bw_hz / bin_spacing)))
    # ~1.2 tones per FFT bin: dense enough to fill the band flatly, sparse
    # enough that a very wide burst (hundreds of bins) doesn't sum to a crest
    # factor that clips the SC16 range.
    num_tones = max(8, int(occupied_bins * 1.2))
    tone_freqs = (
        burst_offset_hz - burst_bw_hz / 2 + np.arange(num_tones) * (burst_bw_hz / (num_tones - 1))
    )
    # Schroeder phases: phi_k = -pi k^2 / N gives a near-constant-envelope
    # multitone (low peak-to-average power ratio).
    k = np.arange(num_tones)
    phases = -np.pi * k * k / num_tones

    t = np.arange(seg, dtype=np.float64) / sample_rate_hz
    burst = np.zeros(seg, dtype=np.complex128)
    for freq, phase in zip(tone_freqs, phases, strict=True):
        burst += np.exp(1j * (2 * np.pi * freq * t + phase))
    burst *= per_tone_amp
    # Safety cap: a very wide comb (many tones) can still sum past the SC16
    # range at high FFT-bin counts. Clamp the peak so packing never clips
    # (which would splatter the spectrum); this only bites the widest combos and
    # leaves their per-bin SNR comfortably above the detection threshold.
    peak = float(np.max(np.abs(burst))) if seg else 0.0
    if peak > 0.8:
        burst *= 0.8 / peak
    burst = burst.astype(np.complex64)

    env = np.ones(seg, dtype=np.float32)
    ramp = max(1, seg // 20)
    env[:ramp] = (0.5 * (1 - np.cos(np.pi * np.arange(ramp) / ramp))).astype(np.float32)
    env[-ramp:] = env[:ramp][::-1]

    iq[i0:i1] += (burst * env).astype(np.complex64)
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
        """Fill ``out_buf[start:]`` with noise MATCHING the planted buffer's level.

        The drain noise must sit at the same power as the buffer's background
        noise (Gaussian, stddev ~0.01 in complex units -> ~328 in SC16 counts).
        If it were much quieter, drain rows entering the rolling window would
        pull the per-bin 10th-percentile noise floor down, and the ordinary
        background noise would then exceed the burst detector's low threshold --
        producing a spurious full-span "burst" spanning the whole band.
        """
        n = len(out_buf) - start
        if n <= 0:
            return
        sd = 0.01 * 32767.0  # match make_iq_with_wideband_burst's default noise_stddev
        noise_i = (self._drain_rng.standard_normal(n) * sd).astype(np.int16)
        noise_q = (self._drain_rng.standard_normal(n) * sd).astype(np.int16)
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
