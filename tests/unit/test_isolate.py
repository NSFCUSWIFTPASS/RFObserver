"""Isolating a burst: its samples, shifted to DC and decimated to the tier rate."""

from __future__ import annotations

import numpy as np

from rfobserver.capture.buffer import CircularBuffer
from rfobserver.models import BurstFingerprint
from rfobserver.processing.isolate import GUARD_SEC, iq_to_complex, isolate_burst

FS = 8_000_000.0
CENTER = 915e6


def _pack(iq: np.ndarray) -> np.ndarray:
    v = np.empty(iq.size * 2, dtype=np.int16)
    v[0::2] = np.clip(iq.real * 20000, -32767, 32767).astype(np.int16)
    v[1::2] = np.clip(iq.imag * 20000, -32767, 32767).astype(np.int16)
    return v.view(np.int32)


def _ring_with_tone(total: int, burst: tuple[int, int], offset_hz: float) -> CircularBuffer:
    rng = np.random.default_rng(1)
    iq = (rng.normal(0, 0.01, total) + 1j * rng.normal(0, 0.01, total)).astype(np.complex64)
    n = np.arange(burst[0], burst[1])
    iq[burst[0] : burst[1]] += 0.5 * np.exp(2j * np.pi * offset_hz / FS * n)
    ring = CircularBuffer(total, dtype=np.int32)
    ring.write(_pack(iq))
    return ring


def _burst(start, stop, peak_hz, bw=250e3) -> BurstFingerprint:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    return BurstFingerprint(
        start_time=now,
        stop_time=now,
        center_freq_hz=peak_hz,
        peak_freq_hz=peak_hz,
        bandwidth_hz=bw,
        peak_power_db=-30.0,
        start_sample=start,
        stop_sample=stop,
    )


def _decode(cs16: bytes) -> np.ndarray:
    v = np.frombuffer(cs16, dtype="<i2").astype(np.float32)
    return v[0::2] + 1j * v[1::2]


def test_burst_is_centered_and_decimated_to_the_tier_rate():
    offset = 1_200_000.0
    ring = _ring_with_tone(400_000, (100_000, 180_000), offset)  # 10 ms burst
    b = _burst(100_000, 180_000, CENTER + offset)
    iso = isolate_burst(
        b,
        read_range=ring.read_range,
        read_all=None,
        sample_rate_hz=FS,
        center_freq_hz=CENTER,
        max_burst_sec=0.5,
    )
    assert not isinstance(iso, str)
    assert iso.rate_hz == 1_600_000  # 250 kHz burst -> SSN tier
    guard = int(GUARD_SEC * FS)
    assert iso.num_source_samples == 80_000 + 2 * guard
    out = _decode(iso.cs16)
    assert abs(len(out) - iso.num_source_samples * 1_600_000 / FS) <= 2
    spec = np.abs(np.fft.fftshift(np.fft.fft(out)))
    freqs = np.fft.fftshift(np.fft.fftfreq(len(out), 1 / 1_600_000))
    assert abs(freqs[np.argmax(spec)]) < 5_000  # the tone now sits at DC
    assert not iso.truncated


def test_narrow_burst_takes_the_default_tier():
    ring = _ring_with_tone(200_000, (50_000, 90_000), 0.0)
    iso = isolate_burst(
        _burst(50_000, 90_000, CENTER, bw=50e3),
        read_range=ring.read_range,
        read_all=None,
        sample_rate_hz=FS,
        center_freq_hz=CENTER,
        max_burst_sec=0.5,
    )
    assert iso.rate_hz == 1_000_000


def test_long_burst_truncated():
    ring = _ring_with_tone(400_000, (10_000, 390_000), 0.0)
    iso = isolate_burst(
        _burst(20_000, 380_000, CENTER),
        read_range=ring.read_range,
        read_all=None,
        sample_rate_hz=FS,
        center_freq_hz=CENTER,
        max_burst_sec=0.01,
    )
    assert iso.truncated
    assert iso.num_source_samples == int(0.01 * FS) + 2 * int(GUARD_SEC * FS)


def test_expired_burst():
    ring = CircularBuffer(100_000, dtype=np.int32)
    ring.write(np.zeros(300_000, dtype=np.int32))  # holds 200k..300k
    iso = isolate_burst(
        _burst(50_000, 60_000, CENTER),
        read_range=ring.read_range,
        read_all=None,
        sample_rate_hz=FS,
        center_freq_hz=CENTER,
        max_burst_sec=0.5,
    )
    assert iso == "iq_expired"


def test_burst_without_positions_uses_the_whole_capture():
    iq = np.exp(2j * np.pi * 300e3 / FS * np.arange(80_000)).astype(np.complex64)
    iso = isolate_burst(
        _burst(None, None, CENTER + 300e3),
        read_range=None,
        read_all=lambda: iq,
        sample_rate_hz=FS,
        center_freq_hz=CENTER,
        max_burst_sec=0.5,
    )
    assert iso.num_source_samples == 80_000 and iso.start_sample is None


def test_burst_without_positions_and_no_whole_capture_is_expired():
    iso = isolate_burst(
        _burst(None, None, CENTER),
        read_range=lambda a, b: None,
        read_all=None,
        sample_rate_hz=FS,
        center_freq_hz=CENTER,
        max_burst_sec=0.5,
    )
    assert iso == "iq_expired"


def test_iq_to_complex_unpacks_sc16():
    packed = _pack(np.array([0.5 + 0.25j], dtype=np.complex64))
    out = iq_to_complex(packed)
    assert out.dtype == np.complex64
    assert abs(out[0] - (10000 + 5000j) / 32768) < 1e-3
