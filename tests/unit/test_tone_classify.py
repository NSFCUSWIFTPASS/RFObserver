"""Tone (unmodulated carrier) classifier: synthetic tones as isolation hands them
over (channelized, peak near DC, 2 ms of noise either side), and signals that
are not a tone, including real RFID and SSM bursts from HCRO."""

from __future__ import annotations

import gzip
from pathlib import Path

import numpy as np

from rfobserver.processing.tone import classify_tone

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
RATE = 1_000_000
GUARD = 0.002


def _cs16(iq: np.ndarray) -> bytes:
    """Pack as channelize_to_cs16 does: <i2 I/Q, peak scaled to 30000."""
    peak = float(np.max(np.abs(iq))) if len(iq) else 1.0
    scaled = iq * (30000.0 / (peak or 1.0))
    out = np.empty(len(iq) * 2, dtype="<i2")
    out[0::2] = scaled.real.astype("<i2")
    out[1::2] = scaled.imag.astype("<i2")
    return out.tobytes()


def _noise(n: int, rng: np.random.Generator) -> np.ndarray:
    return (rng.normal(size=n) + 1j * rng.normal(size=n)) / np.sqrt(2)


def _burst(signal: np.ndarray, snr_db: float, seed: int = 1) -> bytes:
    """``signal`` (unit power while on) between GUARD of noise, noise throughout
    at ``snr_db`` below it per sample."""
    rng = np.random.default_rng(seed)
    g = int(GUARD * RATE)
    x = np.r_[np.zeros(g), signal, np.zeros(g)].astype(complex)
    x += 10 ** (-snr_db / 20) * _noise(len(x), rng)
    return _cs16(x)


def _carrier(seconds: float, offset_hz: float) -> np.ndarray:
    t = np.arange(int(seconds * RATE)) / RATE
    return np.exp(2j * np.pi * offset_hz * t)


def test_a_100_ms_tone_is_recognised() -> None:
    r = classify_tone(_burst(_carrier(0.1, 3_000.0), snr_db=30), RATE, freq_hz=915_000_000.0)
    assert r is not None
    assert r["model"] == "Tone"
    assert r["protocol"] == "CW"
    assert abs(r["tone_hz"] - 915_003_000.0) < 50
    assert abs(r["duration_ms"] - 100.0) < 2.0
    assert r["line_fraction"] > 0.9


def test_a_tone_at_the_detection_threshold_is_recognised() -> None:
    """A tone 30 dB above the floor of a 12.7 kHz detection bin is about 11 dB
    above the noise of the 1 MS/s snippet."""
    r = classify_tone(_burst(_carrier(0.1, -7_500.0), snr_db=10), RATE, freq_hz=915e6)
    assert r is not None
    assert abs(r["tone_hz"] - (915e6 - 7_500.0)) < 50


def test_without_a_frequency_the_offset_is_reported() -> None:
    r = classify_tone(_burst(_carrier(0.05, 1_200.0), snr_db=30), RATE)
    assert r is not None
    assert r["tone_hz"] is None
    assert abs(r["offset_hz"] - 1_200.0) < 50


def test_real_rfid_reader_hop_is_not_a_tone() -> None:
    """A reader's carrier holds 0.68 of the power, but it dips for PIE pulses."""
    cs16 = gzip.decompress((FIXTURES / "rfid_hop_914750kHz_1000k.cs16.gz").read_bytes())
    assert classify_tone(cs16, 1_000_000, freq_hz=914_753_000.0) is None


def test_real_ssm_burst_is_not_a_tone() -> None:
    cs16 = gzip.decompress((FIXTURES / "ssm_hop_917MHz_1600k.cs16.gz").read_bytes())
    assert classify_tone(cs16, 1_600_000) is None


def test_noise_is_not_a_tone() -> None:
    rng = np.random.default_rng(5)
    assert classify_tone(_cs16(_noise(int(0.1 * RATE), rng)), RATE) is None


def test_on_off_keyed_carrier_is_not_a_tone() -> None:
    t = np.arange(int(0.1 * RATE)) / RATE
    keyed = _carrier(0.1, 2_000.0) * ((t * 1_000) % 1.0 < 0.5)
    assert classify_tone(_burst(keyed, snr_db=30), RATE) is None


def test_frequency_keyed_carrier_is_not_a_tone() -> None:
    t = np.arange(int(0.1 * RATE)) / RATE
    bits = (np.floor(t * 10_000) % 2) * 2 - 1  # 10 kbaud alternating
    fsk = np.exp(2j * np.pi * np.cumsum(20_000.0 * bits) / RATE)
    assert classify_tone(_burst(fsk, snr_db=30), RATE) is None


def test_two_tones_are_not_one_tone() -> None:
    two = (_carrier(0.1, -15_000.0) + _carrier(0.1, 15_000.0)) / np.sqrt(2)
    assert classify_tone(_burst(two, snr_db=30), RATE) is None


def test_amplitude_modulated_carrier_is_not_a_tone() -> None:
    t = np.arange(int(0.1 * RATE)) / RATE
    am = _carrier(0.1, 0.0) * (1 + 0.8 * np.sin(2 * np.pi * 300.0 * t))
    assert classify_tone(_burst(am / np.sqrt(1.32), snr_db=30), RATE) is None


def test_too_short_is_not_classified() -> None:
    assert classify_tone(_burst(_carrier(0.002, 0.0), snr_db=30), RATE) is None


# Real snippets from an over-the-air test on 2026-11-18: a B200mini at 915 MHz,
# 26 MS/s receiving a B200mini's 100 ms CW pulses at 915.5 MHz. The receiver
# drops 5 dB for 3.4 ms every 102.4 ms and shows ghost lines 3 MHz either side
# while it does (see docs/debugging/2026-11-18_b200-rx-gain-step.md).


def _fixture(name: str) -> bytes:
    return gzip.decompress((FIXTURES / name).read_bytes())


def test_real_tone_through_a_receiver_gain_step_is_a_tone() -> None:
    """32 ms of a strong pulse with the receiver's 3.4 ms, 5 dB step inside."""
    r = classify_tone(
        _fixture("tone_b200_gainstep_1000k.cs16.gz"), 1_000_000, freq_hz=915_495_117.2
    )
    assert r is not None
    assert abs(r["tone_hz"] - 915_500_860.0) < 100


def test_real_weak_tone_fragment_is_a_tone() -> None:
    """The last 30 ms of a pulse near the detection threshold, as the detector
    split it (it ends inside the snippet)."""
    r = classify_tone(_fixture("tone_weak_fragment_1000k.cs16.gz"), 1_000_000, 915_495_117.2)
    assert r is not None
    assert abs(r["tone_hz"] - 915_500_860.0) < 300


def test_receiver_ghost_lines_are_not_tones() -> None:
    """A ghost line (3.5 ms) and two merged (8.5 ms) are shorter than any tone
    worth labelling."""
    for name in ("b200_ghost_line_1000k.cs16.gz", "b200_ghost_line_merged_1000k.cs16.gz"):
        assert classify_tone(_fixture(name), 1_000_000) is None, name


def test_lightly_amplitude_modulated_carrier_is_not_a_tone() -> None:
    t = np.arange(int(0.1 * RATE)) / RATE
    am = _carrier(0.1, 0.0) * (1 + 0.2 * np.sin(2 * np.pi * 300.0 * t))
    assert classify_tone(_burst(am, snr_db=30), RATE) is None


def test_a_brief_level_step_does_not_stop_a_tone() -> None:
    """A synthetic tone with two 3.4 ms, 5 dB steps (the receiver's)."""
    tone = _carrier(0.1, 2_000.0)
    for start in (0.020, 0.0724):
        a = int(start * RATE)
        tone[a : a + 3_400] *= 0.56
    assert classify_tone(_burst(tone, snr_db=30), RATE) is not None
