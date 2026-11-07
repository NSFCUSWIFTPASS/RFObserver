"""UHF RFID (EPC Gen2) reader classifier: a real reader hop from HCRO, a synthetic
reader at the 1.6 MS/s tier, and signals that are not a reader."""

from __future__ import annotations

import gzip
from pathlib import Path

import numpy as np

from rfobserver.processing.rfid import classify_rfid_reader

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"


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


def _reader(rate_hz: int, tari_us: float, seconds: float, seed: int = 3) -> np.ndarray:
    """A carrier with Gen2 PIE dips (pulse width half a Tari; data-0 one Tari,
    data-1 two), sent in command groups with carrier-only gaps between them
    (where tags would answer), plus noise 30 dB down."""
    rng = np.random.default_rng(seed)
    n = int(seconds * rate_hz)
    env = np.ones(n)
    t_us, end_us = 200.0, seconds * 1e6 - 200.0
    while t_us < end_us:
        for _ in range(40):  # one command
            symbol = tari_us * (2.0 if rng.random() < 0.5 else 1.0)
            a = int(t_us * rate_hz / 1e6)
            env[a : a + int(0.5 * tari_us * rate_hz / 1e6)] = 0.05
            t_us += symbol
        t_us += 600.0  # carrier only
    iq = env * np.exp(2j * np.pi * 800.0 * np.arange(n) / rate_hz)
    return iq + 0.03 * _noise(n, rng)


def test_real_reader_hop_is_classified() -> None:
    cs16 = gzip.decompress((FIXTURES / "rfid_hop_914750kHz_1000k.cs16.gz").read_bytes())
    r = classify_rfid_reader(cs16, 1_000_000, freq_hz=914_753_000.0)
    assert r is not None
    assert r["model"] == "RFID-Reader"
    assert r["protocol"] == "EPC Gen2"
    assert r["channel_mhz"] == 914.75
    assert abs(r["carrier_hz"] - 914_750_770.0) < 1_000
    assert r["carrier_fraction"] > 0.5
    assert r["pulses"] > 100  # 30 ms of a reader hop


def test_synthetic_reader_at_1600k_is_classified() -> None:
    r = classify_rfid_reader(_cs16(_reader(1_600_000, 12.5, 0.05)), 1_600_000)
    assert r is not None
    # No frequency given: no channel claimed.
    assert r["carrier_hz"] is None and r["channel_mhz"] is None


def test_off_raster_carrier_is_still_a_reader_without_a_channel() -> None:
    iq = _reader(1_000_000, 20.0, 0.05)
    r = classify_rfid_reader(_cs16(iq), 1_000_000, freq_hz=915_100_000.0)
    assert r is not None and r["channel_mhz"] is None


def test_ssm_hop_is_not_a_reader() -> None:
    cs16 = gzip.decompress((FIXTURES / "ssm_hop_917MHz_1600k.cs16.gz").read_bytes())
    assert classify_rfid_reader(cs16, 1_600_000, freq_hz=917_000_000.0) is None


def test_noise_is_not_a_reader() -> None:
    iq = _noise(200_000, np.random.default_rng(0))
    assert classify_rfid_reader(_cs16(iq), 1_000_000) is None


def test_plain_carrier_is_not_a_reader() -> None:
    rng = np.random.default_rng(1)
    iq = np.exp(2j * np.pi * 5_000 * np.arange(200_000) / 1e6) + 0.01 * _noise(200_000, rng)
    assert classify_rfid_reader(_cs16(iq), 1_000_000) is None


def test_slow_keying_is_not_a_reader() -> None:
    """A carrier keyed on and off at a few hundred Hz (an OOK remote) has a
    carrier and dips, but not PIE timing."""
    n = 200_000
    t = np.arange(n) / 1e6
    env = np.where(np.sin(2 * np.pi * 300 * t) > 0, 1.0, 0.05)
    iq = env * np.exp(2j * np.pi * 2_000 * t) + 0.01 * _noise(n, np.random.default_rng(2))
    assert classify_rfid_reader(_cs16(iq), 1_000_000) is None


def test_too_short_is_not_classified() -> None:
    iq = _reader(1_000_000, 20.0, 0.002)
    assert classify_rfid_reader(_cs16(iq), 1_000_000) is None
