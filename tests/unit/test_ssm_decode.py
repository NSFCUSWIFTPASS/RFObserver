"""SSM DBPSK decoder: synthetic frames built from the frame definition, at both
isolation tiers, with offset, noise, cuts, and signals that are not SSM."""

from __future__ import annotations

import gzip
from pathlib import Path

import numpy as np
import pytest

from rfobserver.processing.ssm import decode_ssm, frame_bits, modulate

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"

KNOWN = {
    "model": "SSM",
    "src_id": "0200574952470001",
    "payload": "Hello from Boulder, CO!",
    "preamble_errors": 0,
    "bit_errors": 0,
}


def _cs16(iq: np.ndarray) -> bytes:
    """Pack as channelize_to_cs16 does: <i2 I/Q, peak scaled to 30000."""
    peak = float(np.max(np.abs(iq))) if len(iq) else 1.0
    scaled = iq * (30000.0 / (peak or 1.0))
    out = np.empty(len(iq) * 2, dtype="<i2")
    out[0::2] = scaled.real.astype("<i2")
    out[1::2] = scaled.imag.astype("<i2")
    return out.tobytes()


def _noise(n: int, power: float, rng: np.random.Generator) -> np.ndarray:
    return (rng.normal(size=n) + 1j * rng.normal(size=n)) * np.sqrt(power / 2)


def _with_noise(iq: np.ndarray, snr_db: float, seed: int = 1) -> np.ndarray:
    p = float(np.mean(np.abs(iq) ** 2))
    return iq + _noise(len(iq), p / 10 ** (snr_db / 10), np.random.default_rng(seed))


def test_clean_frame_decodes() -> None:
    assert decode_ssm(_cs16(modulate(frame_bits())), 1_000_000) == KNOWN


def test_frame_at_1600k_decodes() -> None:
    f = decode_ssm(_cs16(modulate(frame_bits(), 1_600_000)), 1_600_000)
    assert f is not None
    assert f["src_id"] == "0200574952470001" and f["bit_errors"] == 0


@pytest.mark.parametrize("cfo", [-15_000, -4_000, 0, 3_600, 15_000])
def test_offset_and_phase(cfo: int) -> None:
    iq = modulate(frame_bits())
    t = np.arange(len(iq)) / 1_000_000
    iq = iq * np.exp(1j * (2 * np.pi * cfo * t + 1.1))
    f = decode_ssm(_cs16(iq), 1_000_000)
    assert f is not None and f["bit_errors"] == 0


def test_noise_counts_errors_then_falls_through() -> None:
    iq = modulate(frame_bits())
    clean = decode_ssm(_cs16(_with_noise(iq, 6.0)), 1_000_000)
    assert clean is not None and clean["preamble_errors"] <= 16
    # Per-sample SNR -9 dB: still SSM, but the decode is damaged and says so.
    weak = decode_ssm(_cs16(_with_noise(iq, -9.0)), 1_000_000)
    assert weak is not None and 0 < weak["preamble_errors"] <= 16
    assert decode_ssm(_cs16(_with_noise(iq, -18.0)), 1_000_000) is None


def test_other_mac_and_text() -> None:
    bits = frame_bits(0x0200574952470002, b"SSM2 test frame 0002 ok" * 4)
    f = decode_ssm(_cs16(modulate(bits)), 1_000_000)
    assert f is not None
    assert f["src_id"] == "0200574952470002" and f["bit_errors"] is None
    assert f["payload"] == "SSM2 test frame 0002 ok"


def test_payload_not_repeated_is_reported_whole() -> None:
    text = bytes(range(0x21, 0x21 + 92))
    f = decode_ssm(_cs16(modulate(frame_bits(text=text))), 1_000_000)
    assert f is not None and f["payload"] == text.decode()


def test_non_printable_payload_bytes_are_replaced() -> None:
    text = b"Hello\x07from Boulder, CO!" * 4
    f = decode_ssm(_cs16(modulate(frame_bits(text=text))), 1_000_000)
    assert f is not None and f["payload"] == "Hello?from Boulder, CO!"


def test_text_must_be_92_bytes() -> None:
    with pytest.raises(ValueError):
        frame_bits(text=b"short")


@pytest.mark.parametrize("make", ["noise", "tone", "fsk"])
def test_non_ssm_is_none(make: str) -> None:
    rate = 1_600_000
    n = int(0.083 * rate)
    rng = np.random.default_rng(7)
    t = np.arange(n) / rate
    if make == "noise":
        iq = _noise(n, 1.0, rng)
    elif make == "tone":
        iq = np.exp(2j * np.pi * 10_000 * t) + _noise(n, 0.01, rng)
    else:
        sps = rate // 100_000
        bits = rng.integers(0, 2, n // sps + 1)
        freq = np.repeat(np.where(bits == 1, 50_000.0, -50_000.0), sps)[:n]
        iq = np.exp(2j * np.pi * np.cumsum(freq) / rate) + _noise(n, 0.01, rng)
    assert decode_ssm(_cs16(iq), rate) is None


def test_cut_after_text_matches_and_before_mac_does_not() -> None:
    iq = modulate(frame_bits())
    f = decode_ssm(_cs16(iq[: 1000 * 16]), 1_000_000)
    assert f is not None and f["src_id"] == "0200574952470001" and f["bit_errors"] == 0
    assert decode_ssm(_cs16(iq[: 200 * 16]), 1_000_000) is None


def test_frame_after_30ms_of_noise() -> None:
    iq = modulate(frame_bits())
    p = float(np.mean(np.abs(iq) ** 2))
    lead = _noise(30_000, p / 100, np.random.default_rng(3))
    f = decode_ssm(_cs16(np.concatenate([lead, iq])), 1_000_000)
    assert f is not None and f["bit_errors"] == 0


def test_real_hcro_hop() -> None:
    """One hop from iq_capture_hcro-rpi-002_2025-12-12T19-33-52.92Z_915MHz_26.0Msps_
    20.0s_35dB_ssm_fhss_OVF.dat (ci16, 26 MS/s at 915 MHz): samples from 47 ms to
    140 ms, through channelize_to_cs16 at the detector's peak_freq_hz 917.2979 MHz
    to the 1.6 MS/s tier, gzipped."""
    data = gzip.decompress((FIXTURES / "ssm_hop_917MHz_1600k.cs16.gz").read_bytes())
    assert decode_ssm(data, 1_600_000) == KNOWN


@pytest.mark.parametrize("n", [0, 1, 400])
def test_tiny_input_is_none(n: int) -> None:
    assert decode_ssm(b"\0\0\0\0" * n, 1_000_000) is None
