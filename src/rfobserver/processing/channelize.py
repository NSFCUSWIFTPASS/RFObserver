"""Channelize a detected burst out of wideband IQ into a narrowband .cs16 blob
that rtl_433 can decode. Ported from the validated gr-modules ssn_scan.py:
frequency-shift the burst to DC, resample to the target rate (resample_poly's
polyphase FIR is the anti-alias / low-pass stage), and pack as interleaved
little-endian int16. Pure DSP - no file or subprocess I/O.
"""

from __future__ import annotations

from math import gcd

import numpy as np
from scipy import signal as sig

# Bursts at or above this bandwidth take the 1.6 Msps SSN-mesh tier; narrower
# bursts take the 1.0 Msps full-default-decoder tier. Bandwidth-keyed per the
# spec's fixed two-tier policy.
TIER_BANDWIDTH_HZ: float = 200_000.0

_SSN_FLEX = "n=ssnmesh,m=FSK_PCM,s=16,l=16,r=8000"


def resample_ratio(sample_rate_hz: int, target_rate_hz: int) -> tuple[int, int]:
    """Return the gcd-reduced (up, down) for resample_poly to take
    sample_rate_hz -> target_rate_hz."""
    g = gcd(int(sample_rate_hz), int(target_rate_hz))
    return int(target_rate_hz) // g, int(sample_rate_hz) // g


def channelize_to_cs16(
    iq: np.ndarray, sample_rate_hz: float, offset_hz: float, target_rate_hz: int
) -> bytes:
    """Shift offset_hz to DC, resample to target_rate_hz, pack as <i2 I/Q."""
    n = np.arange(len(iq))
    mixer = np.exp(-2j * np.pi * (offset_hz / sample_rate_hz) * n).astype(np.complex64)
    shifted = iq.astype(np.complex64) * mixer
    up, down = resample_ratio(int(sample_rate_hz), int(target_rate_hz))
    res = sig.resample_poly(shifted, up, down)
    peak = float(np.max(np.abs(res))) if len(res) else 1.0
    scale = 30000.0 / (peak or 1.0)
    scaled = res * scale
    out = np.empty(len(res) * 2, dtype="<i2")
    out[0::2] = scaled.real.astype("<i2")
    out[1::2] = scaled.imag.astype("<i2")
    return out.tobytes()


def select_rate_and_protocols(bandwidth_hz: float) -> tuple[int, list[list[str]]]:
    """Bandwidth-keyed two-tier policy. Returns (target_rate_hz, passes) where
    each pass is an rtl_433 argument list (empty list = full default -R set)."""
    if bandwidth_hz >= TIER_BANDWIDTH_HZ:
        return 1_600_000, [["-R", "383"], ["-X", _SSN_FLEX]]
    return 1_000_000, [[]]
