"""Shared definitions for the OTA burst-validation tooling (TX + validator).

The frequency barcode: each (bandwidth, duration) combo is transmitted at a
distinct center offset, so a detection's (center, bandwidth) pair identifies
exactly which combo it was -- ground truth independent of timing/clock.
"""

from __future__ import annotations

BURST_BWS = [50_000, 150_000, 500_000, 2_000_000, 20_000_000]
BURST_DURATIONS_MS = [1.3, 2.7, 10.24, 83.2, 393.1]
CENTER_HZ = 915_000_000
USABLE_HALF_HZ = 12_000_000  # +/-12 MHz of 915 -> occupied band stays in 902-928 ISM


def barcode_offset(bw_hz: float, duration_ms: float) -> float:
    """Distinct center offset (Hz) for one (bandwidth, duration) combo.

    Within a bandwidth the 5 durations are spread evenly across the range the
    band allows for that width (max = USABLE_HALF - bw/2), giving each duration
    a distinct center; across bandwidths the occupied width differs. So
    ``(center, bandwidth)`` is unique for every combo.
    """
    max_off = max(0.0, USABLE_HALF_HZ - bw_hz / 2.0)
    n = len(BURST_DURATIONS_MS)
    if n == 1:
        return 0.0
    i = BURST_DURATIONS_MS.index(duration_ms)
    frac = (2.0 * i / (n - 1)) - 1.0  # i in [0, n-1] -> [-1, +1]
    return frac * max_off


def all_combos() -> list[tuple[int, float, float]]:
    """``[(bw_hz, duration_ms, offset_hz), ...]`` for the full 5x5 matrix."""
    return [(bw, dur, barcode_offset(bw, dur)) for bw in BURST_BWS for dur in BURST_DURATIONS_MS]
