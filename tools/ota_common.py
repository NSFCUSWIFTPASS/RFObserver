"""Shared definitions for the OTA burst-validation tooling (TX + validator).

The frequency barcode: each (bandwidth, duration) combo is transmitted at a
distinct center offset. Centers are made **globally distinct** (>= MIN_CENTER_
SPACING_HZ apart), so a detection is identified by its center frequency alone --
robust even when a narrow burst's bandwidth is over-measured over the air. TX
and the validator both import ``barcode_offset`` so they agree by construction.
"""

from __future__ import annotations

BURST_BWS = [50_000, 150_000, 500_000, 2_000_000, 20_000_000]
BURST_DURATIONS_MS = [1.3, 2.7, 10.24, 83.2, 393.1]
CENTER_HZ = 915_000_000
USABLE_HALF_HZ = 12_000_000  # +/-12 MHz of 915 -> occupied band stays in 902-928 ISM
MIN_CENTER_SPACING_HZ = 500_000  # >> the validator's center tolerance, so no cross-match
_GRID_STEP_HZ = 250_000


def _assign_offsets() -> dict[tuple[int, float], float]:
    """Deterministically assign each combo a distinct center offset.

    Widest bandwidths first (they have the least offset room, so they claim
    their scarce slots before the narrow bursts fill the band); each combo takes
    the first grid center within its band that is >= MIN_CENTER_SPACING_HZ from
    every already-placed center.
    """
    combos = sorted(
        ((bw, dur) for bw in BURST_BWS for dur in BURST_DURATIONS_MS),
        key=lambda c: (USABLE_HALF_HZ - c[0] / 2.0, c[0], c[1]),
    )
    n_steps = int(USABLE_HALF_HZ // _GRID_STEP_HZ)
    grid = [k * _GRID_STEP_HZ for k in range(-n_steps, n_steps + 1)]
    placed: list[float] = []
    out: dict[tuple[int, float], float] = {}
    for bw, dur in combos:
        max_off = USABLE_HALF_HZ - bw / 2.0
        chosen = 0.0
        for cand in grid:
            if abs(cand) <= max_off and all(abs(cand - p) >= MIN_CENTER_SPACING_HZ for p in placed):
                chosen = cand
                break
        placed.append(chosen)
        out[(bw, dur)] = chosen
    return out


_OFFSETS = _assign_offsets()


def barcode_offset(bw_hz: float, duration_ms: float) -> float:
    """Distinct center offset (Hz) for one (bandwidth, duration) combo."""
    return _OFFSETS[(int(bw_hz), duration_ms)]


def all_combos() -> list[tuple[int, float, float]]:
    """``[(bw_hz, duration_ms, offset_hz), ...]`` for the full 5x5 matrix."""
    return [(bw, dur, barcode_offset(bw, dur)) for bw in BURST_BWS for dur in BURST_DURATIONS_MS]
