"""Shared definitions for the OTA burst-validation tooling (TX + validator).

The frequency barcode: each (bandwidth, duration) combo is transmitted at a
distinct center offset. Centers are made **globally distinct** (>= MIN_CENTER_
SPACING_HZ apart), so a detection is identified by its center frequency alone --
robust even when a narrow burst's bandwidth is over-measured over the air. TX
and the validator both import ``barcode_offset`` so they agree by construction.
"""

from __future__ import annotations

import numpy as np

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


def make_comb_burst(
    bw_hz: float,
    duration_ms: float,
    offset_hz: float,
    sample_rate_hz: int,
    *,
    peak: float = 0.7,
) -> np.ndarray:
    """A flat-band multitone comb burst, built in the frequency domain (fast).

    Fills the occupied band ``[offset - bw/2, offset + bw/2]`` with unit-magnitude
    Schroeder-phased tones (one per FFT bin), IFFTs to the time domain, peak-
    normalizes to ``peak``, and applies a 5% raised-cosine envelope. Equivalent
    to the simulated comb (same occupied bandwidth + duration + low crest factor)
    but O(N log N) instead of an O(tones x samples) tone-sum -- the wide/long
    combos would otherwise take minutes to synthesize for transmit.

    Returns complex64 in [-1, 1]; drive the SDR DAC with it directly.
    """
    n = max(4, int(duration_ms / 1000.0 * sample_rate_hz))
    freqs = np.fft.fftfreq(n, d=1.0 / sample_rate_hz)
    idx = np.where(np.abs(freqs - offset_hz) <= bw_hz / 2.0)[0]
    if idx.size == 0:  # bw narrower than one bin -> nearest bin to the offset
        idx = np.array([int(np.argmin(np.abs(freqs - offset_hz)))])
    spec = np.zeros(n, dtype=np.complex128)
    j = np.arange(idx.size)
    spec[idx] = np.exp(1j * (-np.pi * j * j / idx.size))  # Schroeder -> low crest factor
    burst = np.fft.ifft(spec)
    burst = burst / (np.max(np.abs(burst)) or 1.0) * peak

    env = np.ones(n)
    r = max(1, n // 20)
    ramp = 0.5 * (1 - np.cos(np.pi * np.arange(r) / r))
    env[:r] = ramp
    env[-r:] = ramp[::-1]
    return (burst * env).astype(np.complex64)
