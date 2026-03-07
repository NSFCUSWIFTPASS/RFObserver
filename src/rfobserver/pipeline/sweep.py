"""Frequency sweep logic.

Generates frequency lists for sweep-mode operation. In continuous mode
with a single frequency, this is a simple pass-through.
"""

from __future__ import annotations


def build_frequency_list(start_hz: int, end_hz: int, step_hz: int) -> list[int]:
    """Build a list of center frequencies for sweeping."""
    if step_hz <= 0 or end_hz <= start_hz:
        return [start_hz]

    freqs = []
    f = start_hz
    while f <= end_hz:
        freqs.append(f)
        f += step_hz
    return freqs
