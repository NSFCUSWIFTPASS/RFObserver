"""Tone check: is a CW tone present in an averaged PSD, above the noise floor?

A diagnostic for antenna/environment characterization -- transmit a known CW
tone and see, each averaging interval, whether the sensor sees it a threshold
above the noise floor at the queried (absolute) frequency.
"""

from __future__ import annotations

from typing import Any

import numpy as np

_WINDOW_BINS = 2  # peak search +/- this many bins around the tone frequency
_NOISE_PERCENTILE = 10  # matches processing.spectral.compute_noise_floor


def evaluate_tone_check(
    powers: list[float],
    frequencies: list[float],
    *,
    tone_freq_hz: float,
    threshold_db: float,
) -> dict[str, Any]:
    """Evaluate whether the tone at ``tone_freq_hz`` is present in ``powers``.

    ``frequencies`` is the absolute-Hz axis for ``powers`` (same length). The
    tone power is the peak within +/-2 bins of the nearest bin; the noise floor
    is the 10th percentile of the whole averaged PSD. ``detected`` is true when
    the tone is in band and its SNR meets ``threshold_db``.

    Returns a dict with ``tone_freq_hz, in_band, tone_power_db, noise_floor_db,
    snr_db, detected`` (out-of-band -> power/snr are None, detected False).
    """
    freqs = np.asarray(frequencies, dtype=np.float64)
    pw = np.asarray(powers, dtype=np.float64)
    in_band = bool(freqs.size and freqs.min() <= tone_freq_hz <= freqs.max())

    if not in_band or pw.size == 0:
        return {
            "tone_freq_hz": tone_freq_hz,
            "in_band": in_band,
            "tone_power_db": None,
            "noise_floor_db": None,
            "snr_db": None,
            "detected": False,
        }

    idx = int(np.argmin(np.abs(freqs - tone_freq_hz)))
    lo = max(0, idx - _WINDOW_BINS)
    hi = min(pw.size, idx + _WINDOW_BINS + 1)
    tone_power = float(pw[lo:hi].max())
    noise_floor = float(np.percentile(pw, _NOISE_PERCENTILE))
    snr = tone_power - noise_floor
    return {
        "tone_freq_hz": tone_freq_hz,
        "in_band": True,
        "tone_power_db": tone_power,
        "noise_floor_db": noise_floor,
        "snr_db": snr,
        "detected": bool(snr >= threshold_db),
    }
