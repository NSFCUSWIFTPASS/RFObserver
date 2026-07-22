"""Tests for the pure tone-check evaluation (processing/tone_check.py)."""

from __future__ import annotations

import numpy as np

from rfobserver.processing.tone_check import evaluate_tone_check


def _flat_psd_with_tone(center_hz, rate, nbins, tone_hz, tone_db, floor_db):
    freqs = (np.fft.fftshift(np.fft.fftfreq(nbins, 1.0 / rate)) + center_hz).tolist()
    powers = [floor_db] * nbins
    if min(freqs) <= tone_hz <= max(freqs):
        i = int(np.argmin(np.abs(np.array(freqs) - tone_hz)))
        powers[i] = tone_db
    return powers, freqs


def test_tone_detected_above_threshold():
    powers, freqs = _flat_psd_with_tone(915e6, 28e6, 1024, 915.5e6, -40.0, -90.0)
    r = evaluate_tone_check(powers, freqs, tone_freq_hz=915.5e6, threshold_db=10.0)
    assert r["in_band"] is True
    assert r["detected"] is True
    assert r["snr_db"] > 40.0  # ~50 dB tone over floor
    assert abs(r["tone_power_db"] - (-40.0)) < 1e-6


def test_tone_rejected_below_threshold():
    # tone only 5 dB over floor, threshold 10 -> not detected
    powers, freqs = _flat_psd_with_tone(915e6, 28e6, 1024, 915.5e6, -85.0, -90.0)
    r = evaluate_tone_check(powers, freqs, tone_freq_hz=915.5e6, threshold_db=10.0)
    assert r["in_band"] is True
    assert r["detected"] is False
    assert r["snr_db"] < 10.0


def test_out_of_band_tone_not_detected():
    powers, freqs = _flat_psd_with_tone(915e6, 28e6, 1024, 915.5e6, -40.0, -90.0)
    # ask for a tone at 2.4 GHz, far outside the 915 +/- 14 MHz span
    r = evaluate_tone_check(powers, freqs, tone_freq_hz=2_400_000_000, threshold_db=10.0)
    assert r["in_band"] is False
    assert r["detected"] is False


def test_reports_the_queried_frequency():
    powers, freqs = _flat_psd_with_tone(915e6, 28e6, 1024, 915.5e6, -40.0, -90.0)
    r = evaluate_tone_check(powers, freqs, tone_freq_hz=915.5e6, threshold_db=10.0)
    assert r["tone_freq_hz"] == 915.5e6
    assert "noise_floor_db" in r
