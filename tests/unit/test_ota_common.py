"""Tests for the OTA frequency-barcode helper (tools/ota_common.py)."""

import sys

import numpy as np

sys.path.insert(0, "tools")

import ota_common as oc  # noqa: E402


def test_barcode_offsets_fit_the_band():
    """Every combo's occupied band stays within +/- USABLE_HALF of center."""
    for bw in oc.BURST_BWS:
        for dur in oc.BURST_DURATIONS_MS:
            off = oc.barcode_offset(bw, dur)
            assert abs(off) + bw / 2 <= oc.USABLE_HALF_HZ + 1.0, (bw, dur, off)


def test_same_bw_durations_have_distinct_centers():
    """Within a bandwidth the 5 durations get distinct centers (duration barcode)."""
    for bw in oc.BURST_BWS:
        offs = [oc.barcode_offset(bw, d) for d in oc.BURST_DURATIONS_MS]
        assert len({round(o) for o in offs}) == len(offs), bw


def test_combo_identity_is_unique_by_center_and_bw():
    """(round(center), bw) is unique across all 25 combos -- the barcode."""
    keys = {(round(oc.CENTER_HZ + off), bw) for bw, _d, off in oc.all_combos()}
    assert len(keys) == len(oc.BURST_BWS) * len(oc.BURST_DURATIONS_MS)


def test_all_centers_globally_distinct():
    """Every combo has a distinct center, spaced >= MIN_CENTER_SPACING_HZ apart.

    Distinct centers make identity robust to over-measured bandwidth (a narrow
    burst can't be confused with a wider combo sharing a center).
    """
    offs = sorted(off for _bw, _d, off in oc.all_combos())
    assert len(offs) == len(oc.BURST_BWS) * len(oc.BURST_DURATIONS_MS)
    gaps = [b - a for a, b in zip(offs, offs[1:], strict=False)]
    assert min(gaps) >= oc.MIN_CENTER_SPACING_HZ - 1.0, min(gaps)


def test_widest_burst_offsets_are_small_but_distinct():
    """20 MHz burst can only shift a little, but its 5 durations still differ."""
    offs = [oc.barcode_offset(20_000_000, d) for d in oc.BURST_DURATIONS_MS]
    assert max(abs(o) for o in offs) <= oc.USABLE_HALF_HZ - 20_000_000 / 2 + 1.0
    assert len({round(o) for o in offs}) == len(offs)


def test_comb_burst_shape_and_occupied_band():
    """The fast comb has the right length, no clipping, and fills its band."""
    fs = 28_000_000
    bw = 2_000_000
    offset = 5_000_000
    burst = oc.make_comb_burst(bw, 2.7, offset, fs)
    assert burst.dtype == np.complex64
    assert burst.shape == (int(2.7 / 1000.0 * fs),)
    assert float(np.max(np.abs(burst))) <= 0.75  # peak-normalized, no clip

    # Spectrum: power should sit in [offset-bw/2, offset+bw/2], not far outside.
    spec = np.abs(np.fft.fft(burst.astype(np.complex128))) ** 2
    freqs = np.fft.fftfreq(burst.size, d=1.0 / fs)
    in_band = (freqs >= offset - bw / 2) & (freqs <= offset + bw / 2)
    guard = (freqs >= offset - 4 * bw) & (freqs <= offset - 2 * bw)
    assert spec[in_band].mean() > 100 * spec[guard].mean()


def test_comb_burst_narrow_is_fast_and_nonzero():
    """A 20 MHz / 83.2 ms comb (the slow-by-tone-sum case) builds quickly."""
    burst = oc.make_comb_burst(20_000_000, 83.2, 0.0, 28_000_000)
    assert burst.size == int(83.2 / 1000.0 * 28_000_000)
    assert float(np.max(np.abs(burst))) > 0.0
