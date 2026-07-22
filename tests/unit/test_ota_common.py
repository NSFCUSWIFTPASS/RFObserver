"""Tests for the OTA frequency-barcode helper (tools/ota_common.py)."""

import sys

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


def test_widest_burst_offsets_are_small_but_distinct():
    """20 MHz burst can only shift a little, but its 5 durations still differ."""
    offs = [oc.barcode_offset(20_000_000, d) for d in oc.BURST_DURATIONS_MS]
    assert max(abs(o) for o in offs) <= oc.USABLE_HALF_HZ - 20_000_000 / 2 + 1.0
    assert len({round(o) for o in offs}) == len(offs)
