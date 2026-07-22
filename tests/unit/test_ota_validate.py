"""Tests for the OTA detection validator's barcode matching (tools/ota_validate.py)."""

import sys

sys.path.insert(0, "tools")

import ota_validate as ov  # noqa: E402


def test_match_by_barcode_center_and_bw():
    schedule = [
        {"index": 0, "bw_hz": 500_000, "duration_ms": 10.24, "center_hz": 910_000_000},
        {"index": 1, "bw_hz": 2_000_000, "duration_ms": 83.2, "center_hz": 920_000_000},
    ]
    detections = [
        # matches combo 1 (center ~920M, bw ~2M)
        {
            "center_freq_hz": 920_010_000,
            "bandwidth_hz": 2_100_000,
            "duration_ms": 80.0,
            "peak_power_db": -40,
        },
        # matches combo 0 (center ~910M, bw ~0.5M)
        {
            "center_freq_hz": 909_990_000,
            "bandwidth_hz": 520_000,
            "duration_ms": 10.5,
            "peak_power_db": -45,
        },
        # ambient junk far from any assigned center -> ignored
        {
            "center_freq_hz": 905_000_000,
            "bandwidth_hz": 30_000,
            "duration_ms": 1.0,
            "peak_power_db": -50,
        },
    ]
    results = ov.match_detections(schedule, detections, center_tol_hz=100_000, bw_rel_tol=0.5)
    by_idx = {r["index"]: r for r in results}
    assert by_idx[0]["matched"] and abs(by_idx[0]["meas_duration_ms"] - 10.5) < 1e-6
    assert by_idx[1]["matched"] and abs(by_idx[1]["meas_bandwidth_hz"] - 2_100_000) < 1e-6


def test_strongest_in_window_wins():
    """When two detections sit near one center, the stronger one is chosen."""
    schedule = [{"index": 0, "bw_hz": 2_000_000, "duration_ms": 10.24, "center_hz": 915_000_000}]
    detections = [
        {
            "center_freq_hz": 915_000_000,
            "bandwidth_hz": 2_000_000,
            "duration_ms": 10.0,
            "peak_power_db": -60,
        },
        {
            "center_freq_hz": 915_020_000,
            "bandwidth_hz": 2_050_000,
            "duration_ms": 10.2,
            "peak_power_db": -35,
        },
    ]
    results = ov.match_detections(schedule, detections, center_tol_hz=100_000, bw_rel_tol=0.5)
    assert results[0]["matched"]
    assert abs(results[0]["meas_duration_ms"] - 10.2) < 1e-6  # the -35 dB one


def test_unmatched_combo_reported():
    schedule = [{"index": 0, "bw_hz": 50_000, "duration_ms": 1.3, "center_hz": 926_000_000}]
    results = ov.match_detections(schedule, [], center_tol_hz=100_000, bw_rel_tol=0.5)
    assert results[0]["matched"] is False


def test_wrong_bandwidth_rejected():
    """A detection at the right center but very wrong bandwidth does not match."""
    schedule = [{"index": 0, "bw_hz": 50_000, "duration_ms": 1.3, "center_hz": 912_000_000}]
    detections = [
        {
            "center_freq_hz": 912_000_000,
            "bandwidth_hz": 5_000_000,
            "duration_ms": 1.3,
            "peak_power_db": -30,
        },
    ]
    results = ov.match_detections(schedule, detections, center_tol_hz=100_000, bw_rel_tol=0.5)
    assert results[0]["matched"] is False
