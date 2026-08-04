"""Replay a real FHSS over-the-air capture and confirm the detector recovers the hops.

Validates the narrowband-burst fix (median noise floor + peak_freq_hz) against a
26 MS/s SSM/FHSS capture with a known hop pattern (verified against gr-modules'
waterfall_plot.py). The capture is large and local, so the test skips when absent
so CI hosts without it still pass.

Bandwidth is deliberately NOT asserted per-hop: the signal is a narrowband carrier
(~60 kHz at -3 dB, ~330 kHz only at -30 dB), so any single bandwidth number is
threshold-dependent. The meaningful, stable properties are: no full-span collapse,
~82 ms durations, and peak frequencies landing on the known hop channels.
"""

from __future__ import annotations

import os

import pytest

from rfobserver.pipeline.replay import run_replay

CAP = os.path.expanduser(
    "~/Documents/iq_capture_hcro-rpi-002_2025-12-12T19-33-52.92Z"
    "_915MHz_26.0Msps_20.0s_35dB_ssm_fhss_OVF.dat"
)

# Known hop peak-frequencies (MHz) in the first ~2 s, from the validated
# waterfall_plot.py analysis of this capture.
REF_HOPS_MHZ = [
    917.260,
    923.912,
    906.189,
    922.109,
    913.400,
    909.490,
    919.697,
    926.299,
    902.889,
]


@pytest.mark.skipif(not os.path.exists(CAP), reason="SSM FHSS capture not present on this host")
@pytest.mark.asyncio
async def test_replay_reproduces_reference_hops():
    res = await run_replay(
        CAP,
        sample_rate_hz=26_000_000,
        center_freq_hz=915_000_000,
        datatype="ci16_le",
        threshold_db=40.0,
        max_seconds=2.0,
    )
    dets = res["detections"]

    # Core fix: the full-span (~26 MHz) collapse must be gone.
    assert not any(d["bandwidth_hz"] >= 20_000_000 for d in dets), (
        f"full-span collapse: {[round(d['bandwidth_hz'] / 1e6, 1) for d in dets]}"
    )

    # Roughly the right number of bursts (9 true hops; allow detector spread).
    assert 7 <= len(dets) <= 14, f"unexpected detection count {len(dets)}"

    # Peak frequencies land on the known hop channels. BW is threshold-dependent
    # and intentionally not asserted; ~250 kHz tolerance covers grid/averaging
    # differences from the batch reference.
    peaks_mhz = [d.get("peak_freq_hz", d["center_freq_hz"]) / 1e6 for d in dets]
    matched = sum(any(abs(p - ref) < 0.25 for p in peaks_mhz) for ref in REF_HOPS_MHZ)
    assert matched >= 6, (
        f"only {matched}/9 reference hops matched; peaks={sorted(round(p, 3) for p in peaks_mhz)}"
    )

    # Durations cluster at the ~82 ms hop dwell.
    near_82 = [d for d in dets if abs(d["duration_ms"] - 82.0) < 12.0]
    assert len(near_82) >= 6, (
        f"only {len(near_82)} bursts near 82 ms; "
        f"durations={sorted(round(d['duration_ms'], 1) for d in dets)}"
    )
