"""End-to-end: a recorded SigMF capture replayed through the pipeline detects its burst."""

from __future__ import annotations

import json

import numpy as np
import pytest

from rfobserver.pipeline.replay import run_replay

from ._synth import make_iq_with_wideband_burst


def _write_cf32_sigmf(base, iq, sample_rate, center):
    meta = {
        "global": {
            "core:datatype": "cf32_le",
            "core:sample_rate": sample_rate,
            "core:version": "1.0.0",
        },
        "captures": [{"core:sample_start": 0, "core:frequency": center}],
        "annotations": [],
    }
    base.with_suffix(".sigmf-meta").write_text(json.dumps(meta))
    inter = np.empty(iq.size * 2, dtype=np.float32)
    inter[0::2] = iq.real
    inter[1::2] = iq.imag
    inter.tofile(base.with_suffix(".sigmf-data"))


@pytest.mark.asyncio
async def test_replay_detects_known_burst(tmp_path):
    fs = 4_000_000
    center = 915_000_000
    offset = 500_000  # burst at 915.5 MHz
    bw = 500_000
    dur_ms = 20.0
    margin = 0.02
    iq = make_iq_with_wideband_burst(
        duration_sec=margin + dur_ms / 1000.0 + margin,
        sample_rate_hz=fs,
        burst_start_sec=margin,
        burst_duration_sec=dur_ms / 1000.0,
        burst_bw_hz=bw,
        burst_offset_hz=offset,
        num_bins=512,
        per_tone_amp=0.04,
    )
    base = tmp_path / "synthcap"
    _write_cf32_sigmf(base, iq, float(fs), float(center))

    result = await run_replay(base.with_suffix(".sigmf-data"), threshold_db=20.0)
    dets = result["detections"]

    assert result["capture"]["sample_rate_hz"] == fs
    assert dets, "replay produced no detections"
    expected = center + offset
    near = [d for d in dets if abs(d["center_freq_hz"] - expected) < 200_000]
    assert near, (
        f"no detection near planted burst {expected / 1e6:.3f} MHz; "
        f"got {[round(d['center_freq_hz'] / 1e6, 3) for d in dets[:8]]}"
    )
    b = max(near, key=lambda d: d["peak_power_db"])
    # ~500 kHz occupied, ~20 ms long (loose: replay path + rolling detector).
    assert abs(b["bandwidth_hz"] - bw) < 400_000
    assert abs(b["duration_ms"] - dur_ms) < 10.0
