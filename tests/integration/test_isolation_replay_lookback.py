"""Offline lossless replay with isolation on keeps every picked burst in the ring.

No rtl_433 and no fixtures: a synthetic multi-burst stream is replayed through
run_replay with ISOLATION_ENABLED at the DEFAULT lookback. In lossless mode the
receiver would otherwise run the whole pipeline depth ahead of the burst
thread, and bursts would leave the ring before the stage reads them.
"""

from __future__ import annotations

import json

import numpy as np
import pytest

from rfobserver.config import AppSettings
from rfobserver.pipeline.replay import run_replay

from ._synth import Burst, make_iq_with_bursts

FS = 2_000_000
CENTER = 915_000_000


def _write_cf32_sigmf(base, iq):
    meta = {
        "global": {"core:datatype": "cf32_le", "core:sample_rate": FS, "core:version": "1.0.0"},
        "captures": [{"core:sample_start": 0, "core:frequency": CENTER}],
        "annotations": [],
    }
    base.with_suffix(".sigmf-meta").write_text(json.dumps(meta))
    inter = np.empty(iq.size * 2, dtype=np.float32)
    inter[0::2] = iq.real
    inter[1::2] = iq.imag
    inter.tofile(base.with_suffix(".sigmf-data"))


@pytest.mark.asyncio
async def test_lossless_replay_isolates_every_picked_burst_at_the_default_lookback(tmp_path):
    assert AppSettings(_env_file=None).ISOLATION_LOOKBACK_SEC == 2.0
    offsets = [-600e3, -300e3, 100e3, 400e3, 700e3, -450e3, 250e3, 550e3]
    bursts = [
        Burst(start_sec=0.5 + 0.8 * i, duration_sec=0.02, freq_offset_hz=f, amplitude=0.2)
        for i, f in enumerate(offsets)
    ]
    iq = make_iq_with_bursts(7.0, FS, bursts, noise_stddev=0.01)
    base = tmp_path / "multi"
    _write_cf32_sigmf(base, iq)

    result = await run_replay(
        base.with_suffix(".sigmf-data"),
        threshold_db=20.0,
        overrides={"ISOLATION_ENABLED": True},
    )
    counts = result["isolation"]["counts"]
    assert result["isolation"]["enabled"], result["isolation"]
    assert counts.get("picked", 0) >= len(bursts), counts
    assert counts.get("iq_expired", 0) == 0, counts
    assert counts.get("isolated", 0) + counts.get("too_long", 0) == counts["picked"], counts
    assert counts["received"] == len(result["detections"]), counts
