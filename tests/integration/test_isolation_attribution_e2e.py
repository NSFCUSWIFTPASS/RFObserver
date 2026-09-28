"""End to end: real SSN bursts inside a wideband stream, through the streaming
pipeline in replay mode, isolated and decoded by rtl_433 as SilverSpring-Mesh.

The three fixtures are narrowband cs16 captures (1.6 Msps) that rtl_433 decodes
as protocol 383. They are upsampled to 26 Msps and placed at offsets inside
noise, written as a SigMF capture, and replayed with isolation and attribution
on. Skips where rtl_433 or the fixtures are absent (CI).
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
from scipy import signal as sig

from rfobserver.pipeline.attribution import find_rtl433
from rfobserver.pipeline.replay import run_replay

FIX = Path.home() / "ssn_bursts"
FIXTURES = [
    ("burst_feb4_919MHz_75dB.cs16", 919.4e6),
    ("burst_feb5_917MHz_56dB.cs16", 917.9e6),
    ("burst_feb5_913MHz_47dB.cs16", 913.4e6),
]
FS_IN = 1_600_000
FS = 26_000_000
CENTER = 915e6

pytestmark = pytest.mark.skipif(
    find_rtl433() is None or not all((FIX / f).exists() for f, _ in FIXTURES),
    reason="rtl_433 or the SSN fixtures are not on this host",
)


def _load(name: str) -> np.ndarray:
    v = np.fromfile(FIX / name, dtype="<i2").astype(np.float32) / 32768.0
    return (v[0::2] + 1j * v[1::2]).astype(np.complex64)


def _write_ci16_sigmf(base: Path, iq: np.ndarray) -> None:
    meta = {
        "global": {"core:datatype": "ci16_le", "core:sample_rate": FS, "core:version": "1.0.0"},
        "captures": [{"core:sample_start": 0, "core:frequency": CENTER}],
        "annotations": [],
    }
    base.with_suffix(".sigmf-meta").write_text(json.dumps(meta))
    out = np.empty(iq.size * 2, dtype="<i2")
    peak = float(np.max(np.abs(iq))) or 1.0
    out[0::2] = (iq.real / peak * 20000).astype("<i2")
    out[1::2] = (iq.imag / peak * 20000).astype("<i2")
    out.tofile(base.with_suffix(".sigmf-data"))


@pytest.mark.asyncio
async def test_ssn_bursts_decode_through_the_streaming_pipeline(tmp_path):
    rng = np.random.default_rng(3)
    gap = int(0.3 * FS)
    parts = [np.zeros(gap, dtype=np.complex64)]
    for name, freq in FIXTURES:
        b = sig.resample_poly(_load(name), FS // 200_000, FS_IN // 200_000).astype(np.complex64)
        n = np.arange(b.size)
        parts.append(b * np.exp(2j * np.pi * (freq - CENTER) / FS * n).astype(np.complex64))
        parts.append(np.zeros(gap, dtype=np.complex64))
    iq = np.concatenate(parts)
    iq += (rng.normal(0, 1e-3, iq.size) + 1j * rng.normal(0, 1e-3, iq.size)).astype(np.complex64)
    base = tmp_path / "ssn_wide"
    _write_ci16_sigmf(base, iq)

    result = await run_replay(
        base.with_suffix(".sigmf-data"),
        threshold_db=30.0,
        overrides={"ATTRIBUTION_ENABLED": True},
        attribution_wait_sec=60.0,
    )
    models = {round(d["center_freq_hz"] / 1e5): d.get("model") for d in result["detections"]}
    decoded = [d for d in result["detections"] if d.get("protocol_id") == 383]
    assert len(decoded) >= 2, f"expected SSN decodes, got models {models}"
