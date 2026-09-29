from pathlib import Path

import pytest

from rfobserver.pipeline.attribution import decode_cs16, find_rtl433

FIXTURE = Path.home() / "ssn_bursts" / "burst_feb4_919MHz_75dB.cs16"
RTL = find_rtl433()

pytestmark = pytest.mark.skipif(
    RTL is None or not FIXTURE.exists(),
    reason="rtl_433 or SSN fixtures not present on this host",
)


def test_decode_ssn_fixture():
    # The fixture is already channelized to 1.6 Msps; decode it directly.
    frames = decode_cs16(RTL, FIXTURE.read_bytes(), 1_600_000, [["-R", "383"]])
    assert frames, "expected at least one decoded frame"
    assert frames[0]["model"] == "SilverSpring-Mesh"


def test_decode_noise_returns_empty():
    frames = decode_cs16(RTL, b"\x00\x00" * 4000, 1_600_000, [["-R", "383"]])
    assert frames == []
