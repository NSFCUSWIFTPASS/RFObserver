"""Tests for rfobserver.transport.nats_producer -- serialization logic."""

import json
from datetime import datetime

from rfobserver.models import BurstFingerprint


def test_burst_fingerprint_serialization():
    """Verify burst fingerprints serialize to valid JSON for NATS publishing."""
    burst = BurstFingerprint(
        start_time=datetime(2026, 1, 1, 12, 0, 0),
        stop_time=datetime(2026, 1, 1, 12, 0, 1),
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1000.0,
    )

    data = burst.model_dump_json()
    parsed = json.loads(data)

    assert parsed["center_freq_hz"] == 915e6
    assert parsed["peak_power_db"] == -30.0
    assert "burst_id" in parsed


def test_burst_list_serialization():
    """Multiple bursts should serialize as a JSON array."""
    bursts = [
        BurstFingerprint(
            start_time=datetime(2026, 1, 1),
            stop_time=datetime(2026, 1, 1, 0, 0, 1),
            center_freq_hz=915e6,
            bandwidth_hz=1e6,
            peak_power_db=-30.0,
        ),
        BurstFingerprint(
            start_time=datetime(2026, 1, 1, 0, 1),
            stop_time=datetime(2026, 1, 1, 0, 1, 1),
            center_freq_hz=920e6,
            bandwidth_hz=2e6,
            peak_power_db=-25.0,
        ),
    ]

    data = json.dumps([json.loads(b.model_dump_json()) for b in bursts])
    parsed = json.loads(data)
    assert len(parsed) == 2
