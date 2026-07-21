"""Tests for rfobserver.transport.nats_producer."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from rfobserver.models import (
    BurstFingerprint,
    IQStatistics,
    MetadataRecord,
    ProcessedDataEnvelope,
    PSDData,
)
from rfobserver.transport.nats_producer import (
    STREAM_BURSTS,
    STREAM_CHAMPIONS,
    STREAM_STATS,
    NatsProducer,
)


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


# -- NatsProducer publish behavior ----------------------------------------


def _make_envelope() -> ProcessedDataEnvelope:
    meta = MetadataRecord(
        hostname="rfobs-test",
        organization="TestOrg",
        serial="SN123",
        frequency=2_437_000_000,
        timestamp=datetime(2026, 4, 27, 12, 0, 0),
        source_path=Path("/tmp/x.sc16"),
        gain=40,
        sampling_rate=25_000_000,
        length=0.5,
        interval=10,
    )
    stats = IQStatistics(average=-78.0, max=-40.0, median=-79.0, std=4.5, kurtosis=1.7)
    psd = PSDData(
        powers=[-80.0, -78.0, -76.0],
        frequencies=[-1.0, 0.0, 1.0],
        center_freq=2_437_000_000.0,
        sample_rate=25_000_000,
        num_bins=3,
    )
    return ProcessedDataEnvelope(metadata=meta, statistics=stats, psd_data=psd)


@pytest.mark.asyncio
async def test_publish_stats_routes_to_correct_subject_and_payload():
    p = NatsProducer(url="nats://localhost:4222")
    # Pretend we connected, with a fake jetstream context.
    p._connected = True
    fake_js = AsyncMock()
    fake_js.publish.return_value = AsyncMock(seq=42)
    p._js = fake_js

    envelope = _make_envelope()
    ok = await p.publish_stats(envelope, "rfobs-test")

    assert ok is True
    assert p.stats_count == 1
    assert p.dropped == 0

    fake_js.publish.assert_awaited_once()
    subject, payload = fake_js.publish.await_args.args
    assert subject == f"{STREAM_STATS}.rfobs-test"

    parsed = json.loads(payload.decode())
    assert parsed["metadata"]["hostname"] == "rfobs-test"
    assert parsed["metadata"]["frequency"] == 2_437_000_000
    assert parsed["metadata"]["length"] == 0.5
    assert parsed["metadata"]["interval"] == 10
    assert parsed["statistics"]["average"] == -78.0
    # Stats-only wire contract: the PSD powers array is NOT sent to RFS NATS.
    assert "psd_data" not in parsed


@pytest.mark.asyncio
async def test_publish_stats_when_disconnected_returns_false_and_counts_drop():
    p = NatsProducer(url="nats://localhost:4222")
    # _connected stays False; _js stays None
    envelope = _make_envelope()
    ok = await p.publish_stats(envelope, "rfobs-test")

    assert ok is False
    assert p.stats_count == 0
    assert p.dropped == 1


@pytest.mark.asyncio
async def test_publish_stats_when_publish_raises_counts_drop():
    p = NatsProducer(url="nats://localhost:4222")
    p._connected = True
    fake_js = AsyncMock()
    fake_js.publish.side_effect = RuntimeError("broker down")
    p._js = fake_js

    ok = await p.publish_stats(_make_envelope(), "rfobs-test")

    assert ok is False
    assert p.stats_count == 0
    assert p.dropped == 1


@pytest.mark.asyncio
async def test_publish_champion_is_todo():
    p = NatsProducer(url="nats://localhost:4222")
    with pytest.raises(NotImplementedError):
        await p.publish_champion(_make_envelope(), "h", ["loudest"])


@pytest.mark.asyncio
async def test_publish_burst_is_todo():
    p = NatsProducer(url="nats://localhost:4222")
    with pytest.raises(NotImplementedError):
        await p.publish_burst(object(), "h")


def test_subject_constants_match_documented_layout():
    assert STREAM_STATS == "rfobs.stats"
    assert STREAM_CHAMPIONS == "rfobs.champions"
    assert STREAM_BURSTS == "rfobs.bursts"


def test_stats_envelope_excludes_psd_data():
    """The stats projection carries metadata + statistics but not the PSD array."""
    from rfobserver.models import StatsEnvelope

    env = _make_envelope()
    stats = StatsEnvelope.from_envelope(env)
    parsed = json.loads(stats.model_dump_json())

    assert parsed["metadata"]["hostname"] == env.metadata.hostname
    assert parsed["statistics"]["average"] == env.statistics.average
    assert parsed["message_id"] == env.message_id
    assert "psd_data" not in parsed


@pytest.mark.asyncio
async def test_connection_callbacks_track_connected_state():
    """Disconnect/reconnect callbacks keep the connected flag accurate."""
    p = NatsProducer(url="nats://localhost:4222")
    p._connected = True

    await p._on_disconnected()
    assert p.connected is False  # publishes drop instead of hanging during outage

    await p._on_reconnected()
    assert p.connected is True

    await p._on_closed()
    assert p.connected is False


@pytest.mark.asyncio
async def test_publish_stats_disconnected_does_not_call_publish():
    """While disconnected, no js.publish is attempted (no pileup/hang)."""
    p = NatsProducer(url="nats://localhost:4222")
    p._connected = False
    fake_js = AsyncMock()
    p._js = fake_js

    ok = await p.publish_stats(_make_envelope(), "rfobs-test")

    assert ok is False
    fake_js.publish.assert_not_awaited()
    assert p.dropped == 1
