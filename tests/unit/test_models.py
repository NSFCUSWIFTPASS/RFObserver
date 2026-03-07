"""Tests for rfobserver.models."""

from datetime import datetime
from pathlib import Path

import pytest

from rfobserver.models import (
    BurstFingerprint,
    ChampionRecord,
    IQStatistics,
    MetadataRecord,
    ProcessedDataEnvelope,
    PSDData,
    SensorStatus,
)


def test_metadata_record_creation():
    record = MetadataRecord(
        hostname="sensor-01",
        organization="TestOrg",
        frequency=915_000_000,
        timestamp=datetime(2026, 1, 1, 12, 0, 0),
        source_path=Path("/tmp/test.sc16"),
        gain=35,
        sampling_rate=26_000_000,
    )
    assert record.hostname == "sensor-01"
    assert record.frequency == 915_000_000
    assert record.bit_depth == 16


def test_metadata_record_checksum_validation():
    record = MetadataRecord(
        hostname="sensor-01",
        organization="TestOrg",
        frequency=915_000_000,
        timestamp=datetime(2026, 1, 1),
        source_path=Path("/tmp/test.sc16"),
        checksum="abc123",
        gain=35,
        sampling_rate=26_000_000,
    )
    # Matching checksum should pass
    record.validate_checksum("abc123")

    # Mismatched checksum should raise
    with pytest.raises(ValueError, match="Checksum mismatch"):
        record.validate_checksum("wrong")


def test_metadata_record_empty_checksum():
    record = MetadataRecord(
        hostname="sensor-01",
        organization="TestOrg",
        frequency=915_000_000,
        timestamp=datetime(2026, 1, 1),
        source_path=Path("/tmp/test.sc16"),
        gain=35,
        sampling_rate=26_000_000,
    )
    # Empty checksum should pass any computed value
    record.validate_checksum("anything")


def test_iq_statistics():
    stats = IQStatistics(average=-50.0, max=-30.0, median=-52.0, std=0.1, kurtosis=1.5)
    assert stats.average == -50.0
    assert stats.kurtosis == 1.5


def test_psd_data():
    psd = PSDData(
        powers=[-60.0, -55.0, -58.0],
        frequencies=[914e6, 915e6, 916e6],
        center_freq=915e6,
        sample_rate=26_000_000,
        num_bins=3,
    )
    assert len(psd.powers) == 3
    assert psd.num_bins == 3


def test_burst_fingerprint():
    burst = BurstFingerprint(
        start_time=datetime(2026, 1, 1, 12, 0, 0),
        stop_time=datetime(2026, 1, 1, 12, 0, 1),
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1000.0,
    )
    assert burst.bandwidth_hz == 1e6
    assert burst.burst_id  # auto-generated


def test_processed_data_envelope():
    metadata = MetadataRecord(
        hostname="sensor-01",
        organization="TestOrg",
        frequency=915_000_000,
        timestamp=datetime(2026, 1, 1),
        source_path=Path("/tmp/test.sc16"),
        gain=35,
        sampling_rate=26_000_000,
    )
    stats = IQStatistics(average=-50.0, max=-30.0, median=-52.0, std=0.1, kurtosis=1.5)
    psd = PSDData(
        powers=[-60.0], frequencies=[915e6], center_freq=915e6, sample_rate=26_000_000, num_bins=1
    )
    envelope = ProcessedDataEnvelope(metadata=metadata, statistics=stats, psd_data=psd)
    assert envelope.message_id  # auto-generated


def test_sensor_status():
    status = SensorStatus(hostname="sensor-01", pipeline_running=True, capture_count=42)
    assert status.capture_count == 42
    assert status.sdr_temperature_c is None


def test_champion_record():
    metadata = MetadataRecord(
        hostname="sensor-01",
        organization="TestOrg",
        frequency=915_000_000,
        timestamp=datetime(2026, 1, 1),
        source_path=Path("/tmp/test.sc16"),
        gain=35,
        sampling_rate=26_000_000,
    )
    stats = IQStatistics(average=-50.0, max=-30.0, median=-52.0, std=0.1, kurtosis=1.5)
    champion = ChampionRecord(metadata=metadata, statistics=stats, categories=["loudest", "rfi"])
    assert "loudest" in champion.categories
