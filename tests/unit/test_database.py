"""Tests for rfobserver.storage.database."""

from datetime import datetime, timedelta

import pytest

from rfobserver.storage.database import SensorDatabase


@pytest.fixture
async def db(tmp_path):
    database = SensorDatabase(str(tmp_path / "test.db"))
    await database.connect()
    yield database
    await database.close()


async def test_insert_and_query_detection(db):
    await db.insert_detection(
        burst_id="burst-001",
        start_time=datetime(2026, 1, 1, 12, 0, 0),
        stop_time=datetime(2026, 1, 1, 12, 0, 1),
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1000.0,
        detection_timestamp=datetime(2026, 1, 1, 12, 0, 0),
    )

    results = await db.query_detections(limit=10)
    assert len(results) == 1
    assert results[0]["burst_id"] == "burst-001"


async def test_duplicate_burst_id_ignored(db):
    kwargs = dict(
        burst_id="burst-dup",
        start_time=datetime(2026, 1, 1),
        stop_time=datetime(2026, 1, 1, 0, 0, 1),
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1000.0,
        detection_timestamp=datetime(2026, 1, 1),
    )
    await db.insert_detection(**kwargs)
    await db.insert_detection(**kwargs)  # should not raise
    results = await db.query_detections()
    assert len(results) == 1


async def test_query_with_freq_filter(db):
    await db.insert_detection(
        burst_id="low",
        start_time=datetime(2026, 1, 1),
        stop_time=datetime(2026, 1, 1, 0, 0, 1),
        center_freq_hz=900e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1000.0,
        detection_timestamp=datetime(2026, 1, 1),
    )
    await db.insert_detection(
        burst_id="high",
        start_time=datetime(2026, 1, 1),
        stop_time=datetime(2026, 1, 1, 0, 0, 1),
        center_freq_hz=930e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1000.0,
        detection_timestamp=datetime(2026, 1, 1),
    )
    results = await db.query_detections(min_freq=920e6)
    assert len(results) == 1
    assert results[0]["burst_id"] == "high"


async def test_config_set_and_get(db):
    await db.set_config("gain", "35")
    value = await db.get_config("gain")
    assert value == "35"


async def test_config_get_missing(db):
    value = await db.get_config("nonexistent")
    assert value is None


async def test_config_overwrite(db):
    await db.set_config("gain", "35")
    await db.set_config("gain", "50")
    assert await db.get_config("gain") == "50"


async def test_insert_stats(db):
    await db.insert_stats(datetime(2026, 1, 1), {"avg_power": -50.0})


async def test_cleanup_old_data(db):
    old_time = datetime.utcnow() - timedelta(days=10)
    await db.insert_detection(
        burst_id="old",
        start_time=old_time,
        stop_time=old_time + timedelta(seconds=1),
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1000.0,
        detection_timestamp=old_time,
    )
    await db.insert_detection(
        burst_id="new",
        start_time=datetime.utcnow(),
        stop_time=datetime.utcnow(),
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1000.0,
        detection_timestamp=datetime.utcnow(),
    )

    removed = await db.cleanup_old_data(days=7)
    assert removed >= 1

    results = await db.query_detections()
    assert len(results) == 1
    assert results[0]["burst_id"] == "new"
