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


async def test_insert_with_sdr_context_roundtrips(db):
    await db.insert_detection(
        burst_id="sdr-1",
        start_time=datetime(2026, 1, 1),
        stop_time=datetime(2026, 1, 1, 0, 0, 1),
        center_freq_hz=915.2e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1000.0,
        detection_timestamp=datetime(2026, 1, 1),
        sdr_center_freq_hz=915e6,
        sample_rate_hz=56e6,
        lo_offset_hz=0.0,
        analog_bw_hz=None,
        gain_db=40.0,
        antenna="RX2",
        device_serial="MOCK0001",
    )
    row = (await db.query_detections())[0]
    assert row["sdr_center_freq_hz"] == 915e6
    assert row["sample_rate_hz"] == 56e6
    assert row["gain_db"] == 40.0
    assert row["antenna"] == "RX2"
    assert row["device_serial"] == "MOCK0001"
    assert row["analog_bw_hz"] is None


async def test_insert_without_sdr_context_yields_nulls(db):
    # Legacy call without the new kwargs must still work.
    await db.insert_detection(
        burst_id="legacy-1",
        start_time=datetime(2026, 1, 1),
        stop_time=datetime(2026, 1, 1, 0, 0, 1),
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1000.0,
        detection_timestamp=datetime(2026, 1, 1),
    )
    row = (await db.query_detections())[0]
    assert row["sdr_center_freq_hz"] is None
    assert row["gain_db"] is None


async def test_query_filters_by_sdr_context(db):
    common = dict(
        start_time=datetime(2026, 1, 1),
        stop_time=datetime(2026, 1, 1, 0, 0, 1),
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1000.0,
        detection_timestamp=datetime(2026, 1, 1),
        sample_rate_hz=56e6,
    )
    await db.insert_detection(burst_id="a", sdr_center_freq_hz=915e6, gain_db=40.0, **common)
    await db.insert_detection(burst_id="b", sdr_center_freq_hz=2437e6, gain_db=40.0, **common)
    await db.insert_detection(burst_id="c", sdr_center_freq_hz=915e6, gain_db=30.0, **common)

    by_center = await db.query_detections(sdr_center_freq=915e6)
    assert {r["burst_id"] for r in by_center} == {"a", "c"}

    by_center_gain = await db.query_detections(sdr_center_freq=915e6, gain=40.0)
    assert {r["burst_id"] for r in by_center_gain} == {"a"}


async def test_capture_configs_returns_distinct(db):
    common = dict(
        start_time=datetime(2026, 1, 1),
        stop_time=datetime(2026, 1, 1, 0, 0, 1),
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1000.0,
        detection_timestamp=datetime(2026, 1, 1),
        sample_rate_hz=56e6,
        gain_db=40.0,
    )
    await db.insert_detection(burst_id="a", sdr_center_freq_hz=915e6, **common)
    await db.insert_detection(burst_id="b", sdr_center_freq_hz=915e6, **common)  # dup config
    await db.insert_detection(burst_id="c", sdr_center_freq_hz=2437e6, **common)
    # Legacy row with no SDR context is excluded from the config list.
    await db.insert_detection(
        burst_id="legacy",
        start_time=datetime(2026, 1, 1),
        stop_time=datetime(2026, 1, 1, 0, 0, 1),
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1000.0,
        detection_timestamp=datetime(2026, 1, 1),
    )

    configs = await db.capture_configs()
    centers = sorted(c["sdr_center_freq_hz"] for c in configs)
    assert centers == [915e6, 2437e6]


async def test_migration_adds_sdr_columns_to_old_db(tmp_path):
    # Simulate a database created before the SDR columns existed, using the
    # stdlib sqlite3 driver so it is fully written and closed before the async
    # SensorDatabase opens (and migrates) the same file.
    import sqlite3

    db_path = str(tmp_path / "old.db")
    old_schema = """
        CREATE TABLE detections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            burst_id TEXT UNIQUE NOT NULL,
            start_time TEXT NOT NULL,
            stop_time TEXT NOT NULL,
            center_freq_hz REAL NOT NULL,
            bandwidth_hz REAL NOT NULL,
            peak_power_db REAL NOT NULL,
            duration_ms REAL NOT NULL,
            detection_timestamp TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """
    conn = sqlite3.connect(db_path)
    conn.executescript(old_schema)
    conn.execute(
        """INSERT INTO detections
           (burst_id, start_time, stop_time, center_freq_hz, bandwidth_hz,
            peak_power_db, duration_ms, detection_timestamp)
           VALUES ('old-row', '2026-01-01', '2026-01-01', 915e6, 1e6, -30, 1000, '2026-01-01')"""
    )
    conn.commit()
    conn.close()

    database = SensorDatabase(db_path)
    await database.connect()
    try:
        # New columns now exist; the legacy row reads NULL for them.
        old = (await database.query_detections())[0]
        assert old["burst_id"] == "old-row"
        assert old["sdr_center_freq_hz"] is None
        # And a new insert with SDR context works.
        await database.insert_detection(
            burst_id="new-row",
            start_time=datetime(2026, 1, 2),
            stop_time=datetime(2026, 1, 2, 0, 0, 1),
            center_freq_hz=915e6,
            bandwidth_hz=1e6,
            peak_power_db=-30.0,
            duration_ms=1000.0,
            detection_timestamp=datetime(2026, 1, 2),
            sdr_center_freq_hz=915e6,
            gain_db=40.0,
        )
        new = await database.query_detections(sdr_center_freq=915e6)
        assert {r["burst_id"] for r in new} == {"new-row"}
    finally:
        await database.close()


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
