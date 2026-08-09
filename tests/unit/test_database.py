"""Tests for rfobserver.storage.database."""

from datetime import datetime, timedelta, timezone

import pytest

from rfobserver.storage.database import SensorDatabase


def _dt(i: int) -> datetime:
    return datetime(2026, 1, 1, 0, 0, i)


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


async def test_insert_with_peak_freq_hz_roundtrips(db):
    await db.insert_detection(
        burst_id="peak-1",
        start_time=datetime(2026, 1, 1),
        stop_time=datetime(2026, 1, 1, 0, 0, 1),
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1000.0,
        detection_timestamp=datetime(2026, 1, 1),
        peak_freq_hz=917.3e6,
    )
    row = (await db.query_detections())[0]
    assert row["peak_freq_hz"] == 917.3e6


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


async def _insert_durations(db, durations, **overrides):
    common = dict(
        start_time=datetime(2026, 1, 1),
        stop_time=datetime(2026, 1, 1, 0, 0, 1),
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        detection_timestamp=datetime(2026, 1, 1),
    )
    common.update(overrides)
    for i, d in enumerate(durations):
        await db.insert_detection(burst_id=f"d-{i}-{d}", duration_ms=d, **common)


async def test_duration_histogram_buckets_with_fixed_width(db):
    # Half-open [lo, hi) bins anchored at multiples of the width.
    await _insert_durations(db, [82.0, 83.0, 84.0, 85.0, 91.0])
    hist = await db.duration_histogram(bin_width=5.0)
    assert hist["count"] == 5
    assert hist["bin_width"] == 5.0
    by_lo = {b["lo"]: b["count"] for b in hist["bins"]}
    assert by_lo[80.0] == 3  # 82, 83, 84
    assert by_lo[85.0] == 1  # 85 (lower bound inclusive)
    assert by_lo[90.0] == 1  # 91


async def test_duration_histogram_exact_max_on_boundary_in_last_bin(db):
    # The single global max sitting exactly on a bin edge is clamped into the
    # last bin rather than dropping off the end.
    await _insert_durations(db, [81.0, 90.0])
    hist = await db.duration_histogram(bin_width=5.0)
    assert sum(b["count"] for b in hist["bins"]) == 2
    assert hist["bins"][-1]["count"] == 1  # 90 lands in the final bin


async def test_duration_histogram_auto_width_is_sane(db):
    await _insert_durations(db, [float(x) for x in range(0, 200, 10)])
    hist = await db.duration_histogram()  # auto width
    assert hist["count"] == 20
    assert hist["bin_width"] >= 0.5
    assert sum(b["count"] for b in hist["bins"]) == 20


async def test_duration_histogram_respects_sdr_filter(db):
    await _insert_durations(db, [10.0, 11.0], sdr_center_freq_hz=915e6, sample_rate_hz=56e6)
    await _insert_durations(db, [10.0], sdr_center_freq_hz=2437e6, sample_rate_hz=56e6)
    hist = await db.duration_histogram(bin_width=5.0, sdr_center_freq=915e6)
    assert hist["count"] == 2


async def test_duration_histogram_empty(db):
    hist = await db.duration_histogram(bin_width=5.0)
    assert hist["count"] == 0
    assert hist["bins"] == []
    assert hist["min"] is None


async def test_query_detections_duration_range_is_half_open(db):
    await _insert_durations(db, [79.0, 80.0, 84.9, 85.0])
    rows = await db.query_detections(min_duration_ms=80.0, max_duration_ms=85.0)
    durs = sorted(r["duration_ms"] for r in rows)
    assert durs == [80.0, 84.9]  # 79 excluded (< lo), 85 excluded (== hi, half-open)


async def test_query_detections_until_is_half_open(db):
    common = dict(
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=1000.0,
    )
    old = datetime(2026, 1, 1, 0, 0, 0)
    mid = datetime(2026, 1, 1, 0, 0, 10)
    new = datetime(2026, 1, 1, 0, 0, 20)
    await db.insert_detection(
        burst_id="old",
        start_time=old,
        stop_time=old,
        detection_timestamp=old,
        **common,
    )
    await db.insert_detection(
        burst_id="mid",
        start_time=mid,
        stop_time=mid,
        detection_timestamp=mid,
        **common,
    )
    await db.insert_detection(
        burst_id="new",
        start_time=new,
        stop_time=new,
        detection_timestamp=new,
        **common,
    )

    rows = await db.query_detections(since=old, until=new)
    burst_ids = sorted(r["burst_id"] for r in rows)
    assert burst_ids == ["mid", "old"]  # old included (>=), new excluded (< until)


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


async def test_cleanup_covers_detections_stats_tone_checks(db):
    # Use the tz-aware isoformat the pipeline actually stores (datetime.now(timezone.utc)
    # in streaming/zms), so this exercises the real cutoff comparison, not a naive-only path.
    old = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    new = datetime.now(timezone.utc).isoformat()
    # one old + one recent row in each of the three growing tables
    for ts in (old, new):
        await db._db.execute(
            "INSERT INTO detections (burst_id,start_time,stop_time,center_freq_hz,"
            "bandwidth_hz,peak_power_db,duration_ms,detection_timestamp) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (ts, ts, ts, 1e6, 1e3, -50.0, 1.0, ts),
        )
        await db._db.execute("INSERT INTO stats (timestamp,data) VALUES (?,?)", (ts, "{}"))
        await db._db.execute(
            "INSERT INTO tone_checks (timestamp,tone_freq_hz,sdr_center_freq_hz,"
            "in_band,tone_power_db,noise_floor_db,snr_db,detected) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (ts, 1e6, 1e6, 1, -50.0, -90.0, 40.0, 1),
        )
    await db._db.commit()

    deleted = await db.cleanup_old_data(days=7)
    assert deleted == 3  # one old row per table

    for tbl in ("detections", "stats", "tone_checks"):
        async with db._db.execute(f"SELECT COUNT(*) FROM {tbl}") as c:
            assert (await c.fetchone())[0] == 1  # recent row survives


async def test_tone_check_roundtrips(db):
    await db.insert_tone_check(
        timestamp=datetime(2026, 1, 1, 12, 0, 0),
        tone_freq_hz=915.5e6,
        sdr_center_freq_hz=915e6,
        in_band=True,
        tone_power_db=-40.0,
        noise_floor_db=-90.0,
        snr_db=50.0,
        detected=True,
    )
    await db.insert_tone_check(
        timestamp=datetime(2026, 1, 1, 12, 0, 1),
        tone_freq_hz=2.4e9,
        sdr_center_freq_hz=915e6,
        in_band=False,
        tone_power_db=None,
        noise_floor_db=None,
        snr_db=None,
        detected=False,
    )
    rows = await db.query_tone_checks(limit=10)
    assert len(rows) == 2
    newest = rows[0]  # newest first
    assert newest["in_band"] in (0, False)
    assert newest["detected"] in (0, False)
    detected_row = next(r for r in rows if r["detected"])
    assert detected_row["tone_freq_hz"] == 915.5e6
    assert abs(detected_row["snr_db"] - 50.0) < 1e-6


async def test_count_detections_is_monotonic_marker(db):
    assert await db.count_detections() == 0  # empty -> 0

    for i in range(3):
        await db.insert_detection(
            burst_id=f"b{i}",
            start_time=_dt(i),
            stop_time=_dt(i),
            center_freq_hz=1e6,
            bandwidth_hz=1e3,
            peak_power_db=-50.0,
            duration_ms=1.0,
            detection_timestamp=_dt(i),
        )
    m3 = await db.count_detections()
    assert m3 == 3  # MAX(id) after 3 inserts

    # monotonic across a delete of the oldest row (marker must not go backward)
    await db._db.execute("DELETE FROM detections WHERE id = 1")
    await db._db.commit()
    assert await db.count_detections() == m3  # still 3 (MAX(id) unchanged)
