"""Tests for rfobserver.storage.database."""

import asyncio
import math
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


async def test_retention_keeps_detections_forever(db):
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

    # No avg windows exist, so nothing is pruned.
    removed = await db.prune_avg_psd_blobs(days=7)
    assert removed == 0

    # Detections are never evicted by retention: old AND new survive.
    results = await db.query_detections()
    assert {r["burst_id"] for r in results} == {"old", "new"}


async def test_retention_never_prunes_rows_only_blobs(db):
    # Use the tz-aware isoformat the pipeline actually stores (datetime.now(timezone.utc)
    # in streaming/zms), so this exercises the real cutoff comparison, not a naive-only path.
    old = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    new = datetime.now(timezone.utc).isoformat()
    # one old + one recent row in each of the growing tables
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
    common = dict(
        duration_sec=0.5,
        sample_rate_hz=4.0,
        gain_db=40.0,
        num_bins=2,
        freq_start_hz=0.0,
        freq_step_hz=1.0,
        pwr_avg=-70.0,
        pwr_max=-50.0,
        pwr_median=-72.0,
        pwr_std=3.0,
        kurtosis=1.0,
        powers=[-70.0, -60.0],
    )
    await db.insert_avg_window(
        start_time=datetime.now(timezone.utc) - timedelta(days=30),
        sdr_center_freq_hz=100e6,
        **common,
    )
    await db.insert_avg_window(
        start_time=datetime.now(timezone.utc),
        sdr_center_freq_hz=100e6,
        **common,
    )
    await db._db.commit()

    pruned = await db.prune_avg_psd_blobs(days=7)
    assert pruned == 1  # only the old window's PSD blob

    # Every row survives in every table: retention only nulls blobs.
    for tbl in ("detections", "stats", "tone_checks", "avg_windows"):
        async with db._db.execute(f"SELECT COUNT(*) FROM {tbl}") as c:
            assert (await c.fetchone())[0] == 2

    cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat()
    for win in await db.query_avg_windows(limit=10):
        full = await db.get_avg_window(win["id"])
        assert full is not None
        if win["start_time"] < cutoff:
            # Old window: blob evicted, stats + frequency axis intact.
            assert full["powers"] is None
            assert len(full["frequencies"]) == full["num_bins"]
        else:
            assert full["powers"] == pytest.approx([-70.0, -60.0], abs=1e-3)


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


async def test_insert_and_query_avg_window(db):
    await db.insert_avg_window(
        start_time=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        duration_sec=0.5,
        sdr_center_freq_hz=2_437_000_000.0,
        sample_rate_hz=56_000_000.0,
        gain_db=40.0,
        num_bins=4,
        freq_start_hz=2_409_000_000.0,
        freq_step_hz=14_000_000.0,
        pwr_avg=-70.0,
        pwr_max=-50.0,
        pwr_median=-72.0,
        pwr_std=3.0,
        kurtosis=1.2,
        powers=[-80.0, -70.0, -60.0, -50.0],
    )
    rows = await db.query_avg_windows(limit=10)
    assert len(rows) == 1
    r = rows[0]
    assert r["num_bins"] == 4
    assert r["pwr_avg"] == -70.0
    assert r["sdr_center_freq_hz"] == 2_437_000_000.0
    assert r["interference"] is None
    # The light query does not carry the heavy blobs.
    assert "psd_powers" not in r
    assert "violations" not in r


async def test_query_avg_windows_time_and_tuning_filters(db):
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    common = dict(
        duration_sec=0.5,
        sample_rate_hz=56e6,
        gain_db=40.0,
        num_bins=2,
        freq_start_hz=0.0,
        freq_step_hz=1.0,
        pwr_avg=-70.0,
        pwr_max=-50.0,
        pwr_median=-72.0,
        pwr_std=3.0,
        kurtosis=1.0,
        powers=[-70.0, -60.0],
    )
    await db.insert_avg_window(start_time=base, sdr_center_freq_hz=100e6, **common)
    await db.insert_avg_window(
        start_time=base + timedelta(seconds=10), sdr_center_freq_hz=200e6, **common
    )
    # Time window excludes the first.
    rows = await db.query_avg_windows(since=base + timedelta(seconds=5))
    assert [r["sdr_center_freq_hz"] for r in rows] == [200e6]
    # Tuning filter selects one center.
    rows = await db.query_avg_windows(sdr_center_freq=100e6)
    assert [r["sdr_center_freq_hz"] for r in rows] == [100e6]


async def test_get_avg_window_decodes_psd_and_frequencies(db):
    await db.insert_avg_window(
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        duration_sec=0.5,
        sdr_center_freq_hz=100e6,
        sample_rate_hz=4.0,
        gain_db=40.0,
        num_bins=4,
        freq_start_hz=98.0,
        freq_step_hz=1.0,
        pwr_avg=-70.0,
        pwr_max=-50.0,
        pwr_median=-72.0,
        pwr_std=3.0,
        kurtosis=1.0,
        powers=[-80.0, -70.0, -60.0, -50.0],
    )
    rows = await db.query_avg_windows(limit=1)
    full = await db.get_avg_window(rows[0]["id"])
    assert full is not None
    assert full["powers"] == pytest.approx([-80.0, -70.0, -60.0, -50.0], abs=1e-3)
    assert full["frequencies"] == pytest.approx([98.0, 99.0, 100.0, 101.0], abs=1e-6)
    assert "psd_powers" not in full


async def test_get_avg_window_missing_returns_none(db):
    assert await db.get_avg_window(9999) is None


async def test_detections_for_window_associates_by_start_and_tuning(db):
    win_start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    await db.insert_avg_window(
        start_time=win_start,
        duration_sec=2.0,
        sdr_center_freq_hz=915e6,
        sample_rate_hz=56e6,
        gain_db=40.0,
        num_bins=2,
        freq_start_hz=0.0,
        freq_step_hz=1.0,
        pwr_avg=-70.0,
        pwr_max=-50.0,
        pwr_median=-72.0,
        pwr_std=3.0,
        kurtosis=1.0,
        powers=[-70.0, -60.0],
    )
    win = (await db.query_avg_windows(limit=1))[0]
    full = await db.get_avg_window(win["id"])

    def _det(bid, sec, center=915e6):
        return dict(
            burst_id=bid,
            start_time=win_start + timedelta(seconds=sec),
            stop_time=win_start + timedelta(seconds=sec + 0.1),
            center_freq_hz=915.1e6,
            bandwidth_hz=1e6,
            peak_power_db=-30.0,
            duration_ms=100.0,
            detection_timestamp=win_start + timedelta(seconds=sec),
            sdr_center_freq_hz=center,
            sample_rate_hz=56e6,
            gain_db=40.0,
        )

    await db.insert_detection(**_det("in", 0.5))  # inside window
    await db.insert_detection(**_det("out", 5.0))  # after window
    await db.insert_detection(**_det("wrong-tune", 0.6, center=100e6))  # inside time, wrong center

    dets = await db.detections_for_window(full)
    assert {d["burst_id"] for d in dets} == {"in"}


async def test_prune_evicts_old_psd_keeps_stats(db):
    old = datetime.now(timezone.utc) - timedelta(days=30)
    recent = datetime.now(timezone.utc)
    common = dict(
        duration_sec=0.5,
        sdr_center_freq_hz=100e6,
        sample_rate_hz=4.0,
        gain_db=40.0,
        num_bins=2,
        freq_start_hz=0.0,
        freq_step_hz=1.0,
        pwr_avg=-70.0,
        pwr_max=-50.0,
        pwr_median=-72.0,
        pwr_std=3.0,
        kurtosis=1.0,
        powers=[-70.0, -60.0],
    )
    await db.insert_avg_window(start_time=old, **common)
    await db.insert_avg_window(start_time=recent, **common)
    pruned = await db.prune_avg_psd_blobs(days=7)
    assert pruned == 1
    # Both stats rows survive; only the old window's PSD blob is evicted.
    rows = await db.query_avg_windows(limit=10)
    assert len(rows) == 2
    cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat()
    for win in rows:
        full = await db.get_avg_window(win["id"])
        assert full is not None
        if win["start_time"] < cutoff:
            assert full["powers"] is None
            assert full["pwr_avg"] == -70.0  # stats survive
            assert full["frequencies"] == pytest.approx([0.0, 1.0], abs=1e-6)
        else:
            assert full["powers"] == pytest.approx([-70.0, -60.0], abs=1e-3)
    # A second pass reports nothing left to prune.
    assert await db.prune_avg_psd_blobs(days=7) == 0


async def test_migration_makes_psd_powers_nullable(tmp_path):
    # Simulate a database created before the retention redesign, where
    # avg_windows.psd_powers was BLOB NOT NULL. connect() must rebuild the
    # table so the column is nullable (retention nulls the blob in place).
    import sqlite3

    db_path = str(tmp_path / "old_avg.db")
    old_schema = """
        CREATE TABLE avg_windows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time TEXT NOT NULL,
            duration_sec REAL NOT NULL,
            sdr_center_freq_hz REAL NOT NULL,
            sample_rate_hz REAL NOT NULL,
            gain_db REAL,
            num_bins INTEGER NOT NULL,
            freq_start_hz REAL NOT NULL,
            freq_step_hz REAL NOT NULL,
            pwr_avg REAL,
            pwr_max REAL,
            pwr_median REAL,
            pwr_std REAL,
            kurtosis REAL,
            interference INTEGER,
            psd_powers BLOB NOT NULL,
            violations BLOB,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """
    conn = sqlite3.connect(db_path)
    conn.executescript(old_schema)
    # One row with a 1-element little-endian float32 blob (1.0).
    conn.execute(
        """INSERT INTO avg_windows (start_time, duration_sec, sdr_center_freq_hz,
           sample_rate_hz, gain_db, num_bins, freq_start_hz, freq_step_hz,
           pwr_avg, pwr_max, pwr_median, pwr_std, kurtosis, interference,
           psd_powers, violations)
           VALUES ('2026-01-01T00:00:00+00:00', 0.5, 100e6, 4.0, 40.0, 4,
                   98.0, 1.0, -70.0, -50.0, -72.0, 3.0, 1.0, NULL, X'0000803F', NULL)"""
    )
    conn.commit()
    conn.close()

    database = SensorDatabase(db_path)
    await database.connect()
    try:
        # The column is now nullable...
        async with database._db.execute("PRAGMA table_info(avg_windows)") as c:
            cols = {row[1]: row for row in await c.fetchall()}
        assert cols["psd_powers"][3] == 0  # notnull flag cleared
        # ...the existing row survived with its blob intact...
        full = await database.get_avg_window(1)
        assert full is not None
        assert full["powers"] == pytest.approx([1.0], abs=1e-6)
        # ...and pruning can now null the blob without dropping the row.
        assert await database.prune_avg_psd_blobs(days=7) == 1
        pruned_full = await database.get_avg_window(1)
        assert pruned_full is not None
        assert pruned_full["powers"] is None
        assert pruned_full["pwr_avg"] == -70.0
    finally:
        await database.close()


def _avg_common(**overrides):
    c = dict(
        duration_sec=0.5,
        sdr_center_freq_hz=100e6,
        sample_rate_hz=4.0,
        gain_db=40.0,
        num_bins=4,
        freq_start_hz=98.0,
        freq_step_hz=1.0,
        pwr_avg=-70.0,
        pwr_max=-50.0,
        pwr_median=-72.0,
        pwr_std=3.0,
        kurtosis=1.0,
        powers=[-80.0, -70.0, -60.0, -50.0],
    )
    c.update(overrides)
    return c


async def test_query_avg_waterfall_buckets_by_time(db):
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    # 8 windows, 1s apart, 2s buckets -> 4 buckets of 2 windows.
    for i in range(8):
        await db.insert_avg_window(start_time=base + timedelta(seconds=i), **_avg_common())
    result = await db.query_avg_waterfall(
        since=base, until=base + timedelta(seconds=8), max_rows=4, max_bins=4
    )
    assert result["mode"] == 1  # 8 windows > max_rows=4 -> aggregated
    assert result["bucket_sec"] == pytest.approx(2.0)
    assert len(result["buckets"]) == 4
    assert [b["count"] for b in result["buckets"]] == [2, 2, 2, 2]
    assert [b["duration_sec"] for b in result["buckets"]] == [2.0, 2.0, 2.0, 2.0]
    row0 = result["psd_rows"][0]
    assert row0 == pytest.approx([-80.0, -70.0, -60.0, -50.0], abs=1e-3)
    assert result["min_db"] <= -80.0
    assert result["max_db"] >= -50.0
    assert result["total_windows"] == 8


async def test_query_avg_waterfall_buckets_anchored_while_range_slides(db):
    """Live "Now" ranges slide every poll; a since-anchored bucket grid shifts
    with them, so a narrow peak keeps changing buckets and visibly flickers.
    The grid must anchor to absolute epoch multiples of bucket_sec instead:
    the same sliding window at two poll times yields identical boundaries and
    an identical peak bucket."""
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    # 2 s buckets (8 s span, max_rows=4); noise windows at 1 s spacing...
    for i in range(12):
        await db.insert_avg_window(start_time=base + timedelta(seconds=i), **_avg_common())
    # ...plus one narrow peak at t=4.5, 30 dB above the noise.
    await db.insert_avg_window(
        start_time=base + timedelta(seconds=4, milliseconds=500),
        **_avg_common(pwr_avg=-40.0, pwr_max=-38.0, powers=[-40.0, -40.0, -40.0, -40.0]),
    )

    def peak_bucket(result):
        starts = [b["start_epoch"] for b in result["buckets"]]
        for i, b in enumerate(result["buckets"]):
            if b["count"] > 0 and b["pwr_avg"] > -65.0:  # the peak's bucket stands out
                return starts, i, result["psd_rows"][i]
        raise AssertionError("peak bucket not found")

    a = await db.query_avg_waterfall(
        since=base + timedelta(milliseconds=250),
        until=base + timedelta(seconds=8, milliseconds=250),
        max_rows=4,
        max_bins=4,
    )
    b = await db.query_avg_waterfall(
        since=base + timedelta(seconds=1, milliseconds=250),
        until=base + timedelta(seconds=9, milliseconds=250),
        max_rows=4,
        max_bins=4,
    )
    assert a["mode"] == 1 and b["mode"] == 1
    starts_a, idx_a, row_a = peak_bucket(a)
    starts_b, idx_b, row_b = peak_bucket(b)
    # Both grids anchor to epoch multiples of 2 s: identical boundaries,
    # identical peak bucket, identical averaged PSD — the peak cannot flicker.
    assert starts_a == starts_b
    assert starts_a[0] == pytest.approx(base.timestamp())  # anchored, not since
    assert idx_a == idx_b
    assert row_a == pytest.approx(row_b, abs=1e-3)


async def test_query_avg_stats_anchored_while_range_slides(db):
    """Same absolute anchoring for the stats timeline (power/kurtosis charts)."""
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    for i in range(12):
        await db.insert_avg_window(start_time=base + timedelta(seconds=i), **_avg_common())
    await db.insert_avg_window(
        start_time=base + timedelta(seconds=4, milliseconds=500),
        **_avg_common(pwr_avg=-40.0, pwr_max=-38.0),
    )
    a = await db.query_avg_stats(
        since=base + timedelta(milliseconds=250),
        until=base + timedelta(seconds=8, milliseconds=250),
        max_points=4,
    )
    b = await db.query_avg_stats(
        since=base + timedelta(seconds=1, milliseconds=250),
        until=base + timedelta(seconds=9, milliseconds=250),
        max_points=4,
    )
    starts_a = [p["start_time"] for p in a["points"]]
    starts_b = [p["start_time"] for p in b["points"]]
    assert starts_a == starts_b
    peak_a = [p for p in a["points"] if p["pwr_avg"] is not None and p["pwr_avg"] > -65.0]
    peak_b = [p for p in b["points"] if p["pwr_avg"] is not None and p["pwr_avg"] > -65.0]
    assert len(peak_a) == 1 and len(peak_b) == 1
    assert peak_a[0]["start_time"] == peak_b[0]["start_time"]
    assert peak_a[0]["pwr_avg"] == pytest.approx(peak_b[0]["pwr_avg"])


async def test_query_avg_waterfall_raw_mode_below_max_rows(db):
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    # 3 windows over a 60s span, max_rows=600 -> raw (each window is a row).
    for i in range(3):
        await db.insert_avg_window(start_time=base + timedelta(seconds=10 * i), **_avg_common())
    result = await db.query_avg_waterfall(
        since=base, until=base + timedelta(seconds=60), max_rows=600, max_bins=4
    )
    assert result["mode"] == 0  # 3 windows <= 600 rows -> raw
    assert len(result["buckets"]) == 3  # one row per window, not 600
    assert [b["count"] for b in result["buckets"]] == [1, 1, 1]
    assert [b["duration_sec"] for b in result["buckets"]] == [0.5, 0.5, 0.5]
    # Each row's start_epoch is its window's own start time.
    starts = [b["start_epoch"] for b in result["buckets"]]
    assert starts[0] == pytest.approx(base.timestamp())
    assert starts[1] == pytest.approx((base + timedelta(seconds=10)).timestamp())
    # Each row's PSD is that window's own powers.
    for row in result["psd_rows"]:
        assert row == pytest.approx([-80.0, -70.0, -60.0, -50.0], abs=1e-3)
    assert result["total_windows"] == 3


async def test_query_avg_waterfall_downsamples_bins(db):
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    # num_bins=4, max_bins=2 -> factor 2 mean per group.
    await db.insert_avg_window(
        start_time=base,
        **_avg_common(num_bins=4, powers=[-80.0, -70.0, -60.0, -50.0]),
    )
    result = await db.query_avg_waterfall(
        since=base, until=base + timedelta(seconds=1), max_rows=1, max_bins=2
    )
    assert result["num_bins"] == 2
    assert result["psd_rows"][0] == pytest.approx([-75.0, -55.0], abs=1e-3)
    # Downsampled axis is still uniform: start' = 98 + (2-1)*1/2 = 98.5, step' = 2.
    assert result["freq_start_hz"] == pytest.approx(98.5)
    assert result["freq_step_hz"] == pytest.approx(2.0)


async def test_query_avg_waterfall_pruned_blob_stats_only(db):
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await db.insert_avg_window(start_time=base, **_avg_common(powers=[-70.0, -60.0, -50.0, -40.0]))
    # Prune the blob (retention) -> the window still counts toward stats.
    await db._db.execute("UPDATE avg_windows SET psd_powers = NULL WHERE id = 1")
    await db._db.commit()
    result = await db.query_avg_waterfall(since=base, until=base + timedelta(seconds=1), max_rows=1)
    assert result["total_windows"] == 0  # no PSD windows
    assert result["buckets"][0]["count"] == 1  # stats still counted
    assert all(math.isnan(v) for v in result["psd_rows"][0])
    assert result["buckets"][0]["pwr_avg"] == pytest.approx(-70.0)


async def test_query_avg_waterfall_empty_range(db):
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    result = await db.query_avg_waterfall(since=base, until=base + timedelta(hours=1), max_rows=4)
    assert result["total_windows"] == 0
    assert result["buckets"] == []  # raw mode with no windows -> no rows
    assert result["psd_rows"] == []


async def test_query_avg_stats_works_without_blobs(db):
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await db.insert_avg_window(start_time=base, **_avg_common(pwr_max=-50.0))
    await db.insert_avg_window(start_time=base + timedelta(seconds=1), **_avg_common(pwr_max=-45.0))
    result = await db.query_avg_stats(since=base, until=base + timedelta(seconds=2), max_points=1)
    assert len(result["points"]) == 1
    p = result["points"][0]
    assert p["count"] == 2
    assert p["pwr_avg"] == pytest.approx(-70.0)
    assert p["pwr_max"] == pytest.approx(-45.0)  # max of maxes


async def test_query_avg_stats_raw_mode(db):
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    # 3 windows, max_points=600 -> raw (one point per window).
    for i in range(3):
        await db.insert_avg_window(
            start_time=base + timedelta(seconds=5 * i), **_avg_common(pwr_avg=-70.0 + i)
        )
    result = await db.query_avg_stats(
        since=base, until=base + timedelta(seconds=30), max_points=600
    )
    assert len(result["points"]) == 3
    assert [p["count"] for p in result["points"]] == [1, 1, 1]
    assert [p["pwr_avg"] for p in result["points"]] == [-70.0, -69.0, -68.0]


async def test_avg_window_configs_distinct_and_latest(db):
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await db.insert_avg_window(start_time=base, **_avg_common(sdr_center_freq_hz=100e6))
    await db.insert_avg_window(start_time=base, **_avg_common(sdr_center_freq_hz=200e6))
    await db.insert_avg_window(
        start_time=base + timedelta(seconds=1), **_avg_common(sdr_center_freq_hz=200e6)
    )
    result = await db.avg_window_configs()
    assert {c["sdr_center_freq_hz"] for c in result["configs"]} == {100e6, 200e6}
    assert result["latest"]["sdr_center_freq_hz"] == 200e6


# -- Stuck-write resilience (device hiccup wedge) --


def _sabotage_execute_hang(conn, hang: asyncio.Event) -> None:
    """Replace a connection's execute with one that never returns — simulates
    a storage-device hiccup wedging the aiosqlite worker thread mid-write."""

    def fake_execute(*args, **kwargs):
        async def _coro():
            await hang.wait()
            raise AssertionError("should never complete")

        return _coro()

    conn.execute = fake_execute


async def test_stuck_write_abandons_connection_and_retries(db):
    """A write that never returns must time out, abandon the connection, and
    complete on a fresh one — the device hiccup then costs one timeout
    instead of the whole avg_windows history."""
    corpse = db._db
    db._write_timeout = 0.05
    _sabotage_execute_hang(corpse, asyncio.Event())

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await db.insert_avg_window(start_time=base, **_avg_common())

    assert db._db is not corpse, "stuck connection must be abandoned"
    rows = await db.query_avg_windows(since=base - timedelta(seconds=1), limit=10)
    assert len(rows) == 1, "retried insert lands on the fresh connection"
    await corpse.close()  # sabotage bypassed aiosqlite internals; close is clean


async def test_concurrent_stuck_writes_reconnect_once(db):
    """Two writers timing out on the same corpse must share one reconnect —
    the second sees the fresh connection and retries on it directly."""
    corpse = db._db
    db._write_timeout = 0.05
    _sabotage_execute_hang(corpse, asyncio.Event())

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await asyncio.gather(
        db.insert_avg_window(start_time=base, **_avg_common()),
        db.insert_stats(timestamp=base, data={"k": 1}),
    )
    fresh = db._db
    assert fresh is not corpse
    # Both retries landed on the same fresh connection.
    rows = await db.query_avg_windows(since=base - timedelta(seconds=1), limit=10)
    assert len(rows) == 1
    await corpse.close()


async def test_reconnect_is_noop_when_already_replaced(db):
    """_reconnect(expect=stale) must not clobber a connection that another
    coroutine already refreshed."""
    fresh = db._db
    other = object()
    await db._reconnect(expect=other)
    assert db._db is fresh
