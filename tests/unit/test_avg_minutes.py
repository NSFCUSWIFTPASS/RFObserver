"""Tests for the avg_minutes rollup table and its queries."""

from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.rollup import MinuteSummary, fold_windows

UTC = timezone.utc
T0 = datetime(2026, 9, 19, 3, 0, tzinfo=UTC)


@pytest.fixture
async def db(tmp_path):
    database = SensorDatabase(str(tmp_path / "test.db"))
    await database.connect()
    yield database
    await database.close()


async def _insert_window(db, offset_sec: float, pwr_max: float, center: float = 2.437e9):
    await db.insert_avg_window(
        start_time=T0 + timedelta(seconds=offset_sec),
        duration_sec=0.5,
        sdr_center_freq_hz=center,
        sample_rate_hz=56e6,
        gain_db=40.0,
        num_bins=4,
        freq_start_hz=2.409e9,
        freq_step_hz=1e6,
        powers=np.array([pwr_max, -60.0, -60.0, -60.0], dtype="<f4"),
        pwr_avg=-50.0,
        pwr_max=pwr_max,
        pwr_median=-60.0,
        pwr_std=1.0,
        kurtosis=2.0,
        interference=0,
    )


async def test_rollup_round_trips_through_the_real_insert_path(db):
    # Uses insert_avg_window so the stored timestamp format is the real one.
    await _insert_window(db, 5, -30.0)
    await _insert_window(db, 40, -22.0)
    await _insert_window(db, 70, -35.0)

    rows = []
    async for chunk in db.iter_rollup_windows(since=T0, until=T0 + timedelta(minutes=5)):
        rows.extend(chunk)
    assert len(rows) == 3

    summaries = fold_windows(rows)
    assert await db.upsert_avg_minutes(summaries) == 2

    peaks = await db.query_avg_minute_peaks(
        since=T0, until=T0 + timedelta(minutes=5), metric="pwr_max"
    )
    assert [round(p[1], 1) for p in peaks] == [-22.0, -35.0]
    # The peak timestamp must be the winning window's, not the minute boundary.
    assert peaks[0][0].startswith("2026-09-19T03:00:40")


async def test_upsert_replaces_a_minute_rather_than_duplicating_it(db):
    s = MinuteSummary(
        minute_start="2026-09-19T03:00",
        sdr_center_freq_hz=2.437e9,
        n=1,
        sample_rate_hz=56e6,
        gain_db=40.0,
        pwr_max=-30.0,
        pwr_snr=20.0,
        pwr_avg=-50.0,
        peak_max_time="2026-09-19T03:00:05+00:00",
        peak_snr_time="2026-09-19T03:00:05+00:00",
        peak_avg_time="2026-09-19T03:00:05+00:00",
    )
    await db.upsert_avg_minutes([s])
    await db.upsert_avg_minutes([s._replace(pwr_max=-10.0, n=2)])
    peaks = await db.query_avg_minute_peaks(
        since=T0, until=T0 + timedelta(minutes=5), metric="pwr_max"
    )
    assert len(peaks) == 1
    assert peaks[0][1] == -10.0


async def test_peaks_are_ordered_by_the_requested_metric(db):
    # Minute A: loud peak, high noise floor. Minute B: weaker peak, quiet floor.
    await _insert_window(db, 5, -20.0)
    await _insert_window(db, 65, -30.0)
    rows = []
    async for chunk in db.iter_rollup_windows(since=T0, until=T0 + timedelta(minutes=5)):
        rows.extend(chunk)
    await db.upsert_avg_minutes(fold_windows(rows))

    by_max = await db.query_avg_minute_peaks(
        since=T0, until=T0 + timedelta(minutes=5), metric="pwr_max"
    )
    assert round(by_max[0][1], 1) == -20.0
    by_avg = await db.query_avg_minute_peaks(
        since=T0, until=T0 + timedelta(minutes=5), metric="pwr_avg"
    )
    assert len(by_avg) == 2


async def test_peaks_filter_by_centre_frequency(db):
    await _insert_window(db, 5, -20.0, center=2.437e9)
    await _insert_window(db, 65, -10.0, center=5.8e9)
    rows = []
    async for chunk in db.iter_rollup_windows(since=T0, until=T0 + timedelta(minutes=5)):
        rows.extend(chunk)
    await db.upsert_avg_minutes(fold_windows(rows))

    peaks = await db.query_avg_minute_peaks(
        since=T0, until=T0 + timedelta(minutes=5), metric="pwr_max", sdr_center_freq=2.437e9
    )
    assert len(peaks) == 1
    assert round(peaks[0][1], 1) == -20.0


async def test_peaks_reject_an_unknown_metric(db):
    # The metric name is interpolated into ORDER BY, so it must be whitelisted.
    with pytest.raises(ValueError):
        await db.query_avg_minute_peaks(
            since=T0, until=T0 + timedelta(minutes=5), metric="1; DROP TABLE avg_windows"
        )


async def test_peaks_respect_the_limit(db):
    for i in range(5):
        await _insert_window(db, i * 60 + 5, -30.0 - i)
    rows = []
    async for chunk in db.iter_rollup_windows(since=T0, until=T0 + timedelta(minutes=10)):
        rows.extend(chunk)
    await db.upsert_avg_minutes(fold_windows(rows))
    peaks = await db.query_avg_minute_peaks(
        since=T0, until=T0 + timedelta(minutes=10), metric="pwr_max", limit=2
    )
    assert len(peaks) == 2


async def test_oldest_avg_window_time_is_none_on_an_empty_database(db):
    assert await db.oldest_avg_window_time() is None


async def test_oldest_avg_window_time_returns_the_first_window(db):
    await _insert_window(db, 300, -30.0)
    await _insert_window(db, 5, -30.0)
    oldest = await db.oldest_avg_window_time()
    assert oldest is not None
    assert oldest == T0 + timedelta(seconds=5)
