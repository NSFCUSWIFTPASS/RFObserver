"""Tests for the avg_minutes rollup loop and its watermarks."""

from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from rfobserver.pipeline.app import _rollup_backfill, _rollup_forward, _rollup_span
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.rollup import ROLLUP_NEWEST_KEY, ROLLUP_OLDEST_KEY

UTC = timezone.utc
NOW = datetime(2026, 9, 19, 6, 0, tzinfo=UTC)


@pytest.fixture
async def db(tmp_path):
    database = SensorDatabase(str(tmp_path / "test.db"))
    await database.connect()
    yield database
    await database.close()


async def _insert_window(db, when: datetime, pwr_max: float):
    await db.insert_avg_window(
        start_time=when,
        duration_sec=0.5,
        sdr_center_freq_hz=2.437e9,
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


async def test_span_folds_and_writes(db):
    await _insert_window(db, NOW - timedelta(minutes=30), -25.0)
    written = await _rollup_span(db, NOW - timedelta(hours=1), NOW)
    assert written == 1
    peaks = await db.query_avg_minute_peaks(
        since=NOW - timedelta(hours=1), until=NOW, metric="pwr_max"
    )
    assert round(peaks[0][1], 1) == -25.0


async def test_first_forward_run_seeds_the_watermark_without_scanning(db):
    await _insert_window(db, NOW - timedelta(days=5), -25.0)
    await _rollup_forward(db, NOW)
    assert await db.get_config(ROLLUP_NEWEST_KEY) == "2026-09-19T06:00"
    # Nothing was rolled up yet; that is the backfill's job.
    assert (
        await db.query_avg_minute_peaks(since=NOW - timedelta(days=7), until=NOW, metric="pwr_max")
        == []
    )


async def test_forward_rolls_minutes_that_have_closed(db):
    await db.set_config(ROLLUP_NEWEST_KEY, "2026-09-19T05:00")
    await _insert_window(db, NOW - timedelta(minutes=30), -25.0)
    await _rollup_forward(db, NOW)
    assert await db.get_config(ROLLUP_NEWEST_KEY) == "2026-09-19T06:00"
    peaks = await db.query_avg_minute_peaks(
        since=NOW - timedelta(hours=2), until=NOW, metric="pwr_max"
    )
    assert len(peaks) == 1


async def test_forward_never_rolls_the_open_minute(db):
    await db.set_config(ROLLUP_NEWEST_KEY, "2026-09-19T05:59")
    # A window inside the minute that is still in progress.
    await _insert_window(db, NOW + timedelta(seconds=10), -25.0)
    await _rollup_forward(db, NOW + timedelta(seconds=30))
    peaks = await db.query_avg_minute_peaks(
        since=NOW, until=NOW + timedelta(minutes=5), metric="pwr_max"
    )
    assert peaks == []


async def test_backfill_walks_backwards_and_records_how_far_it_reached(db):
    await db.set_config(ROLLUP_NEWEST_KEY, "2026-09-19T06:00")
    await _insert_window(db, NOW - timedelta(minutes=90), -25.0)
    await _rollup_backfill(db, NOW)
    oldest = await db.get_config(ROLLUP_OLDEST_KEY)
    assert oldest is not None
    assert oldest <= "2026-09-19T04:30"
    peaks = await db.query_avg_minute_peaks(
        since=NOW - timedelta(hours=3), until=NOW, metric="pwr_max"
    )
    assert len(peaks) == 1


async def test_backfill_stops_at_the_oldest_window(db):
    await db.set_config(ROLLUP_NEWEST_KEY, "2026-09-19T06:00")
    await _insert_window(db, NOW - timedelta(minutes=10), -25.0)
    await _rollup_backfill(db, NOW)
    first = await db.get_config(ROLLUP_OLDEST_KEY)
    await _rollup_backfill(db, NOW)
    # Already at the bottom: the watermark must not keep walking into empty time.
    assert await db.get_config(ROLLUP_OLDEST_KEY) == first


async def test_backfill_does_nothing_on_an_empty_database(db):
    await db.set_config(ROLLUP_NEWEST_KEY, "2026-09-19T06:00")
    await _rollup_backfill(db, NOW)
    # oldest_avg_window_time() is None on an empty database, so
    # _rollup_backfill returns before ever touching ROLLUP_OLDEST_KEY.
    assert await db.get_config(ROLLUP_OLDEST_KEY) is None


async def test_rollup_is_idempotent(db):
    await db.set_config(ROLLUP_NEWEST_KEY, "2026-09-19T05:00")
    await _insert_window(db, NOW - timedelta(minutes=30), -25.0)
    await _rollup_forward(db, NOW)
    await db.set_config(ROLLUP_NEWEST_KEY, "2026-09-19T05:00")
    await _rollup_forward(db, NOW)
    peaks = await db.query_avg_minute_peaks(
        since=NOW - timedelta(hours=2), until=NOW, metric="pwr_max"
    )
    assert len(peaks) == 1
