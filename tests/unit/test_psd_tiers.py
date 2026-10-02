"""Stored tier periods (psd_tiers.py) and the Dashboard queries that use them."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.psd_tiers import COVERAGE_KEY, TierAccumulator, Tuning, choose_tier

TUNING = Tuning(915e6, 26e6, 30.0, 1024, -13e6, 26e6 / 1024)


def _add(acc, epoch, powers=None, pwr_max=-50.0):
    return acc.add(
        epoch=epoch,
        tuning=TUNING,
        pwr_avg=-70.0,
        pwr_max=pwr_max,
        pwr_median=-72.0,
        pwr_std=3.0,
        kurtosis=3.0,
        powers=powers,
    )


@pytest.mark.parametrize(
    ("span", "rows", "expected"),
    [
        (3600, 600, None),  # 6 s buckets: finer than any tier
        (3 * 3600, 600, (10, 20.0)),
        (6 * 3600, 600, (10, 40.0)),
        (12 * 3600, 600, (10, 80.0)),  # 1 min would leave 360 rows
        (24 * 3600, 600, (60, 180.0)),
        (2 * 86400, 600, (60, 300.0)),
        (7 * 86400, 600, (600, 1200.0)),
    ],
)
def test_choose_tier_keeps_most_rows(span, rows, expected):
    assert choose_tier(span, rows) == expected


def test_choose_tier_forced_bucket_needs_a_whole_multiple():
    assert choose_tier(86400, 600, forced_bucket=180.0) == (60, 180.0)
    assert choose_tier(86400, 600, forced_bucket=144.0) is None  # raw grid: no tier fits


def test_accumulator_emits_each_period_once_it_ends():
    acc = TierAccumulator(tiers=(10, 60))
    out = []
    for t in range(0, 125):
        out += _add(acc, 1000.0 * 60 + t, powers=np.full(4, float(t)))
    tens = [r for r in out if r.level_sec == 10]
    assert [r.bucket_epoch - 60000 for r in tens] == [float(x) for x in range(0, 120, 10)]
    assert all(r.n == 10 for r in tens)
    assert np.allclose(tens[0].psd_mean(), 4.5)  # mean of 0..9
    minutes = [r for r in out if r.level_sec == 60]
    assert [r.n for r in minutes] == [60, 60]
    # The open periods (5 windows of each tier) come out on drain.
    assert sorted(r.n for r in acc.drain()) == [5, 5]


def test_late_window_is_not_counted_in_a_later_period():
    acc = TierAccumulator(tiers=(10,))
    _add(acc, 100.0)
    _add(acc, 111.0)  # closes [100, 110) and opens [110, 120)
    _add(acc, 105.0)  # late: skipped
    (row,) = acc.drain()
    assert row.bucket_epoch == 110.0 and row.n == 1


def test_stats_floor_uses_windows_with_a_max_only():
    acc = TierAccumulator(tiers=(10,))
    acc.add(
        epoch=0.0,
        tuning=TUNING,
        pwr_avg=-90.0,
        pwr_max=None,
        pwr_median=0,
        pwr_std=0,
        kurtosis=0,
        powers=None,
    )
    _add(acc, 1.0, pwr_max=-40.0)
    (row,) = acc.drain()
    assert row.min_avg == -70.0 and row.max_max == -40.0 and row.n == 2


async def _insert(db, epoch, rng):
    nb = TUNING.num_bins
    await db.insert_avg_window(
        start_time=datetime.fromtimestamp(epoch, tz=timezone.utc),
        duration_sec=1.0,
        sdr_center_freq_hz=TUNING.sdr_center_freq_hz,
        sample_rate_hz=TUNING.sample_rate_hz,
        gain_db=TUNING.gain_db,
        num_bins=nb,
        freq_start_hz=TUNING.freq_start_hz,
        freq_step_hz=TUNING.freq_step_hz,
        pwr_avg=float(rng.normal(-70, 2)),
        pwr_max=None if rng.random() < 0.05 else float(rng.normal(-50, 2)),
        pwr_median=float(rng.normal(-72, 2)),
        pwr_std=float(rng.uniform(1, 3)),
        kurtosis=float(rng.uniform(2, 4)),
        powers=None if rng.random() < 0.05 else [float(x) for x in rng.normal(-95, 5, nb)],
    )


async def test_tiered_query_matches_raw_folding_across_a_restart(tmp_path):
    """A range read from tier rows (plus raw windows at its ends) equals the
    same range folded from every raw window, including a period split by a
    restart."""
    path = str(tmp_path / "tiers.db")
    rng = np.random.default_rng(3)
    t0 = 1_800_000_000.0 + 7  # not on a period boundary
    db = SensorDatabase(path, psd_tiers=True)
    await db.connect()
    for i in range(800):
        await _insert(db, t0 + i, rng)
    await db.close()  # stores the open periods, partial
    db = SensorDatabase(path, psd_tiers=True)
    await db.connect()
    for i in range(800, 1600):
        await _insert(db, t0 + i, rng)
    covered = float(await db.get_config(COVERAGE_KEY.format(10)))
    assert covered == 1_800_000_010.0

    since = datetime.fromtimestamp(covered + 3, tz=timezone.utc)
    until = datetime.fromtimestamp(t0 + 1600 + 5, tz=timezone.utc)  # live edge: unstored tail
    span = (until - since).total_seconds()
    tiered = await db.query_avg_waterfall(since=since, until=until, max_rows=60, max_bins=512)
    level, bucket = choose_tier(span, 60)
    assert (level, tiered["bucket_sec"]) == (10, bucket)
    where = "WHERE start_time >= ? AND start_time < ?"
    raw = await db._waterfall_aggregated(
        where, [since.isoformat(), until.isoformat()], since, until, bucket, 60, 512
    )
    np.testing.assert_allclose(
        np.array(tiered["psd_rows"]), np.array(raw["psd_rows"]), rtol=1e-5, equal_nan=True
    )
    assert tiered["total_windows"] == raw["total_windows"]
    assert tiered["min_db"] == pytest.approx(raw["min_db"])
    assert tiered["max_db"] == pytest.approx(raw["max_db"])
    for a, b in zip(tiered["buckets"], raw["buckets"], strict=True):
        assert a["count"] == b["count"]
        for k in ("pwr_avg", "pwr_max", "pwr_median", "pwr_std", "kurtosis"):
            assert a[k] == pytest.approx(b[k]), k

    stats = await db.query_avg_stats(since=since, until=until, max_points=60)
    raw_stats = await db._stats_aggregated(
        where, [since.isoformat(), until.isoformat()], since, until, bucket, 60
    )
    assert stats["bucket_sec"] == bucket
    assert stats["min_pwr"] == pytest.approx(raw_stats["min_pwr"])
    assert stats["max_pwr"] == pytest.approx(raw_stats["max_pwr"])
    for a, b in zip(stats["points"], raw_stats["points"], strict=True):
        assert a["count"] == b["count"]
        for k in ("pwr_avg", "pwr_max", "pwr_median", "kurtosis"):
            assert a[k] == pytest.approx(b[k]), k
    await db.close()


async def test_range_before_coverage_uses_raw_windows(tmp_path):
    db = SensorDatabase(str(tmp_path / "t.db"), psd_tiers=True)
    await db.connect()
    rng = np.random.default_rng(1)
    t0 = 1_800_000_005.0
    for i in range(1300):
        await _insert(db, t0 + i, rng)
    since = datetime.fromtimestamp(t0, tz=timezone.utc)  # before the first whole period
    assert choose_tier(1150, 60) == (10, 20.0)  # a tier would apply if covered
    out = await db.query_avg_waterfall(
        since=since, until=since + timedelta(seconds=1150), max_rows=60, max_bins=512
    )
    assert out["bucket_sec"] == pytest.approx(1150 / 60)  # the raw grid, not the tier's
    await db.close()
