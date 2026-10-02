"""The vectorized waterfall aggregation matches a per-window reference.

The aggregation folds a whole chunk of windows at once in numpy (a per-window
Python loop held the GIL for seconds on long ranges). This checks it against
the plain loop it replaced, on data with the awkward cases: two bin widths
(one downsampled, one NaN-padded), windows whose PSD blob was pruned, windows
with no pwr_max, and empty buckets.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from rfobserver.storage.database import SensorDatabase

MAX_BINS = 8


def _reference(windows, since, until, bucket_sec):
    anchor = math.floor(since.timestamp() / bucket_sec) * bucket_sec
    n = max(1, math.ceil((until.timestamp() - anchor) / bucket_sec))
    psd_sum = np.zeros((n, MAX_BINS))
    psd_cnt = np.zeros((n, MAX_BINS), dtype=int)
    cnt = [0] * n
    sums = {k: [0.0] * n for k in ("pwr_avg", "pwr_median", "pwr_std", "kurtosis")}
    mx = [-math.inf] * n
    gmin, gmax, total = math.inf, -math.inf, 0
    for w in windows:
        i = min(max(int((w["start_time"].timestamp() - anchor) / bucket_sec), 0), n - 1)
        cnt[i] += 1
        for k in sums:
            sums[k][i] += w[k]
        if w["pwr_max"] is not None:
            mx[i] = max(mx[i], w["pwr_max"])
        if w["powers"] is None:
            continue
        total += 1
        p = np.asarray(w["powers"], dtype=np.float32)
        if p.size > MAX_BINS:
            f = p.size // MAX_BINS
            p = p[: f * MAX_BINS].reshape(MAX_BINS, f).mean(axis=1)
        else:
            p = np.concatenate([p, np.full(MAX_BINS - p.size, np.nan, dtype=np.float32)])
        gmin, gmax = min(gmin, float(np.nanmin(p))), max(gmax, float(np.nanmax(p)))
        ok = ~np.isnan(p)
        psd_sum[i] += np.where(ok, p, 0.0)
        psd_cnt[i] += ok
    rows = []
    for i in range(n):
        with np.errstate(invalid="ignore"):
            rows.append(psd_sum[i] / psd_cnt[i] if psd_cnt[i].any() else np.full(MAX_BINS, np.nan))
    stats = [
        (
            cnt[i],
            sums["pwr_avg"][i] / cnt[i] if cnt[i] else 0.0,
            mx[i] if cnt[i] else 0.0,
            sums["kurtosis"][i] / cnt[i] if cnt[i] else 0.0,
        )
        for i in range(n)
    ]
    return np.array(rows), stats, gmin, gmax, total


@pytest.fixture
async def db(tmp_path):
    database = SensorDatabase(str(tmp_path / "agg.db"))
    await database.connect()
    yield database
    await database.close()


async def test_vectorized_aggregation_matches_per_window_reference(db):
    rng = np.random.default_rng(7)
    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    windows = []
    t = 0.0
    for k in range(2300):  # more than one 2000-row scan chunk
        t += float(rng.uniform(0.2, 1.8))
        if 300 < t < 340:
            continue  # a gap: empty buckets
        width = 32 if k % 3 else 5  # downsampled (32 -> 8) and padded (5 -> 8)
        w = {
            "start_time": base + timedelta(seconds=t),
            "pwr_avg": float(rng.normal(-70, 3)),
            "pwr_max": None if k % 17 == 0 else float(rng.normal(-50, 3)),
            "pwr_median": float(rng.normal(-72, 3)),
            "pwr_std": float(rng.uniform(1, 4)),
            "kurtosis": float(rng.uniform(2, 5)),
            "powers": None if k % 11 == 0 else [float(x) for x in rng.normal(-90, 6, width)],
        }
        windows.append(w)
        await db.insert_avg_window(
            start_time=w["start_time"],
            duration_sec=1.0,
            sdr_center_freq_hz=100e6,
            sample_rate_hz=1e6,
            gain_db=30.0,
            num_bins=width,
            freq_start_hz=0.0,
            freq_step_hz=1.0,
            pwr_avg=w["pwr_avg"],
            pwr_max=w["pwr_max"],
            pwr_median=w["pwr_median"],
            pwr_std=w["pwr_std"],
            kurtosis=w["kurtosis"],
            powers=w["powers"],
        )
    since, until = base, base + timedelta(seconds=t + 1)
    bucket = (until - since).total_seconds() / 100
    got = await db.query_avg_waterfall(
        since=since, until=until, max_rows=100, max_bins=MAX_BINS, bucket_sec=bucket
    )
    rows, stats, gmin, gmax, total = _reference(windows, since, until, bucket)
    # The stored blobs are float32, as the reference's rows are.
    np.testing.assert_allclose(np.array(got["psd_rows"]), rows, rtol=1e-6, equal_nan=True)
    assert got["total_windows"] == total
    assert got["min_db"] == pytest.approx(gmin) and got["max_db"] == pytest.approx(gmax)
    got_stats = [(b["count"], b["pwr_avg"], b["pwr_max"], b["kurtosis"]) for b in got["buckets"]]
    assert len(got_stats) == len(stats)
    for g, r in zip(got_stats, stats, strict=True):
        assert g[0] == r[0]
        assert g[1:] == pytest.approx(r[1:])
