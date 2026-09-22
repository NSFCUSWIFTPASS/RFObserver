"""Seed a dev/test instance so the peak finder and drag-zoom Puppeteer
sections have something real to find.

Two things a freshly started mock instance does not have, both needed by
``tests/ui/puppeteer_avg_history.js``:

1. Several distinct peaks. A mock receiver run for only a few minutes rolls
   up into at most one 30-minute-window peak (the separation rule in
   ``select_peaks`` correctly collapses anything closer than ``window_sec``
   into a single event). Rather than wait ~4 hours of wall clock for enough
   real spread, this inserts six loud ``avg_windows`` rows spread 40 minutes
   apart (40 > the 30-minute default window, so all six survive separation)
   directly into the live instance's database, then rewinds the rollup's
   forward watermark so its next tick folds them into ``avg_minutes``.

2. ~15-20 minutes of continuous recent history. The drag-zoom and raw-mode
   sections of the Puppeteer suite zoom into and inspect a sub-range of the
   default "Last 15 minutes" view; against a freshly started instance that
   range is empty (or nearly so) and those assertions fail even though
   nothing is actually broken. This inserts continuous ``avg_windows`` rows
   at the pipeline's own cadence (2 Hz, i.e. one every ``DURATION_SEC``)
   across the last 20 minutes.

Every inserted row -- both the six peaks and the continuous filler -- uses
``num_bins=2048`` (``AppSettings.NUM_FFT_BINS``'s default, what every real
row in this tuning group already has) and a little-endian float32 PSD blob
shaped like the pipeline's: a flat noise floor with jitter, the six peak rows
additionally carrying one loud bin. That keeps the seeded rows indistinguishable
in kind from real ones, so this is also safe to run against a database a human
is using for manual testing.

Usage (against a running `rfobserver run` instance):
    PYTHONPATH= .venv/bin/python tests/ui/seed_peaks.py [db_path]

`db_path` defaults to `$RFOBS_DB_PATH` or `/tmp/rfobserver/rfobserver.db`
(the default `RFOBS_DB_PATH` / `AppSettings.DB_PATH`). Opening a second
writer connection against the live instance's database is safe under WAL.

Continuous-history seeding is idempotent-ish: if the last 20 minutes already
have a substantial number of matching rows (e.g. a second run, or a
long-running instance), it is skipped with a warning instead of piling on
more rows.

After running this, poll `/api/averaged/peaks` until it returns at least as
many peaks as were seeded rather than sleeping a fixed amount -- the rollup
loop's own tick interval (`RFOBS_PEAKS_ROLLUP_INTERVAL_SEC`) decides how long
that takes.
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
from scipy.stats import kurtosis as _kurtosis

from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.rollup import ROLLUP_NEWEST_KEY

# Matches AppSettings' receiver defaults (WiFi 2.4 GHz, B205mini full BW),
# also used by the mock receiver and by tests/unit/test_avg_minutes.py, so
# the seeded windows land in the same (minute, center-frequency) group the
# real mock data is already writing to.
CENTER_FREQ_HZ = 2_437_000_000.0
SAMPLE_RATE_HZ = 56_000_000.0
GAIN_DB = 40.0
DURATION_SEC = 0.5  # matches AppSettings.DURATION_SEC -- the pipeline's window cadence
NUM_BINS = 2048  # matches AppSettings.NUM_FFT_BINS -- every real row has this

NOISE_FLOOR_DB = -60.0
NOISE_JITTER_DB = 1.0

# 40 minutes apart: farther than the 30-minute default peak-search window, so
# the separation rule in select_peaks keeps every one of these distinct.
OFFSETS_MIN = (20, 60, 100, 140, 180, 220)
PWR_MAX_DB = (-20.0, -22.0, -24.0, -26.0, -28.0, -30.0)

# Continuous filler: 20 minutes at the pipeline's own 2 Hz cadence.
HISTORY_MINUTES = 20
HISTORY_ROWS = int(HISTORY_MINUTES * 60 / DURATION_SEC)  # ~2400
# If the last HISTORY_MINUTES already hold at least this many matching rows,
# treat continuous history as already seeded and skip rather than duplicate it.
HISTORY_PRESENT_THRESHOLD = HISTORY_ROWS // 2


def _make_psd(rng: np.random.Generator, loud_bin_db: float | None) -> np.ndarray:
    """A flat noise floor with jitter, optionally with one loud bin -- the
    same shape a real avg_windows row has, just synthetic."""
    powers = (NOISE_FLOOR_DB + rng.normal(0.0, NOISE_JITTER_DB, NUM_BINS)).astype("<f4")
    if loud_bin_db is not None:
        powers[NUM_BINS // 2] = loud_bin_db
    return powers


def _stats(powers: np.ndarray) -> tuple[float, float, float, float]:
    """(pwr_avg, pwr_max, pwr_median, pwr_std) computed from the actual PSD
    array, so the row's summary columns are consistent with its blob rather
    than hand-picked."""
    return (
        float(np.mean(powers)),
        float(np.max(powers)),
        float(np.median(powers)),
        float(np.std(powers)),
    )


async def _seed_peaks(db: SensorDatabase, now: datetime) -> None:
    rng = np.random.default_rng(1)
    freq_start = CENTER_FREQ_HZ - SAMPLE_RATE_HZ / 2
    freq_step = SAMPLE_RATE_HZ / NUM_BINS
    seeded = []
    for offset_min, pwr_max in zip(OFFSETS_MIN, PWR_MAX_DB, strict=True):
        start_time = now - timedelta(minutes=offset_min)
        powers = _make_psd(rng, loud_bin_db=pwr_max)
        pwr_avg, actual_max, pwr_median, pwr_std = _stats(powers)
        kurt = float(_kurtosis(powers, fisher=True, bias=False))
        await db.insert_avg_window(
            start_time=start_time,
            duration_sec=DURATION_SEC,
            sdr_center_freq_hz=CENTER_FREQ_HZ,
            sample_rate_hz=SAMPLE_RATE_HZ,
            gain_db=GAIN_DB,
            num_bins=NUM_BINS,
            freq_start_hz=freq_start,
            freq_step_hz=freq_step,
            powers=powers.tolist(),
            pwr_avg=pwr_avg,
            pwr_max=actual_max,
            pwr_median=pwr_median,
            pwr_std=pwr_std,
            kurtosis=kurt,
            interference=0,
        )
        seeded.append((start_time.isoformat(), actual_max))

    # Rewind the forward watermark so the rollup loop's next tick re-folds
    # the span we just seeded (5 hours reaches all six, 40 minutes apart, and
    # the continuous 20-minute history below).
    rewind_minute = (now - timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M")
    await db.set_config(ROLLUP_NEWEST_KEY, rewind_minute)

    print(f"Seeded {len(seeded)} loud windows:")
    for start_iso, pwr_max in seeded:
        print(f"  {start_iso}  pwr_max={pwr_max:.1f} dB")
    print(f"Rewound {ROLLUP_NEWEST_KEY} to {rewind_minute} to trigger a re-fold.")


async def _history_already_present(db: SensorDatabase, now: datetime) -> bool:
    assert db._db is not None
    since = (now - timedelta(minutes=HISTORY_MINUTES)).isoformat()
    async with db._db.execute(
        """SELECT COUNT(*) FROM avg_windows
           WHERE start_time >= ? AND sdr_center_freq_hz = ? AND duration_sec = ?""",
        (since, CENTER_FREQ_HZ, DURATION_SEC),
    ) as cur:
        row = await cur.fetchone()
    return bool(row and row[0] >= HISTORY_PRESENT_THRESHOLD)


async def _seed_history(db: SensorDatabase, now: datetime) -> None:
    if await _history_already_present(db, now):
        print(
            f"Continuous history already present (>= {HISTORY_PRESENT_THRESHOLD} rows "
            f"in the last {HISTORY_MINUTES} minutes) -- skipping to avoid duplicating it."
        )
        return

    rng = np.random.default_rng(2)
    freq_start = CENTER_FREQ_HZ - SAMPLE_RATE_HZ / 2
    freq_step = SAMPLE_RATE_HZ / NUM_BINS
    start = now - timedelta(minutes=HISTORY_MINUTES)
    for i in range(HISTORY_ROWS):
        t = start + timedelta(seconds=i * DURATION_SEC)
        powers = _make_psd(rng, loud_bin_db=None)
        pwr_avg, pwr_max, pwr_median, pwr_std = _stats(powers)
        kurt = float(_kurtosis(powers, fisher=True, bias=False))
        await db.insert_avg_window(
            start_time=t,
            duration_sec=DURATION_SEC,
            sdr_center_freq_hz=CENTER_FREQ_HZ,
            sample_rate_hz=SAMPLE_RATE_HZ,
            gain_db=GAIN_DB,
            num_bins=NUM_BINS,
            freq_start_hz=freq_start,
            freq_step_hz=freq_step,
            powers=powers.tolist(),
            pwr_avg=pwr_avg,
            pwr_max=pwr_max,
            pwr_median=pwr_median,
            pwr_std=pwr_std,
            kurtosis=kurt,
            interference=0,
        )
    print(f"Seeded {HISTORY_ROWS} continuous windows across the last {HISTORY_MINUTES} minutes.")


async def seed(db_path: str) -> None:
    db = SensorDatabase(db_path)
    await db.connect()
    try:
        now = datetime.now(timezone.utc)
        await _seed_peaks(db, now)
        await _seed_history(db, now)
    finally:
        await db.close()


def main() -> None:
    db_path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else os.environ.get("RFOBS_DB_PATH", "/tmp/rfobserver/rfobserver.db")
    )
    asyncio.run(seed(db_path))


if __name__ == "__main__":
    main()
