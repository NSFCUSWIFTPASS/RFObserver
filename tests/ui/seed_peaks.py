"""Seed well-separated loud windows so the peak finder has something to find.

The peak finder's UI test needs several distinct peaks, but a mock receiver
run for only a few minutes rolls up into at most one 30-minute-window peak
(the separation rule in `select_peaks` correctly collapses anything closer
than `window_sec` into a single event). Rather than wait ~4 hours of wall
clock for enough real spread, this inserts six loud `avg_windows` rows spread
40 minutes apart (40 > the 30-minute default window, so all six survive
separation) directly into the live instance's database, then rewinds the
rollup's forward watermark so its next tick folds them into `avg_minutes`.

Usage (against a running `rfobserver run` instance):
    PYTHONPATH= .venv/bin/python tests/ui/seed_peaks.py [db_path]

`db_path` defaults to `$RFOBS_DB_PATH` or `/tmp/rfobserver/rfobserver.db`
(the default `RFOBS_DB_PATH` / `AppSettings.DB_PATH`). Opening a second
writer connection against the live instance's database is safe under WAL.

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

from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.rollup import ROLLUP_NEWEST_KEY

# Matches AppSettings' receiver defaults (WiFi 2.4 GHz, B205mini full BW),
# also used by the mock receiver and by tests/unit/test_avg_minutes.py, so
# the seeded windows land in the same (minute, center-frequency) group the
# real mock data is already writing to.
CENTER_FREQ_HZ = 2_437_000_000.0
SAMPLE_RATE_HZ = 56_000_000.0
GAIN_DB = 40.0

# 40 minutes apart: farther than the 30-minute default peak-search window, so
# the separation rule in select_peaks keeps every one of these distinct.
OFFSETS_MIN = (20, 60, 100, 140, 180, 220)
PWR_MAX_DB = (-20.0, -22.0, -24.0, -26.0, -28.0, -30.0)
PWR_MEDIAN_DB = -60.0  # fixed and well below every PWR_MAX_DB, so pwr_snr ranks the same way


async def seed(db_path: str) -> None:
    db = SensorDatabase(db_path)
    await db.connect()
    try:
        now = datetime.now(timezone.utc)
        seeded = []
        for offset_min, pwr_max in zip(OFFSETS_MIN, PWR_MAX_DB, strict=True):
            start_time = now - timedelta(minutes=offset_min)
            await db.insert_avg_window(
                start_time=start_time,
                duration_sec=0.5,
                sdr_center_freq_hz=CENTER_FREQ_HZ,
                sample_rate_hz=SAMPLE_RATE_HZ,
                gain_db=GAIN_DB,
                num_bins=4,
                freq_start_hz=CENTER_FREQ_HZ - SAMPLE_RATE_HZ / 2,
                freq_step_hz=SAMPLE_RATE_HZ / 4,
                powers=np.array([pwr_max, -60.0, -60.0, -60.0], dtype="<f4"),
                pwr_avg=pwr_max - 15.0,
                pwr_max=pwr_max,
                pwr_median=PWR_MEDIAN_DB,
                pwr_std=1.0,
                kurtosis=2.0,
                interference=0,
            )
            seeded.append((start_time.isoformat(), pwr_max))

        # Rewind the forward watermark so the rollup loop's next tick re-folds
        # the span we just seeded (5 hours reaches all six, 40 minutes apart).
        # rollup_oldest is left untouched: it only controls the backfill pass.
        rewind_minute = (now - timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M")
        await db.set_config(ROLLUP_NEWEST_KEY, rewind_minute)

        print(f"Seeded {len(seeded)} loud windows into {db_path}:")
        for start_iso, pwr_max in seeded:
            print(f"  {start_iso}  pwr_max={pwr_max:.1f} dB")
        print(f"Rewound {ROLLUP_NEWEST_KEY} to {rewind_minute} to trigger a re-fold.")
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
