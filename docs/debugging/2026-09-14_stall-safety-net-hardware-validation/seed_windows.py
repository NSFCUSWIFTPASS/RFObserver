"""Seed synthetic avg_windows spanning the past N hours, cloning the tuning
metadata of a real row the live pipeline wrote, so the Dashboard's wide query
has field-scale work (the field box had ~74k windows over 24 h).

Run with the pipeline STOPPED. Usage: seed_windows.py DB_PATH [COUNT] [HOURS]
"""

import sqlite3
import sys
from datetime import datetime, timedelta

import numpy as np

db_path = sys.argv[1]
count = int(sys.argv[2]) if len(sys.argv) > 2 else 74000
hours = float(sys.argv[3]) if len(sys.argv) > 3 else 24.0

con = sqlite3.connect(db_path)
con.row_factory = sqlite3.Row
ref = con.execute("SELECT * FROM avg_windows ORDER BY id DESC LIMIT 1").fetchone()
if ref is None:
    sys.exit("no real avg_windows row to clone; run the pipeline briefly first")
first_real = con.execute("SELECT MIN(start_time) FROM avg_windows").fetchone()[0]
print("reference row start_time:", ref["start_time"], "num_bins:", ref["num_bins"])

# Keep the real rows' tz style (aware or naive) so string comparison in the
# range query orders seeded and real rows consistently.
end = datetime.fromisoformat(first_real)
start = end - timedelta(hours=hours)
step = (end - start) / count
num_bins = int(ref["num_bins"])
rng = np.random.default_rng(0)
base = np.frombuffer(ref["psd_powers"], dtype="<f4") if ref["psd_powers"] else np.full(num_bins, -90.0, "<f4")

rows = []
for i in range(count):
    t = start + step * i
    psd = (base + rng.normal(0, 1.5, num_bins)).astype("<f4")
    rows.append(
        (
            t.isoformat(),
            ref["duration_sec"],
            ref["sdr_center_freq_hz"],
            ref["sample_rate_hz"],
            ref["gain_db"],
            num_bins,
            ref["freq_start_hz"],
            ref["freq_step_hz"],
            float(psd.mean()),
            float(psd.max()),
            float(np.median(psd)),
            float(psd.std()),
            0.0,
            0,
            psd.tobytes(),
            None,
        )
    )
    if len(rows) >= 5000:
        con.executemany(
            """INSERT INTO avg_windows
               (start_time, duration_sec, sdr_center_freq_hz, sample_rate_hz,
                gain_db, num_bins, freq_start_hz, freq_step_hz, pwr_avg, pwr_max,
                pwr_median, pwr_std, kurtosis, interference, psd_powers, violations)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        con.commit()
        rows.clear()
if rows:
    con.executemany(
        """INSERT INTO avg_windows
           (start_time, duration_sec, sdr_center_freq_hz, sample_rate_hz,
            gain_db, num_bins, freq_start_hz, freq_step_hz, pwr_avg, pwr_max,
            pwr_median, pwr_std, kurtosis, interference, psd_powers, violations)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    con.commit()
n = con.execute("SELECT COUNT(*) FROM avg_windows").fetchone()[0]
print(f"seeded {count} windows over {hours}h ending {end.isoformat()}; table now {n} rows")
