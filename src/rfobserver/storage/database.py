"""Local SQLite database for detection history, config, and sensor state.

Uses aiosqlite for async access. Stores recent detections, burst fingerprints,
and sensor configuration. Rolling window cleanup removes old data.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import aiosqlite
import numpy as np

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS detections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    burst_id TEXT UNIQUE NOT NULL,
    start_time TEXT NOT NULL,
    stop_time TEXT NOT NULL,
    center_freq_hz REAL NOT NULL,
    bandwidth_hz REAL NOT NULL,
    peak_power_db REAL NOT NULL,
    duration_ms REAL NOT NULL,
    detection_timestamp TEXT NOT NULL,
    peak_freq_hz REAL,
    -- SDR capture context (how the radio was tuned when the burst was found).
    -- Distinct from center_freq_hz/bandwidth_hz above, which describe the burst
    -- signal itself. Nullable so pre-migration rows and uncalibrated paths work.
    sdr_center_freq_hz REAL,
    sample_rate_hz REAL,
    lo_offset_hz REAL,
    analog_bw_hz REAL,
    gain_db REAL,
    antenna TEXT,
    device_serial TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    data TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS tone_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    tone_freq_hz REAL NOT NULL,
    sdr_center_freq_hz REAL NOT NULL,
    in_band INTEGER NOT NULL,
    tone_power_db REAL,
    noise_floor_db REAL,
    snr_db REAL,
    detected INTEGER NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS avg_windows (
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
    -- Nullable so retention can evict the heavy PSD blob while keeping the
    -- cheap stats row permanently (see prune_avg_psd_blobs).
    psd_powers BLOB,
    violations BLOB,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_detections_time ON detections(start_time);
CREATE INDEX IF NOT EXISTS idx_detections_freq ON detections(center_freq_hz);
CREATE INDEX IF NOT EXISTS idx_stats_time ON stats(timestamp);
CREATE INDEX IF NOT EXISTS idx_tone_checks_time ON tone_checks(timestamp);
CREATE INDEX IF NOT EXISTS idx_avg_windows_time ON avg_windows(start_time);
CREATE INDEX IF NOT EXISTS idx_avg_windows_center_time
    ON avg_windows(sdr_center_freq_hz, start_time);
"""

# Columns added after the original detections schema (SDR capture-context
# fields, plus peak_freq_hz). Existing databases predate them, so connect()
# adds any that are missing via ALTER TABLE (SQLite has no
# "ADD COLUMN IF NOT EXISTS").
_DETECTION_SDR_COLUMNS: dict[str, str] = {
    "sdr_center_freq_hz": "REAL",
    "sample_rate_hz": "REAL",
    "lo_offset_hz": "REAL",
    "analog_bw_hz": "REAL",
    "gain_db": "REAL",
    "antenna": "TEXT",
    "device_serial": "TEXT",
    "peak_freq_hz": "REAL",
}


def _nice_bin_width(span: float) -> float:
    """Pick a human-friendly bin width (1/2/5 x 10^k ms) targeting ~20 bins.

    Floored at 0.5 ms so a tiny or zero range still yields a usable width.
    """
    raw = span / 20.0
    if raw <= 0:
        return 0.5
    exp = math.floor(math.log10(raw))
    base = 10.0**exp
    frac = raw / base
    if frac <= 1:
        nice = 1.0
    elif frac <= 2:
        nice = 2.0
    elif frac <= 5:
        nice = 5.0
    else:
        nice = 10.0
    return max(0.5, nice * base)


class SensorDatabase:
    """Async SQLite database for local sensor state."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self._db_path)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.executescript(SCHEMA)
        await self._migrate_detection_columns()
        await self._migrate_avg_windows_psd_nullable()
        # Created after migration: on a pre-existing DB the indexed column is
        # added by the migration above, so this can't run inside SCHEMA.
        await self._db.execute(
            "CREATE INDEX IF NOT EXISTS idx_detections_sdr_center ON detections(sdr_center_freq_hz)"
        )
        await self._db.commit()
        logger.info("Database connected: %s", self._db_path)

    async def _migrate_detection_columns(self) -> None:
        """Add SDR capture-context columns to an existing detections table.

        Fresh databases get these from SCHEMA; older ones are upgraded in place
        so their pre-existing rows keep working (the new columns read as NULL).
        """
        assert self._db is not None
        async with self._db.execute("PRAGMA table_info(detections)") as cursor:
            existing = {row[1] for row in await cursor.fetchall()}
        for column, col_type in _DETECTION_SDR_COLUMNS.items():
            if column not in existing:
                await self._db.execute(f"ALTER TABLE detections ADD COLUMN {column} {col_type}")
                logger.info("Migrated detections: added column %s", column)

    async def _migrate_avg_windows_psd_nullable(self) -> None:
        """Make avg_windows.psd_powers nullable on DBs created with NOT NULL.

        Retention evicts the heavy PSD blob by setting it to NULL (keeping the
        cheap stats row forever), which a NOT NULL column forbids. SQLite cannot
        drop a NOT NULL constraint in place, so the table is rebuilt -- data is
        copied, the old table dropped, the new one renamed, indexes recreated.
        Fresh databases already get the nullable column from SCHEMA.
        """
        assert self._db is not None
        async with self._db.execute("PRAGMA table_info(avg_windows)") as cursor:
            columns = {row[1]: row for row in await cursor.fetchall()}
        psd = columns.get("psd_powers")
        if psd is None or psd[3] == 0:  # missing (fresh DB) or already nullable
            return
        await self._db.execute("BEGIN")
        try:
            await self._db.execute(
                """CREATE TABLE avg_windows_new (
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
                    psd_powers BLOB,
                    violations BLOB,
                    created_at TEXT DEFAULT (datetime('now'))
                )"""
            )
            await self._db.execute(
                """INSERT INTO avg_windows_new
                   (id, start_time, duration_sec, sdr_center_freq_hz, sample_rate_hz,
                    gain_db, num_bins, freq_start_hz, freq_step_hz, pwr_avg, pwr_max,
                    pwr_median, pwr_std, kurtosis, interference, psd_powers, violations,
                    created_at)
                   SELECT id, start_time, duration_sec, sdr_center_freq_hz, sample_rate_hz,
                          gain_db, num_bins, freq_start_hz, freq_step_hz, pwr_avg, pwr_max,
                          pwr_median, pwr_std, kurtosis, interference, psd_powers, violations,
                          created_at
                   FROM avg_windows"""
            )
            await self._db.execute("DROP TABLE avg_windows")
            await self._db.execute("ALTER TABLE avg_windows_new RENAME TO avg_windows")
            await self._db.execute(
                "CREATE INDEX IF NOT EXISTS idx_avg_windows_time ON avg_windows(start_time)"
            )
            await self._db.execute(
                "CREATE INDEX IF NOT EXISTS idx_avg_windows_center_time "
                "ON avg_windows(sdr_center_freq_hz, start_time)"
            )
            await self._db.commit()
            logger.info("Migrated avg_windows: psd_powers is now nullable")
        except Exception:
            await self._db.rollback()
            raise

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    async def insert_detection(
        self,
        burst_id: str,
        start_time: datetime,
        stop_time: datetime,
        center_freq_hz: float,
        bandwidth_hz: float,
        peak_power_db: float,
        duration_ms: float,
        detection_timestamp: datetime,
        sdr_center_freq_hz: float | None = None,
        sample_rate_hz: float | None = None,
        lo_offset_hz: float | None = None,
        analog_bw_hz: float | None = None,
        gain_db: float | None = None,
        antenna: str | None = None,
        device_serial: str | None = None,
        peak_freq_hz: float = 0.0,
    ) -> None:
        assert self._db is not None
        await self._db.execute(
            """INSERT OR IGNORE INTO detections
               (burst_id, start_time, stop_time, center_freq_hz, bandwidth_hz,
                peak_power_db, duration_ms, detection_timestamp,
                sdr_center_freq_hz, sample_rate_hz, lo_offset_hz, analog_bw_hz,
                gain_db, antenna, device_serial, peak_freq_hz)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                burst_id,
                start_time.isoformat(),
                stop_time.isoformat(),
                center_freq_hz,
                bandwidth_hz,
                peak_power_db,
                duration_ms,
                detection_timestamp.isoformat(),
                sdr_center_freq_hz,
                sample_rate_hz,
                lo_offset_hz,
                analog_bw_hz,
                gain_db,
                antenna,
                device_serial,
                peak_freq_hz,
            ),
        )
        await self._db.commit()

    async def insert_tone_check(
        self,
        *,
        timestamp: datetime,
        tone_freq_hz: float,
        sdr_center_freq_hz: float,
        in_band: bool,
        tone_power_db: float | None,
        noise_floor_db: float | None,
        snr_db: float | None,
        detected: bool,
    ) -> None:
        """Record one tone-check result (one averaging interval)."""
        assert self._db is not None
        await self._db.execute(
            """INSERT INTO tone_checks
               (timestamp, tone_freq_hz, sdr_center_freq_hz, in_band,
                tone_power_db, noise_floor_db, snr_db, detected)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                timestamp.isoformat(),
                tone_freq_hz,
                sdr_center_freq_hz,
                int(in_band),
                tone_power_db,
                noise_floor_db,
                snr_db,
                int(detected),
            ),
        )
        await self._db.commit()

    async def query_tone_checks(self, limit: int = 200) -> list[dict[str, Any]]:
        """Recent tone-check results, newest first."""
        assert self._db is not None
        self._db.row_factory = aiosqlite.Row
        async with self._db.execute(
            """SELECT timestamp, tone_freq_hz, sdr_center_freq_hz, in_band,
                      tone_power_db, noise_floor_db, snr_db, detected
               FROM tone_checks ORDER BY id DESC LIMIT ?""",
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def insert_avg_window(
        self,
        *,
        start_time: datetime,
        duration_sec: float,
        sdr_center_freq_hz: float,
        sample_rate_hz: float,
        gain_db: float | None,
        num_bins: int,
        freq_start_hz: float,
        freq_step_hz: float,
        pwr_avg: float,
        pwr_max: float,
        pwr_median: float,
        pwr_std: float,
        kurtosis: float,
        powers: list[float],
        interference: bool | None = None,
        violations: bytes | None = None,
    ) -> None:
        """Persist one DURATION_SEC-averaged window. ``powers`` is stored as a
        little-endian float32 BLOB (raw dBFS)."""
        assert self._db is not None
        psd_blob = np.asarray(powers, dtype="<f4").tobytes()
        await self._db.execute(
            """INSERT INTO avg_windows
               (start_time, duration_sec, sdr_center_freq_hz, sample_rate_hz,
                gain_db, num_bins, freq_start_hz, freq_step_hz, pwr_avg, pwr_max,
                pwr_median, pwr_std, kurtosis, interference, psd_powers, violations)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                start_time.isoformat(),
                duration_sec,
                sdr_center_freq_hz,
                sample_rate_hz,
                gain_db,
                num_bins,
                freq_start_hz,
                freq_step_hz,
                pwr_avg,
                pwr_max,
                pwr_median,
                pwr_std,
                kurtosis,
                None if interference is None else int(interference),
                psd_blob,
                violations,
            ),
        )
        await self._db.commit()

    # Columns returned by the light range query -- everything except the heavy blobs.
    _AVG_LIGHT_COLUMNS = (
        "id, start_time, duration_sec, sdr_center_freq_hz, sample_rate_hz, gain_db, "
        "num_bins, freq_start_hz, freq_step_hz, pwr_avg, pwr_max, pwr_median, "
        "pwr_std, kurtosis, interference, created_at"
    )

    async def query_avg_windows(
        self,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        sdr_center_freq: float | None = None,
        sample_rate: float | None = None,
        gain: float | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Averaged windows in a time/tuning range, newest first. Excludes the
        psd_powers/violations blobs to keep range listings light."""
        assert self._db is not None
        conditions: list[str] = []
        params: list[Any] = []
        if since is not None:
            conditions.append("start_time >= ?")
            params.append(since.isoformat())
        if until is not None:
            conditions.append("start_time < ?")
            params.append(until.isoformat())
        sdr_conditions, sdr_params = self._sdr_conditions(sdr_center_freq, sample_rate, gain)
        conditions.extend(sdr_conditions)
        params.extend(sdr_params)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = (
            f"SELECT {self._AVG_LIGHT_COLUMNS} FROM avg_windows "
            f"{where} ORDER BY start_time DESC LIMIT ?"
        )
        params.append(limit)
        self._db.row_factory = aiosqlite.Row
        async with self._db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_avg_window(self, window_id: int) -> dict[str, Any] | None:
        """One averaged window with its PSD decoded to a list and the frequency
        axis reconstructed from freq_start_hz + i * freq_step_hz.

        ``powers`` is None when the PSD blob has been pruned by retention (the
        stats row survives); the frequency axis is metadata-derived and always
        present.
        """
        assert self._db is not None
        self._db.row_factory = aiosqlite.Row
        async with self._db.execute(
            "SELECT * FROM avg_windows WHERE id = ?", (window_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        record = dict(row)
        blob = record.pop("psd_powers")
        num_bins = int(record["num_bins"])
        if blob is None:
            record["powers"] = None
        else:
            # Explicit float() comprehension keeps the return concrete (numpy is
            # untyped under the CI mypy config; a bare .tolist() would leak Any).
            record["powers"] = [float(x) for x in np.frombuffer(blob, dtype="<f4")]
        start = float(record["freq_start_hz"])
        step = float(record["freq_step_hz"])
        record["frequencies"] = [start + i * step for i in range(num_bins)]
        return record

    async def detections_for_window(self, window: dict[str, Any]) -> list[dict[str, Any]]:
        """Detections that started inside this averaged window's time span and
        match its SDR tuning. Reuses query_detections' since/until (which range on
        the burst start_time) so a burst is associated with the window it began in."""
        start = datetime.fromisoformat(window["start_time"])
        until = start + timedelta(seconds=float(window["duration_sec"]))
        return await self.query_detections(
            since=start,
            until=until,
            sdr_center_freq=window["sdr_center_freq_hz"],
            sample_rate=window["sample_rate_hz"],
            gain=window["gain_db"],
            limit=1000,
        )

    async def query_avg_waterfall(
        self,
        *,
        since: datetime,
        until: datetime,
        sdr_center_freq: float | None = None,
        sample_rate: float | None = None,
        gain: float | None = None,
        max_rows: int = 600,
        max_bins: int = 512,
    ) -> dict[str, Any]:
        """Averaged windows over a range, adaptive to display density.

        Only aggregates when the data outnumbers what can be displayed: with
        ``max_rows`` or fewer windows in the range (``mode == 0``, raw) each
        window is returned as its own row (its real ``start_epoch`` +
        ``duration_sec`` and its own PSD). With more (``mode == 1``,
        aggregated) the windows are folded into ``max_rows`` time buckets of
        ``bucket_sec = span / max_rows`` each (per-bin mean PSD, scalar stats
        mean-of-means except ``pwr_max`` = max-of-maxes so transient bursts
        stay visible).

        Windows whose blob was pruned by retention contribute stats but no PSD
        (their row is all-NaN). Native bins are downsampled to ``max_bins`` by
        group-mean when larger. Returns plain lists/floats for the web layer's
        binary packer.
        """
        assert self._db is not None
        span = (until - since).total_seconds()
        empty = {
            "bucket_sec": 0.0,
            "num_bins": 0,
            "min_db": 0.0,
            "max_db": 0.0,
            "total_windows": 0,
            "freq_start_hz": 0.0,
            "freq_step_hz": 0.0,
            "mode": 0,
            "buckets": [],
            "psd_rows": [],
        }
        if span <= 0:
            return empty
        bucket_sec = span / max_rows
        conditions, sdr_params = self._sdr_conditions(sdr_center_freq, sample_rate, gain)
        where = "WHERE start_time >= ? AND start_time < ?"
        if conditions:
            where += " AND " + " AND ".join(conditions)
        params: list[Any] = [since.isoformat(), until.isoformat()]
        params.extend(sdr_params)
        # Count first to pick the mode; only aggregate when the windows
        # outnumber the display rows.
        async with self._db.execute(f"SELECT COUNT(*) FROM avg_windows {where}", params) as cur:
            row = await cur.fetchone()
            window_count = int(row[0]) if row else 0
        if window_count == 0:
            return empty
        if window_count <= max_rows:
            return await self._waterfall_raw(where, params, bucket_sec, max_bins)
        return await self._waterfall_aggregated(
            where, params, since, bucket_sec, max_rows, max_bins
        )

    @staticmethod
    def _ds_psd(blob: bytes, num_bins: int, max_bins: int) -> tuple[list[float], float, float]:
        """Decode one PSD blob to max_bins floats (downsample by group-mean when
        larger, NaN-pad when shorter). Returns (powers, min, max)."""
        powers = np.frombuffer(blob, dtype="<f4")
        if powers.size != num_bins:
            num_bins = int(powers.size)
        if num_bins > max_bins:
            factor = num_bins // max_bins
            trim = factor * max_bins
            powers = powers[:trim].reshape(max_bins, factor).mean(axis=1)
        else:
            pad = max_bins - num_bins
            if pad > 0:
                powers = np.concatenate([powers, np.full(pad, np.nan, dtype=np.float32)])
        gmin = float(np.nanmin(powers))
        gmax = float(np.nanmax(powers))
        return [float(x) for x in powers], gmin, gmax

    async def _waterfall_raw(
        self,
        where: str,
        params: list[Any],
        bucket_sec: float,
        max_bins: int,
    ) -> dict[str, Any]:
        """Raw mode: one row per window (no averaging)."""
        assert self._db is not None
        query = (
            "SELECT start_time, duration_sec, num_bins, freq_start_hz, freq_step_hz, "
            "psd_powers, pwr_avg, pwr_max, pwr_median, pwr_std, kurtosis "
            f"FROM avg_windows {where} ORDER BY start_time"
        )
        buckets: list[dict[str, Any]] = []
        psd_rows: list[list[float]] = []
        total_windows = 0
        gmin, gmax = float("inf"), float("-inf")
        freq_start_hz = 0.0
        freq_step_hz = 0.0
        first_axis_num_bins = 0
        first_axis = True
        async with self._db.execute(query, params) as cursor:
            while True:
                rows = await cursor.fetchmany(5000)
                if not rows:
                    break
                for r in rows:
                    t = datetime.fromisoformat(r[0]).timestamp()
                    num_bins = int(r[2])
                    if first_axis:
                        first_axis = False
                        first_axis_num_bins = num_bins
                        freq_start_hz = float(r[3])
                        freq_step_hz = float(r[4])
                    buckets.append(
                        {
                            "start_epoch": t,
                            "duration_sec": float(r[1]),
                            "count": 1,
                            "pwr_avg": float(r[6]),
                            "pwr_max": float(r[7]) if r[7] is not None else 0.0,
                            "pwr_median": float(r[8]),
                            "pwr_std": float(r[9]),
                            "kurtosis": float(r[10]),
                        }
                    )
                    blob = r[5]
                    if blob is None:
                        psd_rows.append([float("nan")] * max_bins)
                    else:
                        total_windows += 1
                        powers, pmin, pmax = self._ds_psd(blob, num_bins, max_bins)
                        gmin = min(gmin, pmin)
                        gmax = max(gmax, pmax)
                        psd_rows.append(powers)
        if first_axis_num_bins > max_bins and freq_step_hz > 0:
            factor = first_axis_num_bins // max_bins
            freq_start_hz = freq_start_hz + (factor - 1) * freq_step_hz / 2.0
            freq_step_hz = factor * freq_step_hz
        return {
            "bucket_sec": bucket_sec,
            "num_bins": max_bins,
            "min_db": gmin if gmin != float("inf") else 0.0,
            "max_db": gmax if gmax != float("-inf") else 0.0,
            "total_windows": total_windows,
            "freq_start_hz": freq_start_hz,
            "freq_step_hz": freq_step_hz,
            "mode": 0,
            "buckets": buckets,
            "psd_rows": psd_rows,
        }

    async def _waterfall_aggregated(
        self,
        where: str,
        params: list[Any],
        since: datetime,
        bucket_sec: float,
        max_rows: int,
        max_bins: int,
    ) -> dict[str, Any]:
        """Aggregated mode: fold the range's windows into max_rows time buckets."""
        assert self._db is not None
        query = (
            "SELECT start_time, num_bins, freq_start_hz, freq_step_hz, psd_powers, "
            "pwr_avg, pwr_max, pwr_median, pwr_std, kurtosis "
            f"FROM avg_windows {where} ORDER BY start_time"
        )
        since_epoch = since.timestamp()
        # Per-bucket accumulators. PSD uses per-bin sums + per-bin counts so a
        # NaN-padded (short) row never poisons a bucket's mean.
        psd_sum = np.zeros((max_rows, max_bins), dtype=np.float64)
        psd_cnt = np.zeros((max_rows, max_bins), dtype=np.int64)
        stat_avg = [0.0] * max_rows
        stat_max = [-float("inf")] * max_rows
        stat_med = [0.0] * max_rows
        stat_std = [0.0] * max_rows
        stat_kurt = [0.0] * max_rows
        stat_n = [0] * max_rows
        total_windows = 0
        gmin, gmax = float("inf"), float("-inf")
        freq_start_hz = 0.0
        freq_step_hz = 0.0
        first_axis_num_bins = 0
        first_axis = True
        async with self._db.execute(query, params) as cursor:
            while True:
                rows = await cursor.fetchmany(5000)
                if not rows:
                    break
                for r in rows:
                    t = datetime.fromisoformat(r[0]).timestamp()
                    idx = int((t - since_epoch) / bucket_sec)
                    idx = max(0, min(idx, max_rows - 1))
                    num_bins = int(r[1])
                    if first_axis:
                        first_axis = False
                        first_axis_num_bins = num_bins
                        freq_start_hz = float(r[2])
                        freq_step_hz = float(r[3])
                    stat_n[idx] += 1
                    stat_avg[idx] += float(r[5])
                    if r[6] is not None:
                        stat_max[idx] = max(stat_max[idx], float(r[6]))
                    stat_med[idx] += float(r[7])
                    stat_std[idx] += float(r[8])
                    stat_kurt[idx] += float(r[9])
                    blob = r[4]
                    if blob is None:
                        continue
                    total_windows += 1
                    powers = np.frombuffer(blob, dtype="<f4")
                    if powers.size != num_bins:
                        num_bins = int(powers.size)
                    if num_bins > max_bins:
                        factor = num_bins // max_bins
                        trim = factor * max_bins
                        powers = powers[:trim].reshape(max_bins, factor).mean(axis=1)
                    else:
                        pad = max_bins - num_bins
                        if pad > 0:
                            powers = np.concatenate(
                                [powers, np.full(pad, np.nan, dtype=np.float32)]
                            )
                    gmin = min(gmin, float(np.nanmin(powers)))
                    gmax = max(gmax, float(np.nanmax(powers)))
                    valid = ~np.isnan(powers)
                    psd_sum[idx] += np.where(valid, powers, 0.0)
                    psd_cnt[idx] += valid
        # The downsampled axis is still uniform: group-mean of a uniform axis
        # shifts the start by (factor-1)*step/2 and multiplies the step.
        if first_axis_num_bins > max_bins and freq_step_hz > 0:
            factor = first_axis_num_bins // max_bins
            freq_start_hz = freq_start_hz + (factor - 1) * freq_step_hz / 2.0
            freq_step_hz = factor * freq_step_hz
        psd_rows: list[list[float]] = []
        for i in range(max_rows):
            if psd_cnt[i].any():
                with np.errstate(invalid="ignore"):
                    mean_row = psd_sum[i] / psd_cnt[i]
                psd_rows.append([float(x) for x in mean_row])
            else:
                psd_rows.append([float("nan")] * max_bins)
        buckets = [
            {
                "start_epoch": since_epoch + i * bucket_sec,
                "duration_sec": bucket_sec,
                "count": stat_n[i],
                "pwr_avg": stat_avg[i] / stat_n[i] if stat_n[i] else 0.0,
                "pwr_max": stat_max[i] if stat_n[i] else 0.0,
                "pwr_median": stat_med[i] / stat_n[i] if stat_n[i] else 0.0,
                "pwr_std": stat_std[i] / stat_n[i] if stat_n[i] else 0.0,
                "kurtosis": stat_kurt[i] / stat_n[i] if stat_n[i] else 0.0,
            }
            for i in range(max_rows)
        ]
        return {
            "bucket_sec": bucket_sec,
            "num_bins": max_bins,
            "min_db": gmin if gmin != float("inf") else 0.0,
            "max_db": gmax if gmax != float("-inf") else 0.0,
            "total_windows": total_windows,
            "freq_start_hz": freq_start_hz,
            "freq_step_hz": freq_step_hz,
            "mode": 1,
            "buckets": buckets,
            "psd_rows": psd_rows,
        }

    async def query_avg_stats(
        self,
        *,
        since: datetime,
        until: datetime,
        sdr_center_freq: float | None = None,
        sample_rate: float | None = None,
        gain: float | None = None,
        max_points: int = 600,
    ) -> dict[str, Any]:
        """Scalar stats timeline for a range. Reads only the light columns, so
        it works after retention prunes PSD blobs and over any range.

        Adaptive like the waterfall: with ``max_points`` or fewer windows each
        is returned as its own point (no averaging); with more, the windows are
        folded into ``max_points`` buckets (mean-of-means, pwr_max max-of-maxes).
        """
        assert self._db is not None
        span = (until - since).total_seconds()
        if span <= 0:
            return {"bucket_sec": 0.0, "min_pwr": 0.0, "max_pwr": 0.0, "points": []}
        bucket_sec = span / max_points
        conditions, sdr_params = self._sdr_conditions(sdr_center_freq, sample_rate, gain)
        where = "WHERE start_time >= ? AND start_time < ?"
        if conditions:
            where += " AND " + " AND ".join(conditions)
        params: list[Any] = [since.isoformat(), until.isoformat()]
        params.extend(sdr_params)
        async with self._db.execute(f"SELECT COUNT(*) FROM avg_windows {where}", params) as cur:
            row = await cur.fetchone()
            window_count = int(row[0]) if row else 0
        if window_count == 0:
            return {"bucket_sec": bucket_sec, "min_pwr": 0.0, "max_pwr": 0.0, "points": []}
        if window_count <= max_points:
            return await self._stats_raw(where, params, bucket_sec)
        return await self._stats_aggregated(where, params, since, bucket_sec, max_points)

    async def _stats_raw(self, where: str, params: list[Any], bucket_sec: float) -> dict[str, Any]:
        """Raw stats: one point per window (no averaging)."""
        assert self._db is not None
        query = (
            "SELECT start_time, pwr_avg, pwr_max, pwr_median, pwr_std, kurtosis "
            f"FROM avg_windows {where} ORDER BY start_time"
        )
        points: list[dict[str, Any]] = []
        gmin, gmax = float("inf"), float("-inf")
        async with self._db.execute(query, params) as cursor:
            while True:
                rows = await cursor.fetchmany(10000)
                if not rows:
                    break
                for r in rows:
                    points.append(
                        {
                            "start_time": r[0],
                            "count": 1,
                            "pwr_avg": float(r[1]),
                            "pwr_max": float(r[2]) if r[2] is not None else None,
                            "pwr_median": float(r[3]),
                            "pwr_std": float(r[4]),
                            "kurtosis": float(r[5]),
                        }
                    )
                    if r[2] is not None:
                        gmin = min(gmin, float(r[1]))
                        gmax = max(gmax, float(r[2]))
        return {
            "bucket_sec": bucket_sec,
            "min_pwr": gmin if gmin != float("inf") else 0.0,
            "max_pwr": gmax if gmax != float("-inf") else 0.0,
            "points": points,
        }

    async def _stats_aggregated(
        self, where: str, params: list[Any], since: datetime, bucket_sec: float, max_points: int
    ) -> dict[str, Any]:
        """Aggregated stats: fold the range's windows into max_points buckets."""
        assert self._db is not None
        query = (
            "SELECT start_time, pwr_avg, pwr_max, pwr_median, pwr_std, kurtosis "
            f"FROM avg_windows {where} ORDER BY start_time"
        )
        since_epoch = since.timestamp()
        n = [0] * max_points
        avg = [0.0] * max_points
        mx = [-float("inf")] * max_points
        med = [0.0] * max_points
        std = [0.0] * max_points
        kurt = [0.0] * max_points
        gmin, gmax = float("inf"), float("-inf")
        async with self._db.execute(query, params) as cursor:
            while True:
                rows = await cursor.fetchmany(10000)
                if not rows:
                    break
                for r in rows:
                    idx = int((datetime.fromisoformat(r[0]).timestamp() - since_epoch) / bucket_sec)
                    idx = max(0, min(idx, max_points - 1))
                    n[idx] += 1
                    avg[idx] += float(r[1])
                    med[idx] += float(r[3])
                    std[idx] += float(r[4])
                    kurt[idx] += float(r[5])
                    if r[2] is not None:
                        mx[idx] = max(mx[idx], float(r[2]))
                        gmin = min(gmin, float(r[1]))
                        gmax = max(gmax, float(r[2]))
        points = [
            {
                "start_time": (since + timedelta(seconds=i * bucket_sec)).isoformat(),
                "count": n[i],
                "pwr_avg": avg[i] / n[i] if n[i] else None,
                "pwr_max": mx[i] if n[i] and mx[i] != -float("inf") else None,
                "pwr_median": med[i] / n[i] if n[i] else None,
                "pwr_std": std[i] / n[i] if n[i] else None,
                "kurtosis": kurt[i] / n[i] if n[i] else None,
            }
            for i in range(max_points)
        ]
        return {
            "bucket_sec": bucket_sec,
            "min_pwr": gmin if gmin != float("inf") else 0.0,
            "max_pwr": gmax if gmax != float("-inf") else 0.0,
            "points": points,
        }

    async def avg_window_configs(self) -> dict[str, Any]:
        """Distinct SDR tuning configs present in avg_windows + the most recent.

        Feeds the averaged-history page's tuning filter (default = latest).
        """
        assert self._db is not None
        self._db.row_factory = aiosqlite.Row
        async with self._db.execute(
            """SELECT DISTINCT sdr_center_freq_hz, sample_rate_hz, gain_db
               FROM avg_windows
               WHERE sdr_center_freq_hz IS NOT NULL
               ORDER BY sdr_center_freq_hz, sample_rate_hz, gain_db"""
        ) as cursor:
            configs = [dict(row) for row in await cursor.fetchall()]
        async with self._db.execute(
            """SELECT sdr_center_freq_hz, sample_rate_hz, gain_db
               FROM avg_windows
               WHERE sdr_center_freq_hz IS NOT NULL
               ORDER BY start_time DESC LIMIT 1"""
        ) as cursor:
            row = await cursor.fetchone()
        return {"configs": configs, "latest": dict(row) if row else None}

    @staticmethod
    def _sdr_conditions(
        sdr_center_freq: float | None,
        sample_rate: float | None,
        gain: float | None,
    ) -> tuple[list[str], list[Any]]:
        """Build the exact-match SDR capture-context WHERE fragments.

        Shared by query_detections and duration_histogram so the two always
        scope detections by the same tuning-config filters.
        """
        conditions: list[str] = []
        params: list[Any] = []
        if sdr_center_freq is not None:
            conditions.append("sdr_center_freq_hz = ?")
            params.append(sdr_center_freq)
        if sample_rate is not None:
            conditions.append("sample_rate_hz = ?")
            params.append(sample_rate)
        if gain is not None:
            conditions.append("gain_db = ?")
            params.append(gain)
        return conditions, params

    async def query_detections(
        self,
        limit: int = 100,
        offset: int = 0,
        min_freq: float | None = None,
        max_freq: float | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        sdr_center_freq: float | None = None,
        sample_rate: float | None = None,
        gain: float | None = None,
        min_duration_ms: float | None = None,
        max_duration_ms: float | None = None,
    ) -> list[dict[str, Any]]:
        assert self._db is not None
        conditions = []
        params: list[Any] = []

        if min_freq is not None:
            conditions.append("center_freq_hz >= ?")
            params.append(min_freq)
        if max_freq is not None:
            conditions.append("center_freq_hz <= ?")
            params.append(max_freq)
        if since is not None:
            conditions.append("start_time >= ?")
            params.append(since.isoformat())
        if until is not None:
            conditions.append("start_time < ?")
            params.append(until.isoformat())
        # Exact-match SDR capture-context filters (categorize by tuning config).
        sdr_conditions, sdr_params = self._sdr_conditions(sdr_center_freq, sample_rate, gain)
        conditions.extend(sdr_conditions)
        params.extend(sdr_params)
        # Half-open [min, max) duration range — matches the histogram buckets so a
        # bar click drills the table to exactly that bucket.
        if min_duration_ms is not None:
            conditions.append("duration_ms >= ?")
            params.append(min_duration_ms)
        if max_duration_ms is not None:
            conditions.append("duration_ms < ?")
            params.append(max_duration_ms)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"SELECT * FROM detections {where} ORDER BY start_time DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        self._db.row_factory = aiosqlite.Row
        async with self._db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def duration_histogram(
        self,
        bin_width: float | None = None,
        sdr_center_freq: float | None = None,
        sample_rate: float | None = None,
        gain: float | None = None,
    ) -> dict[str, Any]:
        """Bucket detection pulse lengths (duration_ms) into fixed-width bins.

        Aggregates over the full set matching the SDR filters (not the table's
        50-row page). bin_width None → an auto width derived from the data range.
        Returns {min, max, count, bin_width, bins:[{lo, hi, count}, ...]} with each
        bin half-open [lo, hi); the final bin includes an exact-max sample.
        """
        assert self._db is not None
        conditions, params = self._sdr_conditions(sdr_center_freq, sample_rate, gain)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"SELECT duration_ms FROM detections {where}"
        async with self._db.execute(query, params) as cursor:
            rows = await cursor.fetchall()

        durations = [float(r[0]) for r in rows if r[0] is not None]
        if not durations:
            return {"min": None, "max": None, "count": 0, "bin_width": bin_width, "bins": []}

        lo_d, hi_d = min(durations), max(durations)
        if bin_width is not None and bin_width > 0:
            width = bin_width
        else:
            width = _nice_bin_width(hi_d - lo_d)

        start = math.floor(lo_d / width) * width
        end = math.ceil(hi_d / width) * width
        n_bins = max(1, int(round((end - start) / width)))

        counts = [0] * n_bins
        for d in durations:
            idx = int((d - start) // width)
            idx = max(0, min(idx, n_bins - 1))  # clamp the exact-max into the last bin
            counts[idx] += 1

        bins = [
            {"lo": start + i * width, "hi": start + (i + 1) * width, "count": counts[i]}
            for i in range(n_bins)
        ]
        return {
            "min": lo_d,
            "max": hi_d,
            "count": len(durations),
            "bin_width": width,
            "bins": bins,
        }

    async def capture_configs(self) -> list[dict[str, Any]]:
        """Return the distinct SDR capture configs present in the detections.

        Feeds the History page filter dropdowns so they only offer tuning
        configurations that actually appear in the stored data.
        """
        assert self._db is not None
        self._db.row_factory = aiosqlite.Row
        async with self._db.execute(
            """SELECT DISTINCT sdr_center_freq_hz, sample_rate_hz, gain_db
               FROM detections
               WHERE sdr_center_freq_hz IS NOT NULL
               ORDER BY sdr_center_freq_hz, sample_rate_hz, gain_db"""
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def count_detections(self) -> int:
        """Monotonic change-marker for the detections table (O(1)).

        Used only by the heartbeat as a "did a new detection arrive" trigger
        (clients refresh when it increments), so the exact value is
        irrelevant; MAX(id) is O(1) via the integer PK and stays monotonic
        (SQLite does not reuse rowids without VACUUM).
        """
        assert self._db is not None
        async with self._db.execute("SELECT MAX(id) FROM detections") as cursor:
            row = await cursor.fetchone()
            return int(row[0]) if row and row[0] is not None else 0

    async def set_config(self, key: str, value: str) -> None:
        assert self._db is not None
        await self._db.execute(
            "INSERT OR REPLACE INTO config (key, value, updated_at) VALUES (?, ?, datetime('now'))",
            (key, value),
        )
        await self._db.commit()

    async def get_config(self, key: str) -> str | None:
        assert self._db is not None
        async with self._db.execute("SELECT value FROM config WHERE key = ?", (key,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    async def insert_stats(self, timestamp: datetime, data: dict[str, Any]) -> None:
        assert self._db is not None
        await self._db.execute(
            "INSERT INTO stats (timestamp, data) VALUES (?, ?)",
            (timestamp.isoformat(), json.dumps(data)),
        )
        await self._db.commit()

    async def prune_avg_psd_blobs(self, days: int = 7) -> int:
        """Evict the PSD blobs of averaged windows older than ``days`` days.

        Only the heavy ``psd_powers``/``violations`` blobs are nulled out; the
        cheap stats row (and detections, stats, tone_checks) is kept
        permanently. A pruned window still answers the light query and its
        detail endpoint (with ``powers: null``): at ~8 KB per window the blob
        is ~98% of the row's storage, so this bounds the DB file without
        losing any statistics. Returns how many blobs were nulled this pass.
        """
        assert self._db is not None
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        cursor = await self._db.execute(
            "UPDATE avg_windows SET psd_powers = NULL, violations = NULL "
            "WHERE start_time < ? AND psd_powers IS NOT NULL",
            (cutoff,),
        )
        await self._db.commit()
        pruned = cursor.rowcount
        if pruned > 0:
            logger.info("Pruned PSD blobs for %d avg windows (cutoff: %s)", pruned, cutoff)
        return pruned
