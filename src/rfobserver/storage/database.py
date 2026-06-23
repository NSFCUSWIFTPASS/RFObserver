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

CREATE INDEX IF NOT EXISTS idx_detections_time ON detections(start_time);
CREATE INDEX IF NOT EXISTS idx_detections_freq ON detections(center_freq_hz);
CREATE INDEX IF NOT EXISTS idx_stats_time ON stats(timestamp);
"""

# SDR capture-context columns added after the original detections schema.
# Existing databases predate them, so connect() adds any that are missing via
# ALTER TABLE (SQLite has no "ADD COLUMN IF NOT EXISTS").
_DETECTION_SDR_COLUMNS: dict[str, str] = {
    "sdr_center_freq_hz": "REAL",
    "sample_rate_hz": "REAL",
    "lo_offset_hz": "REAL",
    "analog_bw_hz": "REAL",
    "gain_db": "REAL",
    "antenna": "TEXT",
    "device_serial": "TEXT",
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
    ) -> None:
        assert self._db is not None
        await self._db.execute(
            """INSERT OR IGNORE INTO detections
               (burst_id, start_time, stop_time, center_freq_hz, bandwidth_hz,
                peak_power_db, duration_ms, detection_timestamp,
                sdr_center_freq_hz, sample_rate_hz, lo_offset_hz, analog_bw_hz,
                gain_db, antenna, device_serial)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
            ),
        )
        await self._db.commit()

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
        """Return the total number of rows in the detections table.

        Used by the WebSocket heartbeat as a monotonic counter — clients
        refresh the detections table when this increments instead of polling
        the HTML endpoint on a fixed interval.
        """
        assert self._db is not None
        async with self._db.execute("SELECT COUNT(*) FROM detections") as cursor:
            row = await cursor.fetchone()
            return int(row[0]) if row else 0

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

    async def cleanup_old_data(self, days: int = 7) -> int:
        """Remove detections and stats older than the given number of days."""
        assert self._db is not None
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()

        cursor = await self._db.execute("DELETE FROM detections WHERE start_time < ?", (cutoff,))
        det_count = cursor.rowcount
        cursor = await self._db.execute("DELETE FROM stats WHERE timestamp < ?", (cutoff,))
        stats_count = cursor.rowcount

        await self._db.commit()
        total: int = det_count + stats_count
        if total > 0:
            logger.info("Cleaned up %d old records (cutoff: %s)", total, cutoff)
        return total
