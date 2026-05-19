"""Local SQLite database for detection history, config, and sensor state.

Uses aiosqlite for async access. Stores recent detections, burst fingerprints,
and sensor configuration. Rolling window cleanup removes old data.
"""

from __future__ import annotations

import json
import logging
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
        await self._db.commit()
        logger.info("Database connected: %s", self._db_path)

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
    ) -> None:
        assert self._db is not None
        await self._db.execute(
            """INSERT OR IGNORE INTO detections
               (burst_id, start_time, stop_time, center_freq_hz, bandwidth_hz,
                peak_power_db, duration_ms, detection_timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                burst_id,
                start_time.isoformat(),
                stop_time.isoformat(),
                center_freq_hz,
                bandwidth_hz,
                peak_power_db,
                duration_ms,
                detection_timestamp.isoformat(),
            ),
        )
        await self._db.commit()

    async def query_detections(
        self,
        limit: int = 100,
        offset: int = 0,
        min_freq: float | None = None,
        max_freq: float | None = None,
        since: datetime | None = None,
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

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"SELECT * FROM detections {where} ORDER BY start_time DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        self._db.row_factory = aiosqlite.Row
        async with self._db.execute(query, params) as cursor:
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
