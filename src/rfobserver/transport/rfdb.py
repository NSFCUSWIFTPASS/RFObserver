"""rf-db, the Grafana Postgres on node1: maps averaged windows to rf-processor's
``metadata``/``outputs`` rows and writes them."""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, NamedTuple, cast

import asyncpg

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

    from rfobserver.config import RfdbSettingsGroup
    from rfobserver.storage.database import WindowStatsRow

logger = logging.getLogger(__name__)

_INSERT_METADATA = """
    INSERT INTO metadata (frequency, sample_rate, bandwidth, gain, length, "interval", bit_depth)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (frequency, sample_rate, bandwidth, gain, length, "interval", bit_depth) DO NOTHING
    RETURNING metadata_id
"""

_SELECT_METADATA = """
    SELECT metadata_id FROM metadata
    WHERE frequency = $1 AND sample_rate = $2 AND bandwidth = $3 AND gain = $4
      AND length = $5 AND "interval" = $6 AND bit_depth = $7
"""

_INSERT_OUTPUT = """
    INSERT INTO outputs
        (hardware_id, metadata_id, created_at, average_db, max_db, median_db, std_dev, kurtosis)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (hardware_id, metadata_id, created_at) DO NOTHING
"""


class MetadataKey(NamedTuple):
    """A capture configuration: one rf-db ``metadata`` row and its unique key."""

    frequency: int
    sample_rate: int
    bandwidth: int
    gain: int
    length: float
    interval: float
    bit_depth: str


class OutputRow(NamedTuple):
    """One rf-db ``outputs`` row, before its ids are resolved."""

    metadata: MetadataKey
    created_at: datetime
    average_db: float
    max_db: float
    median_db: float
    std_dev: float
    kurtosis: float


def to_output_row(window: WindowStatsRow, window_sec: float) -> OutputRow | None:
    """Map a window to its rf-db row, or None if rf-db can't store it.

    ``window_sec`` is the configured DURATION_SEC, not the window's measured
    duration, which varies and would create a metadata row per window.
    """
    values = (
        window.sdr_center_freq_hz,
        window.sample_rate_hz,
        window.gain_db,
        window.pwr_avg,
        window.pwr_max,
        window.pwr_median,
        window.pwr_std,
        window.kurtosis,
    )
    if not all(v is not None and math.isfinite(v) for v in values):
        return None
    freq, rate, gain, avg, peak, median, std, kurt = cast("tuple[float, ...]", values)
    return OutputRow(
        metadata=MetadataKey(
            frequency=round(freq),
            sample_rate=round(rate),
            bandwidth=round(rate),
            gain=round(gain),
            length=window_sec,
            interval=window_sec,
            bit_depth="16",
        ),
        created_at=window.start_time,
        average_db=avg,
        max_db=peak,
        median_db=median,
        std_dev=std,
        kurtosis=kurt,
    )


class RfdbClient:
    """Writes averaged windows into rf-db's ``metadata`` and ``outputs`` tables."""

    def __init__(self, settings: RfdbSettingsGroup) -> None:
        self._settings = settings
        self._pool: asyncpg.Pool | None = None
        self._hardware_ids: dict[str, int] = {}
        self._metadata_ids: dict[MetadataKey, int] = {}

    async def connect(self) -> None:
        s = self._settings
        self._pool = await asyncpg.create_pool(
            host=s.host,
            port=s.port,
            user=s.user,
            password=s.password.get_secret_value(),
            database=s.name,
            min_size=1,
            max_size=1,
            timeout=10,
            command_timeout=30,
        )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def hardware_id(self, hostname: str) -> int | None:
        """This sensor's ``hardware_id`` from its ``rpi`` row, or None if unregistered."""
        if hostname in self._hardware_ids:
            return self._hardware_ids[hostname]
        assert self._pool is not None
        found = await self._pool.fetchval(
            "SELECT hardware_id FROM rpi WHERE hostname = $1", hostname
        )
        if found is None:
            return None
        self._hardware_ids[hostname] = int(found)
        return int(found)

    async def insert_outputs(self, hardware_id: int, rows: Sequence[OutputRow]) -> None:
        """Insert rows in one transaction; rows already in rf-db are skipped."""
        assert self._pool is not None
        values = []
        for r in rows:
            metadata_id = await self._metadata_id(r.metadata)
            values.append(
                (
                    hardware_id,
                    metadata_id,
                    r.created_at,
                    r.average_db,
                    r.max_db,
                    r.median_db,
                    r.std_dev,
                    r.kurtosis,
                )
            )
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.executemany(_INSERT_OUTPUT, values)

    async def _metadata_id(self, key: MetadataKey) -> int:
        """The ``metadata`` row for this configuration, created if missing.

        Resolved outside the outputs transaction: a rolled-back batch must not
        leave a cached id for a row that was rolled back with it.
        """
        if key in self._metadata_ids:
            return self._metadata_ids[key]
        assert self._pool is not None
        found = await self._pool.fetchval(_INSERT_METADATA, *key)
        if found is None:
            found = await self._pool.fetchval(_SELECT_METADATA, *key)
        self._metadata_ids[key] = int(found)
        return int(found)
