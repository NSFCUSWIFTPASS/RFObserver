"""rf-db, the Grafana Postgres on node1: maps averaged windows to rf-processor's
``metadata``/``outputs`` rows and writes them."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, NamedTuple, cast

import asyncpg

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from rfobserver.config import RfdbSettingsGroup
    from rfobserver.storage.database import SensorDatabase, WindowStatsRow

logger = logging.getLogger(__name__)

BOOKMARK_KEY = "rfdb_last_id"
BATCH_SIZE = 1000
_BOOKMARK_SAVE_TIMEOUT_SEC = 10.0
_BACKOFF_MIN_SEC = 5.0
_BACKOFF_MAX_SEC = 60.0
_SKIP_WARN_INTERVAL_SEC = 60.0

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


class RfdbWriter:
    """Sends averaged windows from the local ``avg_windows`` table to rf-db.

    ``avg_windows`` is the queue. The bookmark (config key ``rfdb_last_id``) is
    the id of the last window confirmed in rf-db and only moves after a commit,
    so an outage delays windows but never loses them.
    """

    def __init__(
        self,
        *,
        client: RfdbClient,
        reader: SensorDatabase,
        store: SensorDatabase,
        hostname: str,
        window_sec: Callable[[], float],
        flush_sec: float,
    ) -> None:
        self._client = client
        self._reader = reader
        self._store = store
        self._hostname = hostname
        self._window_sec = window_sec
        self._flush_sec = flush_sec
        self._connected = False
        self._bookmark: int | None = None
        self._registered: bool | None = None
        self._backlog = 0
        self._sent = 0
        self._skipped = 0
        self._last_ok: datetime | None = None
        self._last_error: str | None = None
        self._last_skip_warn = -math.inf

    def status(self) -> dict[str, Any]:
        return {
            "registered": self._registered,
            "bookmark": self._bookmark,
            "backlog": self._backlog,
            "sent": self._sent,
            "skipped": self._skipped,
            "last_ok": self._last_ok.isoformat() if self._last_ok else None,
            "last_error": self._last_error,
        }

    async def run(self) -> None:
        """Flush until cancelled, backing off while rf-db is unreachable."""
        backoff = _BACKOFF_MIN_SEC
        try:
            while True:
                try:
                    more = await self.flush_once()
                except Exception as exc:
                    self._last_error = f"{type(exc).__name__}: {exc}"
                    logger.warning(
                        "rf-db flush failed (%s); retrying in %.0f s", self._last_error, backoff
                    )
                    await self._reset_connection()
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, _BACKOFF_MAX_SEC)
                    continue
                backoff = _BACKOFF_MIN_SEC
                if not more:
                    await asyncio.sleep(self._flush_sec)
        finally:
            await self._client.close()

    async def flush_once(self) -> bool:
        """Send the next batch. True if a full batch went out and more may be waiting."""
        if not self._connected:
            await self._client.connect()
            self._connected = True
        if self._bookmark is None:
            self._bookmark = await self._load_bookmark()

        hardware_id = await self._client.hardware_id(self._hostname)
        self._registered = hardware_id is not None
        if hardware_id is None:
            await self._update_backlog()
            return False

        windows = await self._reader.avg_windows_after(self._bookmark, BATCH_SIZE)
        window_sec = self._window_sec()
        rows = []
        for window in windows:
            row = to_output_row(window, window_sec)
            if row is None:
                self._skip_unstorable(window)
            else:
                rows.append(row)
        if rows:
            await self._insert(hardware_id, rows)
        if windows:
            self._bookmark = windows[-1].id
            await self._save_bookmark(self._bookmark)

        self._last_ok = datetime.now(timezone.utc)
        self._last_error = None
        await self._update_backlog()
        return len(windows) == BATCH_SIZE

    async def _insert(self, hardware_id: int, rows: list[OutputRow]) -> None:
        try:
            await self._client.insert_outputs(hardware_id, rows)
            self._sent += len(rows)
        except asyncpg.DataError:
            # One value rf-db rejects fails the whole batch: retry row by row so
            # only that row is dropped.
            for row in rows:
                try:
                    await self._client.insert_outputs(hardware_id, [row])
                    self._sent += 1
                except asyncpg.DataError as exc:
                    self._skipped += 1
                    logger.warning("rf-db rejected the window starting %s: %s", row.created_at, exc)

    def _skip_unstorable(self, window: WindowStatsRow) -> None:
        self._skipped += 1
        now = time.monotonic()
        if now - self._last_skip_warn >= _SKIP_WARN_INTERVAL_SEC:
            self._last_skip_warn = now
            logger.warning(
                "Skipping window %d: a missing or non-finite value rf-db can't store "
                "(%d skipped so far)",
                window.id,
                self._skipped,
            )

    async def _load_bookmark(self) -> int:
        stored = await self._store.get_config(BOOKMARK_KEY)
        if stored is not None:
            return int(stored)
        newest = await self._reader.newest_avg_window_id()
        logger.info("rf-db writer first start: sending windows after id %d", newest)
        await self._save_bookmark(newest)
        return newest

    async def _save_bookmark(self, value: int) -> None:
        """Persist the bookmark. On a stalled disk it stays in memory only: a
        restart then resends a few windows, which rf-db drops as duplicates."""
        try:
            await asyncio.wait_for(
                self._store.set_config(BOOKMARK_KEY, str(value)),
                timeout=_BOOKMARK_SAVE_TIMEOUT_SEC,
            )
        except (TimeoutError, asyncio.TimeoutError):  # noqa: UP041
            logger.warning("Saving the rf-db bookmark timed out; keeping it in memory")

    async def _update_backlog(self) -> None:
        assert self._bookmark is not None
        newest = await self._reader.newest_avg_window_id()
        self._backlog = max(0, newest - self._bookmark)

    async def _reset_connection(self) -> None:
        self._connected = False
        with contextlib.suppress(Exception):
            await self._client.close()
