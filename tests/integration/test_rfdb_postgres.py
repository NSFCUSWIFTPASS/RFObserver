"""The rf-db writer against a real Postgres loaded with rf-db's schema.

Runs as an ``rfobs_writer`` role holding only the grants a sensor gets, so it
also proves those grants are enough. Needs ``RFDB_TEST_DSN``, a superuser DSN
(CI sets it); skipped otherwise.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import asyncpg
import pytest
from pydantic import SecretStr

from rfobserver.config import RfdbSettingsGroup
from rfobserver.storage.database import SensorDatabase
from rfobserver.transport.rfdb import (
    BOOKMARK_KEY,
    MetadataKey,
    OutputRow,
    RfdbClient,
    RfdbWriter,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

DSN = os.environ.get("RFDB_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="RFDB_TEST_DSN not set")

_SCHEMA = (Path(__file__).parent / "rfdb_schema.sql").read_text()
_WRITER_PASSWORD = "writer-pw"
_GRANTS = """
    GRANT SELECT ON rpi TO rfobs_writer;
    GRANT SELECT, INSERT ON metadata TO rfobs_writer;
    GRANT INSERT ON outputs TO rfobs_writer;
    -- ON CONFLICT reads the conflict columns to detect a duplicate.
    GRANT SELECT (hardware_id, metadata_id, created_at) ON outputs TO rfobs_writer;
"""
_T0 = datetime(2026, 9, 30, 12, 0, 0, tzinfo=timezone.utc)
_KEY = MetadataKey(915_000_000, 26_000_000, 26_000_000, 35, 0.5, 0.5, "16")


@dataclass
class Rfdb:
    admin: asyncpg.Connection
    settings: RfdbSettingsGroup
    hardware_id: int

    async def count(self, table: str) -> int:
        return int(await self.admin.fetchval(f"SELECT count(*) FROM {table}"))


@pytest.fixture
async def rfdb() -> AsyncIterator[Rfdb]:
    """A scratch rf-db with rf-nano-aa registered."""
    assert DSN is not None
    url = urlsplit(DSN)
    name = f"rfdb_test_{uuid.uuid4().hex[:8]}"
    server = await asyncpg.connect(DSN)
    await server.execute(f"CREATE DATABASE {name}")
    for role in ("nrdz NOLOGIN", f"rfobs_writer LOGIN PASSWORD '{_WRITER_PASSWORD}'"):
        await server.execute(
            f"DO $$ BEGIN CREATE ROLE {role}; EXCEPTION WHEN duplicate_object THEN NULL; END $$"
        )
    admin = await asyncpg.connect(url._replace(path=f"/{name}").geturl())
    try:
        await admin.execute(_SCHEMA)
        await admin.execute("SET search_path = public")
        await admin.execute(_GRANTS)
        mount_id = await admin.fetchval(
            "INSERT INTO storage (nfs_mnt, local_mnt, storage_cap, op_status)"
            " VALUES ('', '', 0, 1) RETURNING mount_id"
        )
        hardware_id = await admin.fetchval(
            "INSERT INTO hardware (location, enclosure, op_status, mount_id)"
            " VALUES ('-121.47, 40.82', true, 1, $1) RETURNING hardware_id",
            mount_id,
        )
        await admin.execute(
            "INSERT INTO rpi (hostname, rpi_ip, rpi_mac, rpi_v, os_v, memory, storage_cap,"
            " cpu_type, cpu_cores, op_status, hardware_id) VALUES ('rf-nano-aa', '10.1.42.18',"
            " '00:00:00:00:00:01', 'NVIDIA Jetson Orin Nano', 'Ubuntu 22.04', 8000000,"
            " 250000000, 'ARMv8', 6, 1, $1)",
            hardware_id,
        )
        settings = RfdbSettingsGroup(
            host=url.hostname or "localhost",
            port=url.port or 5432,
            name=name,
            user="rfobs_writer",
            password=SecretStr(_WRITER_PASSWORD),
            flush_sec=5.0,
        )
        yield Rfdb(admin, settings, int(hardware_id))
    finally:
        await admin.close()
        await server.execute(f"DROP DATABASE {name} WITH (FORCE)")
        await server.close()


@pytest.fixture
async def client(rfdb: Rfdb) -> AsyncIterator[RfdbClient]:
    c = RfdbClient(rfdb.settings)
    await c.connect()
    yield c
    await c.close()


def _row(seconds: int, key: MetadataKey = _KEY, **stats: float) -> OutputRow:
    values = dict(average_db=-70.0, max_db=-50.0, median_db=-72.0, std_dev=3.0, kurtosis=1.2)
    values.update(stats)
    return OutputRow(metadata=key, created_at=_T0 + timedelta(seconds=seconds), **values)


async def test_hardware_id_for_registered_and_unknown_hosts(rfdb: Rfdb, client: RfdbClient):
    assert await client.hardware_id("rf-nano-aa") == rfdb.hardware_id
    assert await client.hardware_id("rf-nano-zz") is None


async def test_insert_writes_rows_once(rfdb: Rfdb, client: RfdbClient):
    rows = [_row(0), _row(1)]
    await client.insert_outputs(rfdb.hardware_id, rows)
    await client.insert_outputs(rfdb.hardware_id, rows)

    assert await rfdb.count("outputs") == 2
    meta = await rfdb.admin.fetchrow(
        'SELECT frequency, sample_rate, bandwidth, gain, length, "interval", bit_depth'
        " FROM metadata"
    )
    assert tuple(meta) == (915_000_000, 26_000_000, 26_000_000, 35, 0.5, 0.5, "16")
    out = await rfdb.admin.fetchrow(
        "SELECT created_at, average_db, max_db, kurtosis FROM outputs ORDER BY created_at"
    )
    assert out["created_at"] == _T0
    assert (float(out["average_db"]), float(out["max_db"]), float(out["kurtosis"])) == (
        -70.0,
        -50.0,
        1.2,
    )


async def test_existing_metadata_rows_are_reused(rfdb: Rfdb, client: RfdbClient):
    existing = await rfdb.admin.fetchval(
        'INSERT INTO metadata (frequency, sample_rate, bandwidth, gain, length, "interval",'
        " bit_depth) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING metadata_id",
        *_KEY,
    )
    other_gain = _KEY._replace(gain=40)

    await client.insert_outputs(rfdb.hardware_id, [_row(0), _row(1, key=other_gain)])

    assert await rfdb.count("metadata") == 2
    ids = await rfdb.admin.fetch("SELECT metadata_id FROM outputs ORDER BY created_at")
    assert ids[0]["metadata_id"] == existing


async def test_writer_sends_sqlite_windows_and_isolates_a_rejected_one(rfdb: Rfdb, tmp_path):
    local = SensorDatabase(str(tmp_path / "rfobs.db"))
    await local.connect()
    reader = SensorDatabase(str(tmp_path / "rfobs.db"), read_only=True)
    await reader.connect()
    try:
        common = dict(
            duration_sec=0.52,
            sdr_center_freq_hz=915e6,
            sample_rate_hz=26e6,
            gain_db=35.0,
            num_bins=2,
            freq_start_hz=0.0,
            freq_step_hz=1.0,
            pwr_avg=-70.0,
            pwr_median=-72.0,
            pwr_std=3.0,
            kurtosis=1.2,
            powers=[-70.0, -60.0],
        )
        for i in range(3):
            # 1e6 dB overflows rf-db's numeric(21,16): Postgres rejects that row.
            await local.insert_avg_window(
                start_time=_T0 + timedelta(seconds=i), pwr_max=1e6 if i == 1 else -50.0, **common
            )
        await local._db.commit()
        await local.set_config(BOOKMARK_KEY, "0")

        writer = RfdbWriter(
            client=RfdbClient(rfdb.settings),
            reader=reader,
            store=local,
            hostname="rf-nano-aa",
            window_sec=lambda: 0.5,
            flush_sec=5.0,
        )
        await writer.flush_once()
        await writer._client.close()

        assert await rfdb.count("outputs") == 2
        assert writer.status()["skipped"] == 1
        assert await local.get_config(BOOKMARK_KEY) == "3"
    finally:
        await reader.close()
        await local.close()
