"""The rf-db writer's flush pass, against a fake local database and rf-db client."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from rfobserver.storage.database import WindowStatsRow
from rfobserver.transport import rfdb
from rfobserver.transport.rfdb import BOOKMARK_KEY, RfdbWriter

_T0 = datetime(2026, 9, 30, 12, 0, 0, tzinfo=timezone.utc)


def _window(window_id: int, **overrides: object) -> WindowStatsRow:
    fields: dict[str, object] = dict(
        id=window_id,
        start_time=_T0 + timedelta(seconds=window_id),
        duration_sec=0.5,
        sdr_center_freq_hz=915e6,
        sample_rate_hz=26e6,
        gain_db=35.0,
        pwr_avg=-70.0,
        pwr_max=-50.0,
        pwr_median=-72.0,
        pwr_std=3.0,
        kurtosis=1.2,
    )
    fields.update(overrides)
    return WindowStatsRow(**fields)  # type: ignore[arg-type]


class FakeDb:
    """Stands in for both the read-only reader and the writer's config store."""

    def __init__(self, windows: list[WindowStatsRow], bookmark: int | None = None) -> None:
        self.windows = windows
        self.config: dict[str, str] = {}
        if bookmark is not None:
            self.config[BOOKMARK_KEY] = str(bookmark)

    async def avg_windows_after(self, after_id: int, limit: int) -> list[WindowStatsRow]:
        return [w for w in self.windows if w.id > after_id][:limit]

    async def newest_avg_window_id(self) -> int:
        return max((w.id for w in self.windows), default=0)

    async def get_config(self, key: str) -> str | None:
        return self.config.get(key)

    async def set_config(self, key: str, value: str) -> None:
        self.config[key] = value


class FakeClient:
    def __init__(self, hardware_id: int | None = 1) -> None:
        self.hardware = hardware_id
        self.inserted: list[rfdb.OutputRow] = []
        self.fail: Exception | None = None
        self.rejected_starts: set[datetime] = set()
        self.closes = 0

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        self.closes += 1

    async def hardware_id(self, hostname: str) -> int | None:
        return self.hardware

    async def insert_outputs(self, hardware_id: int, rows: list[rfdb.OutputRow]) -> None:
        if self.fail is not None:
            raise self.fail
        if any(r.created_at in self.rejected_starts for r in rows):
            raise asyncpg.DataError("numeric field overflow")
        self.inserted.extend(rows)


def _writer(db: FakeDb, client: FakeClient, flush_sec: float = 5.0) -> RfdbWriter:
    return RfdbWriter(
        client=client,  # type: ignore[arg-type]
        reader=db,  # type: ignore[arg-type]
        store=db,  # type: ignore[arg-type]
        hostname="rf-nano-aa",
        window_sec=lambda: 0.5,
        flush_sec=flush_sec,
    )


def _sent_ids(client: FakeClient) -> list[int]:
    return [int((r.created_at - _T0).total_seconds()) for r in client.inserted]


async def test_first_start_begins_at_the_newest_window():
    db = FakeDb([_window(i) for i in range(1, 6)])
    client = FakeClient()
    writer = _writer(db, client)

    assert await writer.flush_once() is False
    assert client.inserted == []
    assert db.config[BOOKMARK_KEY] == "5"

    db.windows.append(_window(6))
    await writer.flush_once()

    assert _sent_ids(client) == [6]
    assert db.config[BOOKMARK_KEY] == "6"


async def test_resumes_after_the_saved_bookmark():
    db = FakeDb([_window(i) for i in range(1, 6)], bookmark=2)
    client = FakeClient()

    await _writer(db, client).flush_once()

    assert _sent_ids(client) == [3, 4, 5]
    assert db.config[BOOKMARK_KEY] == "5"


async def test_an_unregistered_host_holds_its_windows():
    db = FakeDb([_window(i) for i in range(1, 6)], bookmark=2)
    client = FakeClient(hardware_id=None)
    writer = _writer(db, client)

    await writer.flush_once()

    assert client.inserted == []
    assert db.config[BOOKMARK_KEY] == "2"
    assert writer.status()["registered"] is False
    assert writer.status()["backlog"] == 3


async def test_a_failed_insert_does_not_move_the_bookmark():
    db = FakeDb([_window(i) for i in range(1, 6)], bookmark=2)
    client = FakeClient()
    client.fail = OSError("connection refused")
    writer = _writer(db, client)

    with pytest.raises(OSError):
        await writer.flush_once()
    assert db.config[BOOKMARK_KEY] == "2"

    client.fail = None
    await writer.flush_once()
    assert _sent_ids(client) == [3, 4, 5]


async def test_windows_rfdb_cannot_store_are_skipped_and_counted():
    db = FakeDb([_window(1), _window(2, gain_db=None), _window(3)], bookmark=0)
    client = FakeClient()
    writer = _writer(db, client)

    await writer.flush_once()

    assert _sent_ids(client) == [1, 3]
    assert db.config[BOOKMARK_KEY] == "3"
    assert writer.status()["skipped"] == 1


async def test_a_row_rfdb_rejects_is_isolated_from_its_batch():
    db = FakeDb([_window(i) for i in range(1, 4)], bookmark=0)
    client = FakeClient()
    client.rejected_starts = {_T0 + timedelta(seconds=2)}
    writer = _writer(db, client)

    await writer.flush_once()

    assert _sent_ids(client) == [1, 3]
    assert db.config[BOOKMARK_KEY] == "3"
    assert writer.status()["skipped"] == 1


async def test_a_full_batch_asks_to_flush_again(monkeypatch):
    monkeypatch.setattr(rfdb, "BATCH_SIZE", 2)
    db = FakeDb([_window(i) for i in range(1, 6)], bookmark=0)
    client = FakeClient()
    writer = _writer(db, client)

    assert await writer.flush_once() is True
    assert _sent_ids(client) == [1, 2]
    assert writer.status()["backlog"] == 3


async def test_run_backs_off_after_a_failure_then_resumes(monkeypatch):
    db = FakeDb([_window(i) for i in range(1, 4)], bookmark=0)
    client = FakeClient()
    client.fail = OSError("connection refused")
    writer = _writer(db, client, flush_sec=7.0)
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        client.fail = None
        if len(sleeps) == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(rfdb.asyncio, "sleep", fake_sleep)
    with pytest.raises(asyncio.CancelledError):
        await writer.run()

    assert sleeps == [5.0, 7.0]  # the failure's backoff, then the normal interval
    assert _sent_ids(client) == [1, 2, 3]
    assert writer.status()["last_error"] is None
    assert client.closes == 2  # once to reset after the failure, once on the way out


async def test_a_changed_window_length_applies_to_the_next_flush():
    db = FakeDb([_window(1)], bookmark=0)
    client = FakeClient()
    window_sec = 0.5
    writer = RfdbWriter(
        client=client,  # type: ignore[arg-type]
        reader=db,  # type: ignore[arg-type]
        store=db,  # type: ignore[arg-type]
        hostname="rf-nano-aa",
        window_sec=lambda: window_sec,
        flush_sec=5.0,
    )

    await writer.flush_once()
    window_sec = 1.0
    db.windows.append(_window(2))
    await writer.flush_once()

    assert [r.metadata.length for r in client.inserted] == [0.5, 1.0]
