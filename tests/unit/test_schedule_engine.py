"""Scheduled captures: the book (persisted schedules) and the engine the
receiver thread feeds.

The stream: rate 1000 Hz, chunks of 100 samples, stream sample i holds i,
chunk c's first sample at T0 + c * 0.1 s.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import numpy as np
import pytest

from rfobserver.capture.buffer import CircularBuffer
from rfobserver.schedules.book import ScheduleBook, ScheduleConflict
from rfobserver.schedules.engine import ScheduleEngine
from rfobserver.schedules.model import parse_schedule
from rfobserver.storage.database import SensorDatabase

if TYPE_CHECKING:
    from pathlib import Path

RATE = 1000.0
N = 100
T0 = 1_793_212_200.0
T0_MS = int(T0 * 1000)
FREQ = 915e6


def sched(sid: str, start_ms: int, duration_ms: int, **extra: Any):
    body = {"id": sid, "start_ms": start_ms, "duration_ms": duration_ms, **extra}
    return parse_schedule(body, now_ms=T0_MS, grace_ms=1000, source="api", new_id=lambda: sid)


class FakeWriter:
    def __init__(self, refuse: bool = False) -> None:
        self.opened: dict[tuple[str, int], Path] = {}
        self.ops: dict[tuple[str, int], list[Any]] = {}
        self.closed: dict[tuple[str, int], dict[str, Any]] = {}
        self.grids: dict[tuple[str, int], list[Any]] = {}
        self.refuse = refuse

    def open(self, key, path, spec) -> None:
        self.opened[key] = path
        self.ops[key] = []

    def put_ops(self, key, ops) -> int:
        if self.refuse:
            n = sum(len(v) for k, v in ops if k == "iq")
            self.ops[key] += [("zeros", len(v)) if k == "iq" else (k, v) for k, v in ops]
            return n
        self.ops[key] += ops
        return 0

    def put_grid(self, key, rows, **kw) -> None:
        self.grids.setdefault(key, []).append((rows, kw))

    def close(self, key, info) -> None:
        self.closed[key] = info

    def samples(self, key) -> np.ndarray:
        parts = [v if k == "iq" else np.zeros(v, np.int32) for k, v in self.ops[key]]
        return np.concatenate(parts)


@pytest.fixture
async def book(tmp_path):
    db = SensorDatabase(str(tmp_path / "e.db"))
    await db.connect()
    b = ScheduleBook(db, now=lambda: T0)
    b.bind_loop(asyncio.get_running_loop())
    await b.load()
    yield b
    await asyncio.sleep(0.05)  # let queued run upserts land
    await db.close()


class Rig:
    def __init__(self, book: ScheduleBook, tmp_path: Path, **kw: Any) -> None:
        self.ring = CircularBuffer(1000, dtype=np.int32)
        self.writer = FakeWriter(refuse=kw.pop("refuse", False))
        self.refusal: str | None = None
        self.engine = ScheduleEngine(
            book,
            self.writer,
            directory=lambda: tmp_path,
            refusal=lambda: self.refusal,
            capture_meta=lambda: {"gain_db": 40.0, "hostname": "h", "serial": "S"},
            grace_ms=1000,
            now=lambda: T0,
        )
        self.c = 0

    def chunks(self, count: int, freq: float = FREQ) -> None:
        for _ in range(count):
            buf = np.arange(self.c * N, (self.c + 1) * N, dtype=np.int32)
            start = self.ring.total_written
            self.ring.write(buf)
            self.engine.on_chunk(buf, start, T0 + self.c * N / RATE, [], freq, RATE, self.ring)
            self.c += 1


async def test_one_off_is_cut_and_reported(book: ScheduleBook, tmp_path: Path) -> None:
    await book.add(sched("a", T0_MS + 250, 120))
    rig = Rig(book, tmp_path)
    rig.chunks(5)
    key = ("a", 0)
    assert np.array_equal(rig.writer.samples(key), np.arange(250, 370))
    assert key in rig.writer.closed
    path = rig.writer.opened[key]
    assert path.parent == tmp_path and path.name.endswith("-sched-a-0.sc16")
    info = rig.writer.closed[key]
    assert info["status"] == "recorded" and info["gain_db"] == 40.0
    assert "clock_source" in info
    assert book.latest("a", 0)["status"] == "recording"  # final status comes from the writer


async def test_repeating_runs_land_at_their_instants(book: ScheduleBook, tmp_path: Path) -> None:
    await book.add(sched("r", T0_MS + 100, 50, every_ms=300, count=3))
    rig = Rig(book, tmp_path)
    rig.chunks(12)
    for k, first in enumerate((100, 400, 700)):
        assert np.array_equal(rig.writer.samples(("r", k)), np.arange(first, first + 50))
    assert ("r", 3) not in rig.writer.opened


async def test_more_than_two_at_once_is_busy(book: ScheduleBook, tmp_path: Path) -> None:
    for sid in ("x", "y", "z"):
        await book.add(sched(sid, T0_MS + 100, 300))
    rig = Rig(book, tmp_path)
    rig.chunks(6)
    assert len(rig.writer.opened) == 2
    missed = [s for s in ("x", "y", "z") if book.latest(s, 0)["status"] == "missed"]
    assert len(missed) == 1 and book.latest(missed[0], 0)["reason"] == "busy"


async def test_storage_refusal_is_missed(book: ScheduleBook, tmp_path: Path) -> None:
    await book.add(sched("a", T0_MS + 100, 50))
    rig = Rig(book, tmp_path)
    rig.refusal = "Storage is full"
    rig.chunks(3)
    assert rig.writer.opened == {}
    assert book.latest("a", 0) == {
        "status": "missed",
        "reason": "storage",
        "start_ms": T0_MS + 100,
        "filename": None,
        "info": {"detail": "Storage is full"},
    }


async def test_added_mid_stream_is_picked_up(book: ScheduleBook, tmp_path: Path) -> None:
    rig = Rig(book, tmp_path)
    rig.chunks(2)
    await book.add(sched("late", T0_MS + 400, 50))
    rig.chunks(6)
    assert np.array_equal(rig.writer.samples(("late", 0)), np.arange(400, 450))


async def test_delete_mid_run_finishes_the_run_only(book: ScheduleBook, tmp_path: Path) -> None:
    await book.add(sched("d", T0_MS + 100, 150, every_ms=300, count=5))
    rig = Rig(book, tmp_path)
    rig.chunks(2)  # run 0 started
    assert await book.delete("d") is True
    rig.chunks(10)
    assert len(rig.writer.samples(("d", 0))) == 150
    assert ("d", 1) not in rig.writer.opened


async def test_stop_mid_run_is_partial(book: ScheduleBook, tmp_path: Path) -> None:
    await book.add(sched("s", T0_MS + 100, 500))
    rig = Rig(book, tmp_path)
    rig.chunks(3)
    rig.engine.stop("stopped")
    info = rig.writer.closed[("s", 0)]
    assert info["status"] == "partial" and info["reason"] == "stopped"


async def test_retune_mid_run_is_partial(book: ScheduleBook, tmp_path: Path) -> None:
    await book.add(sched("t", T0_MS + 100, 500))
    rig = Rig(book, tmp_path)
    rig.chunks(3)
    rig.chunks(1, freq=FREQ + 1e6)
    assert rig.writer.closed[("t", 0)]["reason"] == "retuned"


async def test_writer_refusal_is_partial_and_never_waits(
    book: ScheduleBook, tmp_path: Path
) -> None:
    await book.add(sched("w", T0_MS + 100, 50))
    rig = Rig(book, tmp_path, refuse=True)
    rig.chunks(3)
    info = rig.writer.closed[("w", 0)]
    assert info["status"] == "partial" and info["writer_refused_samples"] == 50


async def test_window_wholly_before_the_ring_is_missed(book: ScheduleBook, tmp_path: Path) -> None:
    # Accepted (within grace) but the stream resumes after its whole window.
    await book.add(sched("old", T0_MS - 900, 100))
    rig = Rig(book, tmp_path)
    rig.chunks(1)
    assert rig.writer.opened == {}
    assert book.latest("old", 0)["status"] == "missed"
    assert book.latest("old", 0)["reason"] == "not_running"


async def test_mark_missed_after_downtime_is_bounded(book: ScheduleBook, tmp_path: Path) -> None:
    s = sched("m", T0_MS - 1000, 1, every_ms=10, count=10**6)
    await book.add(s)
    rig = Rig(book, tmp_path)
    rig.engine.mark_missed(T0_MS + 10_000_000)
    lat = book.latest_runs("m")
    assert len(lat) == 500 and all(v["status"] == "missed" for v in lat.values())
    assert max(lat) == (10_000_000 + 1000 - 1000) // 10 - 1


async def test_grid_rows_follow_the_run(book: ScheduleBook, tmp_path: Path) -> None:
    await book.add(sched("g", T0_MS + 100, 200))
    rig = Rig(book, tmp_path)
    rig.chunks(2)

    class Grid:
        grid = np.ones((10, 4), dtype=np.float32)  # 10 rows of 10 samples per chunk
        freq_axis = np.linspace(-1, 1, 4)
        time_axis = np.arange(10) * 0.01

    rig.engine.on_grid(Grid(), 100, 10, None)
    rig.engine.on_grid(Grid(), 200, 10, None)
    ((rows, kw), (rows2, _)) = rig.writer.grids[("g", 0)]
    assert rows.shape[0] == 10 and kw["offset_samples"] == 0 and kw["slice_samples"] == 10
    assert rows2.shape[0] == 10


async def test_book_add_is_idempotent_and_conflicts(book: ScheduleBook) -> None:
    s = sched("i", T0_MS + 100, 50)
    assert (await book.add(s))[1] is True
    again, created = await book.add(sched("i", T0_MS + 100, 50))
    assert created is False and again == s
    with pytest.raises(ScheduleConflict):
        await book.add(sched("i", T0_MS + 100, 60))


async def test_book_reloads_from_the_database(book: ScheduleBook, tmp_path: Path) -> None:
    await book.add(sched("p", T0_MS + 100, 50, every_ms=1000, count=3))
    book.report_run("p", 0, "recorded", start_ms=T0_MS + 100, filename="f.sc16")
    await asyncio.sleep(0.05)
    again = ScheduleBook(book.db, now=lambda: T0)
    await again.load()
    assert [s.id for s in again.snapshot()[1]] == ["p"]
    assert again.last_run("p") == 0
    runs = await again.runs("p", 10)
    assert runs[0]["status"] == "recorded" and runs[0]["filename"] == "f.sc16"
