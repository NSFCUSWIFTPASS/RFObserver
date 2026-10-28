"""Scheduled captures end to end: a real StreamingProcessor on the mock
receiver records a scheduled window to the sample, in parallel with a manual
recording, and counts it in the collection statistics."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import numpy as np

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import StreamingProcessor
from rfobserver.schedules.book import ScheduleBook
from rfobserver.schedules.model import parse_schedule
from rfobserver.storage.collection import CollectionStats
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.local import LocalStorage

RATE = 2_000_000


def _settings(tmp_path: Path) -> AppSettings:
    storage = tmp_path / "st"
    storage.mkdir()
    return AppSettings(
        FREQUENCY_START=915_000_000,
        FREQUENCY_END=915_000_000,
        BANDWIDTH=RATE,
        DURATION_SEC=0.5,
        GAIN=30,
        NUM_FFT_BINS=256,
        PSD_TIME_RESOLUTION_MS=0.5,
        STREAMING_CHUNK_SLICES=10,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage),
        DB_PATH=str(tmp_path / "t.db"),
        ARCHIVE_MAX_GB=1,
        _env_file=None,
    )


async def test_scheduled_window_records_beside_a_manual_capture(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        book = ScheduleBook(db)
        book.bind_loop(asyncio.get_running_loop())
        await book.load()
        receiver = MockReceiver(
            ReceiverConfig(
                gain_db=settings.GAIN,
                bandwidth_hz=settings.BANDWIDTH,
                duration_sec=settings.DURATION_SEC,
            )
        )
        receiver.initialize()
        storage = LocalStorage(storage_path=settings.STORAGE_PATH, max_gb=settings.ARCHIVE_MAX_GB)
        collection = CollectionStats()
        proc = StreamingProcessor(
            receiver=receiver,
            database=db,
            local_storage=storage,
            settings=settings,
            collection=collection,
            schedules=book,
        )

        async def driver() -> None:
            for _ in range(500):  # streaming started
                if proc._capture_count > 2:
                    break
                await asyncio.sleep(0.02)
            proc.start_recording()  # a manual capture across the window
            now_ms = int(time.time() * 1000)
            s = parse_schedule(
                {"id": "it", "start_ms": now_ms + 1000, "duration_ms": 300, "label": "e2e"},
                now_ms=now_ms,
                grace_ms=1000,
                source="api",
                new_id=lambda: "it",
            )
            await book.add(s)
            for _ in range(500):
                st = (book.latest("it", 0) or {}).get("status")
                if st in ("recorded", "partial", "missed", "failed"):
                    break
                await asyncio.sleep(0.02)
            await asyncio.to_thread(proc.stop_recording)
            await asyncio.sleep(0.1)
            proc.stop()

        await asyncio.wait_for(asyncio.gather(proc.run(), driver()), timeout=60.0)

        run = book.latest("it", 0)
        assert run is not None and run["status"] == "recorded", run
        manual = Path(settings.STORAGE_PATH) / "manual"
        sched = manual / run["filename"]
        assert sched.exists() and "-sched-it-0" in sched.name
        data = np.fromfile(sched, dtype=np.int32)
        assert len(data) == round(0.3 * RATE)
        meta = json.loads(sched.with_suffix(".json").read_text())
        assert meta["origin"] == "scheduled" and meta["total_samples"] == len(data)
        block = meta["schedule"]
        assert block["id"] == "it" and block["label"] == "e2e" and block["status"] == "recorded"
        # Within half a sample of the requested instant on the host clock.
        assert abs(block["start_error_ns"]) <= 0.5e9 / RATE + 300

        # The manual capture is its own file, unaffected by the schedule.
        others = [p for p in manual.glob("*.sc16") if p != sched]
        assert len(others) == 1
        assert "schedule" not in json.loads(others[0].with_suffix(".json").read_text())

        counted = collection.take()
        assert sum(h.get("scheduled_captures", 0) for h in counted.values()) == 1
        assert sum(h.get("scheduled_bytes", 0) for h in counted.values()) == len(data) * 4
    finally:
        await asyncio.sleep(0.1)
        await db.close()
