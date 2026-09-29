import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from rfobserver.pipeline.attribution import (
    AttributionItem,
    AttributionWorker,
    find_rtl433,
)
from rfobserver.storage.database import SensorDatabase

FIXTURE = Path.home() / "ssn_bursts" / "burst_feb4_919MHz_75dB.cs16"
RTL = find_rtl433()

pytestmark = pytest.mark.skipif(
    RTL is None or not FIXTURE.exists(),
    reason="rtl_433 or SSN fixtures not present on this host",
)


@pytest.mark.asyncio
async def test_worker_writes_attribution(tmp_path):
    db = SensorDatabase(str(tmp_path / "t.db"))
    await db.connect()
    try:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        await db.insert_detection(
            burst_id="w1",
            start_time=now,
            stop_time=now,
            center_freq_hz=919.4e6,
            bandwidth_hz=250e3,
            peak_power_db=-30.0,
            duration_ms=20.0,
            detection_timestamp=now,
        )
        # query_detections' "until" is a strict upper bound (start_time < until),
        # so it must be after `now`, not equal to it, or the exact-match row
        # at start_time == now is always excluded.
        until = now + timedelta(seconds=1)
        worker = AttributionWorker(db, RTL)
        worker.queue.put_nowait(
            AttributionItem(
                burst_id="w1",
                cs16=FIXTURE.read_bytes(),
                target_rate_hz=1_600_000,
                passes=[["-R", "383"]],
                power_db=-30.0,
            )
        )
        task = asyncio.create_task(worker.run())
        for _ in range(200):
            rows = await db.query_detections(since=now, until=until)
            if rows and rows[0]["model"]:
                break
            await asyncio.sleep(0.05)
        worker.stop()
        task.cancel()
        with pytest.raises((asyncio.CancelledError, Exception)):
            await task
        rows = await db.query_detections(since=now, until=until)
        row = next(r for r in rows if r["burst_id"] == "w1")
        assert row["model"] == "SilverSpring-Mesh"
        assert row["protocol_id"] == 383
        assert json.loads(row["attribution"])["decoded"] is True
    finally:
        await db.close()
