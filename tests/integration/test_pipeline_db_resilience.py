"""A DB write that raises must not kill the pipeline's consumer loop."""

from __future__ import annotations

import pytest

from rfobserver.pipeline.streaming import StreamingProcessor


class _BoomDB:
    """Stands in for SensorDatabase: insert_detection always raises."""

    def __init__(self) -> None:
        self.calls = 0

    async def insert_detection(self, **kwargs) -> None:
        self.calls += 1
        raise RuntimeError("simulated disk-full")


@pytest.mark.asyncio
async def test_drain_burst_results_survives_db_error():
    # Build a processor shell without running the full pipeline: we only drive
    # _drain_burst_results directly with one burst result queued.
    from datetime import datetime, timezone

    from rfobserver.models import BurstFingerprint

    proc = StreamingProcessor.__new__(StreamingProcessor)
    proc._db = _BoomDB()
    proc._replay_mode = False
    proc._receiver = object()
    proc._settings = type("S", (), {"BANDWIDTH": 2_000_000, "GAIN": 30})()
    import asyncio as _a

    proc._burst_result_queue = _a.Queue()
    now = datetime.now(timezone.utc)
    burst = BurstFingerprint(
        start_time=now,
        stop_time=now,
        center_freq_hz=915e6,
        peak_freq_hz=915e6,
        bandwidth_hz=1e5,
        peak_power_db=-40.0,
        duration_ms=10.0,
    )
    await proc._burst_result_queue.put(([burst], 915e6))
    # Must NOT raise despite the DB blowing up.
    await proc._drain_burst_results()
    assert proc._db.calls == 1
