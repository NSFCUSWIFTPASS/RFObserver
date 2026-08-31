"""Tests for the pipeline app's scheduled retention loop (_cleanup_loop)."""

import asyncio
import contextlib

from rfobserver.config import AppSettings
from rfobserver.pipeline.app import _cleanup_loop


class _FakeDB:
    def __init__(self) -> None:
        self.calls: list[int] = []

    async def prune_avg_psd_blobs(self, days: int) -> int:
        self.calls.append(days)
        return 0


async def test_cleanup_loop_invokes_prune_with_retention_days():
    settings = AppSettings(_env_file=None)
    settings.DB_RETENTION_DAYS = 5
    settings.DB_CLEANUP_INTERVAL_SEC = 0.001
    db = _FakeDB()

    task = asyncio.create_task(_cleanup_loop(settings, db))
    # Let the immediate run (and at least one loop iteration) execute.
    for _ in range(5):
        await asyncio.sleep(0)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert len(db.calls) >= 1
    assert db.calls[0] == 5
