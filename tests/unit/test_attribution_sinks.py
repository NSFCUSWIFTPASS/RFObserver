"""Attribution results go to sinks: the DB (with one retry) live, a file in replay."""

from __future__ import annotations

import asyncio
import json

import pytest

from rfobserver.pipeline import attribution as attr
from rfobserver.pipeline.attribution import (
    AttributionItem,
    AttributionResult,
    AttributionWorker,
    ReplayFileSink,
    StrongestQueue,
    db_sink,
)


def _item(bid="b1", power=-30.0):
    return AttributionItem(
        bid,
        b"\0\0" * 10,
        1_600_000,
        [["-R", "383"]],
        power,
        meta={"freq_hz": 919.4e6, "start_time_ms": 1.0, "stop_time_ms": 2.0},
    )


class _DB:
    def __init__(self, rows_first=0):
        self.calls = []
        self._first = rows_first

    async def update_detection_attribution(self, **kw):
        self.calls.append(kw)
        return self._first if len(self.calls) == 1 else 1


async def test_db_sink_retries_once_when_the_row_is_not_there_yet():
    db = _DB(rows_first=0)
    sink = db_sink(db, retry_delay_sec=0.01)
    await sink(AttributionResult(_item(), "SilverSpring-Mesh", 383, "{}", "decoded"))
    await asyncio.sleep(0.1)  # the retry runs as its own task
    assert len(db.calls) == 2 and db.calls[0]["burst_id"] == "b1"


async def test_db_sink_retry_does_not_block_the_next_result():
    db = _DB(rows_first=0)
    sink = db_sink(db, retry_delay_sec=0.3)
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    await sink(AttributionResult(_item(), "SilverSpring-Mesh", 383, "{}", "decoded"))
    assert loop.time() - t0 < 0.1  # returned without sitting out the retry delay
    assert len(db.calls) == 1
    await asyncio.sleep(0.5)
    assert len(db.calls) == 2


async def test_db_sink_close_lets_a_pending_retry_finish():
    db = _DB(rows_first=0)
    sink = db_sink(db, retry_delay_sec=0.2)
    await sink(AttributionResult(_item(), None, None, "{}", "not_decoded"))
    assert len(sink.pending) == 1 and len(db.calls) == 1
    await sink.aclose()
    assert not sink.pending and len(db.calls) == 2  # the retry landed


async def test_db_sink_close_cancels_a_retry_that_hangs():
    class _HangingDB(_DB):
        async def update_detection_attribution(self, **kw):
            if self.calls:
                await asyncio.Event().wait()  # the retry never returns
            return await super().update_detection_attribution(**kw)

    db = _HangingDB(rows_first=0)
    sink = db_sink(db, retry_delay_sec=0.05)
    await sink(AttributionResult(_item(), None, None, "{}", "not_decoded"))
    retry = next(iter(sink.pending))
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    await sink.aclose()
    assert loop.time() - t0 < 2.0  # bounded: one delay plus a second
    assert retry.cancelled() and not sink.pending


async def test_worker_stop_settles_the_db_sinks_pending_retries(monkeypatch):
    monkeypatch.setattr(attr, "decode_cs16", lambda *a, **k: [])
    db = _DB(rows_first=0)
    sink = db_sink(db, retry_delay_sec=0.2)
    q = StrongestQueue(4)
    w = AttributionWorker(db, "/bin/true", queue=q, sinks=[sink])
    q.put_nowait(_item())
    task = asyncio.create_task(w.run())
    for _ in range(100):
        await asyncio.sleep(0.01)
        if sink.pending:
            break
    retry = next(iter(sink.pending))
    w.stop()
    task.cancel()  # as _stop_isolation does
    with pytest.raises(asyncio.CancelledError):
        await task
    assert retry.done() and not sink.pending
    assert len(db.calls) == 2


async def test_db_sink_no_retry_when_updated():
    db = _DB(rows_first=1)
    await db_sink(db, retry_delay_sec=0.01)(
        AttributionResult(_item(), None, None, "{}", "not_decoded")
    )
    assert len(db.calls) == 1


async def test_replay_file_sink_appends_json_lines(tmp_path):
    p = tmp_path / "bursts" / "replay-x" / "attribution.jsonl"
    sink = ReplayFileSink(p)
    await sink(
        AttributionResult(_item("b1"), "SilverSpring-Mesh", 383, '{"decoded": true}', "decoded")
    )
    await sink(AttributionResult(_item("b2"), None, None, "{}", "not_decoded"))
    rows = [json.loads(line) for line in p.read_text().splitlines()]
    assert [r["burst_id"] for r in rows] == ["b1", "b2"]
    assert rows[0]["model"] == "SilverSpring-Mesh" and rows[0]["protocol_id"] == 383
    assert rows[0]["freq_hz"] == 919.4e6 and rows[1]["outcome"] == "not_decoded"


@pytest.mark.parametrize(
    "frames,outcome", [([{"model": "SilverSpring-Mesh"}], "decoded"), ([], "not_decoded")]
)
async def test_worker_sends_results_to_every_sink_and_counts_outcomes(monkeypatch, frames, outcome):
    monkeypatch.setattr(attr, "decode_cs16", lambda *a, **k: frames)
    got, outcomes = [], []

    async def sink(r):
        got.append(r)

    q = StrongestQueue(4)
    w = AttributionWorker(
        None, "/bin/true", queue=q, sinks=[sink, sink], on_outcome=outcomes.append
    )
    q.put_nowait(_item())
    task = asyncio.create_task(w.run())
    for _ in range(50):
        await asyncio.sleep(0.01)
        if len(got) == 2:
            break
    task.cancel()
    assert len(got) == 2 and got[0].outcome == outcome and outcomes == [outcome]
    if outcome == "decoded":
        assert got[0].protocol_id == 383


async def test_worker_counts_a_decoder_crash_as_failed(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("x")

    monkeypatch.setattr(attr, "decode_cs16", boom)
    outcomes = []
    q = StrongestQueue(4)
    w = AttributionWorker(None, "/bin/true", queue=q, sinks=[], on_outcome=outcomes.append)
    q.put_nowait(_item())
    task = asyncio.create_task(w.run())
    await asyncio.sleep(0.1)
    task.cancel()
    assert outcomes == ["failed"]


async def test_update_detection_attribution_returns_rowcount(tmp_path):
    from rfobserver.storage.database import SensorDatabase

    db = SensorDatabase(str(tmp_path / "a.db"))
    await db.connect()
    try:
        n = await db.update_detection_attribution(
            burst_id="nope", model=None, protocol_id=None, attribution="{}"
        )
        assert n == 0
    finally:
        await db.close()


async def test_a_decode_timeout_is_not_decoded_and_the_pass_timeout_is_5s(monkeypatch):
    import subprocess

    seen = []

    def slow(cmd, **kw):
        seen.append(kw["timeout"])
        raise subprocess.TimeoutExpired(cmd, kw["timeout"])

    monkeypatch.setattr(attr.subprocess, "run", slow)
    assert attr.decode_cs16("/bin/true", b"\0\0" * 8, 1_600_000, [["-R", "383"], []]) == []
    assert seen == [5.0, 5.0]  # every pass, 5 s each

    got, outcomes = [], []

    async def sink(r):
        got.append(r)

    q = StrongestQueue(4)
    w = AttributionWorker(None, "/bin/true", queue=q, sinks=[sink], on_outcome=outcomes.append)
    q.put_nowait(_item())
    task = asyncio.create_task(w.run())
    for _ in range(100):
        await asyncio.sleep(0.01)
        if got:
            break
    task.cancel()
    assert outcomes == ["not_decoded"] and got[0].outcome == "not_decoded"
