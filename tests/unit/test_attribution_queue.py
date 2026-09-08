import asyncio

import pytest

from rfobserver.pipeline.attribution import AttributionItem, StrongestQueue


def _item(power: float, tag: str) -> AttributionItem:
    return AttributionItem(
        burst_id=tag, cs16=b"", target_rate_hz=1_600_000, passes=[[]], power_db=power
    )


def test_queue_keeps_strongest_on_overflow():
    q = StrongestQueue(maxsize=2)
    assert q.put_nowait(_item(-50.0, "a")) is True
    assert q.put_nowait(_item(-40.0, "b")) is True
    # Full. A stronger item evicts the weakest ("a", -50).
    assert q.put_nowait(_item(-30.0, "c")) is True
    assert q.qsize() == 2
    assert q.dropped == 1
    # A weaker-than-all item is itself dropped, queue unchanged.
    assert q.put_nowait(_item(-99.0, "d")) is False
    assert q.qsize() == 2
    assert q.dropped == 2


@pytest.mark.asyncio
async def test_get_returns_strongest_first():
    q = StrongestQueue(maxsize=4)
    for p, tag in [(-50.0, "a"), (-20.0, "b"), (-35.0, "c")]:
        q.put_nowait(_item(p, tag))
    first = await asyncio.wait_for(q.get(), timeout=1.0)
    assert first.burst_id == "b"  # -20 is strongest
