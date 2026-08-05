import pytest

from rfobserver.web.websocket import LiveBroadcast


@pytest.mark.asyncio
async def test_wants_psd_gates_only_psd_messages():
    b = LiveBroadcast()
    sub = b.subscribe()
    sub.wants_psd = False
    await b.publish({"type": "heartbeat"})
    await b.publish({"type": "psd", "powers": [1.0]})
    got = []
    while not sub.queue.empty():
        got.append(sub.queue.get_nowait())
    assert [m["type"] for m in got] == ["heartbeat"]  # psd dropped


@pytest.mark.asyncio
async def test_wants_psd_true_receives_psd():
    b = LiveBroadcast()
    sub = b.subscribe()  # default wants_psd True
    await b.publish({"type": "psd", "powers": [1.0]})
    assert sub.queue.get_nowait()["type"] == "psd"


def test_has_high_res_counts_only_viewing():
    b = LiveBroadcast()
    s = b.subscribe()
    s.high_res = True
    s.wants_psd = False
    assert b.has_high_res_subscribers() is False
    s.wants_psd = True
    assert b.has_high_res_subscribers() is True
