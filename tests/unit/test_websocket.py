import asyncio

import pytest
from fastapi import WebSocketDisconnect

from rfobserver.web.websocket import LiveBroadcast, websocket_endpoint


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


@pytest.mark.asyncio
async def test_disconnect_cancels_send_loop_no_leak():
    b = LiveBroadcast()
    sent = []

    class FakeWS:
        def __init__(self):
            self._recv = 0

        async def accept(self):
            pass

        async def send_json(self, data):
            sent.append(data)

        async def receive_text(self):
            # one control message, then disconnect
            self._recv += 1
            if self._recv == 1:
                return '{"type": "set_view", "psd_visible": false}'
            raise WebSocketDisconnect(1000)

    before = len(asyncio.all_tasks())
    await websocket_endpoint(FakeWS(), b)
    await asyncio.sleep(0)  # let cancellations settle
    assert b._subscribers == set()  # unsubscribed
    # no lingering handler task (send_loop not orphaned)
    assert len([t for t in asyncio.all_tasks() if not t.done()]) <= before
