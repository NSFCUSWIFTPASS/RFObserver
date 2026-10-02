import asyncio

import pytest
from fastapi import WebSocketDisconnect

from rfobserver.web.websocket import (
    LiveBroadcast,
    _Subscriber,
    shape_for_client,
    websocket_endpoint,
)


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
        query_params: dict[str, str] = {}

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


def _psd(freqs, nf, powers=(-115.7665786743164, -98.04)):
    return {
        "type": "psd",
        "powers": list(powers),
        "max_powers": [p + 1.23456 for p in powers],
        "noise_floor_per_bin": list(nf),
        "frequencies": list(freqs),
    }


@pytest.mark.asyncio
async def test_publish_rounds_psd_arrays_to_tenth_db():
    b = LiveBroadcast()
    sub = b.subscribe()
    await b.publish(_psd([1.0, 2.0], [-120.04, -119.96]))
    msg = sub.queue.get_nowait()
    assert msg["powers"] == [-115.8, -98.0]
    assert msg["max_powers"] == [-114.5, -96.8]
    assert msg["noise_floor_per_bin"] == [-120.0, -120.0]
    assert msg["frequencies"] == [1.0, 2.0]  # Hz, never rounded


def test_shape_sends_frequencies_only_when_they_change():
    sub = _Subscriber()
    first = shape_for_client(sub, _psd([1.0, 2.0], [-120.0, -120.0]), now=0.0)
    second = shape_for_client(sub, _psd([1.0, 2.0], [-120.0, -120.0]), now=0.1)
    retuned = shape_for_client(sub, _psd([5.0, 6.0], [-120.0, -120.0]), now=0.2)
    assert first["frequencies"] == [1.0, 2.0]
    assert "frequencies" not in second
    assert retuned["frequencies"] == [5.0, 6.0]
    assert second["powers"] and second["max_powers"]  # per-frame data stays


def test_shape_sends_noise_floor_at_most_once_per_second():
    sub = _Subscriber()
    sent = [
        "noise_floor_per_bin" in shape_for_client(sub, _psd([1.0, 2.0], [-120.0, -120.0]), now=t)
        for t in (0.0, 0.5, 0.99, 1.0, 1.5)
    ]
    assert sent == [True, False, False, True, False]
    # A bin-count change resends it at once, so the client never pairs a
    # noise floor with the wrong axis.
    wider = shape_for_client(sub, _psd([1.0, 2.0, 3.0], [-1.0, -1.0, -1.0]), now=1.6)
    assert wider["noise_floor_per_bin"] == [-1.0, -1.0, -1.0]


def test_shape_leaves_other_messages_alone():
    sub = _Subscriber()
    hb = {"type": "heartbeat", "frequencies": [1.0]}
    assert shape_for_client(sub, hb, now=0.0) is hb


@pytest.mark.asyncio
async def test_psd_query_param_opts_out_of_psd_frames():
    b = LiveBroadcast()
    sent = []

    class FakeWS:
        query_params = {"psd": "0"}

        async def accept(self):
            pass

        async def send_json(self, data):
            sent.append(data)

        async def receive_text(self):
            await asyncio.sleep(0.05)
            raise WebSocketDisconnect(1000)

    task = asyncio.create_task(websocket_endpoint(FakeWS(), b))
    await asyncio.sleep(0.01)
    await b.publish(_psd([1.0], [-1.0], powers=(-1.0,)))
    await b.publish({"type": "heartbeat"})
    await task
    assert [m["type"] for m in sent] == ["heartbeat"]
