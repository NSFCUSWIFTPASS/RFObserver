"""Thread-to-loop handoff must stay bounded while the event loop is blocked."""

import asyncio

from rfobserver.pipeline.streaming import _LoopHandoff


def test_handoff_bounded_while_loop_not_running() -> None:
    loop = asyncio.new_event_loop()
    try:
        q: asyncio.Queue[int] = asyncio.Queue(maxsize=4)
        h = _LoopHandoff(q)
        # Loop is not running: nothing drains, like a wedged loop.
        accepted = sum(h.submit(loop, i) for i in range(100))
        assert accepted == 4, "only maxsize callbacks may be in flight"
        assert h.dropped == 96

        loop.run_until_complete(asyncio.sleep(0))  # deliver pending callbacks
        assert q.qsize() == 4

        while not q.empty():
            q.get_nowait()
        assert h.submit(loop, 999), "delivered callbacks must free their slots"
    finally:
        loop.close()


def test_handoff_after_loop_closed_is_dropped_not_raised() -> None:
    loop = asyncio.new_event_loop()
    loop.close()
    h = _LoopHandoff(asyncio.Queue(maxsize=2))
    assert h.submit(loop, 1) is False
