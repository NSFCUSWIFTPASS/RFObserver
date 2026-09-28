"""Thread-to-loop handoff must stay bounded while the event loop is blocked."""

import asyncio
import threading
import time

import pytest

from rfobserver.pipeline.streaming import _LoopHandoff


def test_handoff_rejects_unbounded_queue() -> None:
    with pytest.raises(ValueError, match="bounded queue"):
        _LoopHandoff(asyncio.Queue())


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


def test_handoff_concurrent_producers_and_consumer() -> None:
    """4 producer threads submit 1000 items each while a real loop (running in
    a background thread) drains the queue via a consumer coroutine.

    Regardless of timing, accepted + dropped must equal the total submitted,
    and once every scheduled callback has run, the pending counter must be
    back to 0 (no leaked in-flight accounting).
    """
    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()

    q: asyncio.Queue[tuple[int, int]] = asyncio.Queue(maxsize=16)
    h = _LoopHandoff(q)
    stop_consumer = threading.Event()

    async def consumer() -> None:
        while not stop_consumer.is_set() or not q.empty():
            try:
                await asyncio.wait_for(q.get(), timeout=0.05)
            except (TimeoutError, asyncio.TimeoutError):  # noqa: UP041
                continue

    consumer_future = asyncio.run_coroutine_threadsafe(consumer(), loop)

    n_producers = 4
    n_per_producer = 1000
    accepted_counts = [0] * n_producers

    def producer(idx: int) -> None:
        count = 0
        for i in range(n_per_producer):
            if h.submit(loop, (idx, i)):
                count += 1
        accepted_counts[idx] = count

    threads = [threading.Thread(target=producer, args=(i,)) for i in range(n_producers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = sum(accepted_counts)
    assert accepted + h.dropped == n_producers * n_per_producer

    # Let the consumer drain everything still pending, then stop it.
    time.sleep(0.3)
    stop_consumer.set()
    consumer_future.result(timeout=2.0)

    # Confirm every scheduled call_soon_threadsafe callback has run (pending
    # counter conserved back to 0) by round-tripping a marker through the loop.
    processed = threading.Event()
    loop.call_soon_threadsafe(processed.set)
    assert processed.wait(timeout=2.0)
    assert h._pending == 0

    loop.call_soon_threadsafe(loop.stop)
    loop_thread.join(timeout=2.0)
    loop.close()
