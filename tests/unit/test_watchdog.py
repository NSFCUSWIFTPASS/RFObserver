import asyncio
import threading

from rfobserver.pipeline.beacon import ProgressBeacon
from rfobserver.utils.watchdog import PipelineWatchdog


def _loop_in_thread():
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=loop.run_forever, daemon=True)
    t.start()
    return loop


def test_no_action_when_fresh():
    b = ProgressBeacon()
    b.mark()
    exits = []
    restarts = []

    async def restart():
        restarts.append(1)

    loop = _loop_in_thread()
    wd = PipelineWatchdog(
        b,
        is_active=lambda: True,
        restart=restart,
        loop=loop,
        timeout_sec=1.0,
        restart_deadline_sec=1.0,
        exit_fn=lambda code: exits.append(code),
    )
    wd._tick()
    assert restarts == [] and exits == []
    loop.call_soon_threadsafe(loop.stop)


def test_no_action_when_inactive_even_if_stale():
    b = ProgressBeacon()
    import time as _t

    _t.sleep(0.05)
    exits, restarts = [], []

    async def restart():
        restarts.append(1)

    loop = _loop_in_thread()
    wd = PipelineWatchdog(
        b,
        is_active=lambda: False,
        restart=restart,
        loop=loop,
        timeout_sec=0.01,
        restart_deadline_sec=1.0,
        exit_fn=lambda code: exits.append(code),
    )
    wd._tick()
    assert restarts == [] and exits == []
    loop.call_soon_threadsafe(loop.stop)


def test_stale_active_triggers_restart_not_exit():
    b = ProgressBeacon()
    import time as _t

    _t.sleep(0.05)
    exits, restarts = [], []

    async def restart():
        restarts.append(1)

    loop = _loop_in_thread()
    wd = PipelineWatchdog(
        b,
        is_active=lambda: True,
        restart=restart,
        loop=loop,
        timeout_sec=0.01,
        restart_deadline_sec=1.0,
        exit_fn=lambda code: exits.append(code),
    )
    wd._tick()
    assert restarts == [1] and exits == []  # restart succeeded, no exit
    loop.call_soon_threadsafe(loop.stop)


def test_wedged_loop_escalates_to_exit():
    b = ProgressBeacon()
    import time as _t

    _t.sleep(0.05)
    exits = []

    async def restart():
        await asyncio.sleep(5.0)  # simulate a wedged / slow loop past the deadline

    loop = _loop_in_thread()
    wd = PipelineWatchdog(
        b,
        is_active=lambda: True,
        restart=restart,
        loop=loop,
        timeout_sec=0.01,
        restart_deadline_sec=0.2,
        exit_fn=lambda code: exits.append(code),
        exit_code=90,
    )
    wd._tick()
    assert exits == [90]
    loop.call_soon_threadsafe(loop.stop)
