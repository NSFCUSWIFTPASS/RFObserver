"""Shutdown signal ownership: run() handles SIGINT/SIGTERM, not uvicorn.

See docs/debugging/2026-09-14_shutdown-signals.md for why: uvicorn re-raises
SIGTERM with SIG_DFL after serving (cleanup skipped), and on Python 3.10 a
SIGINT KeyboardInterrupt makes asyncio.run cancel every task mid-cleanup.
"""

from __future__ import annotations

import asyncio
import os
import signal
import threading

import uvicorn

from rfobserver.pipeline import app as app_mod


def _handlers() -> tuple[object, object]:
    return signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)


def _assert_installed() -> None:
    # Guard: never send a real signal while the default handler is in place.
    sigint, sigterm = _handlers()
    assert sigint is not signal.default_int_handler, "SIGINT handler not installed"
    assert sigterm not in (signal.SIG_DFL, signal.SIG_IGN, None), "SIGTERM handler not installed"


async def _wait_for(predicate, timeout: float = 2.0) -> None:  # type: ignore[no-untyped-def]
    for _ in range(int(timeout / 0.01)):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met in time")


async def test_first_signal_sets_stop_second_forces_exit() -> None:
    stop = asyncio.Event()
    exits: list[int] = []
    remove = app_mod.install_stop_signals(asyncio.get_running_loop(), stop, force_exit=exits.append)
    try:
        _assert_installed()
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(stop.wait(), timeout=2)
        assert exits == [], "the first signal only requests a stop"
        os.kill(os.getpid(), signal.SIGINT)
        await _wait_for(lambda: bool(exits))
        assert exits == [128 + signal.SIGINT]
    finally:
        remove()
    assert _handlers() == (signal.default_int_handler, signal.SIG_DFL)


async def test_sigint_sets_stop() -> None:
    stop = asyncio.Event()
    remove = app_mod.install_stop_signals(
        asyncio.get_running_loop(), stop, force_exit=lambda c: None
    )
    try:
        _assert_installed()
        os.kill(os.getpid(), signal.SIGINT)
        await asyncio.wait_for(stop.wait(), timeout=2)
    finally:
        remove()


def test_outside_main_thread_is_a_noop() -> None:
    before = _handlers()
    result: list[object] = []

    def worker() -> None:
        loop = asyncio.new_event_loop()
        try:
            remove = app_mod.install_stop_signals(loop, asyncio.Event())
            remove()
            result.append("ok")
        except BaseException as exc:  # pragma: no cover - reported below
            result.append(exc)
        finally:
            loop.close()

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=5)
    assert result == ["ok"]
    assert _handlers() == before


def test_web_server_leaves_signal_handlers_alone() -> None:
    server = app_mod._build_web_server(uvicorn.Config(app=lambda *a: None))
    assert isinstance(server, uvicorn.Server)
    before = _handlers()
    with server.capture_signals():
        assert _handlers() == before, "uvicorn must not install its own handlers"
    assert _handlers() == before


async def test_run_web_server_exits_on_stop_without_touching_signals() -> None:
    from types import SimpleNamespace

    from rfobserver.config import AppSettings
    from rfobserver.pipeline.beacon import ProgressBeacon
    from rfobserver.web.websocket import LiveBroadcast

    settings = AppSettings(_env_file=None)
    settings.WEB_HOST = "127.0.0.1"
    settings.WEB_PORT = 0  # ephemeral port
    supervisor = SimpleNamespace(processor=None, _on_processor_change=None)
    stop = asyncio.Event()
    before = _handlers()
    task = asyncio.create_task(
        app_mod._run_web_server(
            settings, supervisor, None, None, LiveBroadcast(), ProgressBeacon(), stop
        )
    )
    await asyncio.sleep(0.3)  # let uvicorn start serving
    assert not task.done(), "the server should still be serving"
    assert _handlers() == before, "uvicorn must not install its own handlers"
    stop.set()
    await asyncio.wait_for(task, timeout=5)
    assert _handlers() == before
