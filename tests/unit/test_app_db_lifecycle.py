"""run() lifecycle: DB connections, and an ordered stop on SIGINT/SIGTERM."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from typing import Any

import pytest

from rfobserver.config import AppSettings
from rfobserver.pipeline import app as app_mod


class _Registry:
    def __init__(self) -> None:
        self.instances: list[Any] = []
        self.fail_reader_connect = False
        self.fail_reader_close = False
        self.web_args: tuple[Any, ...] | None = None
        self.web_ignores_stop = False
        self.web_stopped_by_event = False
        self.set_active_calls: list[bool] = []
        self.fail_set_active_false = False
        self.cancel_set_active_false = False


@pytest.fixture
def reg(monkeypatch: pytest.MonkeyPatch) -> _Registry:
    registry = _Registry()

    class FakeDB:
        def __init__(self, db_path: str, *, read_only: bool = False) -> None:
            self.read_only = read_only
            self.connected = False
            self.closed = False
            registry.instances.append(self)

        async def connect(self) -> None:
            if self.read_only and registry.fail_reader_connect:
                raise RuntimeError("reader connect failed")
            self.connected = True

        async def close(self) -> None:
            self.closed = True
            if self.read_only and registry.fail_reader_close:
                raise RuntimeError("reader close failed")

    async def fake_web_server(*args: Any, **kwargs: Any) -> None:
        registry.web_args = args
        stop = args[-1]
        assert isinstance(stop, asyncio.Event)
        if registry.web_ignores_stop:
            await asyncio.Event().wait()
        await stop.wait()
        registry.web_stopped_by_event = True

    async def fake_heartbeat(*args: Any, **kwargs: Any) -> None:
        await asyncio.Event().wait()

    async def fake_set_active(self: Any, active: bool) -> None:
        registry.set_active_calls.append(active)
        if not active and registry.cancel_set_active_false:
            raise asyncio.CancelledError()
        if not active and registry.fail_set_active_false:
            raise RuntimeError("pipeline stop failed")

    monkeypatch.setattr("rfobserver.storage.database.SensorDatabase", FakeDB)
    monkeypatch.setattr(app_mod, "_run_web_server", fake_web_server)
    monkeypatch.setattr(app_mod, "_heartbeat_loop", fake_heartbeat)
    monkeypatch.setattr(
        "rfobserver.pipeline.supervisor.PipelineSupervisor.set_active", fake_set_active
    )
    return registry


def _settings(tmp_path: Any, web_port: int) -> AppSettings:
    s = AppSettings(_env_file=None)
    s.WEB_PORT = web_port
    s.DB_PATH = str(tmp_path / "db.sqlite")
    s.STORAGE_PATH = str(tmp_path / "storage")
    s.DB_RETENTION_DAYS = 0
    s.SENSOR_ACTIVE = False
    s.WATCHDOG_ENABLED = False
    s.NATS_ENABLED = False
    s.ZMS_ENABLED = False
    return s


async def _start_then_cancel(settings: AppSettings) -> BaseException | None:
    task = asyncio.create_task(app_mod.run(settings))
    for _ in range(50):
        await asyncio.sleep(0)
    assert not task.done(), "run() should still be serving"
    task.cancel()
    try:
        await task
    except BaseException as exc:  # CancelledError, or what the finally raised
        return exc
    return None


async def test_headless_run_opens_no_reader(reg: _Registry, tmp_path: Any) -> None:
    """With the web server off nothing uses the reader (the heartbeat is off too)."""
    exc = await _start_then_cancel(_settings(tmp_path, web_port=0))
    assert isinstance(exc, asyncio.CancelledError)
    assert [db.read_only for db in reg.instances] == [False], "only the writer is opened"
    assert reg.instances[0].closed
    assert reg.web_args is None


async def test_web_run_serves_reader_and_closes_both(reg: _Registry, tmp_path: Any) -> None:
    exc = await _start_then_cancel(_settings(tmp_path, web_port=8888))
    assert isinstance(exc, asyncio.CancelledError)
    writer, reader = reg.instances
    assert not writer.read_only and reader.read_only
    assert reg.web_args is not None
    assert reg.web_args[2] is reader and reg.web_args[3] is writer
    assert writer.closed and reader.closed
    _assert_handlers_restored()


async def test_reader_connect_failure_closes_the_writer(reg: _Registry, tmp_path: Any) -> None:
    reg.fail_reader_connect = True
    with pytest.raises(RuntimeError, match="reader connect failed"):
        await app_mod.run(_settings(tmp_path, web_port=8888))
    writer = reg.instances[0]
    assert writer.connected and writer.closed, "writer must not leak when the reader fails"


async def test_reader_close_failure_still_closes_the_writer(reg: _Registry, tmp_path: Any) -> None:
    reg.fail_reader_close = True
    exc = await _start_then_cancel(_settings(tmp_path, web_port=8888))
    assert isinstance(exc, RuntimeError) and "reader close failed" in str(exc)
    writer, reader = reg.instances
    assert reader.closed
    assert writer.closed, "a failed reader close must not skip the writer close"


def _assert_handlers_installed() -> None:
    # Guard: a real SIGINT/SIGTERM with the default handler would end pytest.
    assert signal.getsignal(signal.SIGINT) is not signal.default_int_handler
    assert signal.getsignal(signal.SIGTERM) not in (signal.SIG_DFL, signal.SIG_IGN, None)


def _assert_handlers_restored() -> None:
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


async def _start(settings: AppSettings) -> asyncio.Task[None]:
    task = asyncio.create_task(app_mod.run(settings))
    for _ in range(50):
        await asyncio.sleep(0)
    assert not task.done(), "run() should still be serving"
    _assert_handlers_installed()
    return task


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
@pytest.mark.parametrize("web_port", [0, 8888])
async def test_signal_stops_run_in_order(
    reg: _Registry,
    tmp_path: Any,
    sig: signal.Signals,
    web_port: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="rfobserver.pipeline.app")
    task = await _start(_settings(tmp_path, web_port=web_port))
    os.kill(os.getpid(), sig)
    await asyncio.wait_for(task, timeout=5)  # returns normally: exit code 0
    assert reg.set_active_calls == [False], "the pipeline is stopped once"
    assert all(db.closed for db in reg.instances)
    if web_port:
        assert reg.web_stopped_by_event, "the web server exits on the stop event"
    assert "Shutdown complete" in caplog.text
    _assert_handlers_restored()


async def test_web_server_that_ignores_stop_is_cancelled(
    reg: _Registry, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    reg.web_ignores_stop = True
    monkeypatch.setattr(app_mod, "_WEB_SHUTDOWN_TIMEOUT_SEC", 0.1)
    task = await _start(_settings(tmp_path, web_port=8888))
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(task, timeout=5)
    assert reg.set_active_calls == [False]
    assert all(db.closed for db in reg.instances)
    _assert_handlers_restored()


async def test_pipeline_stop_failure_still_closes_the_dbs(reg: _Registry, tmp_path: Any) -> None:
    reg.fail_set_active_false = True
    task = await _start(_settings(tmp_path, web_port=8888))
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(task, timeout=5)
    assert all(db.closed for db in reg.instances)
    _assert_handlers_restored()


async def test_cancelled_pipeline_stop_still_closes_the_dbs(reg: _Registry, tmp_path: Any) -> None:
    reg.cancel_set_active_false = True
    task = await _start(_settings(tmp_path, web_port=8888))
    os.kill(os.getpid(), signal.SIGTERM)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5)
    assert all(db.closed for db in reg.instances)
    _assert_handlers_restored()


async def test_worker_failure_propagates_after_cleanup(
    reg: _Registry, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def dying_heartbeat(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("heartbeat died")

    monkeypatch.setattr(app_mod, "_heartbeat_loop", dying_heartbeat)
    with pytest.raises(RuntimeError, match="heartbeat died"):
        await asyncio.wait_for(app_mod.run(_settings(tmp_path, web_port=8888)), timeout=5)
    assert reg.set_active_calls == [False]
    assert all(db.closed for db in reg.instances)
    _assert_handlers_restored()
