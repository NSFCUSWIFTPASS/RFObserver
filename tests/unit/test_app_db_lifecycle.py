"""run() opens the web reader only with the web server and always closes the writer."""

from __future__ import annotations

import asyncio
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

    async def fake_web_server(*args: Any) -> None:
        registry.web_args = args
        await asyncio.Event().wait()

    async def fake_heartbeat(*args: Any) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr("rfobserver.storage.database.SensorDatabase", FakeDB)
    monkeypatch.setattr(app_mod, "_run_web_server", fake_web_server)
    monkeypatch.setattr(app_mod, "_heartbeat_loop", fake_heartbeat)
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
