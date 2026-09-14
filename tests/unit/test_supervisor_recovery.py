"""Tests for PipelineSupervisor crash recovery -- restart() and the task done-callback."""

import asyncio
import logging
import time

import pytest

import rfobserver.pipeline.supervisor as supervisor_mod
from rfobserver.pipeline.supervisor import PipelineSupervisor


class _FakeReceiver:
    def initialize(self) -> None: ...
    def close(self) -> None: ...


class _CrashProcessor:
    """run() raises after a beat; stop() is a no-op."""

    def __init__(self) -> None:
        self.started = 0

    async def run(self) -> None:
        self.started += 1
        await asyncio.sleep(0.01)
        raise RuntimeError("boom")

    def stop(self) -> None: ...


class _LongRunningProcessor:
    """run() blocks until stop() is called; never crashes on its own."""

    def __init__(self) -> None:
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        await self._stop_event.wait()

    def stop(self) -> None:
        self._stop_event.set()


@pytest.mark.asyncio
async def test_task_death_triggers_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    # No backoff delay so the restart lands well inside the test's sleep window.
    monkeypatch.setattr(supervisor_mod, "_CRASH_BACKOFF_CAP_SEC", 0.0)

    procs: list[_CrashProcessor] = []

    def build_proc(receiver: object, *, replay_mode: bool = False) -> _CrashProcessor:
        p = _CrashProcessor()
        procs.append(p)
        return p

    sup = PipelineSupervisor(build_receiver=_FakeReceiver, build_processor=build_proc)
    await sup.set_active(True)
    # First processor crashes; the done-callback should build+start a second.
    await asyncio.sleep(0.2)
    assert len(procs) >= 2, "a crashed pipeline task must be restarted"
    await sup.set_active(False)


@pytest.mark.asyncio
async def test_restart_noop_when_inactive() -> None:
    sup = PipelineSupervisor(build_receiver=_FakeReceiver, build_processor=lambda r, **k: None)
    await sup.restart()  # not active -> no raise, no start
    assert not sup.active


@pytest.mark.asyncio
async def test_replay_crash_does_not_auto_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    """A processor crash while replaying must not auto-restart into live SDR mode."""
    monkeypatch.setattr(supervisor_mod, "_CRASH_BACKOFF_CAP_SEC", 0.0)

    procs: list[_CrashProcessor] = []

    def build_proc(receiver: object, *, replay_mode: bool = False) -> _CrashProcessor:
        p = _CrashProcessor()
        procs.append(p)
        return p

    sup = PipelineSupervisor(build_receiver=_FakeReceiver, build_processor=build_proc)
    await sup.start_replay(_FakeReceiver())
    # The replay processor crashes; the done-callback must see self._replay and no-op.
    await asyncio.sleep(0.2)
    assert len(procs) == 1, "a crash during replay must not trigger an auto-restart"
    await sup.stop_replay()
    assert not sup.active


@pytest.mark.asyncio
async def test_persistent_crash_stops_after_max_restarts(monkeypatch: pytest.MonkeyPatch) -> None:
    """A processor that always crashes must not restart forever.

    It gives up after _MAX_CONSECUTIVE_CRASH_RESTARTS and leaves the sensor
    inactive rather than thrashing receiver.initialize()/close() forever.
    """
    monkeypatch.setattr(supervisor_mod, "_CRASH_BACKOFF_CAP_SEC", 0.0)

    procs: list[_CrashProcessor] = []

    def build_proc(receiver: object, *, replay_mode: bool = False) -> _CrashProcessor:
        p = _CrashProcessor()
        procs.append(p)
        return p

    sup = PipelineSupervisor(build_receiver=_FakeReceiver, build_processor=build_proc)
    await sup.set_active(True)
    # Let every crash + restart, and the eventual give-up, play out.
    await asyncio.sleep(1.0)

    assert len(procs) == supervisor_mod._MAX_CONSECUTIVE_CRASH_RESTARTS + 1, (
        "auto-restart must stop after the cap, not loop forever"
    )
    assert not sup.active, "the sensor must end inactive once auto-restart gives up"


@pytest.mark.asyncio
async def test_crash_outside_reset_window_resets_counter(monkeypatch: pytest.MonkeyPatch) -> None:
    """A crash long after the reset window must not inherit a prior crash streak."""
    monkeypatch.setattr(supervisor_mod, "_CRASH_BACKOFF_CAP_SEC", 0.0)

    procs: list[_LongRunningProcessor] = []

    def build_proc(receiver: object, *, replay_mode: bool = False) -> _LongRunningProcessor:
        p = _LongRunningProcessor()
        procs.append(p)
        return p

    sup = PipelineSupervisor(build_receiver=_FakeReceiver, build_processor=build_proc)
    await sup.set_active(True)
    assert len(procs) == 1

    # Pretend a crash streak already reached the cap, but long enough ago that
    # the reset window should wipe it out on the next crash rather than pile
    # on top of it and give up immediately.
    sup._consecutive_crashes = supervisor_mod._MAX_CONSECUTIVE_CRASH_RESTARTS
    sup._last_crash_ts = time.monotonic() - supervisor_mod._CRASH_RESET_WINDOW_SEC - 1.0

    await sup._restart_after_crash()

    assert sup._consecutive_crashes == 1, "a stale streak must reset, not accumulate"
    assert sup.active, "the pipeline must actually restart, not give up"
    assert len(procs) == 2, "a fresh processor must have been built"
    await sup.set_active(False)


class _HungProcessor:
    """run() ignores stop() and only ends when cancelled."""

    def __init__(self) -> None:
        self.cancelled = False

    async def run(self) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise

    def stop(self) -> None: ...


@pytest.mark.asyncio
async def test_stop_timeout_warns_and_cancels(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A processor that ignores stop() must be cancelled AND reported as a timeout.

    On Python 3.10 asyncio.wait_for raises asyncio.TimeoutError, which is not
    the builtin TimeoutError, so a bare `except TimeoutError` misses it and the
    timeout was mislabelled as an already-raised task exception at DEBUG.
    """
    monkeypatch.setattr(supervisor_mod, "_STOP_TIMEOUT_SEC", 0.1)
    procs: list[_HungProcessor] = []

    def build_proc(receiver: object, *, replay_mode: bool = False) -> _HungProcessor:
        p = _HungProcessor()
        procs.append(p)
        return p

    sup = PipelineSupervisor(build_receiver=_FakeReceiver, build_processor=build_proc)
    await sup.set_active(True)
    with caplog.at_level(logging.WARNING, logger="rfobserver.pipeline.supervisor"):
        await asyncio.wait_for(sup.set_active(False), timeout=2.0)

    assert procs[0].cancelled, "the hung task must be cancelled"
    assert "did not stop in time" in caplog.text, "the timeout must be reported as one"
    assert not sup.active
