"""Tests for PipelineSupervisor crash recovery -- restart() and the task done-callback."""

import asyncio

import pytest

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


@pytest.mark.asyncio
async def test_task_death_triggers_restart() -> None:
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
