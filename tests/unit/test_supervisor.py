"""Tests for PipelineSupervisor — runtime sensor start/stop."""

import asyncio

import pytest

from rfobserver.pipeline.supervisor import PipelineSupervisor


class FakeReceiver:
    def __init__(self) -> None:
        self.initialized = 0
        self.closed = 0

    def initialize(self) -> None:
        self.initialized += 1

    def close(self) -> None:
        self.closed += 1


class FakeProcessor:
    def __init__(self, receiver: FakeReceiver) -> None:
        self.receiver = receiver
        self.started = False
        self._stop = asyncio.Event()

    async def run(self) -> None:
        self.started = True
        await self._stop.wait()

    def stop(self) -> None:
        self._stop.set()


def _make() -> tuple[PipelineSupervisor, tuple[list, list]]:
    receivers: list[FakeReceiver] = []
    changes: list[object] = []

    def build_receiver() -> FakeReceiver:
        r = FakeReceiver()
        receivers.append(r)
        return r

    sup = PipelineSupervisor(
        build_receiver=build_receiver,
        build_processor=lambda r: FakeProcessor(r),
        on_processor_change=changes.append,
    )
    return sup, (receivers, changes)


@pytest.mark.asyncio
async def test_enable_initializes_and_starts() -> None:
    sup, (receivers, changes) = _make()
    result = await sup.set_active(True)
    assert result is True
    assert sup.active is True
    assert receivers[0].initialized == 1
    await asyncio.sleep(0)  # let the processor task begin running
    assert sup.processor is not None and sup.processor.started is True
    assert changes[-1] is sup.processor


@pytest.mark.asyncio
async def test_disable_stops_and_closes() -> None:
    sup, (receivers, changes) = _make()
    await sup.set_active(True)
    result = await sup.set_active(False)
    assert result is False
    assert sup.active is False
    assert sup.processor is None
    assert receivers[0].closed == 1
    assert changes[-1] is None


@pytest.mark.asyncio
async def test_redundant_calls_are_noops() -> None:
    sup, (receivers, _) = _make()
    await sup.set_active(True)
    await sup.set_active(True)  # no second receiver built
    assert len(receivers) == 1


@pytest.mark.asyncio
async def test_reenable_uses_fresh_receiver() -> None:
    sup, (receivers, _) = _make()
    await sup.set_active(True)
    await sup.set_active(False)
    await sup.set_active(True)
    assert len(receivers) == 2
    assert receivers[1].initialized == 1
