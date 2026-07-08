"""The receiver factory must not initialize hardware until activation."""

import asyncio

import pytest

from rfobserver.pipeline.supervisor import PipelineSupervisor


class SpyReceiver:
    def __init__(self) -> None:
        self.initialized = False

    def initialize(self) -> None:
        self.initialized = True

    def close(self) -> None:
        pass


class Proc:
    def __init__(self, r: SpyReceiver) -> None:
        self._e = asyncio.Event()

    async def run(self) -> None:
        await self._e.wait()

    def stop(self) -> None:
        self._e.set()


@pytest.mark.asyncio
async def test_receiver_not_initialized_until_active() -> None:
    spies: list[SpyReceiver] = []

    def build() -> SpyReceiver:
        s = SpyReceiver()
        spies.append(s)
        return s

    sup = PipelineSupervisor(build_receiver=build, build_processor=lambda r: Proc(r))
    # Constructing the supervisor and staying inactive must not build/init a receiver.
    assert spies == []
    await sup.set_active(True)
    assert spies[0].initialized is True
    await sup.set_active(False)
