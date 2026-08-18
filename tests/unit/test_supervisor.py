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
        build_processor=lambda r, **kwargs: FakeProcessor(r),
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


class _FakeReplayReceiver:
    def __init__(self, name: str) -> None:
        self.source_name = name
        self.speed = 1.0
        self.loop = True

    def initialize(self) -> None:  # off-loop in _start
        pass

    def close(self) -> None:
        pass


class _FakeReplayProc:
    def __init__(self) -> None:
        self.stopped = False

    async def run(self) -> None:
        while not self.stopped:
            await asyncio.sleep(0.01)

    def stop(self) -> None:
        self.stopped = True


@pytest.mark.asyncio
async def test_start_replay_uses_override_and_reports_status() -> None:
    seen: dict[str, object] = {}

    def build_receiver() -> _FakeReplayReceiver:
        return _FakeReplayReceiver("SDR")

    def build_processor(receiver: object, *, replay_mode: bool = False) -> _FakeReplayProc:
        seen["replay_mode"] = replay_mode
        seen["receiver"] = receiver
        return _FakeReplayProc()

    sup = PipelineSupervisor(build_receiver, build_processor)
    assert sup.replay_status() is None

    rx = _FakeReplayReceiver("ssm_fhss_OVF.dat")
    rx.speed = 2.0
    await sup.start_replay(rx)
    assert seen["replay_mode"] is True
    assert seen["receiver"] is rx
    st = sup.replay_status()
    assert st == {"source": "ssm_fhss_OVF.dat", "speed": 2.0, "looping": True}

    await sup.stop_replay()
    assert sup.replay_status() is None
    assert not sup.active


@pytest.mark.asyncio
async def test_standby_toggle_clears_replay_and_returns_to_live_sdr() -> None:
    """A plain Standby off/on (set_active) must drop replay state, not resume it."""
    receivers: list[FakeReceiver] = []
    calls: list[dict[str, object]] = []

    def build_receiver() -> FakeReceiver:
        r = FakeReceiver()
        receivers.append(r)
        return r

    def build_processor(receiver: object, *, replay_mode: bool = False) -> _FakeReplayProc:
        calls.append({"receiver": receiver, "replay_mode": replay_mode})
        return _FakeReplayProc()

    sup = PipelineSupervisor(build_receiver, build_processor)

    rx = _FakeReplayReceiver("replay.dat")
    await sup.start_replay(rx)
    assert sup.replay_status() is not None
    assert calls[-1]["replay_mode"] is True
    assert calls[-1]["receiver"] is rx
    assert len(receivers) == 0  # override receiver used, live build_receiver not called

    # Plain Standby toggle off then on -- must NOT resume the replay receiver.
    await sup.set_active(False)
    assert sup.active is False
    assert sup.replay_status() is None

    await sup.set_active(True)
    assert sup.active is True
    assert sup.replay_status() is None
    assert calls[-1]["replay_mode"] is False
    assert len(receivers) == 1
    assert calls[-1]["receiver"] is receivers[0]
    assert sup.receiver is receivers[0]
    assert sup.receiver is not rx


@pytest.mark.asyncio
async def test_start_replay_stops_active_live_pipeline_first() -> None:
    """start_replay() with a live pipeline running stops it before switching over."""
    receivers: list[FakeReceiver] = []
    calls: list[dict[str, object]] = []

    def build_receiver() -> FakeReceiver:
        r = FakeReceiver()
        receivers.append(r)
        return r

    def build_processor(receiver: object, *, replay_mode: bool = False) -> _FakeReplayProc:
        calls.append({"receiver": receiver, "replay_mode": replay_mode})
        return _FakeReplayProc()

    sup = PipelineSupervisor(build_receiver, build_processor)

    await sup.set_active(True)
    assert sup.active is True
    live_receiver = receivers[0]
    assert live_receiver.closed == 0
    assert calls[-1]["replay_mode"] is False

    rx = _FakeReplayReceiver("replay.dat")
    await sup.start_replay(rx)

    assert live_receiver.closed == 1  # the live pipeline was stopped first
    assert sup.active is True
    assert sup.receiver is rx
    assert calls[-1]["replay_mode"] is True
    assert calls[-1]["receiver"] is rx
    assert sup.replay_status() is not None
