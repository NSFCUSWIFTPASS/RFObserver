"""Fleet Live: the hub that relays every sensor's live socket to browsers."""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from fastapi.testclient import TestClient

from rfobserver.fleet import live as live_mod
from rfobserver.fleet.app import create_fleet_app
from rfobserver.fleet.live import LiveHub, _Viewer, compact_frame, live_url
from rfobserver.fleet.registry import Registry, Sensor
from rfobserver.fleet.sensors import SensorHub

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

FREQS = [914e6, 915e6, 916e6]


def psd(power: float, *, freqs: bool = True, t: float = 1000.0, **extra: Any) -> str:
    frame: dict[str, Any] = {
        "type": "psd",
        "powers": [power, power + 1, power + 2],
        "max_powers": [0.0, 0.0, 0.0],
        "noise_floor_per_bin": [-100.0, -100.0, -100.0],
        "avg_power_db": power,
        "kurtosis": 3.0,
        "center_freq_hz": 915e6,
        "chunk_time_ms": t,
        **extra,
    }
    if freqs:
        frame["frequencies"] = FREQS
    return json.dumps(frame)


class FakeSock:
    def __init__(self, frames: list[str]) -> None:
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        for f in frames:
            self.queue.put_nowait(f)

        self.sent: list[dict] = []

    async def recv(self) -> str:
        return await self.queue.get()

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))


class FakeSensors:
    """Live sockets by URL: a list of frames to send, or an exception to raise."""

    def __init__(self) -> None:
        self.plan: dict[str, list[str] | Exception] = {}
        self.socks: dict[str, FakeSock] = {}
        self.connects: list[str] = []

    @contextlib.asynccontextmanager
    async def connect(self, url: str) -> AsyncIterator[FakeSock]:
        self.connects.append(url)
        item = self.plan.get(url, ConnectionRefusedError("refused"))
        if isinstance(item, Exception):
            raise item
        sock = self.socks[url] = FakeSock(item)
        yield sock


def make_hub(tmp_path: Path, fake: FakeSensors) -> tuple[Registry, LiveHub]:
    reg = Registry(tmp_path / "fleet.json")
    transport = httpx.MockTransport(lambda r: httpx.Response(503))
    hub = LiveHub(reg, SensorHub(reg, transport=transport), connect=fake.connect)
    return reg, hub


async def drain(hub: LiveHub, viewer: _Viewer, until: Any, timeout: float = 2.0) -> list[dict]:
    """Messages sent to ``viewer`` (shaped) until ``until(msgs)`` holds."""
    got: list[dict] = []
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not until(got):
        left = deadline - loop.time()
        if left <= 0:
            raise AssertionError(f"timed out; got {got}")
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(viewer.wake.wait(), left)
        got.extend(hub.shape(viewer, m) for m in viewer.take())
    return got


def kinds(msgs: list[dict], kind: str) -> list[dict]:
    return [m for m in msgs if m.get("type") == kind]


# --- pure parts ---


def test_compact_frame_drops_arrays_and_applies_calibration() -> None:
    data = json.loads(psd(-50.0, cal_offset_db=-10.0, scale_min_db=-120, scale_max_db=-20))
    out = compact_frame("a", data)
    assert out["id"] == "a" and out["unit"] == "dBm/Hz"
    assert out["powers"] == [-60.0, -59.0, -58.0]
    assert "max_powers" not in out and "noise_floor_per_bin" not in out
    assert "frequencies" not in out  # sent separately, once per axis
    assert (out["scale_min_db"], out["scale_max_db"]) == (-120, -20)
    assert compact_frame("a", json.loads(psd(-50.0)))["unit"] == "dBFS"


def test_compact_frame_time_is_the_capture_time() -> None:
    # chunk_time_ms is the publish time or stored window start; capture time wins.
    avg = compact_frame("a", json.loads(psd(-50.0, t=2000.0, capture_time_ms=1500.0, dur_ms=999.0)))
    assert (avg["t"], avg["dur_ms"]) == (1500.0, 999.0)
    # Sensors that predate capture times: chunk_time_ms stands in.
    hr = compact_frame("a", json.loads(psd(-50.0, t=2000.0)))
    assert (hr["t"], hr["dur_ms"]) == (2000.0, None)


def test_live_url() -> None:
    assert live_url(Sensor("a", "10.0.0.1")) == "ws://10.0.0.1/ws/live"
    assert live_url(Sensor("a", "10.0.0.1", 8888)) == "ws://10.0.0.1:8888/ws/live"


def test_viewer_keeps_only_the_newest_message_per_sensor() -> None:
    v = _Viewer()
    v.put(("a", "psd"), {"n": 1})
    v.put(("b", "psd"), {"n": 2})
    v.put(("a", "psd"), {"n": 3})
    assert v.take() == [{"n": 3}, {"n": 2}]
    assert v.dropped == 1 and not v.wake.is_set()


# --- the hub ---


async def test_no_sensor_sockets_without_viewers(tmp_path: Path) -> None:
    fake = FakeSensors()
    reg, hub = make_hub(tmp_path, fake)
    reg.add("a", "10.0.0.1", 80, source="manual")
    hub._reconcile()
    await asyncio.sleep(0.05)
    assert fake.connects == []
    await hub.close()


async def test_viewer_gets_roster_status_and_frames(tmp_path: Path) -> None:
    fake = FakeSensors()
    reg, hub = make_hub(tmp_path, fake)
    a = reg.add("a", "10.0.0.1", 80, source="manual")
    reg.update("a", label="NNE 60")
    fake.plan[live_url(a)] = [psd(-50.0), psd(-49.0, freqs=False, t=2000.0)]
    try:
        v = hub.join()
        msgs = await drain(hub, v, lambda m: len(kinds(m, "psd")) >= 1)
        roster = kinds(msgs, "sensors")[0]["sensors"]
        assert [(s["id"], s["label"], s["colour"]) for s in roster] == [("a", "NNE 60", a.colour)]
        assert any(m["state"] == "live" for m in kinds(msgs, "status"))
        frame = kinds(msgs, "psd")[-1]
        # The first frame this browser sees carries the axis, later ones not.
        if frame["avg_power_db"] == -50.0:
            assert frame["frequencies"] == FREQS
            msgs = await drain(hub, v, lambda m: len(kinds(m, "psd")) >= 1)
            frame = kinds(msgs, "psd")[-1]
            assert "frequencies" not in frame
        assert frame["avg_power_db"] == -49.0

        # A browser joining later gets the newest frame, with the axis.
        v2 = hub.join()
        msgs = await drain(hub, v2, lambda m: len(kinds(m, "psd")) >= 1)
        assert kinds(msgs, "psd")[0]["avg_power_db"] == -49.0
        assert kinds(msgs, "psd")[0]["frequencies"] == FREQS
        assert len(fake.connects) == 1  # one sensor socket for both browsers
    finally:
        await hub.close()


async def test_heartbeat_recording_state_is_relayed(tmp_path: Path) -> None:
    fake = FakeSensors()
    reg, hub = make_hub(tmp_path, fake)
    a = reg.add("a", "10.0.0.1", 80, source="manual")
    hb = json.dumps({"type": "heartbeat", "recording": {"state": "recording"}})
    fake.plan[live_url(a)] = [hb]
    try:
        v = hub.join()
        msgs = await drain(
            hub, v, lambda m: any(s.get("recording") == "recording" for s in kinds(m, "status"))
        )
        assert kinds(msgs, "status")[-1]["state"] == "standby"  # connected, no spectrum
    finally:
        await hub.close()


async def test_failed_sensor_goes_reconnecting_then_offline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(live_mod, "BACKOFF_MIN_SEC", 0.01)
    monkeypatch.setattr(live_mod, "BACKOFF_MAX_SEC", 0.01)
    fake = FakeSensors()
    reg, hub = make_hub(tmp_path, fake)
    reg.add("a", "10.0.0.1", 80, source="manual")
    try:
        v = hub.join()
        msgs = await drain(
            hub, v, lambda m: any(s["state"] == "offline" for s in kinds(m, "status"))
        )
        states = [s["state"] for s in kinds(msgs, "status")]
        assert "reconnecting" in states
        assert "ConnectionRefusedError" in kinds(msgs, "status")[-1]["error"]
        assert len(fake.connects) >= live_mod.OFFLINE_AFTER_FAILURES
    finally:
        await hub.close()


async def test_silent_sensor_socket_is_reopened(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(live_mod, "SILENT_SEC", 0.05)
    monkeypatch.setattr(live_mod, "BACKOFF_MIN_SEC", 0.01)
    fake = FakeSensors()
    reg, hub = make_hub(tmp_path, fake)
    a = reg.add("a", "10.0.0.1", 80, source="manual")
    fake.plan[live_url(a)] = [psd(-50.0)]  # then nothing
    try:
        hub.join()
        for _ in range(100):
            if len(fake.connects) >= 2:
                break
            await asyncio.sleep(0.01)
        assert len(fake.connects) >= 2
    finally:
        await hub.close()


async def test_feeds_stop_after_the_last_viewer_leaves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeSensors()
    reg, hub = make_hub(tmp_path, fake)
    a = reg.add("a", "10.0.0.1", 80, source="manual")
    fake.plan[live_url(a)] = [psd(-50.0)]
    try:
        v = hub.join()
        await drain(hub, v, lambda m: bool(kinds(m, "psd")))
        hub.leave(v)
        hub._reconcile()
        assert "a" in hub._feeds  # still within the grace period
        monkeypatch.setattr(live_mod, "IDLE_GRACE_SEC", 0.0)
        hub._reconcile()
        assert hub._feeds == {}
    finally:
        await hub.close()


async def test_hidden_sensor_is_dropped_from_roster_and_feeds(tmp_path: Path) -> None:
    fake = FakeSensors()
    reg, hub = make_hub(tmp_path, fake)
    a = reg.add("a", "10.0.0.1", 80, source="manual")
    b = reg.add("b", "10.0.0.2", 80, source="manual")
    fake.plan[live_url(a)] = [psd(-50.0)]
    fake.plan[live_url(b)] = [psd(-40.0)]
    try:
        v = hub.join()
        await drain(hub, v, lambda m: len(kinds(m, "psd")) >= 2)
        reg.update("b", enabled=False)
        hub._reconcile()
        msgs = await drain(hub, v, lambda m: bool(kinds(m, "sensors")))
        assert [s["id"] for s in kinds(msgs, "sensors")[-1]["sensors"]] == ["a"]
        assert set(hub._feeds) == {"a"}
    finally:
        await hub.close()


async def test_high_res_follows_the_viewers(tmp_path: Path) -> None:
    fake = FakeSensors()
    reg, hub = make_hub(tmp_path, fake)
    a = reg.add("a", "10.0.0.1", 80, source="manual")
    fake.plan[live_url(a)] = [psd(-50.0)]
    try:
        v = hub.join()
        await drain(hub, v, lambda m: bool(kinds(m, "psd")))
        sock = fake.socks[live_url(a)]
        assert sock.sent == []  # averaged mode until someone asks
        hub.set_high_res(v, True)
        await asyncio.sleep(0.01)
        assert sock.sent == [{"type": "set_mode", "high_res": True}]
        v2 = hub.join()
        hub.set_high_res(v2, False)
        hub.leave(v)  # v2 does not want it: back to averaged
        await asyncio.sleep(0.01)
        assert sock.sent[-1] == {"type": "set_mode", "high_res": False}
    finally:
        await hub.close()


async def test_reconnected_sensor_is_asked_for_high_res_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(live_mod, "SILENT_SEC", 0.05)
    monkeypatch.setattr(live_mod, "BACKOFF_MIN_SEC", 0.01)
    fake = FakeSensors()
    reg, hub = make_hub(tmp_path, fake)
    a = reg.add("a", "10.0.0.1", 80, source="manual")
    fake.plan[live_url(a)] = [psd(-50.0)]
    try:
        v = hub.join()
        hub.set_high_res(v, True)
        for _ in range(100):
            if len(fake.connects) >= 2 and fake.socks[live_url(a)].sent:
                break
            await asyncio.sleep(0.01)
        assert len(fake.connects) >= 2
        assert fake.socks[live_url(a)].sent == [{"type": "set_mode", "high_res": True}]
    finally:
        await hub.close()


async def test_new_viewer_gets_recent_frame_powers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(live_mod, "POWER_HISTORY_SEC", 1.0)
    fake = FakeSensors()
    reg, hub = make_hub(tmp_path, fake)
    a = reg.add("a", "10.0.0.1", 80, source="manual")
    frames = [
        psd(-50.0, t=1000.0),
        psd(-49.0, freqs=False, t=1500.0, kurtosis=3.5),
        psd(-48.0, freqs=False, t=1500.0),  # same time: ignored
        psd(-47.0, freqs=False, t=2400.0, kurtosis=None),  # no kurtosis: power only
    ]  # pushes t=1000 out of the 1 s span
    fake.plan[live_url(a)] = frames
    try:
        v = hub.join()
        await drain(hub, v, lambda m: any(f["avg_power_db"] == -47.0 for f in kinds(m, "psd")))
        v2 = hub.join()
        msgs = await drain(hub, v2, lambda m: bool(kinds(m, "power_history")))
        hist = kinds(msgs, "power_history")[0]
        assert hist["id"] == "a"
        assert [tuple(p) for p in hist["points"]] == [(1500.0, -49.0), (2400.0, -47.0)]
        assert [tuple(p) for p in hist["kurtosis"]] == [(1500.0, 3.5)]
    finally:
        await hub.close()


# --- through the app ---


def test_fleet_live_socket_and_page(tmp_path: Path) -> None:
    fake = FakeSensors()
    app = create_fleet_app(
        tmp_path,
        mdns=False,
        live_connect=fake.connect,
        transport=httpx.MockTransport(lambda r: httpx.Response(503)),
    )
    a = app.state.registry.add("a", "10.0.0.1", 80, source="manual")
    fake.plan[live_url(a)] = [psd(-50.0)]
    with TestClient(app) as client:
        r = client.get("/live")
        assert r.status_code == 200
        assert "fleet-live.js" in r.text and 'href="/live"' in r.text
        with client.websocket_connect("/fleet/ws/live") as ws:
            seen: list[dict] = []
            while not kinds(seen, "psd"):
                seen.append(ws.receive_json())
            assert kinds(seen, "sensors")[0]["sensors"][0]["id"] == "a"
            assert kinds(seen, "psd")[0]["frequencies"] == FREQS
            ws.send_text(json.dumps({"type": "set_mode", "high_res": True}))
            ws.send_text("not json")  # ignored
            while not app.state.live.wants_high_res():
                seen.append(ws.receive_json())


async def test_frames_are_encoded_once_for_every_browser(tmp_path: Path) -> None:
    fake = FakeSensors()
    reg, hub = make_hub(tmp_path, fake)
    a = reg.add("a", "10.0.0.1", 80, source="manual")
    fake.plan[live_url(a)] = [psd(-50.0)]
    try:
        v1, v2 = hub.join(), hub.join()
        for _ in range(100):
            if hub._feeds["a"].frame is not None:
                break
            await asyncio.sleep(0.01)
        frame = hub._feeds["a"].frame
        assert frame is not None
        first1, first2 = hub.encode(v1, frame), hub.encode(v2, frame)
        assert first1 is first2  # the same text object, encoded once
        assert json.loads(first1)["frequencies"] == FREQS
        again1, again2 = hub.encode(v1, frame), hub.encode(v2, frame)
        assert again1 is again2 and "frequencies" not in json.loads(again1)
        # Messages that are not the current frame are encoded as shaped.
        assert json.loads(hub.encode(v1, {"type": "tick", "t": 1.0})) == {"type": "tick", "t": 1.0}
    finally:
        await hub.close()


# --- burst overlay ---


def _burst(i: str, t: float, **extra: Any) -> dict[str, Any]:
    return {
        "id": i,
        "freq_low_hz": 915.0e6,
        "freq_high_hz": 915.1e6,
        "peak_power_db": -40.0,
        "duration_ms": 5.0,
        "start_time_ms": t + 400.0,  # the detector's clock, late of capture
        "stop_time_ms": t + 405.0,
        **extra,
    }


def _ov_frame(t: float, bursts=(), labels=()) -> dict[str, Any]:
    return {
        "capture_time_ms": t,
        "chunk_time_ms": t + 100.0,
        "bursts": list(bursts),
        "attributions": list(labels),
    }


def test_overlay_uses_capture_times_and_forwards_new_bursts_a_while() -> None:
    ov = live_mod._Overlay()
    b = _burst("b1", 10_000.0, capture_start_ms=10_000.0, capture_stop_ms=10_005.0)
    out = ov.update(_ov_frame(10_050.0, [b]), now=0.0)
    assert [(x["id"], x["t0"], x["t1"]) for x in out["bursts"]] == [("b1", 10_000.0, 10_005.0)]
    # Repeated unchanged: still forwarded within BURST_RESEND_SEC, then not.
    assert ov.update(_ov_frame(10_090.0, [b]), now=0.5)["bursts"]
    assert ov.update(_ov_frame(10_130.0, [b]), now=1.5)["bursts"] == []
    # Grown: forwarded again.
    grown = {**b, "capture_stop_ms": 10_009.0}
    out = ov.update(_ov_frame(10_170.0, [grown]), now=1.6)
    assert [x["t1"] for x in out["bursts"]] == [10_009.0]


def test_overlay_estimates_times_for_an_older_sensor() -> None:
    ov = live_mod._Overlay()
    out = ov.update(_ov_frame(10_050.0, [_burst("b1", 10_000.0)]), now=0.0)
    # Detector times less the frame's publish-to-capture delay (100 ms).
    assert (out["bursts"][0]["t0"], out["bursts"][0]["t1"]) == (10_300.0, 10_305.0)


def test_overlay_labels_once_and_never_resent_while_the_sensor_keeps_them() -> None:
    ov = live_mod._Overlay()
    lab = {
        "id": "b1",
        "model": "SCMplus",
        "freq_low_hz": 915.0e6,
        "freq_high_hz": 915.1e6,
        "capture_start_ms": 10_000.0,
        "capture_stop_ms": 10_005.0,
    }
    nolabel = {**lab, "id": "b2", "model": None}  # not decoded: no label
    out = ov.update(_ov_frame(10_050.0, labels=[lab, nolabel]), now=0.0)
    assert [(x["id"], x["model"]) for x in out["labels"]] == [("b1", "SCMplus")]
    assert ov.update(_ov_frame(12_000.0, labels=[lab]), now=2.5)["labels"] == []
    # The sensor still sends it a minute on: remembered, so not sent again.
    assert ov.update(_ov_frame(70_000.0, labels=[lab]), now=60.0)["labels"] == []
    # Gone from the sensor for OVERLAY_FORGET_SEC: forgotten.
    ov.update(_ov_frame(100_000.0), now=100.0)
    assert "b1" not in ov.labels


def test_overlay_does_not_forward_items_already_off_the_waterfall() -> None:
    ov = live_mod._Overlay()
    old = {
        "id": "old",
        "model": "SCMplus",
        "freq_low_hz": 1.0,
        "freq_high_hz": 2.0,
        "capture_start_ms": 1_000.0,
        "capture_stop_ms": 1_005.0,
    }
    out = ov.update(_ov_frame(60_000.0, labels=[old]), now=0.0)
    assert out["labels"] == [] and "old" in ov.labels


@pytest.mark.parametrize("averaged", [False, True])
async def test_high_res_frames_carry_the_overlay_averaged_do_not(
    tmp_path: Path, averaged: bool
) -> None:
    # One frame per case: a browser that falls behind only gets the newest.
    fake = FakeSensors()
    reg, hub = make_hub(tmp_path, fake)
    a = reg.add("a", "10.0.0.1", 80, source="manual")
    b = _burst("b1", 1000.0, capture_start_ms=1000.0, capture_stop_ms=1005.0)
    extra: dict[str, Any] = {"chunks_averaged": 10} if averaged else {}
    fake.plan[live_url(a)] = [psd(-50.0, t=1000.0, bursts=[b], capture_time_ms=1000.0, **extra)]
    try:
        v = hub.join()
        msgs = await drain(hub, v, lambda m: bool(kinds(m, "psd")))
        (frame,) = kinds(msgs, "psd")
        if averaged:
            assert "bursts" not in frame
        else:
            assert [x["id"] for x in frame["bursts"]] == ["b1"]
    finally:
        await hub.close()
