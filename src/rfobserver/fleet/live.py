"""The fleet Live hub: one live socket per sensor, fanned out to every browser.

A sensor's ``/ws/live`` sends a spectrum frame per averaged window and a
heartbeat every second. The hub opens it only while someone is watching the
fleet Live page (field sensors are load-sensitive, and nobody needs frames
nobody sees), keeps each sensor's newest frame, and sends browsers a compact
copy: spectrum, power and kurtosis, without the max-hold and noise-floor
arrays the tiles do not draw. A browser that falls behind gets each
sensor's newest frame, never a queue.

High resolution (a frame per chunk, about 27 a second, instead of one per
averaged window) is asked of every sensor while any viewer wants it, the
same request the sensor's own Live page makes by default.

The frequency axis is sent to a browser only when it changes for that
sensor, as the sensor itself does, so the hub remembers each sensor's axis.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import json
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable

    from rfobserver.fleet.registry import Registry, Sensor
    from rfobserver.fleet.sensors import SensorHub

logger = logging.getLogger(__name__)

# A sensor socket with no message (heartbeats come every second) for this
# long is dead, even if it never closed.
SILENT_SEC = 5.0
# No spectrum for this long while connected: the sensor is in Standby.
PSD_STALE_SEC = 3.0
BACKOFF_MIN_SEC = 1.0
BACKOFF_MAX_SEC = 30.0
# Failed connects in a row before a sensor counts as offline, not reconnecting.
OFFLINE_AFTER_FAILURES = 3
# Keep sensor sockets this long after the last viewer leaves (page reloads).
IDLE_GRACE_SEC = 30.0
TICK_SEC = 1.0
# Each sensor's recent frame powers, kept so a browser opening the Live page
# draws the power plot at once (the plot shows this span in High Res).
POWER_HISTORY_SEC = 60.0
# Frames are about 60 KB of JSON; leave room for a large bin count.
MAX_MESSAGE_BYTES = 8 * 1024 * 1024


class LiveSocket(Protocol):
    async def recv(self) -> str | bytes: ...

    async def send(self, message: str) -> None: ...


def _websockets_connect(url: str) -> contextlib.AbstractAsyncContextManager[LiveSocket]:
    from websockets.asyncio.client import connect

    cm: contextlib.AbstractAsyncContextManager[LiveSocket] = connect(
        url, open_timeout=5, max_size=MAX_MESSAGE_BYTES, ping_interval=None
    )
    return cm


def live_url(sensor: Sensor) -> str:
    return sensor.base_url.replace("http://", "ws://", 1) + "/ws/live"


def compact_frame(sensor_id: str, data: dict[str, Any]) -> dict[str, Any]:
    """The browser's copy of a sensor ``psd`` frame (calibration applied)."""
    powers = data.get("powers") or []
    cal = data.get("cal_offset_db")
    calibrated = isinstance(cal, (int, float))
    if calibrated and cal:
        powers = [round(p + cal, 1) for p in powers]
    return {
        "type": "psd",
        "id": sensor_id,
        "t": data.get("chunk_time_ms"),
        "powers": powers,
        "unit": "dBm/Hz" if calibrated else "dBFS",
        "avg_power_db": data.get("avg_power_db"),
        "kurtosis": data.get("kurtosis"),
        "center_freq_hz": data.get("center_freq_hz"),
        "bandwidth_hz": data.get("bandwidth_hz"),
        "scale_min_db": data.get("scale_min_db"),
        "scale_max_db": data.get("scale_max_db"),
    }


@dataclass
class _Feed:
    """One sensor's live socket and what it last said."""

    sensor_id: str
    task: asyncio.Task[None] | None = None
    sock: LiveSocket | None = None
    high_res: bool = False  # what the sensor was last asked for
    connected: bool = False
    failures: int = 0
    last_message: float = 0.0  # monotonic
    last_psd: float = 0.0  # monotonic
    last_seen: float | None = None  # wall time of the last frame
    frame: dict[str, Any] | None = None
    frequencies: list[float] | None = None
    recording: str | None = None
    error: str | None = None
    published_status: dict[str, Any] | None = None
    # (chunk_time_ms, avg_power_db) of recent frames, oldest first.
    power: collections.deque[tuple[float, float]] = field(default_factory=collections.deque)

    def add_power(self, t_ms: Any, value: Any) -> None:
        if not isinstance(t_ms, (int, float)) or not isinstance(value, (int, float)):
            return
        if self.power and t_ms <= self.power[-1][0]:
            return
        self.power.append((float(t_ms), round(float(value), 2)))
        cut = t_ms - POWER_HISTORY_SEC * 1000.0
        while self.power and self.power[0][0] < cut:
            self.power.popleft()

    def state(self, now: float) -> str:
        if self.connected:
            return "live" if now - self.last_psd < PSD_STALE_SEC else "standby"
        if self.task is None:
            return "idle"
        if self.last_seen is None and self.failures == 0:
            return "connecting"
        return "offline" if self.failures >= OFFLINE_AFTER_FAILURES else "reconnecting"

    def status(self, now: float) -> dict[str, Any]:
        return {
            "type": "status",
            "id": self.sensor_id,
            "state": self.state(now),
            "last_seen": self.last_seen,
            "recording": self.recording,
            "error": self.error,
        }


@dataclass(eq=False)  # kept in a set, by identity
class _Viewer:
    """One browser on the Live page: the newest message per (sensor, kind)."""

    pending: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    order: list[tuple[str, str]] = field(default_factory=list)
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    # The frequency axis this browser holds, per sensor.
    freq_sig: dict[str, tuple[int, float, float]] = field(default_factory=dict)
    dropped: int = 0
    high_res: bool = False

    def put(self, key: tuple[str, str], msg: dict[str, Any]) -> None:
        if key in self.pending:
            self.dropped += 1
        else:
            self.order.append(key)
        self.pending[key] = msg
        self.wake.set()

    def take(self) -> list[dict[str, Any]]:
        out = [self.pending[k] for k in self.order]
        self.pending.clear()
        self.order.clear()
        self.wake.clear()
        return out


class LiveHub:
    def __init__(
        self,
        registry: Registry,
        sensors: SensorHub,
        connect: Callable[[str], contextlib.AbstractAsyncContextManager[LiveSocket]] | None = None,
    ) -> None:
        self.registry = registry
        self.sensors = sensors
        self._connect = connect or _websockets_connect
        self._feeds: dict[str, _Feed] = {}
        self._viewers: set[_Viewer] = set()
        self._idle_since: float | None = None
        self._tick_task: asyncio.Task[None] | None = None
        self._roster_sig: str | None = None

    # --- viewers ---

    def roster(self) -> dict[str, Any]:
        out = []
        for s in self.registry.all():
            if not s.enabled:
                continue
            ident = self.sensors.status(s.id).identity or {}
            out.append(
                {
                    "id": s.id,
                    "label": self.sensors.display_label(s),
                    "colour": s.colour,
                    "base_url": s.base_url,
                    "center_freq_hz": ident.get("center_freq_hz"),
                    "sample_rate_hz": ident.get("sample_rate_hz"),
                }
            )
        return {"type": "sensors", "sensors": out}

    def join(self) -> _Viewer:
        viewer = _Viewer()
        self._viewers.add(viewer)
        self._idle_since = None
        self._reconcile()
        viewer.put(("*", "sensors"), self.roster())
        now = time.monotonic()
        for feed in self._feeds.values():
            viewer.put((feed.sensor_id, "status"), feed.status(now))
            if feed.power:
                viewer.put(
                    (feed.sensor_id, "power_history"),
                    {"type": "power_history", "id": feed.sensor_id, "points": list(feed.power)},
                )
            if feed.frame is not None:
                viewer.put((feed.sensor_id, "psd"), feed.frame)
        return viewer

    def leave(self, viewer: _Viewer) -> None:
        self._viewers.discard(viewer)
        if not self._viewers:
            self._idle_since = time.monotonic()
        self._apply_mode()

    def set_high_res(self, viewer: _Viewer, on: bool) -> None:
        viewer.high_res = on
        self._apply_mode()

    def wants_high_res(self) -> bool:
        return any(v.high_res for v in self._viewers)

    def _apply_mode(self) -> None:
        want = self.wants_high_res()
        for feed in self._feeds.values():
            if feed.connected and feed.high_res != want:
                asyncio.create_task(self._send_mode(feed, want))

    async def _send_mode(self, feed: _Feed, on: bool) -> None:
        sock = feed.sock
        if sock is None:
            return
        feed.high_res = on
        try:
            await sock.send(json.dumps({"type": "set_mode", "high_res": on}))
        except Exception:
            feed.high_res = not on  # retried on the next change or reconnect
            logger.debug("Fleet live: set_mode to %s failed", feed.sensor_id, exc_info=True)

    def shape(self, viewer: _Viewer, msg: dict[str, Any]) -> dict[str, Any]:
        """Add the frequency axis to a frame if this browser lacks it."""
        if msg.get("type") != "psd":
            return msg
        feed = self._feeds.get(msg["id"])
        freqs = feed.frequencies if feed is not None else None
        if not freqs:
            return msg
        sig = (len(freqs), float(freqs[0]), float(freqs[-1]))
        if viewer.freq_sig.get(msg["id"]) == sig:
            return msg
        viewer.freq_sig[msg["id"]] = sig
        return {**msg, "frequencies": freqs}

    def _publish(self, key: tuple[str, str], msg: dict[str, Any]) -> None:
        for viewer in list(self._viewers):
            viewer.put(key, msg)

    # --- sensor feeds ---

    def _reconcile(self) -> None:
        """Run a feed for every enabled sensor while anyone watches."""
        watching = bool(self._viewers) or (
            self._idle_since is not None and time.monotonic() - self._idle_since < IDLE_GRACE_SEC
        )
        wanted = {s.id for s in self.registry.all() if s.enabled} if watching else set()
        for sid in list(self._feeds):
            if sid not in wanted:
                feed = self._feeds.pop(sid)
                if feed.task is not None:
                    feed.task.cancel()
        for sid in wanted:
            feed = self._feeds.setdefault(sid, _Feed(sid))
            if feed.task is None or feed.task.done():
                feed.task = asyncio.create_task(self._run(feed))
        sig = json.dumps(self.roster(), sort_keys=True)
        if sig != self._roster_sig:
            self._roster_sig = sig
            self._publish(("*", "sensors"), self.roster())

    async def _run(self, feed: _Feed) -> None:
        backoff = BACKOFF_MIN_SEC
        while True:
            sensor = self.registry.get(feed.sensor_id)
            if sensor is None:
                return
            url = live_url(sensor)
            try:
                async with self._connect(url) as sock:
                    feed.sock, feed.high_res = sock, False
                    feed.connected, feed.failures, feed.error = True, 0, None
                    feed.last_message = time.monotonic()
                    backoff = BACKOFF_MIN_SEC
                    self._publish_status(feed)
                    if self.wants_high_res():
                        await self._send_mode(feed, True)
                    while True:
                        raw = await asyncio.wait_for(sock.recv(), timeout=SILENT_SEC)
                        self._handle(feed, raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # any failure: reconnect
                feed.error = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
            if feed.connected:
                logger.info("Fleet live: %s disconnected (%s)", feed.sensor_id, feed.error)
            feed.sock = None
            feed.connected = False
            feed.failures += 1
            self._publish_status(feed)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX_SEC)

    def _handle(self, feed: _Feed, raw: str | bytes) -> None:
        now = time.monotonic()
        feed.last_message = now
        try:
            data = json.loads(raw)
        except ValueError:
            return
        if not isinstance(data, dict):
            return
        kind = data.get("type")
        if kind == "psd":
            freqs = data.get("frequencies")
            if isinstance(freqs, list) and freqs:
                feed.frequencies = freqs
            feed.frame = compact_frame(feed.sensor_id, data)
            feed.add_power(data.get("chunk_time_ms"), data.get("avg_power_db"))
            was_standby = now - feed.last_psd >= PSD_STALE_SEC
            feed.last_psd, feed.last_seen = now, time.time()
            self._publish((feed.sensor_id, "psd"), feed.frame)
            if was_standby:
                self._publish_status(feed)
        elif kind == "heartbeat":
            rec = data.get("recording")
            state = rec.get("state") if isinstance(rec, dict) else None
            if state != feed.recording:
                feed.recording = state
                self._publish_status(feed)

    def _publish_status(self, feed: _Feed) -> None:
        status = feed.status(time.monotonic())
        if status != feed.published_status:
            feed.published_status = status
            self._publish((feed.sensor_id, "status"), status)

    async def _tick_loop(self) -> None:
        while True:
            await asyncio.sleep(TICK_SEC)
            try:
                self._reconcile()
                for feed in self._feeds.values():
                    self._publish_status(feed)  # live -> standby when frames stop
                self._publish(("*", "tick"), {"type": "tick", "t": time.time()})
            except Exception:
                logger.exception("Fleet live: tick failed")

    def start(self) -> None:
        if self._tick_task is None:
            self._tick_task = asyncio.create_task(self._tick_loop())

    async def close(self) -> None:
        tasks = [f.task for f in self._feeds.values() if f.task is not None]
        if self._tick_task is not None:
            tasks.append(self._tick_task)
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        self._feeds.clear()
        self._tick_task = None
