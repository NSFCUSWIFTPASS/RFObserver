"""WebSocket endpoint for live spectrogram and detection streaming."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from typing import Any

import numpy as np
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)

_QueueType = asyncio.Queue[dict[str, Any]]

# Per-bin dB arrays in a psd frame. Rounded to 0.1 dB (the UI shows one
# decimal) so each value serializes as ~6 characters instead of ~18.
_DB_ARRAYS = ("powers", "max_powers", "noise_floor_per_bin")

# The per-bin noise floor is a rolling percentile that moves slowly, so a
# client gets it at most this often (and at once when the bin count changes).
NOISE_FLOOR_RESEND_SEC = 1.0

# A single send blocking this long means the client's link has stalled (the
# socket's buffers are full). Logged at most once per LOG_EVERY_SEC per client,
# as are dropped frames (a full per-client queue).
SLOW_SEND_SEC = 1.0
LOG_EVERY_SEC = 10.0


class _Subscriber:
    """Per-client subscriber state."""

    __slots__ = (
        "queue",
        "high_res",
        "wants_psd",
        "freq_sig",
        "nf_bins",
        "nf_sent_at",
        "sent",
        "dropped",
        "dropped_logged",
        "last_drop_log",
    )

    def __init__(self) -> None:
        self.queue: _QueueType = asyncio.Queue(maxsize=10)
        self.high_res: bool = False
        self.wants_psd: bool = True
        # What this client already holds, so unchanged data is not resent.
        self.freq_sig: tuple[int, float, float] | None = None
        self.nf_bins: int = -1
        self.nf_sent_at: float = 0.0
        # Diagnostics for the close log and the slow-client warnings.
        self.sent = 0
        self.dropped = 0
        self.dropped_logged = 0
        self.last_drop_log = 0.0


def shape_for_client(sub: _Subscriber, data: dict[str, Any], now: float) -> dict[str, Any]:
    """Drop the parts of a psd frame this client already has.

    ``frequencies`` is sent only when the axis changes (retune, bin count),
    and ``noise_floor_per_bin`` at most every NOISE_FLOOR_RESEND_SEC. The
    client keeps the last copy of each. Runs at send time, not publish time,
    so a frame dropped on a full queue never takes the only copy with it.
    """
    if data.get("type") != "psd":
        return data
    msg = data
    freqs = data.get("frequencies")
    if isinstance(freqs, list) and freqs:
        sig = (len(freqs), float(freqs[0]), float(freqs[-1]))
        if sig == sub.freq_sig:
            msg = {k: v for k, v in msg.items() if k != "frequencies"}
        else:
            sub.freq_sig = sig
    nf = data.get("noise_floor_per_bin")
    if isinstance(nf, list):
        if len(nf) == sub.nf_bins and now - sub.nf_sent_at < NOISE_FLOOR_RESEND_SEC:
            msg = {k: v for k, v in msg.items() if k != "noise_floor_per_bin"}
        else:
            sub.nf_bins = len(nf)
            sub.nf_sent_at = now
    return msg


def _round_db(values: Any) -> Any:
    if not isinstance(values, list) or not values:
        return values
    # float64 so tolist() yields -115.8, not float32's -115.80000305175781.
    return np.round(np.asarray(values, dtype=np.float64), 1).tolist()


class LiveBroadcast:
    """Broadcast channel for live data to WebSocket subscribers."""

    def __init__(self) -> None:
        self._subscribers: set[_Subscriber] = set()

    def subscribe(self) -> _Subscriber:
        sub = _Subscriber()
        self._subscribers.add(sub)
        return sub

    def unsubscribe(self, sub: _Subscriber) -> None:
        self._subscribers.discard(sub)

    def has_high_res_subscribers(self) -> bool:
        return any(s.high_res and s.wants_psd for s in self._subscribers)

    async def publish(self, data: dict[str, Any]) -> None:
        # Separate grid_rows from the base message — only send to high_res clients
        grid_rows = data.pop("grid_rows", None)
        is_psd = data.get("type") == "psd"
        if is_psd:
            # Once per frame, shared by every subscriber.
            data = {**data, **{k: _round_db(data[k]) for k in _DB_ARRAYS if k in data}}

        for sub in list(self._subscribers):
            if is_psd and not sub.wants_psd:
                continue
            msg = data
            if sub.high_res and grid_rows is not None:
                msg = {**data, "grid_rows": grid_rows}
            try:
                sub.queue.put_nowait(msg)
            except asyncio.QueueFull:
                sub.dropped += 1


async def websocket_endpoint(websocket: WebSocket, broadcast: LiveBroadcast) -> None:
    """Handle a WebSocket connection for live data streaming."""
    await websocket.accept()
    sub = broadcast.subscribe()
    client = getattr(websocket, "client", None)
    peer = f"{client.host}:{client.port}" if client else "?"
    opened = time.monotonic()
    # Pages that only want the heartbeat connect with ?psd=0, so they never
    # receive spectrum frames (not even before a set_view message arrives).
    if websocket.query_params.get("psd") == "0":
        sub.wants_psd = False

    async def send_loop() -> None:
        while True:
            data = await sub.queue.get()
            t0 = time.monotonic()
            try:
                await websocket.send_json(shape_for_client(sub, data, t0))
            except RuntimeError:
                # The client closed the socket (the Live watchdog abandons a
                # silent one) and the server already answered the close: the
                # send races the disconnect. That is an ordinary disconnect.
                return
            sub.sent += 1
            now = time.monotonic()
            slow = now - t0 >= SLOW_SEND_SEC
            new_drops = sub.dropped - sub.dropped_logged
            if (slow or new_drops) and now - sub.last_drop_log >= LOG_EVERY_SEC:
                sub.last_drop_log = now
                sub.dropped_logged = sub.dropped
                logger.warning(
                    "Live client %s is behind: last send took %.1f s, %d frames dropped "
                    "since the last warning (link stalled or too slow?)",
                    peer,
                    now - t0,
                    new_drops,
                )

    async def recv_loop() -> None:
        while True:
            text = await websocket.receive_text()
            try:
                msg = json.loads(text)
            except json.JSONDecodeError:
                continue
            if msg.get("type") == "hello":
                # Why the page opened this socket: first load, the watchdog
                # after a silence, or the previous socket closing.
                logger.info(
                    "Live client %s connected: reason=%s silent_ms=%s",
                    peer,
                    msg.get("reason"),
                    msg.get("silent_ms"),
                )
            elif msg.get("type") == "freeze":
                # The page ran no code for late_ms (its own timer measured it):
                # a long task in the page, or the browser pausing the tab.
                logger.warning(
                    "Live client %s: page frozen %s ms (hidden=%s, longest task %s ms)",
                    peer,
                    msg.get("late_ms"),
                    msg.get("hidden"),
                    msg.get("longest_task_ms"),
                )
            elif msg.get("type") == "set_mode":
                sub.high_res = bool(msg.get("high_res", False))
                logger.info("Client %s set high_res=%s", peer, sub.high_res)
            elif msg.get("type") == "set_view":
                sub.wants_psd = bool(msg.get("psd_visible", True))
                logger.info(
                    "Client %s set wants_psd=%s (hidden=%s in_view=%s why=%s)",
                    peer,
                    sub.wants_psd,
                    msg.get("hidden"),
                    msg.get("in_view"),
                    msg.get("why"),
                )

    tasks = [asyncio.create_task(send_loop()), asyncio.create_task(recv_loop())]
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        # surface a non-cancel, non-disconnect error from a finished task
        for t in done:
            exc = t.exception()
            if exc and not isinstance(exc, (WebSocketDisconnect, asyncio.CancelledError)):
                raise exc
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("WebSocket handler error")
    finally:
        for t in tasks:
            t.cancel()
        with contextlib.suppress(BaseException):
            await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(
            "Live client %s closed after %.0f s: sent=%d dropped=%d",
            peer,
            time.monotonic() - opened,
            sub.sent,
            sub.dropped,
        )
        broadcast.unsubscribe(sub)
