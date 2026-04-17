"""WebSocket endpoint for live spectrogram and detection streaming."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)

_QueueType = asyncio.Queue[dict[str, Any]]


class _Subscriber:
    """Per-client subscriber state."""

    __slots__ = ("queue", "high_res")

    def __init__(self) -> None:
        self.queue: _QueueType = asyncio.Queue(maxsize=10)
        self.high_res: bool = False


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
        return any(s.high_res for s in self._subscribers)

    async def publish(self, data: dict[str, Any]) -> None:
        # Separate grid_rows from the base message — only send to high_res clients
        grid_rows = data.pop("grid_rows", None)

        for sub in list(self._subscribers):
            msg = data
            if sub.high_res and grid_rows is not None:
                msg = {**data, "grid_rows": grid_rows}
            with contextlib.suppress(asyncio.QueueFull):
                sub.queue.put_nowait(msg)


async def websocket_endpoint(websocket: WebSocket, broadcast: LiveBroadcast) -> None:
    """Handle a WebSocket connection for live data streaming."""
    await websocket.accept()
    sub = broadcast.subscribe()

    async def send_loop() -> None:
        while True:
            data = await sub.queue.get()
            await websocket.send_json(data)

    async def recv_loop() -> None:
        while True:
            text = await websocket.receive_text()
            try:
                msg = json.loads(text)
            except json.JSONDecodeError:
                continue
            if msg.get("type") == "set_mode":
                sub.high_res = bool(msg.get("high_res", False))
                logger.info("Client set high_res=%s", sub.high_res)

    try:
        await asyncio.gather(send_loop(), recv_loop())
    except WebSocketDisconnect:
        pass
    finally:
        broadcast.unsubscribe(sub)
