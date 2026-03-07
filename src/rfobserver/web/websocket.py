"""WebSocket endpoint for live spectrogram and detection streaming."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)

_QueueType = asyncio.Queue[dict[str, Any]]


class LiveBroadcast:
    """Broadcast channel for live data to WebSocket subscribers."""

    def __init__(self) -> None:
        self._subscribers: set[_QueueType] = set()

    def subscribe(self) -> _QueueType:
        queue: _QueueType = asyncio.Queue(maxsize=10)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: _QueueType) -> None:
        self._subscribers.discard(queue)

    async def publish(self, data: dict[str, Any]) -> None:
        for queue in list(self._subscribers):
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(data)


async def websocket_endpoint(websocket: WebSocket, broadcast: LiveBroadcast) -> None:
    """Handle a WebSocket connection for live data streaming."""
    await websocket.accept()
    queue = broadcast.subscribe()

    try:
        while True:
            data = await queue.get()
            await websocket.send_json(data)
    except WebSocketDisconnect:
        pass
    finally:
        broadcast.unsubscribe(queue)
