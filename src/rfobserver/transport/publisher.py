"""Abstract publisher interface for outbound message transport."""

from __future__ import annotations

from typing import Protocol


class IPublisher(Protocol):
    """Protocol for publishing messages to the companion server."""

    async def connect(self) -> None: ...

    async def publish(self, subject: str, data: bytes) -> None: ...

    async def close(self) -> None: ...
