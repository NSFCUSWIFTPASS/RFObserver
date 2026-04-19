"""Base class for upstream signal processing modules."""

from __future__ import annotations

import asyncio
import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing import Any

    import numpy as np


class UpstreamModule(ABC):
    """Abstract base for signal processing modules.

    Modules receive raw SC16 IQ chunks from the receiver thread via
    ``feed()`` and produce output (e.g. audio PCM) on an asyncio queue.
    All heavy DSP should run on the GPU (CuPy) to avoid CPU contention
    with the existing PSD pipeline.
    """

    module_type: str = "base"

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        self.module_id: str = str(uuid.uuid4())[:8]
        self._params: dict[str, Any] = params or {}
        self._output_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=64)

    @abstractmethod
    def configure(self, params: dict[str, Any]) -> None:
        """Update module parameters at runtime."""

    @abstractmethod
    def feed(self, sc16_buf: np.ndarray, center_freq_hz: int, sample_rate: int) -> None:
        """Receive a chunk of SC16 IQ data from the receiver thread.

        Must not block — enqueue internally for async processing.
        """

    @abstractmethod
    def start(self) -> None:
        """Start the module's processing thread."""

    @abstractmethod
    def stop(self) -> None:
        """Stop the module and release resources."""

    @abstractmethod
    def status(self) -> dict[str, Any]:
        """Return current module status for the API."""

    @property
    def output_queue(self) -> asyncio.Queue[bytes]:
        """Queue of output data (e.g. PCM audio) for WebSocket streaming."""
        return self._output_queue
