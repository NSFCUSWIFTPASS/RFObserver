"""Base class for upstream signal processing modules."""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import numpy as np


@dataclass(frozen=True, slots=True)
class ParamDescriptor:
    """Describes a tunable module parameter for generic UI rendering."""

    name: str  # key in _params dict
    label: str  # human-readable label
    type: str  # "number" | "range" | "select"
    default: float | int | str
    unit: str = ""  # e.g. "MHz", "Hz", "%"
    min: float | None = None
    max: float | None = None
    step: float | None = None
    options: list[str] = field(default_factory=list)  # for "select" type


class UpstreamModule(ABC):
    """Abstract base for signal processing modules.

    Modules receive raw SC16 IQ chunks from the receiver thread via
    ``feed()`` and produce output (e.g. audio PCM) on an asyncio queue.
    The queue belongs to the event loop the module was created on and is not
    thread-safe, so a processing thread hands output over with ``emit()``.
    All heavy DSP should run on the GPU (CuPy) to avoid CPU contention
    with the existing PSD pipeline.
    """

    module_type: str = "base"
    has_audio_output: bool = False
    audio_sample_rate: int = 48_000

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        self.module_id: str = str(uuid.uuid4())[:8]
        self._params: dict[str, Any] = params or {}
        self._output_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=64)
        # The loop the queue's reader runs on (modules are created from a
        # request handler); None outside one (tests), where emit() puts directly.
        try:
            self._loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None
        self.output_dropped = 0

    @classmethod
    @abstractmethod
    def parameters(cls) -> list[ParamDescriptor]:
        """Declare tunable parameters for the dashboard UI."""

    @abstractmethod
    def configure(self, params: dict[str, Any]) -> None:
        """Update module parameters at runtime."""

    @abstractmethod
    def feed(self, sc16_buf: np.ndarray, center_freq_hz: int, sample_rate: int) -> None:
        """Receive a chunk of SC16 IQ data from the receiver thread."""

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

    def emit(self, data: bytes) -> None:
        """Queue output from any thread. A full queue drops its oldest item, so
        a slow or absent reader gets the newest output, not a growing delay."""
        if self._loop is None:
            self._put_latest(data)
            return
        with contextlib.suppress(RuntimeError):  # the loop has closed (shutdown)
            self._loop.call_soon_threadsafe(self._put_latest, data)

    def _put_latest(self, data: bytes) -> None:
        if self._output_queue.full():
            self._output_queue.get_nowait()
            self.output_dropped += 1
        self._output_queue.put_nowait(data)

    def feed_burst(self, iq: np.ndarray, sample_rate: int, meta: dict[str, Any]) -> None:
        """Receive one isolated burst (complex64 at ``sample_rate``, its peak at
        DC). Called on the isolation worker thread; keep it quick. Optional:
        modules that only use the wideband stream ignore it."""
        return None
