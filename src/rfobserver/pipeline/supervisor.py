"""Runtime start/stop of the capture pipeline (the "Sensor Active" toggle).

Owns the receiver + processor lifecycle so the sensor can be put into Standby
(processor stopped, SDR released) and brought back on demand, with the caller
awaiting the actual transition for confirmation.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from rfobserver.capture.receiver import IReceiver

logger = logging.getLogger(__name__)

# Bound on how long to wait for a stopped processor's run() to drain before
# cancelling it, so a wedged pipeline can't hang the toggle forever.
_STOP_TIMEOUT_SEC = 15.0


class PipelineSupervisor:
    """Starts/stops the capture pipeline and releases the SDR when inactive."""

    def __init__(
        self,
        build_receiver: Callable[[], IReceiver],
        build_processor: Callable[[IReceiver], Any],
        on_processor_change: Callable[[Any | None], None] | None = None,
    ) -> None:
        self._build_receiver = build_receiver
        self._build_processor = build_processor
        self._on_processor_change = on_processor_change
        self._receiver: IReceiver | None = None
        self._processor: Any | None = None
        self._task: asyncio.Task[Any] | None = None
        self._active = False
        self._lock = asyncio.Lock()

    @property
    def active(self) -> bool:
        return self._active

    @property
    def processor(self) -> Any | None:
        return self._processor

    @property
    def receiver(self) -> IReceiver | None:
        return self._receiver

    async def set_active(self, active: bool) -> bool:
        """Transition to ``active`` and return the actual resulting state.

        Redundant calls (already in the requested state) are no-ops. The return
        value is the confirmation the API/UI settle on.
        """
        async with self._lock:
            if active and not self._active:
                await self._start()
            elif not active and self._active:
                await self._stop()
            return self._active

    async def _start(self) -> None:
        loop = asyncio.get_running_loop()
        receiver = self._build_receiver()
        # initialize() claims + configures hardware (blocking) — run off-loop.
        await loop.run_in_executor(None, receiver.initialize)
        processor = self._build_processor(receiver)
        self._receiver = receiver
        self._processor = processor
        self._task = asyncio.create_task(processor.run())
        self._active = True
        logger.info("Sensor activated")
        self._notify(processor)

    async def _stop(self) -> None:
        loop = asyncio.get_running_loop()
        processor, task, receiver = self._processor, self._task, self._receiver
        if processor is not None:
            processor.stop()
        if task is not None:
            try:
                await asyncio.wait_for(task, timeout=_STOP_TIMEOUT_SEC)
            except TimeoutError:
                logger.warning("Processor did not stop in time; cancelling")
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.exception("Processor raised during cancellation")
        if receiver is not None:
            await loop.run_in_executor(None, receiver.close)
        self._processor = None
        self._receiver = None
        self._task = None
        self._active = False
        logger.info("Sensor deactivated (SDR released)")
        self._notify(None)

    def _notify(self, processor: Any | None) -> None:
        if self._on_processor_change is not None:
            self._on_processor_change(processor)
