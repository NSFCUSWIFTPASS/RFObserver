"""Runtime start/stop of the capture pipeline (the "Sensor Active" toggle).

Owns the receiver + processor lifecycle so the sensor can be put into Standby
(processor stopped, SDR released) and brought back on demand, with the caller
awaiting the actual transition for confirmation.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from rfobserver.capture.receiver import IReceiver

logger = logging.getLogger(__name__)

# Bound on how long to wait for a stopped processor's run() to drain before
# cancelling it, so a wedged pipeline can't hang the toggle forever.
_STOP_TIMEOUT_SEC = 15.0

# Crash-restart flap protection: without this, a persistent processor.run()
# crash would thrash receiver.initialize()/close() on the real SDR in a tight
# loop. A crash streak that goes quiet for _CRASH_RESET_WINDOW_SEC resets;
# otherwise each restart backs off (capped) and the streak gives up entirely
# past _MAX_CONSECUTIVE_CRASH_RESTARTS, leaving the sensor inactive.
_CRASH_RESET_WINDOW_SEC = 120.0
_MAX_CONSECUTIVE_CRASH_RESTARTS = 5
_CRASH_BACKOFF_CAP_SEC = 30.0


class PipelineSupervisor:
    """Starts/stops the capture pipeline and releases the SDR when inactive."""

    def __init__(
        self,
        build_receiver: Callable[[], IReceiver],
        build_processor: Callable[..., Any],
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
        self._receiver_override: IReceiver | None = None
        self._replay = False
        self._stopping = False
        self._consecutive_crashes = 0
        self._last_crash_ts = 0.0

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
                # A deliberate manual activation clears any prior crash streak.
                self._consecutive_crashes = 0
                await self._start()
            elif not active and self._active:
                await self._stop()
            return self._active

    async def start_replay(self, receiver: IReceiver) -> None:
        """Stop any live pipeline and start with `receiver` in replay mode."""
        if self._active:
            await self.set_active(False)
        async with self._lock:
            self._receiver_override = receiver
            self._replay = True
            await self._start()

    async def stop_replay(self) -> None:
        """Stop the replay pipeline and clear the override (leaves sensor stopped)."""
        async with self._lock:
            if self._active:
                await self._stop()
            self._receiver_override = None
            self._replay = False

    def replay_status(self) -> dict[str, Any] | None:
        if not self._replay or self._receiver is None:
            return None
        rx = self._receiver
        return {
            "source": getattr(rx, "source_name", "") or "replay",
            "speed": float(getattr(rx, "speed", 1.0)),
            "looping": bool(getattr(rx, "loop", False)),
        }

    async def _start(self) -> None:
        loop = asyncio.get_running_loop()
        receiver = self._receiver_override or self._build_receiver()
        # initialize() claims + configures hardware (blocking) — run off-loop.
        await loop.run_in_executor(None, receiver.initialize)
        processor = self._build_processor(receiver, replay_mode=self._replay)
        self._receiver = receiver
        self._processor = processor
        self._task = asyncio.create_task(processor.run())
        self._task.add_done_callback(self._on_task_done)
        self._active = True
        logger.info("Sensor activated")
        self._notify(processor)

    async def _stop(self, timeout: float | None = None) -> None:
        self._stopping = True
        try:
            stop_timeout = _STOP_TIMEOUT_SEC if timeout is None else timeout
            loop = asyncio.get_running_loop()
            processor, task, receiver = self._processor, self._task, self._receiver
            if processor is not None:
                processor.stop()
            if task is not None:
                try:
                    await asyncio.wait_for(task, timeout=stop_timeout)
                except (TimeoutError, asyncio.TimeoutError):  # noqa: UP041 - not the builtin on 3.10
                    logger.warning("Processor did not stop in time; cancelling")
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        logger.exception("Processor raised during cancellation")
                except Exception:
                    # The task may already be done-with-exception (e.g. we are
                    # stopping it after a crash, from _restart_after_crash) --
                    # already logged by _on_task_done, so don't let it escape.
                    logger.debug(
                        "Stop observed an already-raised task exception (already reported)",
                        exc_info=True,
                    )
            if receiver is not None:
                await loop.run_in_executor(None, receiver.close)
            self._processor = None
            self._receiver = None
            self._task = None
            self._active = False
            self._receiver_override = None
            self._replay = False
            logger.info("Sensor deactivated (SDR released)")
            self._notify(None)
        finally:
            self._stopping = False

    def _notify(self, processor: Any | None) -> None:
        if self._on_processor_change is not None:
            self._on_processor_change(processor)

    def _on_task_done(self, task: asyncio.Task[Any]) -> None:
        """Detect an unexpected pipeline-task death and schedule a restart.

        Runs for every task completion, including the deliberate cancel/await
        inside `_stop()` -- `_stopping` is what tells those apart from a crash.
        """
        if task.cancelled() or self._stopping:
            return
        exc = task.exception()
        if exc is None:
            return
        logger.error("Pipeline task died unexpectedly; restarting", exc_info=exc)
        if self._active:
            asyncio.get_running_loop().create_task(self._restart_after_crash())

    async def _restart_after_crash(self) -> None:
        now = time.monotonic()
        if now - self._last_crash_ts > _CRASH_RESET_WINDOW_SEC:
            self._consecutive_crashes = 0
        self._last_crash_ts = now
        self._consecutive_crashes += 1

        if self._consecutive_crashes > _MAX_CONSECUTIVE_CRASH_RESTARTS:
            logger.error(
                "Pipeline crashed %d times within %.0fs; giving up auto-restart, "
                "leaving sensor inactive",
                self._consecutive_crashes,
                _CRASH_RESET_WINDOW_SEC,
            )
            async with self._lock:
                if self._active and not self._replay:
                    await self._stop()
            return

        backoff = min(2.0 ** (self._consecutive_crashes - 1), _CRASH_BACKOFF_CAP_SEC)
        logger.warning(
            "Restarting pipeline after crash in %.1fs (attempt %d)",
            backoff,
            self._consecutive_crashes,
        )
        # Sleep BEFORE acquiring the lock so a concurrent deliberate stop can
        # win the race: it stops the sensor while we wait, and when we wake we
        # see _active False (or _replay True) and no-op instead of restarting.
        await asyncio.sleep(backoff)
        async with self._lock:
            if not self._active or self._replay:
                return
            await self._stop()
            await self._start()

    async def restart(self, stop_timeout: float | None = None) -> None:
        """Stop then start the live pipeline. No-op if inactive or replaying.

        ``stop_timeout`` bounds how long to wait for the old task before
        cancelling it; the watchdog passes a value well inside its restart
        deadline so a hung-but-cancellable pipeline restarts in-process.
        """
        async with self._lock:
            if not self._active or self._replay:
                return
            await self._stop(timeout=stop_timeout)
            await self._start()
