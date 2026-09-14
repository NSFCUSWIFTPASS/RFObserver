"""Main async orchestrator for the RFObserver pipeline.

Manages the full capture -> process -> detect -> store -> publish loop
with concurrent web server operation.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
from typing import TYPE_CHECKING, Any

from rfobserver.web.websocket import LiveBroadcast

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    import uvicorn

    from rfobserver.capture.receiver import IReceiver
    from rfobserver.config import AppSettings
    from rfobserver.pipeline.beacon import ProgressBeacon
    from rfobserver.pipeline.supervisor import PipelineSupervisor

logger = logging.getLogger(__name__)

_GIVE_UP_EXIT_CODE = 91
_GIVE_UP_EXIT_DELAY_SEC = 5.0
_STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM)
_WEB_SHUTDOWN_TIMEOUT_SEC = 5.0
# uvicorn cancels its own request and websocket tasks (e.g. a quiet /ws/audio
# that never calls receive) before our 5s bound above, which stays as a backstop.
_WEB_GRACEFUL_SHUTDOWN_SEC = 3  # int: uvicorn types it as int | None


def make_give_up_handler(
    enabled: bool,
    exit_fn: Callable[[int], object] = os._exit,
    delay_sec: float = _GIVE_UP_EXIT_DELAY_SEC,
) -> Callable[[], None]:
    """Build the supervisor's give-up hook: exit for systemd after a short delay.

    The delay lets /api/health report the give-up and the log flush first. The
    SDR is already released by the supervisor's stop before this runs.
    """

    def handler() -> None:
        if not enabled:
            return
        logger.error(
            "Pipeline auto-restart gave up; exiting (code %d) in %.0fs so systemd "
            "starts a fresh process",
            _GIVE_UP_EXIT_CODE,
            delay_sec,
        )
        asyncio.get_running_loop().call_later(delay_sec, exit_fn, _GIVE_UP_EXIT_CODE)

    return handler


def install_stop_signals(
    loop: asyncio.AbstractEventLoop,
    stop: asyncio.Event,
    force_exit: Callable[[int], object] = os._exit,
) -> Callable[[], None]:
    """Route SIGINT and SIGTERM to ``stop`` so run() can tear down in order.

    Left to the defaults, uvicorn re-raises SIGTERM with SIG_DFL after serving
    (the process dies before run()'s cleanup), and on Python 3.10 a SIGINT's
    KeyboardInterrupt makes asyncio.run cancel every task, which aborts the
    cleanup before the DB close and hangs exit on aiosqlite's non-daemon
    thread. See docs/debugging/2026-09-14_shutdown-signals.md.

    The first signal sets ``stop``; a second one exits at once with
    128 + signal number. Returns a function that removes the handlers. Signal
    handlers can only be installed from the main thread: elsewhere this logs a
    warning and returns a no-op.
    """
    received: list[signal.Signals] = []

    def on_signal(sig: signal.Signals) -> None:
        if received:
            logger.error("Second %s during shutdown; exiting now", sig.name)
            force_exit(128 + sig.value)
            return
        received.append(sig)
        logger.info("Received %s; shutting down", sig.name)
        stop.set()

    def remove() -> None:
        for sig in _STOP_SIGNALS:
            loop.remove_signal_handler(sig)

    try:
        for sig in _STOP_SIGNALS:
            loop.add_signal_handler(sig, on_signal, sig)
    except (RuntimeError, ValueError):
        remove()
        logger.warning("Cannot install SIGINT/SIGTERM handlers outside the main thread")
        return lambda: None
    return remove


def _build_web_server(config: uvicorn.Config) -> uvicorn.Server:
    """A uvicorn server that leaves SIGINT and SIGTERM to run()."""
    import uvicorn

    class _AppSignalsServer(uvicorn.Server):
        @contextlib.contextmanager
        def capture_signals(self) -> Generator[None, None, None]:
            # The stock version re-raises the captured signal after serving;
            # for SIGTERM that is SIG_DFL, which kills the process before
            # run()'s cleanup. run() owns the signals (install_stop_signals).
            yield

    return _AppSignalsServer(config)


async def run(settings: AppSettings) -> None:
    """Start the full sensor pipeline."""
    from rfobserver.capture.mock_receiver import MockReceiver
    from rfobserver.capture.receiver import ReceiverConfig
    from rfobserver.pipeline.beacon import ProgressBeacon
    from rfobserver.pipeline.supervisor import PipelineSupervisor
    from rfobserver.storage.database import SensorDatabase
    from rfobserver.storage.local import LocalStorage

    logger.info("RFObserver pipeline starting (hostname=%s)", settings.HOSTNAME)

    beacon = ProgressBeacon()

    receiver_config = ReceiverConfig(
        gain_db=settings.GAIN,
        bandwidth_hz=settings.BANDWIDTH,
        duration_sec=settings.DURATION_SEC,
    )

    db = SensorDatabase(settings.DB_PATH)
    await db.connect()

    # Web-layer reader (spec Cut 3b): Dashboard reads get their own connection so
    # they never queue pipeline writes. Connect after the writer (schema owner).
    # Only the web server and its heartbeat use it, so headless runs skip it.
    read_db: SensorDatabase | None = None
    if settings.WEB_PORT > 0:
        read_db = SensorDatabase(settings.DB_PATH, read_only=True)
        try:
            await read_db.connect()
        except BaseException:
            await db.close()
            raise

    local_storage = LocalStorage(settings.STORAGE_PATH, max_gb=settings.ARCHIVE_MAX_GB)
    broadcast = LiveBroadcast()

    # ZMS monitor (optional). Two conditions both required:
    #   settings.zms       — all four URLs/tokens populated (settings valid)
    #   settings.ZMS_ENABLED — user has it toggled on (persisted in .env)
    zms_monitor = None
    if settings.zms and settings.ZMS_ENABLED:
        from rfobserver.zms.monitor import ZmsMonitor

        zms_monitor = ZmsMonitor(settings.zms)
        await zms_monitor.start()
        logger.info("ZMS monitor enabled (id=%s)", settings.zms.monitor_id)

    # NATS producer (optional). Pipeline tolerates connection failure: on
    # error we log and proceed without NATS rather than aborting startup.
    nats_producer = None
    if settings.NATS_ENABLED:
        from rfobserver.transport.nats_producer import NatsProducer

        token = settings.NATS_TOKEN.get_secret_value() if settings.NATS_TOKEN else None
        nats_producer = NatsProducer(url=settings.NATS_URL, token=token)
        try:
            await nats_producer.connect()
            logger.info("NATS producer connected (%s)", settings.NATS_URL)
        except Exception:
            logger.exception("NATS connect failed; continuing without NATS")
            nats_producer = None

    # The receiver and processor are built lazily by the supervisor when the
    # sensor is activated, so a Standby start never claims the SDR.
    def build_receiver() -> IReceiver:
        if settings.MOCK_RECEIVER:
            logger.info("Using mock receiver")
            return MockReceiver(receiver_config)
        from rfobserver.capture.receiver import Receiver

        return Receiver(receiver_config)

    def build_processor(receiver: IReceiver, *, replay_mode: bool = False) -> Any:
        # streaming for single-freq / trigger, batch for sweeps
        is_sweep = settings.FREQUENCY_STEP > 0 and settings.FREQUENCY_END > settings.FREQUENCY_START
        use_streaming = settings.TRIGGER_ENABLED or not is_sweep
        if use_streaming:
            from rfobserver.modules.manager import ModuleManager
            from rfobserver.pipeline.streaming import StreamingProcessor

            proc: Any = StreamingProcessor(
                receiver=receiver,
                database=db,
                local_storage=local_storage,
                settings=settings,
                broadcast=broadcast,
                zms_monitor=zms_monitor,
                nats_producer=nats_producer,
                replay_mode=replay_mode,
                beacon=beacon,
            )
            # Attach module manager for upstream signal processing
            proc._module_manager = ModuleManager()
            logger.info("Using streaming pipeline")
            return proc
        from rfobserver.pipeline.continuous import ContinuousProcessor

        logger.info("Using batch pipeline (sweep mode)")
        return ContinuousProcessor(
            receiver=receiver,
            database=db,
            local_storage=local_storage,
            settings=settings,
            broadcast=broadcast,
            zms_monitor=zms_monitor,
            nats_producer=nats_producer,
            beacon=beacon,
        )

    supervisor = PipelineSupervisor(
        build_receiver=build_receiver,
        build_processor=build_processor,
        on_give_up=make_give_up_handler(settings.EXIT_ON_CRASH_GIVE_UP),
    )
    if settings.SENSOR_ACTIVE:
        await supervisor.set_active(True)
    else:
        logger.info("Sensor starting in Standby (SENSOR_ACTIVE=false)")

    watchdog = None
    if settings.WATCHDOG_ENABLED:
        from rfobserver.utils.watchdog import PipelineWatchdog

        if settings.WATCHDOG_STOP_TIMEOUT_SEC + 3.0 >= settings.WATCHDOG_RESTART_DEADLINE_SEC:
            logger.warning(
                "WATCHDOG_STOP_TIMEOUT_SEC (%.1fs) + SDR re-init (~2.3s) leaves no "
                "room inside WATCHDOG_RESTART_DEADLINE_SEC (%.1fs); watchdog restarts "
                "will likely escalate to process exit",
                settings.WATCHDOG_STOP_TIMEOUT_SEC,
                settings.WATCHDOG_RESTART_DEADLINE_SEC,
            )

        watchdog = PipelineWatchdog(
            beacon,
            is_active=lambda: supervisor.active,
            restart=lambda: supervisor.restart(stop_timeout=settings.WATCHDOG_STOP_TIMEOUT_SEC),
            loop=asyncio.get_running_loop(),
            timeout_sec=settings.WATCHDOG_TIMEOUT_SEC,
            restart_deadline_sec=settings.WATCHDOG_RESTART_DEADLINE_SEC,
        )
        watchdog.start()
        logger.info("Pipeline watchdog enabled (timeout=%.0fs)", settings.WATCHDOG_TIMEOUT_SEC)

    stop = asyncio.Event()
    remove_stop_signals = install_stop_signals(asyncio.get_running_loop(), stop)

    web_task: asyncio.Task[None] | None = None
    workers: list[asyncio.Task[Any]] = []
    if zms_monitor is not None:
        workers.append(asyncio.create_task(zms_monitor.run()))
    if read_db is not None:
        web_task = asyncio.create_task(
            _run_web_server(settings, supervisor, read_db, db, broadcast, beacon, stop)
        )
        workers.append(web_task)
        workers.append(
            asyncio.create_task(
                _heartbeat_loop(settings, supervisor, read_db, local_storage, broadcast)
            )
        )
    if settings.DB_RETENTION_DAYS > 0:
        workers.append(asyncio.create_task(_cleanup_loop(settings, db)))
    # Serve until a stop signal. The supervisor owns the processor task
    # independently of these, so a Standby or headless run waits here too.
    stop_task = asyncio.create_task(stop.wait())

    try:
        pending: set[asyncio.Task[Any]] = {stop_task, *workers}
        while not stop.is_set():
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for finished in done:
                if finished is stop_task or finished.cancelled():
                    continue
                exc = finished.exception()
                if exc is not None:
                    raise exc
    finally:
        # The DB closes and remove_stop_signals() must run even if a step
        # below raises a BaseException (e.g. CancelledError): otherwise the
        # non-daemon aiosqlite thread hangs interpreter exit forever. Each
        # step inside this try is still isolated with except Exception so one
        # failure cannot skip the next; CancelledError is not caught here and
        # propagates after the closes below run.
        try:
            if watchdog is not None:
                watchdog.stop()
            await _stop_workers(stop, stop_task, web_task, workers)
            # Each step is isolated so one failure cannot skip the DB close.
            try:
                await supervisor.set_active(False)
            except Exception:
                logger.exception("Shutdown: stopping the pipeline failed; continuing")
            if zms_monitor is not None:
                try:
                    await zms_monitor.stop()
                except Exception:
                    logger.exception("Shutdown: stopping the ZMS monitor failed; continuing")
            if nats_producer is not None:
                try:
                    await nats_producer.close()
                except Exception:
                    logger.exception("Shutdown: closing NATS failed; continuing")
        finally:
            try:
                try:
                    if read_db is not None:
                        await read_db.close()
                finally:
                    await db.close()
                logger.info("Shutdown complete")
            finally:
                remove_stop_signals()


async def _stop_workers(
    stop: asyncio.Event,
    stop_task: asyncio.Task[Any],
    web_task: asyncio.Task[None] | None,
    workers: list[asyncio.Task[Any]],
) -> None:
    """Let the web server finish (bounded), then cancel the other loops."""
    stop.set()  # _run_web_server turns this into uvicorn's should_exit
    if web_task is not None and not web_task.done():
        _, still_running = await asyncio.wait({web_task}, timeout=_WEB_SHUTDOWN_TIMEOUT_SEC)
        if still_running:
            logger.warning(
                "Web server did not stop within %.0fs; cancelling", _WEB_SHUTDOWN_TIMEOUT_SEC
            )
    for task in (stop_task, *workers):
        task.cancel()
    results = await asyncio.gather(stop_task, *workers, return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
            logger.warning("Worker ended with an error during shutdown: %r", result)


async def _heartbeat_loop(
    settings: AppSettings,
    supervisor: PipelineSupervisor,
    db: object,
    local_storage: object,
    broadcast: LiveBroadcast,
    interval_sec: float = 1.0,
) -> None:
    """Push slow-changing state to /ws/live so each page can stop polling.

    Carries one ``type: "heartbeat"`` message per ``interval_sec`` containing
    everything dashboard / config / captures / history used to fetch on a
    periodic timer. Two monotonic counters (``detection_count``,
    ``capture_count``) let clients trigger HTML-fragment refreshes only when
    the underlying state actually changes — REST endpoints stay intact for
    automations that aren't on a websocket. Reads the supervisor's current
    processor each tick so the state reflects Standby (no processor) live.
    """
    from pathlib import Path

    from rfobserver.web.routes.api import (
        build_nats_status_payload,
        build_status_bar_html,
        build_zms_status_payload,
    )
    from rfobserver.web.routes.modules import build_modules_payload

    storage_path = Path(getattr(local_storage, "storage_path", "."))

    while True:
        try:
            processor = supervisor.processor
            module_manager = getattr(processor, "_module_manager", None)
            if processor is not None and hasattr(processor, "recording_status"):
                rec_status: dict[str, object] = processor.recording_status()
            else:
                rec_status = {"state": "idle", "file": None, "bytes": 0, "duration_sec": 0}

            try:
                detection_count = (
                    await db.count_detections() if hasattr(db, "count_detections") else 0
                )
            except Exception:
                detection_count = 0

            try:
                # rglob: captures live under auto/ and manual/ subdirs now.
                capture_count = (
                    sum(1 for _ in storage_path.rglob("*.sc16")) if storage_path.exists() else 0
                )
            except Exception:
                capture_count = 0

            await broadcast.publish(
                {
                    "type": "heartbeat",
                    "status_bar_html": build_status_bar_html(settings, active=supervisor.active),
                    "recording": rec_status,
                    "replay": supervisor.replay_status(),
                    "zms": build_zms_status_payload(settings, processor),
                    "nats": build_nats_status_payload(settings, processor),
                    "modules": build_modules_payload(module_manager),
                    "detection_count": detection_count,
                    "capture_count": capture_count,
                }
            )
        except Exception:
            logger.exception("Heartbeat publish failed; continuing")

        await asyncio.sleep(interval_sec)


async def _cleanup_loop(settings: AppSettings, db: Any) -> None:
    """Scheduled DB retention: null out PSD blobs older than DB_RETENTION_DAYS.

    Only the heavy PSD/violations blobs of ``avg_windows`` are evicted; the
    stats rows, detections, and tone_checks are kept permanently. Runs one
    pass immediately, then repeats every ``DB_CLEANUP_INTERVAL_SEC``. Each
    pass is wrapped in try/except so a transient DB error never kills the
    process (the pipeline keeps running).
    """
    while True:
        try:
            removed = await db.prune_avg_psd_blobs(settings.DB_RETENTION_DAYS)
            logger.info(
                "Retention: pruned PSD blobs for %d windows older than %d days",
                removed,
                settings.DB_RETENTION_DAYS,
            )
        except Exception:
            logger.exception("Retention cleanup failed; continuing")

        await asyncio.sleep(settings.DB_CLEANUP_INTERVAL_SEC)


async def _run_web_server(
    settings: AppSettings,
    supervisor: PipelineSupervisor,
    database: object,
    write_database: object,
    broadcast: LiveBroadcast,
    beacon: ProgressBeacon,
    stop: asyncio.Event,
) -> None:
    """Run the FastAPI web server as an async task."""
    import uvicorn

    from rfobserver.web.app import create_app

    app = create_app(settings)
    app.state.supervisor = supervisor
    app.state.beacon = beacon
    app.state.database = database
    app.state.write_database = write_database
    app.state.broadcast = broadcast
    app.state.processor = supervisor.processor

    # Keep app.state.processor pointed at the live processor (or None in
    # Standby) as the supervisor starts/stops it.
    def _sync_processor(processor: object | None) -> None:
        app.state.processor = processor

    supervisor._on_processor_change = _sync_processor

    config = uvicorn.Config(
        app,
        host=settings.WEB_HOST,
        port=settings.WEB_PORT,
        log_level=settings.LOG_LEVEL.lower(),
        timeout_graceful_shutdown=_WEB_GRACEFUL_SHUTDOWN_SEC,
    )
    server = _build_web_server(config)

    async def _exit_on_stop() -> None:
        await stop.wait()
        server.should_exit = True

    watcher = asyncio.create_task(_exit_on_stop())
    try:
        await server.serve()
    finally:
        watcher.cancel()
