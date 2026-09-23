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
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rfobserver.storage.rollup import ROLLUP_NEWEST_KEY, ROLLUP_OLDEST_KEY, WindowRow, fold_windows
from rfobserver.web.websocket import LiveBroadcast

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    import uvicorn

    from rfobserver.capture.receiver import IReceiver
    from rfobserver.config import AppSettings
    from rfobserver.pipeline.beacon import ProgressBeacon
    from rfobserver.pipeline.supervisor import PipelineSupervisor
    from rfobserver.storage.database import SensorDatabase

logger = logging.getLogger(__name__)

_GIVE_UP_EXIT_CODE = 91
_GIVE_UP_EXIT_DELAY_SEC = 5.0
_STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM)
_WEB_SHUTDOWN_TIMEOUT_SEC = 5.0
# uvicorn cancels its own request and websocket tasks (e.g. a quiet /ws/audio
# that never calls receive) before our 5s bound above, which stays as a backstop.
_WEB_GRACEFUL_SHUTDOWN_SEC = 3  # int: uvicorn types it as int | None
# iter_rollup_windows chunks each span into execute_fetchall calls on the writer
# connection, and aiosqlite serialises all operations on that connection through
# one worker thread. The streaming pipeline awaits insert_avg_window inline on
# that same connection, feeding a bounded queue that drops (rather than blocks)
# once the pipeline falls behind, so one oversized rollup statement can stall
# the writer long enough to lose live data. The span here is only an indirect
# cap on rows per statement; the explicit chunk= passed to iter_rollup_windows
# below is what actually bounds it regardless of span or DURATION_SEC.
_ROLLUP_SPAN = timedelta(minutes=15)
# Wall-clock budget per pass, so a cold backfill of a month finishes in minutes
# without any single pass blocking the loop.
_ROLLUP_BUDGET_SEC = 5.0


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

    # Where uvicorn is not installed (the CI lint job) mypy sees Server as Any;
    # where it is, the ignore is unused, hence both codes.
    class _AppSignalsServer(uvicorn.Server):  # type: ignore[misc,unused-ignore]
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

    from rfobserver.storage.governor import DEGRADED_CONFIG_KEY, StorageGovernor

    storage_governor = StorageGovernor()
    try:
        storage_governor.restore_degraded(await db.get_config(DEGRADED_CONFIG_KEY))
    except Exception:
        logger.exception("Could not read the persisted storage degraded flag")
    retention_wake = asyncio.Event()

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
                storage_governor=storage_governor,
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
            _run_web_server(
                settings,
                supervisor,
                read_db,
                db,
                broadcast,
                beacon,
                stop,
                storage_governor=storage_governor,
            )
        )
        workers.append(web_task)
        workers.append(
            asyncio.create_task(
                _heartbeat_loop(
                    settings,
                    supervisor,
                    read_db,
                    local_storage,
                    broadcast,
                    governor=storage_governor,
                )
            )
        )
    # Retention always runs: even with DB_RETENTION_DAYS=0 the storage governor
    # may need the step 2 pressure cutoffs.
    workers.append(
        asyncio.create_task(_cleanup_loop(settings, db, storage_governor, retention_wake))
    )
    workers.append(
        asyncio.create_task(
            _storage_loop(settings, storage_governor, db, local_storage, supervisor, retention_wake)
        )
    )
    if settings.PEAKS_ROLLUP_INTERVAL_SEC > 0:
        workers.append(asyncio.create_task(_rollup_loop(settings, db)))
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
    governor: Any = None,
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
                    "storage": governor.state.to_health() if governor is not None else None,
                }
            )
        except Exception:
            logger.exception("Heartbeat publish failed; continuing")

        await asyncio.sleep(interval_sec)


def _retention_days(configured: int, pressure_cap: int, pressure: bool) -> int:
    """Retention in days for one class of data: the configured value, cut to
    the pressure cap at storage step >= 2 (which applies even when the
    configured retention is disabled). 0 = do not prune."""
    if not pressure:
        return configured
    return pressure_cap if configured <= 0 else min(configured, pressure_cap)


async def _run_retention(settings: AppSettings, db: Any, *, pressure: bool) -> None:
    """One retention pass. Each part has its own try so one failure does not
    stop the rest, and the pipeline keeps running regardless."""
    from rfobserver.storage.governor import PRESSURE_DETECTION_DAYS, PRESSURE_PSD_DAYS

    parts: list[tuple[str, int]] = [
        ("blobs", _retention_days(settings.DB_RETENTION_DAYS, PRESSURE_PSD_DAYS, pressure)),
        (
            "detections",
            _retention_days(settings.STATS_RETENTION_DAYS, PRESSURE_DETECTION_DAYS, pressure),
        ),
        ("avg_windows", settings.STATS_RETENTION_DAYS),
        ("avg_minutes", settings.STATS_RETENTION_DAYS),
    ]
    for what, days in parts:
        if days <= 0:
            continue
        try:
            if what == "blobs":
                await db.prune_avg_psd_blobs(days)
            else:
                await db.delete_older_than(what, days)
        except Exception:
            logger.exception("Retention of %s failed; continuing", what)


async def _cleanup_loop(
    settings: AppSettings,
    db: Any,
    governor: Any = None,
    wake: asyncio.Event | None = None,
) -> None:
    """Scheduled DB retention.

    PSD blobs expire after DB_RETENTION_DAYS; stats rows, detections and
    minute rollups after STATS_RETENTION_DAYS. At storage step >= 2 the blob
    and detection cutoffs tighten to the pressure caps. Runs one pass
    immediately, then every DB_CLEANUP_INTERVAL_SEC, or at once when ``wake``
    is set (the storage loop sets it on entering step 2).
    """
    while True:
        pressure = governor is not None and governor.state.pressure
        await _run_retention(settings, db, pressure=pressure)
        if wake is None:
            await asyncio.sleep(settings.DB_CLEANUP_INTERVAL_SEC)
            continue
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(wake.wait(), timeout=settings.DB_CLEANUP_INTERVAL_SEC)
        wake.clear()


def _active_capture_names(supervisor: Any) -> set[str]:
    """The capture being recorded or finalized, which eviction must not take."""
    proc = getattr(supervisor, "processor", None)
    if proc is None or not hasattr(proc, "recording_status"):
        return set()
    st = proc.recording_status()
    name = st.get("file")
    if st.get("state") in ("recording", "finalizing") and name:
        return {str(name)}
    return set()


async def _storage_tick(
    settings: AppSettings,
    governor: Any,
    db: Any,
    local_storage: Any,
    supervisor: Any,
    retention_wake: asyncio.Event,
) -> None:
    """One governor tick: sample, decide, act, persist the sticky flag."""
    from rfobserver.storage.governor import DEGRADED_CONFIG_KEY

    active = _active_capture_names(supervisor)
    db_file, db_reusable = await db.file_stats()
    sample = await asyncio.to_thread(
        local_storage.sample,
        db_path=Path(settings.DB_PATH),
        active_names=active,
        db_file_bytes=db_file,
        db_reusable_bytes=db_reusable,
    )
    prev_step = governor.state.step
    actions = governor.tick(
        sample, min_free_gb=settings.DISK_MIN_FREE_GB, now=datetime.now(timezone.utc)
    )
    st = governor.state
    if st.step != prev_step:
        log = logger.warning if st.step > prev_step else logger.info
        log(
            "Storage step %d -> %d (%s): %.1f GB free, floor %.1f GB",
            prev_step,
            st.step,
            st.to_health()["step_text"],
            sample.data.free_bytes / 1024**3,
            st.floor_bytes / 1024**3,
        )
    if actions.evict_to_free_bytes is not None:
        await asyncio.to_thread(
            local_storage.evict_until_free, actions.evict_to_free_bytes, exclude=active
        )
    if actions.start_pressure_prune:
        retention_wake.set()
    changed, value = governor.take_degraded_change()
    if changed:
        try:
            await db.set_config(DEGRADED_CONFIG_KEY, value)
        except Exception:
            logger.exception("Could not persist the storage degraded flag")


async def _storage_loop(
    settings: AppSettings,
    governor: Any,
    db: Any,
    local_storage: Any,
    supervisor: Any,
    retention_wake: asyncio.Event,
) -> None:
    """Every STORAGE_CHECK_SEC: one governor tick. A failed tick is logged and
    the loop continues; the published state keeps its last value."""
    while True:
        try:
            await _storage_tick(settings, governor, db, local_storage, supervisor, retention_wake)
        except Exception:
            logger.exception("Storage check failed; continuing")
        await asyncio.sleep(max(1.0, float(settings.STORAGE_CHECK_SEC)))


def _minute_str(when: datetime) -> str:
    """Minute-resolution key, matching avg_minutes.minute_start."""
    return when.strftime("%Y-%m-%dT%H:%M")


def _parse_minute(key: str) -> datetime:
    return datetime.fromisoformat(key + ":00+00:00")


async def _rollup_span(db: SensorDatabase, since: datetime, until: datetime) -> int:
    """Fold one bounded span of windows into avg_minutes."""
    rows: list[WindowRow] = []
    async for chunk in db.iter_rollup_windows(since=since, until=until, chunk=1000):
        rows.extend(chunk)
    if not rows:
        return 0
    written: int = await db.upsert_avg_minutes(fold_windows(rows))
    return written


async def _rollup_forward(db: SensorDatabase, now: datetime) -> None:
    """Fold every minute that has closed since the last run."""
    closed = now.replace(second=0, microsecond=0)
    key = await db.get_config(ROLLUP_NEWEST_KEY)
    if key is None:
        # First run: anchor at the current minute and let the backfill reach
        # back, so a fresh start does not scan the whole table up front.
        await db.set_config(ROLLUP_NEWEST_KEY, _minute_str(closed))
        return
    since = _parse_minute(key)
    deadline = time.monotonic() + _ROLLUP_BUDGET_SEC
    while since < closed and time.monotonic() < deadline:
        until = min(since + _ROLLUP_SPAN, closed)
        await _rollup_span(db, since, until)
        since = until
        await db.set_config(ROLLUP_NEWEST_KEY, _minute_str(since))


async def _rollup_backfill(db: SensorDatabase, now: datetime) -> None:
    """Extend the rollup backwards, newest history first."""
    oldest_window = await db.oldest_avg_window_time()
    if oldest_window is None:
        return
    key = await db.get_config(ROLLUP_OLDEST_KEY)
    if key is None:
        key = await db.get_config(ROLLUP_NEWEST_KEY)
        if key is None:
            return
        await db.set_config(ROLLUP_OLDEST_KEY, key)
    until = _parse_minute(key)
    floor = oldest_window.replace(second=0, microsecond=0)
    deadline = time.monotonic() + _ROLLUP_BUDGET_SEC
    while until > floor and time.monotonic() < deadline:
        since = max(until - _ROLLUP_SPAN, floor)
        await _rollup_span(db, since, until)
        until = since
        await db.set_config(ROLLUP_OLDEST_KEY, _minute_str(until))


async def _rollup_loop(settings: AppSettings, db: SensorDatabase) -> None:
    """Keep avg_minutes in step with avg_windows.

    The forward pass folds minutes that have just closed (about 120 windows).
    The backfill pass deepens history newest-first, so the peak finder works on
    recent data immediately instead of waiting for a full pass over the table.
    """
    while True:
        try:
            now = datetime.now(timezone.utc)
            await _rollup_forward(db, now)
            await _rollup_backfill(db, now)
        except Exception:
            logger.exception("avg_minutes rollup pass failed")
        await asyncio.sleep(settings.PEAKS_ROLLUP_INTERVAL_SEC)


async def _run_web_server(
    settings: AppSettings,
    supervisor: PipelineSupervisor,
    database: object,
    write_database: object,
    broadcast: LiveBroadcast,
    beacon: ProgressBeacon,
    stop: asyncio.Event,
    storage_governor: Any = None,
) -> None:
    """Run the FastAPI web server as an async task."""
    import uvicorn

    from rfobserver.web.app import create_app

    app = create_app(settings)
    app.state.supervisor = supervisor
    app.state.beacon = beacon
    app.state.database = database
    app.state.write_database = write_database
    app.state.storage_governor = storage_governor
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
