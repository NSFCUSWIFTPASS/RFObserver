"""Main async orchestrator for the RFObserver pipeline.

Manages the full capture -> process -> detect -> store -> publish loop
with concurrent web server operation.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from rfobserver.web.websocket import LiveBroadcast

if TYPE_CHECKING:
    from rfobserver.capture.receiver import IReceiver
    from rfobserver.config import AppSettings

logger = logging.getLogger(__name__)


async def run(settings: AppSettings) -> None:
    """Start the full sensor pipeline."""
    from rfobserver.capture.mock_receiver import MockReceiver
    from rfobserver.capture.receiver import ReceiverConfig
    from rfobserver.storage.database import SensorDatabase
    from rfobserver.storage.local import LocalStorage

    logger.info("RFObserver pipeline starting (hostname=%s)", settings.HOSTNAME)

    receiver_config = ReceiverConfig(
        gain_db=settings.GAIN,
        bandwidth_hz=settings.BANDWIDTH,
        duration_sec=settings.DURATION_SEC,
    )

    receiver: IReceiver
    if settings.MOCK_RECEIVER:
        receiver = MockReceiver(receiver_config)
        logger.info("Using mock receiver")
    else:
        from rfobserver.capture.receiver import Receiver

        receiver = Receiver(receiver_config)

    receiver.initialize()

    db = SensorDatabase(settings.DB_PATH)
    await db.connect()

    local_storage = LocalStorage(settings.STORAGE_PATH, max_gb=settings.ARCHIVE_MAX_GB)
    broadcast = LiveBroadcast()

    # ZMS monitor (optional)
    zms_monitor = None
    if settings.zms:
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

    # Choose pipeline mode: streaming for single-freq / trigger, batch for sweeps
    is_sweep = settings.FREQUENCY_STEP > 0 and settings.FREQUENCY_END > settings.FREQUENCY_START
    use_streaming = settings.TRIGGER_ENABLED or not is_sweep

    if use_streaming:
        from rfobserver.pipeline.streaming import StreamingProcessor

        processor = StreamingProcessor(
            receiver=receiver,
            database=db,
            local_storage=local_storage,
            settings=settings,
            broadcast=broadcast,
            zms_monitor=zms_monitor,
            nats_producer=nats_producer,
        )

        # Attach module manager for upstream signal processing
        from rfobserver.modules.manager import ModuleManager

        processor._module_manager = ModuleManager()

        logger.info("Using streaming pipeline")
    else:
        from rfobserver.pipeline.continuous import ContinuousProcessor

        processor = ContinuousProcessor(  # type: ignore[assignment]
            receiver=receiver,
            database=db,
            local_storage=local_storage,
            settings=settings,
            broadcast=broadcast,
            zms_monitor=zms_monitor,
            nats_producer=nats_producer,
        )
        logger.info("Using batch pipeline (sweep mode)")

    tasks = [processor.run()]
    if zms_monitor is not None:
        tasks.append(zms_monitor.run())
    if settings.WEB_PORT > 0:
        tasks.append(_run_web_server(settings, processor, db, broadcast))
        tasks.append(_heartbeat_loop(settings, processor, broadcast))

    try:
        await asyncio.gather(*tasks)
    finally:
        if zms_monitor is not None:
            await zms_monitor.stop()
        if nats_producer is not None:
            await nats_producer.close()
        await db.close()


async def _heartbeat_loop(
    settings: AppSettings,
    processor: object,
    broadcast: LiveBroadcast,
    interval_sec: float = 1.0,
) -> None:
    """Push slow-changing state to /ws/live so the dashboard can stop polling.

    Sends one ``type: "heartbeat"`` message per ``interval_sec`` to every
    WebSocket subscriber. Carries the rendered status-bar HTML and the
    recording-status dict — the two things the dashboard used to fetch
    on a 2 s + 1 s polling cadence.
    """
    from rfobserver.web.routes.api import build_status_bar_html

    while True:
        try:
            rec_status: dict[str, object]
            if hasattr(processor, "recording_status"):
                rec_status = processor.recording_status()
            else:
                rec_status = {"state": "idle", "file": None, "bytes": 0, "duration_sec": 0}

            await broadcast.publish(
                {
                    "type": "heartbeat",
                    "status_bar_html": build_status_bar_html(settings),
                    "recording": rec_status,
                }
            )
        except Exception:
            logger.exception("Heartbeat publish failed; continuing")

        await asyncio.sleep(interval_sec)


async def _run_web_server(
    settings: AppSettings,
    processor: object,
    database: object,
    broadcast: LiveBroadcast,
) -> None:
    """Run the FastAPI web server as an async task."""
    import uvicorn

    from rfobserver.web.app import create_app

    app = create_app(settings)
    app.state.processor = processor
    app.state.database = database
    app.state.broadcast = broadcast

    config = uvicorn.Config(
        app,
        host=settings.WEB_HOST,
        port=settings.WEB_PORT,
        log_level=settings.LOG_LEVEL.lower(),
    )
    server = uvicorn.Server(config)
    await server.serve()
