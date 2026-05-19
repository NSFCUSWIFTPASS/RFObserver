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
        tasks.append(_heartbeat_loop(settings, processor, db, local_storage, broadcast))

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
    automations that aren't on a websocket.
    """
    from pathlib import Path

    from rfobserver.web.routes.api import (
        build_nats_status_payload,
        build_status_bar_html,
        build_zms_status_payload,
    )
    from rfobserver.web.routes.modules import build_modules_payload

    storage_path = Path(getattr(local_storage, "storage_path", "."))
    module_manager = getattr(processor, "_module_manager", None)

    while True:
        try:
            if hasattr(processor, "recording_status"):
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
                capture_count = (
                    sum(1 for _ in storage_path.glob("*.sc16")) if storage_path.exists() else 0
                )
            except Exception:
                capture_count = 0

            await broadcast.publish(
                {
                    "type": "heartbeat",
                    "status_bar_html": build_status_bar_html(settings),
                    "recording": rec_status,
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
