"""FastAPI application for the local WebUI."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from rfobserver.__about__ import __version__
from rfobserver.config import AppSettings
from rfobserver.web.websocket import LiveBroadcast, websocket_endpoint

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"


def create_app(settings: AppSettings | None = None) -> FastAPI:
    """Create and configure the FastAPI application."""
    if settings is None:
        settings = AppSettings()

    # API docs page removed from the UI; the /api/* data routes stay available
    # for external automations.
    app = FastAPI(
        title="RFObserver",
        version=__version__,
        docs_url=None,
        redoc_url=None,
    )

    app.state.settings = settings
    app.state.templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    app.state.processor = None
    app.state.database = None
    app.state.write_database = None
    app.state.broadcast = None

    # One heavy Dashboard aggregation of each kind at a time. On a field-size DB a
    # 24 h waterfall decodes ~74k PSD blobs on the shared event loop; four at once
    # pegged the loop and tripped the pipeline watchdog on nano-super. Extra tabs
    # wait their turn instead of starving the pipeline.
    app.state.waterfall_sem = asyncio.Semaphore(1)
    app.state.stats_sem = asyncio.Semaphore(1)

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    from rfobserver.web.routes import api, averaged, captures, config, dashboard, history, modules

    app.include_router(dashboard.router)
    app.include_router(config.router, prefix="/config")
    app.include_router(history.router, prefix="/history")
    app.include_router(captures.router, prefix="/captures")
    app.include_router(averaged.router)
    app.include_router(api.router, prefix="/api")
    app.include_router(modules.router, prefix="/api")

    @app.get("/api/health")
    async def health() -> dict[str, Any]:
        body: dict[str, Any] = {"status": "ok", "version": __version__}
        sup = getattr(app.state, "supervisor", None)
        if sup is not None:
            beacon = getattr(app.state, "beacon", None)
            proc = sup.processor
            has_loss = proc is not None and hasattr(proc, "receive_loss")
            loss = proc.receive_loss() if has_loss else None
            body["pipeline"] = {
                "active": sup.active,
                "gave_up": sup.gave_up,
                "consecutive_crashes": sup.consecutive_crashes,
                # Only meaningful while running; a stale age while active is a stall.
                "beacon_age_sec": round(beacon.age(), 1)
                if beacon is not None and sup.active
                else None,
                "overflow_events": loss["overflow_events"] if loss else None,
                "overflow_lost_samples": loss["overflow_lost_samples"] if loss else None,
            }
            if sup.gave_up:
                body["status"] = "degraded"
        return body

    @app.websocket("/ws/live")
    async def ws_live(websocket: WebSocket) -> None:
        broadcast: LiveBroadcast | None = getattr(app.state, "broadcast", None)
        if broadcast is None:
            await websocket.close(code=1011)
            return

        await websocket_endpoint(websocket, broadcast)

    return app
