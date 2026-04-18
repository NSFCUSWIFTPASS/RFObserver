"""FastAPI application for the local WebUI."""

from __future__ import annotations

from pathlib import Path

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

    app = FastAPI(
        title="RFObserver",
        version=__version__,
        docs_url="/api/docs",
        redoc_url=None,
    )

    app.state.settings = settings
    app.state.templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    app.state.processor = None
    app.state.database = None
    app.state.broadcast = None

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    from rfobserver.web.routes import api, captures, config, dashboard, history

    app.include_router(dashboard.router)
    app.include_router(config.router, prefix="/config")
    app.include_router(history.router, prefix="/history")
    app.include_router(captures.router, prefix="/captures")
    app.include_router(api.router, prefix="/api")

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    @app.websocket("/ws/live")
    async def ws_live(websocket: WebSocket) -> None:
        broadcast: LiveBroadcast | None = getattr(app.state, "broadcast", None)
        if broadcast is None:
            await websocket.close(code=1011)
            return

        await websocket_endpoint(websocket, broadcast)

    return app
