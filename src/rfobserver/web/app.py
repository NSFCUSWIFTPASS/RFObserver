"""FastAPI application for the local WebUI."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from rfobserver.__about__ import __version__
from rfobserver.config import AppSettings

if TYPE_CHECKING:
    from rfobserver.web.websocket import LiveBroadcast

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

    from rfobserver.web.routes import api, config, dashboard, history

    app.include_router(dashboard.router)
    app.include_router(config.router, prefix="/config")
    app.include_router(history.router, prefix="/history")
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

        await websocket.accept()
        queue = broadcast.subscribe()
        try:
            while True:
                data = await queue.get()
                await websocket.send_json(data)
        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            broadcast.unsubscribe(queue)

    return app
