"""Dashboard page route -- the averaged-history view, served as the landing page."""

from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from rfobserver.web.uiprefs import UI_PREFS_KEY, ui_theme

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
@router.get("/averaged/", response_class=HTMLResponse)
async def averaged_page(request: Request) -> Any:
    """The historical averaged-window view: range selector, stats timeline,
    time-bucketed PSD waterfall with selector line, and the range's detections.

    This is the landing Dashboard ("/"); "/averaged/" remains as the original
    URL. The page is fully client-driven: configs/waterfall/stats/detections
    are fetched from the JSON + binary API endpoints by averaged.js.
    """
    templates = request.app.state.templates
    settings = request.app.state.settings
    return templates.TemplateResponse(
        request,
        "averaged.html",
        {
            "ui_theme": await ui_theme(request),
            "display_name": settings.SENSOR_NAME or settings.HOSTNAME,
            "boot": await _boot_data(request),
        },
    )


async def _boot_data(request: Request) -> dict[str, Any]:
    """The tuning configs and display preferences the page needs before its
    first data request, embedded so it does not wait two round trips for
    them. Anything missing here the page fetches itself."""
    from rfobserver.web.routes.api import _normalize_prefs

    db = getattr(request.app.state, "database", None)
    boot: dict[str, Any] = {}
    if db is None:
        return boot
    try:
        boot["configs"] = await db.avg_window_configs()
    except Exception:
        logger.exception("Dashboard boot data: configs unavailable")
    try:
        raw = await db.get_config(UI_PREFS_KEY)
        boot["prefs"] = _normalize_prefs(json.loads(raw) if raw else {})
    except Exception:
        logger.exception("Dashboard boot data: prefs unavailable")
    return boot
