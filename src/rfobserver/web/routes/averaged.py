"""Dashboard page route -- the averaged-history view, served as the landing page."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from rfobserver.web.uiprefs import ui_theme

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
    return templates.TemplateResponse(
        request, "averaged.html", {"ui_theme": await ui_theme(request)}
    )
