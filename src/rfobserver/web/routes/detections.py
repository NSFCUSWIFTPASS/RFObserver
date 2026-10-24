"""Detections page route (formerly the History page at /history)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from rfobserver.web.uiprefs import ui_theme

router = APIRouter()

# Mounted without a prefix: permanent redirects from the old /history URL.
legacy_router = APIRouter()


@router.get("", response_class=HTMLResponse, include_in_schema=False)
@router.get("/", response_class=HTMLResponse)
async def detections_page(request: Request) -> Any:
    """The fleet's Detections page, for this sensor alone (its data from
    /api/detections/hourly, /api/detections/list and /api/detection)."""
    from rfobserver.web.routes.api import _sensor_meta

    return request.app.state.templates.TemplateResponse(
        request,
        "detections.html",
        {"ui_theme": await ui_theme(request), "sensor": _sensor_meta(request)},
    )


@legacy_router.get("/history", include_in_schema=False)
@legacy_router.get("/history/", include_in_schema=False)
async def history_redirect(request: Request) -> RedirectResponse:
    """308 to /detections, keeping the query string (a permanent move)."""
    url = "/detections"
    if request.url.query:
        url += "?" + request.url.query
    return RedirectResponse(url, status_code=308)
