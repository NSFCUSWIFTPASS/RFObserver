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
    templates = request.app.state.templates

    # Distinct SDR capture configs and decoded models present in the data, for
    # the filter dropdowns.
    configs: list[dict[str, Any]] = []
    models: list[str] = []
    db = getattr(request.app.state, "database", None)
    if db is not None:
        try:
            configs = await db.capture_configs()
        except Exception:
            configs = []
        try:
            models = await db.detection_models()
        except Exception:
            models = []

    # Derive the distinct option sets each filter offers.
    centers = sorted({c["sdr_center_freq_hz"] for c in configs if c.get("sdr_center_freq_hz")})
    sample_rates = sorted({c["sample_rate_hz"] for c in configs if c.get("sample_rate_hz")})
    gains = sorted({c["gain_db"] for c in configs if c.get("gain_db") is not None})

    return templates.TemplateResponse(
        request,
        "detections.html",
        {
            "ui_theme": await ui_theme(request),
            "centers": centers,
            "sample_rates": sample_rates,
            "gains": gains,
            "models": models,
        },
    )


@legacy_router.get("/history", include_in_schema=False)
@legacy_router.get("/history/", include_in_schema=False)
async def history_redirect(request: Request) -> RedirectResponse:
    """308 to /detections, keeping the query string (a permanent move)."""
    url = "/detections"
    if request.url.query:
        url += "?" + request.url.query
    return RedirectResponse(url, status_code=308)
