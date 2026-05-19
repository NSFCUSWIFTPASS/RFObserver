"""Dashboard route -- live spectrogram, detections, system status."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> Any:
    templates = request.app.state.templates
    settings = request.app.state.settings
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "settings": settings,
            "hostname": settings.HOSTNAME,
            "display_name": settings.SENSOR_NAME or settings.HOSTNAME,
            "frequency_start": settings.FREQUENCY_START,
            "frequency_end": settings.FREQUENCY_END,
            "bandwidth": settings.BANDWIDTH,
        },
    )
