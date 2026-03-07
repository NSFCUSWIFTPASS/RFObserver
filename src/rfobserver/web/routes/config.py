"""Sensor configuration route."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def config_page(request: Request) -> Any:
    templates = request.app.state.templates
    settings = request.app.state.settings
    return templates.TemplateResponse(
        request,
        "config.html",
        {
            "settings": settings,
        },
    )
