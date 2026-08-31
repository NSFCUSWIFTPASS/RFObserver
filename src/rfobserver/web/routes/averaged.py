"""Averaged-history page route."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/averaged/", response_class=HTMLResponse)
async def averaged_page(request: Request) -> Any:
    """The historical averaged-window view: range selector, stats timeline,
    time-bucketed PSD waterfall with selector line, and the range's detections.

    The page is fully client-driven: configs/waterfall/stats/detections are
    fetched from the JSON + binary API endpoints by averaged.js.
    """
    templates = request.app.state.templates
    return templates.TemplateResponse(request, "averaged.html", {})
