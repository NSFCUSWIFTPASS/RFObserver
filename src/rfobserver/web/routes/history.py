"""Detection history route."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def history_page(request: Request) -> Any:
    templates = request.app.state.templates
    return templates.TemplateResponse(request, "history.html")
