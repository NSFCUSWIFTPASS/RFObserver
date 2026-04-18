"""Captures page — list and inspect IQ recordings."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def captures_page(request: Request) -> Any:
    templates = request.app.state.templates
    return templates.TemplateResponse(request, "captures.html")


@router.get("/list")
async def captures_list(request: Request) -> list[dict[str, Any]]:
    """List all .sc16 capture files with metadata."""
    settings = request.app.state.settings
    from pathlib import Path

    storage = Path(settings.STORAGE_PATH)
    if not storage.exists():
        return []

    captures: list[dict[str, Any]] = []
    for sc16 in sorted(storage.glob("*.sc16"), key=lambda f: f.stat().st_mtime, reverse=True):
        entry: dict[str, Any] = {
            "filename": sc16.name,
            "size_bytes": sc16.stat().st_size,
        }

        # Load companion .json if it exists
        json_path = sc16.with_suffix(".json")
        if json_path.exists():
            try:
                meta = json.loads(json_path.read_text())
                entry["meta"] = meta
            except (json.JSONDecodeError, OSError):
                entry["meta"] = None
        else:
            entry["meta"] = None

        captures.append(entry)

    return captures


@router.get("/detail/{filename}")
async def capture_detail(request: Request, filename: str) -> dict[str, Any]:
    """Get details for a single capture file."""
    settings = request.app.state.settings
    from pathlib import Path

    storage = Path(settings.STORAGE_PATH)
    sc16_path = storage / filename

    if not sc16_path.exists() or not filename.endswith(".sc16"):
        return {"error": "File not found"}

    result: dict[str, Any] = {
        "filename": filename,
        "size_bytes": sc16_path.stat().st_size,
    }

    json_path = sc16_path.with_suffix(".json")
    if json_path.exists():
        try:
            result["meta"] = json.loads(json_path.read_text())
        except (json.JSONDecodeError, OSError):
            result["meta"] = None
    else:
        result["meta"] = None

    return result
