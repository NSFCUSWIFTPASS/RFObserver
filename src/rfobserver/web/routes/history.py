"""Detection history route."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def history_page(request: Request) -> Any:
    templates = request.app.state.templates

    # Distinct SDR capture configs present in the data, for the filter dropdowns.
    configs: list[dict[str, Any]] = []
    db = getattr(request.app.state, "database", None)
    if db is not None:
        try:
            configs = await db.capture_configs()
        except Exception:
            configs = []

    # Derive the distinct option sets each filter offers.
    centers = sorted({c["sdr_center_freq_hz"] for c in configs if c.get("sdr_center_freq_hz")})
    sample_rates = sorted({c["sample_rate_hz"] for c in configs if c.get("sample_rate_hz")})
    gains = sorted({c["gain_db"] for c in configs if c.get("gain_db") is not None})

    return templates.TemplateResponse(
        request,
        "history.html",
        {
            "centers": centers,
            "sample_rates": sample_rates,
            "gains": gains,
        },
    )
