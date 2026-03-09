"""Sensor configuration route."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from rfobserver.capture.receiver import ReceiverConfig

logger = logging.getLogger(__name__)

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


@router.post("/apply")
async def apply_config(request: Request) -> dict[str, Any]:
    """Apply configuration changes to the running pipeline."""
    settings = request.app.state.settings
    processor = getattr(request.app.state, "processor", None)

    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON") from exc

    # Map form fields to settings attributes
    field_map: dict[str, tuple[str, type]] = {
        "frequency_start": ("FREQUENCY_START", int),
        "frequency_end": ("FREQUENCY_END", int),
        "frequency_step": ("FREQUENCY_STEP", int),
        "bandwidth": ("BANDWIDTH", int),
        "gain": ("GAIN", int),
        "duration_sec": ("DURATION_SEC", float),
        "burst_threshold_high_db": ("BURST_THRESHOLD_HIGH_DB", float),
        "burst_threshold_low_ratio": ("BURST_THRESHOLD_LOW_RATIO", float),
        "psd_time_resolution_ms": ("PSD_TIME_RESOLUTION_MS", float),
        "num_fft_bins": ("NUM_FFT_BINS", int),
        "archive_max_gb": ("ARCHIVE_MAX_GB", float),
        "history_days": ("HISTORY_DAYS", int),
    }

    changed = []
    for form_key, (attr, cast) in field_map.items():
        if form_key not in body:
            continue
        try:
            new_val = cast(body[form_key])
        except (ValueError, TypeError) as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid value for {form_key}",
            ) from exc

        old_val = getattr(settings, attr)
        if old_val != new_val:
            object.__setattr__(settings, attr, new_val)
            changed.append(attr)

    # If receiver-affecting settings changed, reconfigure the receiver
    receiver_fields = {"BANDWIDTH", "GAIN", "DURATION_SEC"}
    if changed and receiver_fields & set(changed) and processor is not None:
        receiver = getattr(processor, "_receiver", None)
        if receiver is not None:
            new_config = ReceiverConfig(
                gain_db=settings.GAIN,
                bandwidth_hz=settings.BANDWIDTH,
                duration_sec=settings.DURATION_SEC,
            )
            await receiver.reconfigure(new_config)
            logger.info("Receiver reconfigured: %s", changed)

    logger.info("Config applied: %s", changed if changed else "no changes")
    return {"status": "ok", "changed": changed}
