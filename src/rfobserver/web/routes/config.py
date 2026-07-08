"""Sensor configuration route."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import SecretStr

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def config_page(request: Request) -> Any:
    templates = request.app.state.templates
    settings = request.app.state.settings
    # A supervisor is only attached when the full pipeline is running. In
    # web-only mode the Sensor Active toggle can't act, so render it disabled and
    # reflect the live state when a supervisor is present.
    supervisor = getattr(request.app.state, "supervisor", None)
    sensor_available = supervisor is not None
    sensor_active = supervisor.active if supervisor is not None else settings.SENSOR_ACTIVE
    return templates.TemplateResponse(
        request,
        "config.html",
        {
            "settings": settings,
            "sensor_available": sensor_available,
            "sensor_active": sensor_active,
        },
    )


def _persist_settings(settings: Any) -> None:
    """Write current settings to .env for persistence across restarts.

    Pydantic-settings reads .env on startup (env_prefix=RFOBS_),
    so this ensures all runtime changes survive a restart.
    """
    from rfobserver.config import AppSettings

    defaults = AppSettings(_env_file=None)
    env_path = Path(".env")

    lines: list[str] = []
    for field_name in type(settings).model_fields:
        if field_name in ("NATS_URL", "zms"):
            continue  # computed properties, not settable
        val = getattr(settings, field_name)
        default_val = getattr(defaults, field_name)

        # Skip unchanged defaults to keep .env clean
        if val == default_val:
            continue

        # Handle SecretStr
        if isinstance(val, SecretStr):
            val = val.get_secret_value()
        elif val is None:
            continue

        lines.append(f"RFOBS_{field_name}={val}")

    try:
        env_path.write_text("\n".join(lines) + "\n" if lines else "")
        logger.debug("Settings persisted to %s (%d values)", env_path, len(lines))
    except OSError:
        logger.warning("Failed to persist settings to %s", env_path)


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
        "trigger_threshold_db": ("TRIGGER_THRESHOLD_DB", float),
        "burst_threshold_high_db": ("BURST_THRESHOLD_HIGH_DB", float),
        "burst_threshold_low_ratio": ("BURST_THRESHOLD_LOW_RATIO", float),
        "psd_time_resolution_ms": ("PSD_TIME_RESOLUTION_MS", float),
        "num_fft_bins": ("NUM_FFT_BINS", int),
        "archive_max_gb": ("ARCHIVE_MAX_GB", float),
        "history_days": ("HISTORY_DAYS", int),
        # Sensor identity + location (all optional — empty input clears)
        "sensor_name": ("SENSOR_NAME", str),
        "latitude": ("LATITUDE", float),
        "longitude": ("LONGITUDE", float),
        # Display (all optional — empty input reverts to uncalibrated/auto)
        "cal_offset_db": ("CAL_OFFSET_DB", float),
        "psd_scale_min_db": ("PSD_SCALE_MIN_DB", float),
        "psd_scale_max_db": ("PSD_SCALE_MAX_DB", float),
        # NATS
        "nats_host": ("NATS_HOST", str),
        "nats_port": ("NATS_PORT", int),
        # ZMS
        "zms_zmc_http": ("ZMS_ZMC_HTTP", str),
        "zms_dst_http": ("ZMS_DST_HTTP", str),
        "zms_identity_http": ("ZMS_IDENTITY_HTTP", str),
        "zms_monitor_id": ("ZMS_MONITOR_ID", str),
        "zms_monitor_name": ("ZMS_MONITOR_NAME", str),
    }

    # Tokens are SecretStr and use a "(unchanged)" placeholder in the UI;
    # only update when a non-empty value is submitted.
    secret_map: dict[str, str] = {
        "nats_token": "NATS_TOKEN",
        "zms_token": "ZMS_TOKEN",
    }

    # Settings that legitimately accept None — submitting an empty input on
    # the config form clears them. Keep this list tight; for required fields
    # we want the cast to raise so the user notices.
    nullable_attrs = {
        "SENSOR_NAME",
        "LATITUDE",
        "LONGITUDE",
        "CAL_OFFSET_DB",
        "PSD_SCALE_MIN_DB",
        "PSD_SCALE_MAX_DB",
    }

    changed = []
    for form_key, (attr, cast) in field_map.items():
        if form_key not in body:
            continue
        raw = body[form_key]
        if attr in nullable_attrs and (raw is None or raw == ""):
            new_val = None
        else:
            try:
                new_val = cast(raw)
            except (ValueError, TypeError) as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid value for {form_key}",
                ) from exc

        # Validate FFT bins: must be a power of 2 in [256, 8192]
        if attr == "NUM_FFT_BINS":
            valid_bins = {256, 512, 1024, 2048, 4096, 8192}
            if new_val not in valid_bins:
                raise HTTPException(
                    status_code=400,
                    detail=f"num_fft_bins must be one of {sorted(valid_bins)}",
                )

        old_val = getattr(settings, attr)
        if old_val != new_val:
            object.__setattr__(settings, attr, new_val)
            changed.append(attr)

    for form_key, attr in secret_map.items():
        if form_key not in body:
            continue
        raw = body[form_key]
        if raw is None or raw == "":
            continue  # placeholder: don't overwrite existing secret
        object.__setattr__(settings, attr, SecretStr(str(raw)))
        changed.append(attr)

    if not changed:
        logger.info("Config applied: no changes")
        return {"status": "ok", "changed": changed}

    # Signal the processor to pick up pipeline-affecting changes.
    # The receiver loop will stop streaming, reconfigure hardware if needed,
    # rebuild buffers, and resume — all safely between stream stop/start.
    pipeline_fields = {
        "BANDWIDTH",
        "GAIN",
        "DURATION_SEC",
        "NUM_FFT_BINS",
        "PSD_TIME_RESOLUTION_MS",
        "FREQUENCY_START",
        "FREQUENCY_END",
        "FREQUENCY_STEP",
        "TRIGGER_THRESHOLD_DB",
        "BURST_THRESHOLD_HIGH_DB",
        "BURST_THRESHOLD_LOW_RATIO",
    }
    if pipeline_fields & set(changed) and processor is not None:
        reconfigure = getattr(processor, "reconfigure", None)
        if reconfigure is not None:
            reconfigure()
            logger.info("Pipeline reconfigured: %s", changed)

    # Persist settings to .env so they survive restarts
    _persist_settings(settings)

    logger.info("Config applied: %s", changed)
    return {"status": "ok", "changed": changed}
