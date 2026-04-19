"""Upstream module REST API + audio WebSocket."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_manager(request: Request) -> Any:
    proc = getattr(request.app.state, "processor", None)
    if proc is None:
        return None
    return getattr(proc, "_module_manager", None)


@router.get("/modules")
async def list_modules(request: Request) -> dict[str, Any]:
    mgr = _get_manager(request)
    if mgr is None:
        return {"modules": [], "available_types": {}}
    modules = mgr.list_modules()
    # Add has_audio to each module status
    for m_status in modules:
        mid = m_status.get("module_id")
        mod = mgr.get_module(mid) if mid else None
        if mod:
            m_status["has_audio"] = mod.has_audio_output
    return {
        "modules": modules,
        "available_types": mgr.registry_info(),
    }


@router.post("/modules")
async def create_module(request: Request) -> dict[str, Any]:
    mgr = _get_manager(request)
    if mgr is None:
        raise HTTPException(status_code=503, detail="Pipeline not running")

    body = await request.json()
    module_type = body.get("type")
    params = body.get("params", {})

    if not module_type:
        raise HTTPException(status_code=400, detail="'type' is required")

    try:
        module = mgr.create_module(module_type, params)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    result: dict[str, Any] = module.status()
    result["has_audio"] = module.has_audio_output
    return result


@router.get("/modules/{module_id}")
async def get_module(request: Request, module_id: str) -> dict[str, Any]:
    mgr = _get_manager(request)
    if mgr is None:
        raise HTTPException(status_code=503, detail="Pipeline not running")

    module = mgr.get_module(module_id)
    if module is None:
        raise HTTPException(status_code=404, detail="Module not found")

    result: dict[str, Any] = module.status()
    result["has_audio"] = module.has_audio_output
    return result


@router.patch("/modules/{module_id}")
async def update_module(request: Request, module_id: str) -> dict[str, Any]:
    mgr = _get_manager(request)
    if mgr is None:
        raise HTTPException(status_code=503, detail="Pipeline not running")

    module = mgr.get_module(module_id)
    if module is None:
        raise HTTPException(status_code=404, detail="Module not found")

    body = await request.json()
    module.configure(body)
    result: dict[str, Any] = module.status()
    result["has_audio"] = module.has_audio_output
    return result


@router.delete("/modules/{module_id}")
async def delete_module(request: Request, module_id: str) -> dict[str, str]:
    mgr = _get_manager(request)
    if mgr is None:
        raise HTTPException(status_code=503, detail="Pipeline not running")

    module = mgr.get_module(module_id)
    if module is None:
        raise HTTPException(status_code=404, detail="Module not found")

    mgr.remove_module(module_id)
    return {"status": "removed", "module_id": module_id}


@router.websocket("/ws/audio/{module_id}")
async def audio_websocket(websocket: WebSocket, module_id: str) -> None:
    """Stream PCM audio from a module to the browser."""
    proc = getattr(websocket.app.state, "processor", None)
    mgr = getattr(proc, "_module_manager", None) if proc else None

    if mgr is None:
        await websocket.close(code=1011)
        return

    module = mgr.get_module(module_id)
    if module is None:
        await websocket.close(code=1011)
        return

    await websocket.accept()

    # Send config frame first
    import json

    config = {
        "type": "audio_config",
        "sample_rate": module.audio_sample_rate,
        "channels": 1,
        "format": "int16",
    }
    await websocket.send_text(json.dumps(config))

    try:
        while True:
            try:
                pcm_data = await asyncio.wait_for(module.output_queue.get(), timeout=2.0)
            except TimeoutError:
                if not module._running:
                    break
                continue
            await websocket.send_bytes(pcm_data)
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("Audio WebSocket error for module %s", module_id)
