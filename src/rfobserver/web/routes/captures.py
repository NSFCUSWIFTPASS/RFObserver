"""Captures page — list, inspect, and view IQ recordings."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

router = APIRouter()


def _get_storage(request: Request) -> Path:
    return Path(request.app.state.settings.STORAGE_PATH)


def _validate_filename(filename: str, storage: Path) -> Path:
    """Validate filename has no path traversal and resolve to storage path."""
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    resolved = (storage / filename).resolve()
    if not str(resolved).startswith(str(storage.resolve())):
        raise HTTPException(status_code=400, detail="Invalid filename")
    return resolved


@router.get("/", response_class=HTMLResponse)
async def captures_page(request: Request) -> Any:
    templates = request.app.state.templates
    return templates.TemplateResponse(request, "captures.html")


@router.get("/list")
async def captures_list(request: Request) -> list[dict[str, Any]]:
    """List all .sc16 capture files with metadata."""
    storage = _get_storage(request)
    if not storage.exists():
        return []

    captures: list[dict[str, Any]] = []
    for sc16 in sorted(storage.glob("*.sc16"), key=lambda f: f.stat().st_mtime, reverse=True):
        entry: dict[str, Any] = {
            "filename": sc16.name,
            "size_bytes": sc16.stat().st_size,
            "has_psd": sc16.with_suffix(".npz").exists(),
        }

        json_path = sc16.with_suffix(".json")
        if json_path.exists():
            try:
                entry["meta"] = json.loads(json_path.read_text())
            except (json.JSONDecodeError, OSError):
                entry["meta"] = None
        else:
            entry["meta"] = None

        captures.append(entry)

    return captures


@router.get("/detail/{filename}")
async def capture_detail(request: Request, filename: str) -> dict[str, Any]:
    """Get details for a single capture file."""
    storage = _get_storage(request)
    sc16_path = _validate_filename(filename, storage)

    if not sc16_path.exists() or not filename.endswith(".sc16"):
        raise HTTPException(status_code=404, detail="File not found")

    result: dict[str, Any] = {
        "filename": filename,
        "size_bytes": sc16_path.stat().st_size,
        "has_psd": sc16_path.with_suffix(".npz").exists(),
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


@router.get("/psd/{filename}")
async def capture_psd(
    request: Request,
    filename: str,
    start: int = 0,
    count: int = 500,
    max_bins: int = 512,
) -> dict[str, Any]:
    """Serve PSD grid data from a .npz companion file.

    Query params:
        start: row offset (default 0)
        count: max rows to return (default 500)
        max_bins: downsample frequency bins if needed (default 512)
    """
    storage = _get_storage(request)

    # Resolve .npz path from filename (accept both .sc16 and .npz names)
    base = filename.replace(".sc16", "").replace(".npz", "")
    npz_path = _validate_filename(base + ".npz", storage)

    if not npz_path.exists():
        raise HTTPException(status_code=404, detail="PSD data not found")

    data = np.load(npz_path)
    grid = data["grid"]  # shape: (total_rows, num_bins)
    freq_axis = data["freq_axis"]
    time_res = float(data["time_resolution_s"])
    center_freq = int(data["center_freq_hz"])
    bandwidth = int(data["bandwidth_hz"])

    total_rows, num_bins = grid.shape

    # Global range over the whole grid so the waterfall/PSD colour mapping
    # stays stable while the client lazy-loads pages on scroll (a per-page
    # min/max would make colours jump between pages).
    grid_min = float(grid.min()) if total_rows else -120.0
    grid_max = float(grid.max()) if total_rows else -40.0

    # Display calibration baked in at record time (absent on uncalibrated or
    # pre-existing captures → client falls back to dBFS).
    cal_offset_db = float(data["cal_offset_db"]) if "cal_offset_db" in data.files else None

    # Slice rows
    start = max(0, min(start, total_rows))
    end = min(start + count, total_rows)
    sliced = grid[start:end]

    # Downsample bins if needed
    if num_bins > max_bins:
        factor = num_bins // max_bins
        trim = factor * max_bins
        sliced = sliced[:, :trim].reshape(sliced.shape[0], max_bins, factor).mean(axis=2)
        freq_axis = freq_axis[:trim].reshape(max_bins, factor).mean(axis=1)
        num_bins = max_bins

    return {
        "grid": sliced.tolist(),
        "freq_axis": freq_axis.tolist(),
        "time_resolution_s": time_res,
        "total_rows": total_rows,
        "num_bins": num_bins,
        "grid_min": grid_min,
        "grid_max": grid_max,
        "cal_offset_db": cal_offset_db,
        "start": start,
        "count": end - start,
        "center_freq_hz": center_freq,
        "bandwidth_hz": bandwidth,
    }
