"""Captures page — list, inspect, and view IQ recordings."""

from __future__ import annotations

import asyncio
import json
import logging
import struct
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from rfobserver.storage import psd_grid

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_storage(request: Request) -> Path:
    return Path(request.app.state.settings.STORAGE_PATH)


def _has_psd(sc16_path: Path) -> bool:
    """True if a PSD companion exists (new raw+sidecar, or legacy .npz)."""
    raw, meta = psd_grid.grid_paths(sc16_path)
    return (raw.exists() and meta.exists()) or sc16_path.with_suffix(".npz").exists()


def _strip_suffix(name: str, *suffixes: str) -> str:
    """Strip each given suffix from the end of ``name``, in order, if present.

    Anchored to the end of the string (unlike ``str.replace``, which would
    also strip a match found mid-string).
    """
    for suf in suffixes:
        if name.endswith(suf):
            name = name[: -len(suf)]
    return name


def _capture_dirs(storage: Path) -> list[Path]:
    """Locations captures may live in: the auto/ and manual/ subdirs, plus the
    legacy root (pre-split captures, before startup migration moves them)."""
    return [storage / "manual", storage / "auto", storage]


def _validate_filename(filename: str, storage: Path) -> Path:
    """Validate a bare capture filename (no path traversal) and resolve it.

    Captures are split across auto/ and manual/; the filename stays a unique
    basename, so resolve it by searching those subdirs (then the legacy root)
    for the existing file. Falls back to the root path when nothing matches so
    callers still get a clean 404 via a later ``.exists()`` check.
    """
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    for d in _capture_dirs(storage):
        candidate = (d / filename).resolve()
        if not str(candidate).startswith(str(storage.resolve())):
            raise HTTPException(status_code=400, detail="Invalid filename")
        if candidate.exists():
            return candidate
    return (storage / filename).resolve()


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

    # Scan auto/ and manual/ (and the legacy root for any not-yet-migrated
    # captures), newest first across all of them.
    found: list[tuple[Path, str]] = []
    for d in _capture_dirs(storage):
        if d.exists():
            origin = d.name if d != storage else "manual"
            found.extend((sc16, origin) for sc16 in d.glob("*.sc16"))

    captures: list[dict[str, Any]] = []
    for sc16, origin in sorted(found, key=lambda t: t[0].stat().st_mtime, reverse=True):
        entry: dict[str, Any] = {
            "filename": sc16.name,
            "origin": origin,
            "size_bytes": sc16.stat().st_size,
            "has_psd": _has_psd(sc16),
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
        "has_psd": _has_psd(sc16_path),
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


def _open_psd(sc16_path: Path) -> tuple[np.ndarray[Any, np.dtype[Any]], dict[str, Any]] | None:
    """Load the PSD grid (new raw .psd first, then legacy .npz) plus its metadata.

    Returns ``(grid, info)`` where ``grid`` may be a read-only memmap (only the
    windowed slice is ever materialized) and ``info`` carries freq_axis (np array),
    time_resolution_s, center_freq_hz, bandwidth_hz, total_rows, num_bins, grid_min,
    grid_max, and cal_offset_db. Returns ``None`` if no PSD companion exists.
    """
    loaded = psd_grid.load_grid(sc16_path)
    if loaded is not None:
        grid, meta = loaded
        return grid, {
            "freq_axis": np.asarray(meta["freq_axis"]),
            "time_resolution_s": float(meta["time_resolution_s"]),
            "center_freq_hz": int(meta["center_freq_hz"]),
            "bandwidth_hz": int(meta["bandwidth_hz"]),
            "total_rows": int(meta["rows"]),
            "num_bins": int(meta["num_bins"]),
            "grid_min": float(meta["grid_min"]),
            "grid_max": float(meta["grid_max"]),
            "cal_offset_db": float(meta["cal_offset_db"]) if "cal_offset_db" in meta else None,
        }

    npz_path = sc16_path.with_suffix(".npz")
    if not npz_path.exists():
        return None
    data = np.load(npz_path)
    grid = data["grid"]  # shape: (total_rows, num_bins)
    total_rows, num_bins = grid.shape
    return grid, {
        "freq_axis": data["freq_axis"],
        "time_resolution_s": float(data["time_resolution_s"]),
        "center_freq_hz": int(data["center_freq_hz"]),
        "bandwidth_hz": int(data["bandwidth_hz"]),
        "total_rows": int(total_rows),
        "num_bins": int(num_bins),
        # Global range over the whole grid so the waterfall colour mapping stays
        # stable while the client lazy-loads pages (a per-page min/max would jump).
        "grid_min": float(grid.min()) if total_rows else -120.0,
        "grid_max": float(grid.max()) if total_rows else -40.0,
        # Display calibration baked in at record time (absent → client uses dBFS).
        "cal_offset_db": float(data["cal_offset_db"]) if "cal_offset_db" in data.files else None,
    }


def _slice_psd(
    grid: np.ndarray[Any, np.dtype[Any]],
    freq_axis: np.ndarray[Any, np.dtype[Any]],
    num_bins: int,
    start: int,
    count: int,
    max_bins: int,
) -> tuple[np.ndarray[Any, np.dtype[Any]], np.ndarray[Any, np.dtype[Any]], int]:
    """Row-slice + optional bin-downsample, shared by the HTTP and WS endpoints.

    Returns ``(sliced, ds_freq_axis, ds_num_bins)`` where ``sliced`` is a
    contiguous C-order float32 array ready for ``.tolist()`` / ``.tobytes()``.
    """
    total_rows = grid.shape[0]
    start = max(0, min(start, total_rows))
    end = min(start + count, total_rows)
    sliced = grid[start:end]

    if num_bins > max_bins:
        factor = num_bins // max_bins
        trim = factor * max_bins
        sliced = sliced[:, :trim].reshape(sliced.shape[0], max_bins, factor).mean(axis=2)
        freq_axis = freq_axis[:trim].reshape(max_bins, factor).mean(axis=1)
        num_bins = max_bins

    return np.ascontiguousarray(sliced, dtype=np.float32), np.asarray(freq_axis), num_bins


def _psd_frame_bytes(start: int, sliced: np.ndarray[Any, np.dtype[Any]]) -> bytes:
    """Pack a PSD window into a binary WS frame.

    Little-endian 12-byte header ``int32 start, count, num_bins`` followed by
    ``count*num_bins`` float32 (C-order, row-major).
    """
    header = struct.pack("<iii", int(start), int(sliced.shape[0]), int(sliced.shape[1]))
    # Annotate the buffer explicitly: CI type-checks without numpy installed, so
    # ndarray.tobytes() infers as Any there and trips no-any-return otherwise.
    frame: bytes = np.ascontiguousarray(sliced, dtype="<f4").tobytes()
    return header + frame


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

    # New format (raw .psd + .psd.json, memmap) first, then legacy .npz.
    base = filename.replace(".sc16", "").replace(".npz", "")
    sc16_path = _validate_filename(base + ".sc16", storage)
    opened = _open_psd(sc16_path)
    if opened is None:
        raise HTTPException(status_code=404, detail="PSD data not found")
    grid, info = opened
    total_rows = info["total_rows"]

    start = max(0, min(start, total_rows))
    sliced, freq_axis, num_bins = _slice_psd(
        grid, info["freq_axis"], info["num_bins"], start, count, max_bins
    )

    return {
        "grid": sliced.tolist(),
        "freq_axis": freq_axis.tolist(),
        "time_resolution_s": info["time_resolution_s"],
        "total_rows": total_rows,
        "num_bins": num_bins,
        "grid_min": info["grid_min"],
        "grid_max": info["grid_max"],
        "cal_offset_db": info["cal_offset_db"],
        "start": start,
        "count": int(sliced.shape[0]),
        "center_freq_hz": info["center_freq_hz"],
        "bandwidth_hz": info["bandwidth_hz"],
    }


@router.get("/detections/{filename}")
async def capture_detections(request: Request, filename: str) -> dict[str, Any]:
    """Return a capture's detections sidecar (`<base>.detections.json`).

    Serves the sidecar file if it exists. Otherwise, if the capture ended more
    than ``DETECTIONS_SIDECAR_GRACE_SEC`` ago and a database is wired, lazily
    builds and writes the sidecar (backstop for captures whose record-stop
    deferred write never ran). A still-fresh capture with no sidecar returns
    ``{"detections": [], "pending": True}`` so the client can retry.
    """
    from datetime import datetime, timezone

    from rfobserver.storage import detections_sidecar as ds

    storage = _get_storage(request)
    base = (
        filename.replace(".sc16", "")
        .replace(".detections", "")
        .replace(".json", "")
        .replace(".npz", "")
    )
    sc16_path = _validate_filename(base + ".sc16", storage)

    sidecar = ds.sidecar_path(sc16_path)
    if sidecar.exists():
        cached = ds._read_json(sidecar)
        if cached is not None:
            return cached

    if not sc16_path.exists():
        raise HTTPException(status_code=404, detail="Capture not found")

    db = getattr(request.app.state, "database", None)
    grace = float(request.app.state.settings.DETECTIONS_SIDECAR_GRACE_SEC)

    meta = ds._read_json(sc16_path.with_suffix(".json")) or {}
    start_iso = meta.get("start_time")
    dur = float(meta.get("duration_sec", 0.0))
    too_new = False
    if start_iso:
        try:
            end = datetime.fromisoformat(start_iso).timestamp() + dur
            too_new = (datetime.now(timezone.utc).timestamp() - end) < grace
        except ValueError:
            too_new = False

    if db is None or too_new:
        return {"detections": [], "pending": True}

    return await ds.write_sidecar(sc16_path, db)


def _num(body: dict[str, Any], key: str, default: float) -> float:
    """Read a numeric field from a request body, defaulting on missing OR null.

    A blank field in the UI posts JSON ``null`` (not an absent key), and
    ``float(None)`` raises ``TypeError`` -> 500. Treat null the same as
    missing: fall back to ``default``.
    """
    value = body.get(key)
    return default if value is None else float(value)


@router.post("/redetect/{filename}")
async def capture_redetect(request: Request, filename: str) -> dict[str, Any]:
    """Re-run burst detection on a capture's stored PSD grid at given thresholds.

    Works on any capture that has a stored `.psd`/`.psd.json` grid, independent
    of when it was recorded. Rewrites `<base>.detections.json` and returns the
    new sidecar payload. All body fields are optional and default to the
    configured `BURST_*` settings.
    """
    from rfobserver.processing.burst import BurstDetectionConfig
    from rfobserver.storage import detections_sidecar as ds
    from rfobserver.storage import psd_grid

    storage = _get_storage(request)
    base = _strip_suffix(filename, ".sc16", ".json", ".detections")
    sc16 = _validate_filename(base + ".sc16", storage)
    if psd_grid.load_grid(sc16) is None:
        raise HTTPException(status_code=404, detail="No PSD grid for this capture")

    try:
        body = await request.json()
    except Exception:
        body = {}

    s = request.app.state.settings
    cfg = BurstDetectionConfig(
        threshold_high_db=_num(body, "threshold_high_db", s.BURST_THRESHOLD_HIGH_DB),
        threshold_low_ratio=_num(body, "threshold_low_ratio", s.BURST_THRESHOLD_LOW_RATIO),
        noise_floor_percentile=_num(body, "noise_floor_percentile", s.BURST_NOISE_FLOOR_PERCENTILE),
        merge_time_sec=_num(body, "merge_time_ms", s.BURST_MERGE_TIME_MS) / 1000.0,
        merge_freq_bins=int(_num(body, "merge_freq_bins", s.BURST_MERGE_FREQ_BINS)),
        min_duration_sec=_num(body, "min_duration_ms", 1.0) / 1000.0,
    )
    # detect_bursts on a full grid is multi-second CPU work; run it off the event
    # loop so the live WS/heartbeat stay responsive during a re-detect.
    payload: dict[str, Any] = await asyncio.to_thread(ds.write_sidecar_from_grid, sc16, cfg)
    return payload


@router.websocket("/ws/psd/{filename}")
async def capture_psd_ws(websocket: WebSocket, filename: str) -> None:
    """Stream PSD windows as binary frames, pushing scroll-ahead neighbours.

    Client sends JSON range requests ``{start, count, max_bins, have?}``; the
    server replies with a binary frame for the requested window, then proactively
    for the next window, the window two ahead, and the previous window (bounded
    to the grid, skipping any starts the client lists in ``have``). Reached at
    ``/captures/ws/psd/{filename}``.
    """
    storage = Path(websocket.app.state.settings.STORAGE_PATH)
    base = filename.replace(".sc16", "").replace(".npz", "")
    try:
        sc16_path = _validate_filename(base + ".sc16", storage)
    except HTTPException:
        await websocket.close(code=1011)
        return

    opened = _open_psd(sc16_path)
    if opened is None:
        await websocket.close(code=1011)
        return
    grid, info = opened
    total_rows = int(info["total_rows"])

    await websocket.accept()

    # Meta frame uses the default-max_bins downsample so num_bins matches the data
    # frames the client will receive for a default request.
    default_max_bins = 512
    _, ds_axis, ds_num_bins = _slice_psd(
        grid, info["freq_axis"], info["num_bins"], 0, 0, default_max_bins
    )
    await websocket.send_json(
        {
            "type": "meta",
            "freq_axis": ds_axis.tolist(),
            "time_resolution_s": info["time_resolution_s"],
            "total_rows": total_rows,
            "num_bins": ds_num_bins,
            "grid_min": info["grid_min"],
            "grid_max": info["grid_max"],
            "cal_offset_db": info["cal_offset_db"],
            "center_freq_hz": info["center_freq_hz"],
            "bandwidth_hz": info["bandwidth_hz"],
        }
    )

    async def serve(s: int, count: int, max_bins: int, have: set[int], skip_have: bool) -> None:
        if s < 0 or s >= total_rows:
            return
        if skip_have and s in have:
            return
        sliced, _fa, _nb = _slice_psd(grid, info["freq_axis"], info["num_bins"], s, count, max_bins)
        if sliced.shape[0] == 0:
            return
        await websocket.send_bytes(_psd_frame_bytes(s, sliced))

    try:
        while True:
            msg = await websocket.receive_json()
            start = int(msg.get("start", 0))
            count = int(msg.get("count", 500))
            max_bins = int(msg.get("max_bins", default_max_bins))
            have = {int(x) for x in msg.get("have", [])}

            # Requested window first, then the bounded push-ahead neighbours:
            # next, two-ahead, then previous.
            await serve(start, count, max_bins, have, skip_have=False)
            await serve(start + count, count, max_bins, have, skip_have=True)
            await serve(start + 2 * count, count, max_bins, have, skip_have=True)
            await serve(start - count, count, max_bins, have, skip_have=True)
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("Captures PSD WebSocket error for %s", filename)
