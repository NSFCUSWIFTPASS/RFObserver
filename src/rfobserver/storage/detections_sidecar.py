"""Detections sidecar: `<base>.detections.json` companion for a recording.

Independent of the live DB at read time: the sidecar is built once (from the
DB, windowed to the capture's time span and tuning) and written next to the
`.sc16`/`.json`/`.psd`/`.psd.json` companions, so the captures viewer can load
detection overlays without a DB round-trip. Lives in `storage/` (not
`web/` or `pipeline/`) so both the recording pipeline and the web routes can
import it without introducing a web<->pipeline coupling.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import numpy as np

from rfobserver.processing.burst import BurstDetectionConfig, detect_bursts
from rfobserver.processing.spectral import PSDGridResult
from rfobserver.storage import psd_grid

if TYPE_CHECKING:
    from pathlib import Path

    from rfobserver.storage.database import SensorDatabase


def _base(sc16_path: Path) -> Path:
    """Strip a trailing `.sc16` suffix, returning the shared companion-file base."""
    if sc16_path.suffix == ".sc16":
        return sc16_path.with_suffix("")
    return sc16_path


def sidecar_path(sc16_path: Path) -> Path:
    """Return the `<base>.detections.json` path for a recording's `.sc16` path."""
    return _base(sc16_path).with_suffix(".detections.json")


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        data: dict[str, Any] = json.loads(path.read_text())
        return data
    except (OSError, ValueError):
        return None


async def build_sidecar_payload(sc16_path: Path, db: SensorDatabase) -> dict[str, Any]:
    """Build the sidecar payload for a recording: capture meta + windowed detections.

    Reads the capture's `<base>.json` metadata and `<base>.psd.json` grid
    metadata (for `time_resolution_s`), then queries `db` for detections
    inside the capture's time span with matching SDR tuning. Missing/invalid
    companion metadata degrades to an empty `detections` list rather than
    raising.
    """
    base = _base(sc16_path)
    meta = _read_json(base.with_suffix(".json")) or {}
    _raw_path, psd_meta_path = psd_grid.grid_paths(sc16_path)
    psd_meta = _read_json(psd_meta_path) or {}

    start_iso = meta.get("start_time")
    dur = float(meta.get("duration_sec", 0.0))
    grid_rows = int(psd_meta.get("rows", 0))
    # Effective time resolution: the PSD grid's nominal `time_resolution_s` does
    # NOT satisfy rows * tres == duration_sec, so a nominal-tres row mapping
    # places detections far outside the grid. Use a consistent effective
    # resolution (duration / rows) for BOTH row placement here and the waterfall
    # time axis, so detections queried within [start, start+dur] always land in
    # [0, rows] and align with the grid the viewer shows.
    eff_tres = (dur / grid_rows) if (dur and grid_rows > 0) else None
    center = meta.get("center_freq_hz")
    sample_rate = meta.get("sample_rate_hz")
    gain = meta.get("gain_db")

    out: dict[str, Any] = {
        "capture_start_time": start_iso,
        "time_resolution_s": eff_tres,
        "center_freq_hz": center,
        "sample_rate_hz": sample_rate,
        "gain_db": gain,
        "detections": [],
    }
    if not (start_iso and dur and grid_rows):
        return out

    start = datetime.fromisoformat(start_iso)
    rows = await db.query_detections(
        since=start,
        until=start + timedelta(seconds=dur),
        sdr_center_freq=center,
        sample_rate=sample_rate,
        gain=gain,
        limit=100000,
    )
    cap_start = start.timestamp()
    detections: list[dict[str, Any]] = []
    for row in rows:
        det_start = datetime.fromisoformat(row["start_time"]).timestamp()
        det_stop = datetime.fromisoformat(row["stop_time"]).timestamp()
        # Clamp to [0, grid_rows]: a burst that starts in-window but ends after
        # the capture's last row would otherwise map row_stop past the grid.
        row_start = int(round((det_start - cap_start) / dur * grid_rows))
        row_stop = int(round((det_stop - cap_start) / dur * grid_rows))
        row_start = max(0, min(grid_rows, row_start))
        row_stop = max(0, min(grid_rows, row_stop))
        detections.append(
            {
                "start_time": row["start_time"],
                "stop_time": row["stop_time"],
                "center_freq_hz": row["center_freq_hz"],
                "bandwidth_hz": row["bandwidth_hz"],
                "peak_freq_hz": row.get("peak_freq_hz"),
                "peak_power_db": row["peak_power_db"],
                "duration_ms": row["duration_ms"],
                "row_start": row_start,
                "row_stop": row_stop,
            }
        )
    out["detections"] = detections
    return out


async def write_sidecar(sc16_path: Path, db: SensorDatabase) -> dict[str, Any]:
    """Build the sidecar payload and write it to `<base>.detections.json`."""
    payload = await build_sidecar_payload(sc16_path, db)
    sidecar_path(sc16_path).write_text(json.dumps(payload, indent=2))
    return payload


def build_sidecar_from_grid(sc16_path: Path, config: BurstDetectionConfig) -> dict[str, Any]:
    """Build the sidecar payload by re-running burst detection on a stored PSD grid.

    Unlike `build_sidecar_payload` (which queries the live DB for detections
    made during the original recording), this runs `detect_bursts` directly
    against the capture's `<base>.psd`/`.psd.json` grid, so it works for any
    capture that has a stored grid -- independent of the DB and reusable with
    an arbitrary `config` (e.g. a re-detect endpoint or a replay record-stop
    path). Missing grid/capture metadata degrades to an empty payload rather
    than raising.
    """
    meta_cap = _read_json(_base(sc16_path).with_suffix(".json")) or {}
    loaded = psd_grid.load_grid(sc16_path)
    start_iso = meta_cap.get("start_time")
    out: dict[str, Any] = {
        "capture_start_time": start_iso,
        "time_resolution_s": None,
        "center_freq_hz": meta_cap.get("center_freq_hz"),
        "sample_rate_hz": meta_cap.get("sample_rate_hz"),
        "gain_db": meta_cap.get("gain_db"),
        "detections": [],
        "params": {
            "threshold_high_db": config.threshold_high_db,
            "threshold_low_ratio": config.threshold_low_ratio,
            "noise_floor_percentile": config.noise_floor_percentile,
            "merge_time_ms": config.merge_time_sec * 1000.0,
            "merge_freq_bins": config.merge_freq_bins,
            "min_duration_ms": config.min_duration_sec * 1000.0,
        },
    }
    if loaded is None or not start_iso:
        return out
    grid, gmeta = loaded
    tres = float(gmeta.get("time_resolution_s") or 0.0)
    rows = int(gmeta.get("rows") or grid.shape[0])
    out["time_resolution_s"] = tres or None
    if tres <= 0 or rows <= 0:
        return out
    freq_axis = np.asarray(gmeta.get("freq_axis", []), dtype=np.float64)
    if freq_axis.size == 0:
        return out
    center = float(gmeta.get("center_freq_hz") or 0.0)
    psd = PSDGridResult(
        grid=np.asarray(grid),
        time_axis=np.arange(rows, dtype=np.float64) * tres,
        freq_axis=freq_axis,
        ffts_per_slice=0,
        total_ffts=rows,
    )
    cap_start = datetime.fromisoformat(start_iso)
    result = detect_bursts(psd, config, center, cap_start)
    dets: list[dict[str, Any]] = []
    for b in result.bursts:
        t0 = (b.start_time - cap_start).total_seconds()
        t1 = (b.stop_time - cap_start).total_seconds()
        row_start = max(0, min(rows, int(round(t0 / tres))))
        row_stop = max(0, min(rows, int(round(t1 / tres))))
        dets.append(
            {
                "start_time": b.start_time.isoformat(),
                "stop_time": b.stop_time.isoformat(),
                "center_freq_hz": b.center_freq_hz,
                "bandwidth_hz": b.bandwidth_hz,
                "peak_freq_hz": b.peak_freq_hz,
                "peak_power_db": b.peak_power_db,
                "duration_ms": b.duration_ms,
                "row_start": row_start,
                "row_stop": row_stop,
            }
        )
    out["detections"] = dets
    return out


def write_sidecar_from_grid(sc16_path: Path, config: BurstDetectionConfig) -> dict[str, Any]:
    """Build the from-grid sidecar payload and write it to `<base>.detections.json`."""
    payload = build_sidecar_from_grid(sc16_path, config)
    sidecar_path(sc16_path).write_text(json.dumps(payload, indent=2))
    return payload
