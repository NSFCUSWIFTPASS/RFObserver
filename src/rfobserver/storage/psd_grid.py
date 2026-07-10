"""On-disk PSD grid companion for recordings.

Grid is stored as raw C-contiguous float32 ``(rows, num_bins)`` in ``<base>.psd``
with a JSON sidecar ``<base>.psd.json``. Raw + memmap keeps both the writer
(streaming append) and reader (windowed slice) off the RAM heap -- unlike the old
compressed ``.npz`` which materialized the whole grid.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np


def grid_paths(sc16_path: Path) -> tuple[Path, Path]:
    """Return (raw .psd, meta .psd.json) paths for a recording's .sc16 path."""
    base = sc16_path
    if base.suffix == ".sc16":
        base = base.with_suffix("")
    return base.with_suffix(".psd"), base.with_suffix(".psd.json")


def write_meta(
    meta_path: Path,
    *,
    rows: int,
    num_bins: int,
    time_resolution_s: float,
    center_freq_hz: int,
    bandwidth_hz: int,
    freq_axis: np.ndarray[Any, np.dtype[Any]],
    grid_min: float,
    grid_max: float,
    cal_offset_db: float | None,
) -> None:
    """Write the JSON sidecar describing the raw .psd grid."""
    meta: dict[str, Any] = {
        "rows": int(rows),
        "num_bins": int(num_bins),
        "time_resolution_s": float(time_resolution_s),
        "center_freq_hz": int(center_freq_hz),
        "bandwidth_hz": int(bandwidth_hz),
        "freq_axis": [float(x) for x in np.asarray(freq_axis).tolist()],
        "grid_min": float(grid_min),
        "grid_max": float(grid_max),
    }
    if cal_offset_db is not None:
        meta["cal_offset_db"] = float(cal_offset_db)
    meta_path.write_text(json.dumps(meta))


def load_grid(sc16_path: Path) -> tuple[np.ndarray[Any, np.dtype[Any]], dict[str, Any]] | None:
    """Memmap the raw grid + parse meta, or None if the companion is absent/invalid."""
    raw_path, meta_path = grid_paths(sc16_path)
    if not raw_path.exists() or not meta_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text())
        rows = int(meta["rows"])
        num_bins = int(meta["num_bins"])
    except (OSError, ValueError, KeyError):
        return None
    if rows == 0:
        # Empty grid: return a real (0, num_bins) array (memmap can't be zero-length).
        return np.zeros((0, num_bins), dtype=np.float32), meta
    mm = np.memmap(raw_path, dtype=np.float32, mode="r", shape=(rows, num_bins))
    return mm, meta
