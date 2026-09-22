"""On-disk PSD grid companion for recordings.

Grid is stored as raw C-contiguous float32 ``(rows, num_bins)`` in ``<base>.psd``
with a JSON sidecar ``<base>.psd.json``. Raw + memmap keeps both the writer
(streaming append) and reader (windowed slice) off the RAM heap -- unlike the old
compressed ``.npz`` which materialized the whole grid.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from pathlib import Path


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
    start_sample_offset: int = 0,
    slice_samples: int = 0,
) -> None:
    """Write the JSON sidecar describing the raw .psd grid.

    ``start_sample_offset`` and ``slice_samples`` pin the grid to the companion
    ``.sc16``: row ``k`` covers IQ samples ``[start_sample_offset +
    k*slice_samples, start_sample_offset + (k+1)*slice_samples)``. Without them
    a reader can only assume row 0 starts at the IQ's first sample, which is
    how a pipeline-latency misalignment stayed invisible for so long (see
    docs/debugging/2026-09-22_trigger-psd-iq-misalignment.md).
    """
    meta: dict[str, Any] = {
        "rows": int(rows),
        "num_bins": int(num_bins),
        "time_resolution_s": float(time_resolution_s),
        "center_freq_hz": int(center_freq_hz),
        "bandwidth_hz": int(bandwidth_hz),
        "freq_axis": [float(x) for x in np.asarray(freq_axis).tolist()],
        "grid_min": float(grid_min),
        "grid_max": float(grid_max),
        # Alignment to the .sc16 (see docstring).
        "start_sample_offset": int(start_sample_offset),
        "slice_samples": int(slice_samples),
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
