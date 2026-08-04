"""Minimal SigMF reader for replaying recorded captures through the pipeline.

Reads the ``.sigmf-meta`` sidecar for the sample rate, datatype, and capture
center frequency, and memory-maps the ``.sigmf-data`` so large captures (GBs)
are streamed, not loaded into RAM. Supports the two datatypes our captures use:
``ci16_le`` (interleaved int16 I/Q -- byte-identical to the pipeline's SC16) and
``cf32_le`` (interleaved float32 I/Q).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

# Supported SigMF datatypes -> numpy dtype of the interleaved (I, Q, I, Q, ...) stream.
_DTYPES = {
    "ci16_le": np.int16,
    "cf32_le": np.float32,
}


@dataclass
class SigmfCapture:
    """A memory-mapped SigMF capture ready to stream."""

    datatype: str
    sample_rate_hz: float
    center_freq_hz: float
    raw: np.ndarray[Any, np.dtype[Any]]  # 1-D memmap of interleaved I/Q (file's native dtype)
    meta: dict[str, Any]

    @property
    def num_samples(self) -> int:
        return int(self.raw.size // 2)


def _meta_path(data_path: Path) -> Path:
    if data_path.suffix == ".sigmf-data":
        return data_path.with_suffix(".sigmf-meta")
    # allow passing the base name or the meta path directly
    cand = data_path.with_suffix(".sigmf-meta")
    return cand if cand.exists() else data_path


def load_sigmf(path: str | Path) -> SigmfCapture:
    """Open a SigMF capture (pass the .sigmf-data, .sigmf-meta, or base path)."""
    p = Path(path)
    if p.suffix == ".sigmf-meta":
        meta_path = p
        data_path = p.with_suffix(".sigmf-data")
    elif p.suffix == ".sigmf-data":
        data_path = p
        meta_path = p.with_suffix(".sigmf-meta")
    else:
        data_path = p.with_suffix(".sigmf-data")
        meta_path = p.with_suffix(".sigmf-meta")

    if not meta_path.exists():
        raise FileNotFoundError(f"SigMF metadata not found: {meta_path}")
    if not data_path.exists():
        raise FileNotFoundError(f"SigMF data not found: {data_path}")

    meta = json.loads(meta_path.read_text())
    g = meta.get("global", {})
    datatype = g.get("core:datatype")
    if datatype not in _DTYPES:
        raise ValueError(f"unsupported SigMF datatype {datatype!r}; supported: {sorted(_DTYPES)}")
    sample_rate = float(g.get("core:sample_rate", 0.0))
    if sample_rate <= 0:
        raise ValueError("SigMF metadata missing a positive core:sample_rate")
    captures = meta.get("captures", [{}])
    center = float(captures[0].get("core:frequency", 0.0)) if captures else 0.0

    raw = np.memmap(data_path, dtype=_DTYPES[datatype], mode="r")
    return SigmfCapture(
        datatype=datatype,
        sample_rate_hz=sample_rate,
        center_freq_hz=center,
        raw=raw,
        meta=meta,
    )


def load_raw(
    path: str | Path,
    *,
    datatype: str,
    sample_rate_hz: float,
    center_freq_hz: float = 0.0,
) -> SigmfCapture:
    """Open a headerless interleaved-I/Q recording (a raw ``.dat``, no SigMF sidecar).

    The capture parameters live only in the filename for these recordings, so the
    caller supplies them. The data is memory-mapped like ``load_sigmf``.
    """
    p = Path(path)
    if datatype not in _DTYPES:
        raise ValueError(f"unsupported datatype {datatype!r}; supported: {sorted(_DTYPES)}")
    if sample_rate_hz <= 0:
        raise ValueError("sample_rate_hz must be positive for a raw capture")
    if not p.exists():
        raise FileNotFoundError(f"raw capture not found: {p}")

    raw = np.memmap(p, dtype=_DTYPES[datatype], mode="r")
    return SigmfCapture(
        datatype=datatype,
        sample_rate_hz=float(sample_rate_hz),
        center_freq_hz=float(center_freq_hz),
        raw=raw,
        meta={"raw": True},
    )


def to_sc16_int32(
    interleaved: np.ndarray[Any, np.dtype[Any]], datatype: str
) -> np.ndarray[Any, np.dtype[Any]]:
    """Convert an interleaved I/Q slice (native dtype) to the pipeline's SC16 int32.

    SC16 int32 packs I in the low 16 bits and Q in the high 16 (what
    ``convert_sc16_to_complex`` expects). ``ci16_le`` is already that layout, so
    it's a zero-copy view; ``cf32_le`` in [-1, 1] is scaled by 32767 and packed.
    """
    if datatype == "ci16_le":
        return np.ascontiguousarray(interleaved).view(np.int32)
    if datatype == "cf32_le":
        i16 = np.clip(interleaved * 32767.0, -32768, 32767).astype(np.int16)
        return np.ascontiguousarray(i16).view(np.int32)
    raise ValueError(f"unsupported datatype {datatype!r}")
