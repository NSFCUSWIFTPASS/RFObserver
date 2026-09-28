"""SigMF metadata for a recorded IQ capture.

A capture's ``.sc16`` is interleaved little-endian int16 I/Q, byte-identical to
SigMF ``ci16_le``, so exporting it as SigMF needs no data conversion: the same
bytes are served as ``.sigmf-data`` and this module builds the ``.sigmf-meta``
from the capture's own ``.json``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from rfobserver.__about__ import __version__

SIGMF_VERSION = "1.2.0"


def _sigmf_datetime(dt: datetime) -> str:
    """ISO-8601 UTC with a trailing Z, as SigMF requires."""
    return dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat() + "Z"


def iq_sigmf_meta(capture: dict[str, Any]) -> dict[str, Any]:
    """Build a ``.sigmf-meta`` document from a capture's ``.json`` metadata.

    Each overflow gap (``[file_sample_position, lost_samples]``) becomes a new
    ``captures`` segment starting at that file position, with a datetime that
    accounts for every sample lost before it. That is how SigMF expresses a
    discontinuity, so timestamps after an overflow stay correct in any SigMF
    tool rather than drifting by the lost span.
    """
    rate = float(capture.get("sample_rate_hz") or capture.get("bandwidth_hz") or 0)
    freq = capture.get("center_freq_hz")

    hw_parts = ["USRP"]
    if capture.get("serial"):
        hw_parts.append(f"serial {capture['serial']}")
    if capture.get("gain_db") is not None:
        hw_parts.append(f"gain {capture['gain_db']} dB")

    global_meta: dict[str, Any] = {
        "core:datatype": "ci16_le",
        "core:sample_rate": rate,
        "core:version": SIGMF_VERSION,
        "core:num_channels": 1,
        "core:hw": ", ".join(hw_parts),
        "core:recorder": f"RFObserver {__version__}",
    }
    if capture.get("hostname"):
        global_meta["core:description"] = f"RFObserver capture from {capture['hostname']}"

    start: datetime | None = None
    if capture.get("start_time"):
        start = datetime.fromisoformat(str(capture["start_time"]))
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)

    def segment(sample_start: int, lost_before: int) -> dict[str, Any]:
        seg: dict[str, Any] = {"core:sample_start": int(sample_start)}
        if freq is not None:
            seg["core:frequency"] = freq
        if start is not None and rate > 0:
            offset = timedelta(seconds=(sample_start + lost_before) / rate)
            seg["core:datetime"] = _sigmf_datetime(start + offset)
        return seg

    captures = [segment(0, 0)]
    lost = 0
    for pos, n_lost in capture.get("gaps") or []:
        lost += int(n_lost)
        captures.append(segment(int(pos), lost))

    return {"global": global_meta, "captures": captures, "annotations": []}
