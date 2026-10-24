"""Fleet Detections: every sensor's detections merged, with attribution detail.

A detection's ``attribution`` is the rtl_433 record its sensor stored: a
decoded one lists frames (model, decoded fields, raw rows), a tried one says
nothing decoded, and a never-tried detection has none. The fleet sends the
page a short summary of it rather than the raw record (which carries the
payload rows and can run to kilobytes). Decoded fields come from the air:
the page escapes them like any other untrusted text.
"""

from __future__ import annotations

import contextlib
import json
from datetime import datetime, timezone
from typing import Any

# rtl_433 frame keys that are bookkeeping, not decoded content.
_FRAME_SKIP = {
    "time", "model", "count", "num_rows", "rows", "codes", "mod", "freq", "freq1", "freq2",
    "rssi", "snr", "noise", "protocol", "mic",
}  # fmt: skip
_MAX_FIELDS = 4
_DATA_PREVIEW = 16
_DATA_TITLE = 256

ROW_FIELDS = (
    "burst_id",
    "start_time",
    "center_freq_hz",
    "bandwidth_hz",
    "duration_ms",
    "peak_power_db",
    "model",
    "protocol_id",
)


def attribution_summary(row: dict[str, Any]) -> dict[str, Any]:
    """{status, fields, bits, data, data_full} for one detection row.

    status: "decoded" (rtl_433 found a model), "tried" (it ran, nothing
    decoded) or "not_tried".
    """
    model = row.get("model")
    raw = row.get("attribution")
    out: dict[str, Any] = {"status": "not_tried", "fields": [], "bits": None, "frames": 0}
    if model:
        out["status"] = "decoded"
    elif raw:
        out["status"] = "tried"
    if not model or not raw:
        return out
    try:
        record = json.loads(raw) if isinstance(raw, str) else raw
    except ValueError:
        return out
    frames = record.get("frames") if isinstance(record, dict) else None
    if not isinstance(frames, list) or not frames or not isinstance(frames[0], dict):
        return out
    out["frames"] = len(frames)
    first = frames[0]
    fields = [
        [str(k), v]
        for k, v in first.items()
        if k not in _FRAME_SKIP and isinstance(v, (str, int, float)) and not isinstance(v, bool)
    ]
    out["fields"] = fields[:_MAX_FIELDS]
    rows = first.get("rows")
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        bits = rows[0].get("len")
        data = rows[0].get("data")
        if isinstance(bits, int):
            out["bits"] = bits
        if isinstance(data, str):
            out["data"] = data[:_DATA_PREVIEW]
            out["data_full"] = data[:_DATA_TITLE]
    return out


def _start_key(row: dict[str, Any]) -> float:
    try:
        dt = datetime.fromisoformat(str(row.get("start_time")))
    except ValueError:
        return 0.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def want_attributed(attributed: str | None, status: str | None) -> bool | None:
    """The sensor-side ``attributed`` filter for a page's attributed/status
    parameters: tried and not tried are both not attributed."""
    if status in ("tried", "not_tried"):
        return False
    if status == "decoded":
        return True
    return {"true": True, "false": False}.get(attributed or "")


def matches(
    row: dict[str, Any], attributed: bool | None, model: str | None, status: str | None
) -> bool:
    """Apply the page's filters fleet-side too: a sensor that predates the
    attributed/model parameters ignores them and returns everything."""
    summary_status = attribution_summary(row)["status"]
    if attributed is True and summary_status != "decoded":
        return False
    if attributed is False and summary_status == "decoded":
        return False
    if model is not None and row.get("model") != model:
        return False
    return status is None or summary_status == status


def merge(
    per_sensor: list[tuple[dict[str, Any], list[dict[str, Any]]]], limit: int
) -> list[dict[str, Any]]:
    """The newest ``limit`` rows across sensors, each tagged with its sensor."""
    out: list[dict[str, Any]] = []
    for meta, rows in per_sensor:
        for r in rows:
            item = {k: r.get(k) for k in ROW_FIELDS}
            item["sensor"] = meta
            item["attribution"] = attribution_summary(r)
            out.append(item)
    out.sort(key=_start_key, reverse=True)
    return out[:limit]


def detection_doc(sensor: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    """One detection with its full attribution record (the popup, and its
    download)."""
    record = row.get("attribution")
    if isinstance(record, str):
        with contextlib.suppress(ValueError):  # else keep the text as stored
            record = json.loads(record)
    return {
        "sensor": sensor,
        "detection": {k: v for k, v in row.items() if k != "attribution"},
        "attribution": record,
    }


def download_headers(sensor_id: str, burst_id: str) -> dict[str, str]:
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in f"{sensor_id}-{burst_id}")
    return {"Content-Disposition": f'attachment; filename="{safe}.json"'}
