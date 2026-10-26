"""Fleet Captures: every sensor's captures as one list, newest first.

Each sensor pages its own list newest first (``/captures/list``, by its
capture index). The fleet asks every sensor for its next page and merges the
heads: it always takes the newest of the sensors' next captures, so each
sensor's list is consumed in its own order and the cursor (how far into each
sensor's list the fleet has shown) never skips or repeats a capture.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

# Paths under /captures/ the fleet reads through, by capture file name.
READ_KINDS = ("detail", "psd", "detections")


def _start_key(entry: dict[str, Any]) -> float:
    meta = entry.get("meta")
    t = meta.get("start_time") if isinstance(meta, dict) else None
    try:
        dt = datetime.fromisoformat(str(t))
    except ValueError:
        return float("-inf")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def merge_page(
    per_sensor: list[tuple[dict[str, Any], list[dict[str, Any]], int]], limit: int
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Up to ``limit`` captures from (sensor meta, its next captures, its
    offset) lists, each tagged with its sensor, and each sensor's new offset."""
    heads = [0] * len(per_sensor)
    out: list[dict[str, Any]] = []
    while len(out) < limit:
        best = -1
        best_key = float("-inf")
        for i, (_, entries, _) in enumerate(per_sensor):
            if heads[i] < len(entries):
                key = _start_key(entries[heads[i]])
                if best < 0 or key > best_key:
                    best, best_key = i, key
        if best < 0:
            break
        meta, entries, _ = per_sensor[best]
        out.append({**entries[heads[best]], "sensor": meta})
        heads[best] += 1
    cursor = {meta["id"]: offset + heads[i] for i, (meta, _, offset) in enumerate(per_sensor)}
    return out, cursor


def parse_cursor(raw: str | None) -> dict[str, int]:
    """``"a:3,b:0"`` as {sensor id: offset}; malformed parts are ignored."""
    out: dict[str, int] = {}
    for part in (raw or "").split(","):
        sid, _, num = part.rpartition(":")
        if sid and num.isdigit():
            out[sid] = int(num)
    return out


def format_cursor(cursor: dict[str, int]) -> str:
    return ",".join(f"{sid}:{n}" for sid, n in cursor.items())
