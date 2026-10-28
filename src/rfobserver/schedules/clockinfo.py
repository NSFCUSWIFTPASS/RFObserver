"""What disciplines the host clock, for scheduled captures' metadata.

Read from chrony when it runs (it reports the current offset), else from
systemd-timesyncd (server and jitter; it does not expose an offset). Cached
for a minute; never raises and never waits more than a second per command.
"""

from __future__ import annotations

import re
import subprocess
import time
from typing import Any

CACHE_SEC = 60.0
_TIMEOUT_SEC = 1.0
_cache: tuple[float, dict[str, Any]] | None = None


def clear_cache() -> None:
    global _cache
    _cache = None


def _run(cmd: list[str]) -> str | None:
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=_TIMEOUT_SEC, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout if out.returncode == 0 else None


def _unknown() -> dict[str, Any]:
    return {
        "clock_source": "unknown",
        "clock_server": None,
        "clock_offset_ms": None,
        "clock_jitter_ms": None,
        "clock_synced": None,
    }


def _synced() -> bool | None:
    out = _run(["timedatectl", "show", "-p", "NTPSynchronized", "--value"])
    if out is None:
        return None
    return out.strip() == "yes"


def _chrony() -> dict[str, Any] | None:
    out = _run(["chronyc", "-c", "tracking"])
    if not out:
        return None
    parts = out.strip().split(",")
    if len(parts) < 5:
        return None
    try:
        offset_ms = float(parts[4]) * 1000
    except ValueError:
        offset_ms = None
    info = _unknown()
    info.update(clock_source="ntp", clock_server=parts[1] or None, clock_offset_ms=offset_ms)
    info["clock_synced"] = parts[-1].strip() == "Normal"
    return info


def _timesyncd() -> dict[str, Any] | None:
    out = _run(["timedatectl", "show-timesync", "-p", "ServerName", "-p", "NTPMessage", "--value"])
    if not out:
        return None
    lines = [ln.strip() for ln in out.splitlines()]
    info = _unknown()
    info["clock_source"] = "ntp"
    info["clock_server"] = lines[0] or None
    m = re.search(r"Jitter=([0-9.]+)(us|ms|s)\b", out)
    if m:
        scale = {"us": 1e-3, "ms": 1.0, "s": 1e3}[m.group(2)]
        info["clock_jitter_ms"] = round(float(m.group(1)) * scale, 6)
    info["clock_synced"] = _synced()
    return info


def clock_info() -> dict[str, Any]:
    global _cache
    now = time.monotonic()
    if _cache is not None and now - _cache[0] < CACHE_SEC:
        return dict(_cache[1])
    info = _chrony() or _timesyncd() or _unknown()
    _cache = (now, info)
    return dict(info)
