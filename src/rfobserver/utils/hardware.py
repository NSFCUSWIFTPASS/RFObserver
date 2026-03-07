"""Hardware and system information utilities."""

from __future__ import annotations

import os
import shutil


def get_cpu_percent() -> float:
    """Get CPU usage percentage (simple /proc/stat based)."""
    try:
        with open("/proc/loadavg") as f:
            load_1m = float(f.read().split()[0])
        cpu_count = os.cpu_count() or 1
        return min(load_1m / cpu_count * 100, 100.0)
    except (OSError, ValueError):
        return 0.0


def get_memory_percent() -> float:
    """Get memory usage percentage."""
    try:
        with open("/proc/meminfo") as f:
            lines = f.readlines()
        info = {}
        for line in lines:
            parts = line.split(":")
            if len(parts) == 2:
                info[parts[0].strip()] = int(parts[1].strip().split()[0])
        total = info.get("MemTotal", 1)
        available = info.get("MemAvailable", total)
        return (1 - available / total) * 100
    except (OSError, ValueError, KeyError):
        return 0.0


def get_disk_usage(path: str) -> tuple[float, float]:
    """Get disk usage in GB: (used, total)."""
    usage = shutil.disk_usage(path)
    return usage.used / (1024**3), usage.total / (1024**3)
