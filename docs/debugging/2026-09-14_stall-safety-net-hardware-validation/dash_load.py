"""Drive Dashboard-shaped load: each 'tab' repeatedly fires the same 4
requests the Averaged page's loadAll() fires (waterfall, stats, detections,
iq-captures) over a sliding 24 h 'Now' range, so the waterfall cache never
hits. Logs per-request latency and a final summary.

Usage: dash_load.py BASE_URL TABS DURATION_SEC [HOURS]
"""

import sys
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

base, tabs, duration = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
hours = float(sys.argv[4]) if len(sys.argv) > 4 else 24.0
paths = ["/api/averaged/waterfall", "/api/averaged/stats", "/api/detections.json", "/api/iq-captures"]
lat: dict[str, list[float]] = {p: [] for p in paths}
errors: list[str] = []
lock = threading.Lock()
stop_at = time.monotonic() + duration


def fetch(path: str, qs: str) -> None:
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(f"{base}{path}?{qs}", timeout=120) as r:
            r.read()
        dt = time.monotonic() - t0
        with lock:
            lat[path].append(dt)
        if dt > 5:
            print(f"{datetime.now():%H:%M:%S} slow {path} {dt:.1f}s", flush=True)
    except Exception as e:  # noqa: BLE001
        with lock:
            errors.append(f"{path}: {e!r}")
        print(f"{datetime.now():%H:%M:%S} ERROR {path} {e!r}", flush=True)


def tab() -> None:
    while time.monotonic() < stop_at:
        now = datetime.now(timezone.utc)
        qs = urllib.parse.urlencode(
            {
                "since": (now - timedelta(hours=hours)).isoformat().replace("+00:00", "Z"),
                "until": now.isoformat().replace("+00:00", "Z"),
                "max_rows": "600",
                "max_bins": "512",
            }
        )
        ts = [threading.Thread(target=fetch, args=(p, qs)) for p in paths]
        for t in ts:
            t.start()
        for t in ts:
            t.join()


threads = [threading.Thread(target=tab) for _ in range(tabs)]
for t in threads:
    t.start()
for t in threads:
    t.join()
for p in paths:
    v = sorted(lat[p])
    if v:
        print(f"{p}: n={len(v)} p50={v[len(v) // 2]:.2f}s p95={v[int(len(v) * 0.95)]:.2f}s max={v[-1]:.2f}s")
    else:
        print(f"{p}: n=0")
print(f"errors={len(errors)}")
