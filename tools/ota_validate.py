"""Validate OTA burst detections against the barcode schedule.

Identity is spectral (the frequency barcode), not timing: each scheduled combo
is matched to a detection by its assigned center + bandwidth. Fetches the
sensor's JSON detections over the LAN and prints a per-combo report.

Usage:
  PYTHONPATH= .venv/bin/python tools/ota_validate.py \
      --schedule ota_schedule.json --jetson 192.168.97.153 --port 8888
"""

from __future__ import annotations

import argparse
import json
import urllib.request
from typing import Any


def match_detections(
    schedule: list[dict[str, Any]],
    detections: list[dict[str, Any]],
    *,
    center_tol_hz: float,
    bw_rel_tol: float,
) -> list[dict[str, Any]]:
    """Match each scheduled combo to a detection by barcode (center + bandwidth).

    A detection matches a combo if its center is within ``center_tol_hz`` of the
    combo's assigned center AND its bandwidth is within ``bw_rel_tol`` (relative)
    of the combo's bandwidth. The strongest (``peak_power_db``) qualifying
    detection wins. Timing is not used.
    """
    results: list[dict[str, Any]] = []
    for combo in schedule:
        center = combo["center_hz"]
        bw = combo["bw_hz"]
        bw_tol = bw_rel_tol * bw + center_tol_hz
        cands = [
            d
            for d in detections
            if abs(d["center_freq_hz"] - center) <= center_tol_hz
            and abs(d["bandwidth_hz"] - bw) <= bw_tol
        ]
        base = {
            "index": combo["index"],
            "bw_hz": bw,
            "duration_ms": combo["duration_ms"],
            "center_hz": center,
        }
        if not cands:
            results.append({**base, "matched": False})
            continue
        best = max(cands, key=lambda d: d.get("peak_power_db", -1e9))
        results.append(
            {
                **base,
                "matched": True,
                "meas_center_hz": best["center_freq_hz"],
                "meas_bandwidth_hz": best["bandwidth_hz"],
                "meas_duration_ms": best["duration_ms"],
                "meas_peak_db": best.get("peak_power_db"),
            }
        )
    return results


def _fetch_detections(jetson: str, port: int, limit: int = 1000) -> list[dict[str, Any]]:
    url = f"http://{jetson}:{port}/api/detections.json?limit={limit}"
    with urllib.request.urlopen(url, timeout=15) as r:  # noqa: S310 (trusted LAN host)
        return json.load(r)["detections"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--schedule", default="ota_schedule.json")
    ap.add_argument("--jetson", default="192.168.97.153")
    ap.add_argument("--port", type=int, default=8888)
    ap.add_argument("--center-tol-hz", type=float, default=100_000)
    ap.add_argument("--bw-rel-tol", type=float, default=0.6)
    args = ap.parse_args()

    with open(args.schedule) as f:
        schedule = json.load(f)
    detections = _fetch_detections(args.jetson, args.port)

    results = match_detections(
        schedule, detections, center_tol_hz=args.center_tol_hz, bw_rel_tol=args.bw_rel_tol
    )
    n_match = sum(1 for r in results if r["matched"])
    print(f"{n_match}/{len(results)} combos detected ({len(detections)} raw detections)\n")
    for r in results:
        head = (
            f"[{r['index']:>2}] bw={r['bw_hz'] / 1e3:>6.0f}k dur={r['duration_ms']:>6}ms "
            f"@ {r['center_hz'] / 1e6:.3f}MHz"
        )
        if r["matched"]:
            de = abs(r["meas_duration_ms"] - r["duration_ms"])
            we = abs(r["meas_bandwidth_hz"] - r["bw_hz"])
            print(
                f"{head} -> dur_err={de:>7.2f}ms bw_err={we / 1e3:>7.0f}kHz "
                f"meas_center={r['meas_center_hz'] / 1e6:.3f}MHz peak={r['meas_peak_db']}"
            )
        else:
            print(f"{head} -> NOT DETECTED")


if __name__ == "__main__":
    main()
