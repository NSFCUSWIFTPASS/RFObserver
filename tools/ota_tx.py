"""Transmit the OTA burst set (frequency barcode) via the workstation B200mini.

Reuses the exact comb generator the simulated matrix uses, so the transmitted
waveforms match. Each combo is sent at its barcode center offset with a gap
between bursts; a ground-truth schedule is written to JSON for the validator.

Usage:
  # offline check (no radio): prints combos + writes schedule
  PYTHONPATH= .venv/bin/python tools/ota_tx.py --dry-run --subset

  # live transmit (B200mini on TX/RX, 915 MHz, 28 MHz)
  PYTHONPATH= .venv/bin/python tools/ota_tx.py --tx-gain 60 --gap 3 --subset
"""

from __future__ import annotations

import argparse
import json
import sys
import time

import numpy as np

sys.path.insert(0, "tools")

import ota_common as oc  # noqa: E402

TX_RATE = 28_000_000
# Representative subset to close the loop before the full 5x5.
SUBSET = [
    (150_000, 2.7),
    (2_000_000, 2.7),
    (20_000_000, 2.7),
    (150_000, 83.2),
    (2_000_000, 83.2),
    (20_000_000, 83.2),
]


def make_tx_burst(bw_hz: float, dur_ms: float, offset_hz: float) -> np.ndarray:
    """Fast flat-band comb burst (~0.7 peak) at the TX sample rate."""
    return oc.make_comb_burst(bw_hz, dur_ms, offset_hz, TX_RATE, peak=0.7)


def _combos(subset: bool) -> list[tuple[int, float]]:
    if subset:
        return list(SUBSET)
    return [(bw, dur) for bw in oc.BURST_BWS for dur in oc.BURST_DURATIONS_MS]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tx-gain", type=float, default=60.0)
    ap.add_argument("--gap", type=float, default=3.0)
    ap.add_argument("--subset", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="no radio; generate + write schedule")
    ap.add_argument("--schedule", default="ota_schedule.json")
    args = ap.parse_args()

    combos = _combos(args.subset)

    streamer = None
    if not args.dry_run:
        import uhd

        usrp = uhd.usrp.MultiUSRP()
        usrp.set_tx_rate(TX_RATE, 0)
        usrp.set_tx_freq(uhd.libpyuhd.types.tune_request(oc.CENTER_HZ), 0)
        usrp.set_tx_gain(args.tx_gain, 0)
        usrp.set_tx_antenna("TX/RX", 0)
        streamer = usrp.get_tx_stream(uhd.usrp.StreamArgs("fc32", "fc32"))

    schedule = []
    for i, (bw, dur) in enumerate(combos):
        off = oc.barcode_offset(bw, dur)
        burst = make_tx_burst(bw, dur, off)
        center = oc.CENTER_HZ + off
        schedule.append(
            {
                "index": i,
                "tx_wallclock": time.time(),
                "bw_hz": bw,
                "duration_ms": dur,
                "offset_hz": off,
                "center_hz": center,
                "tx_gain": args.tx_gain,
                "n_samples": int(burst.size),
                "peak": float(np.max(np.abs(burst))),
            }
        )
        print(
            f"[{i}] bw={bw / 1e3:.0f}kHz dur={dur}ms "
            f"center={center / 1e6:.3f}MHz samples={burst.size} peak={schedule[-1]['peak']:.2f}"
        )
        if not args.dry_run:
            import uhd

            md = uhd.types.TXMetadata()
            md.start_of_burst = True
            md.end_of_burst = True
            streamer.send(burst, md)
            time.sleep(args.gap)  # gap only matters on the air, not for a dry-run

    with open(args.schedule, "w") as f:
        json.dump(schedule, f, indent=2)
    print(f"wrote {args.schedule} ({len(schedule)} bursts)")


if __name__ == "__main__":
    main()
