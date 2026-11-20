"""Daily status check: transmit the tone steps and two SSM epochs from an
ssm-tx radio, hold the sensors in high sensitivity while it does, then read
back what each sensor detected and score it.

1. Stage the run on the TX host: the tone sequence (its steps shifted by
   --tone-offset-db), its waveforms, and an ssm-tx config per SSM gain (one
   epoch, DEBUG so every hop is logged with its time).
2. Schedule on every sensor: a high-sensitivity-only run from --hs-pre before
   the tones for --hs-seconds, and (unless --capture-ms 0) a short IQ capture
   of the tones.
3. Transmit: the tone sequence starts at T0 with ssm-tx --utc-start; then one
   SSM epoch at each --ssm-gains level, back to back.
4. Check each sensor after the run ends:
   - tones: the Tone rows at each tone frequency, matched to the steps by time;
     levels against the planned step sizes;
   - SSM: the SSM rows during each epoch, matched to the hops ssm-tx logged
     (frequency and time); hops decoded per epoch and their level;
   - the run: its schedule status and the receiver overflows in its summary.

Needs only the Python standard library. ssm-tx runs over ssh on --tx-host
(key login), or locally without it.

usage:
  python3 tools/daily_check.py --sensor http://192.168.97.177 \\
      --tx-host ocollaco@192.168.97.153 --ssm-tx '~/GitHub/ssm-tx/.venv/bin/ssm-tx' \\
      --sequence tone_steps_sequence.json --waveforms DIR \\
      --tone-offset-db -30 --ssm-gains 60 45
"""

from __future__ import annotations

import argparse
import json
import math
import shlex
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ssm-tx: EIRP (dBm) = TX gain (dB) - 81.8 (8 dBm at the 89.8 dB maximum).
EIRP_AT_ZERO_GAIN = -81.8
MAX_GAIN_DB = 89.8
# The SSM band (ssm_config.yaml: 83 channels, 902.6 to 927.2 MHz).
SSM_BAND = (902.6e6, 927.2e6)
HOP_FREQ_TOL_HZ = 150e3
HOP_TIME_TOL_S = 1.5
TONE_FREQS = (914.85e6, 915.10e6)


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def iso(t: float) -> str:
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_iso(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


# ---------------------------------------------------------------- sensors


def http(method: str, url: str, body: Any = None, timeout: float = 10.0) -> tuple[int, Any]:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"null")
        except ValueError:
            return e.code, None


def get(url: str, **params: Any) -> Any:
    q = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    code, body = http("GET", url + ("?" + q if q else ""), timeout=30.0)
    if code != 200:
        raise RuntimeError(f"GET {url} -> {code}: {body}")
    return body


def preflight_sensor(url: str, seq_freqs: list[float]) -> dict[str, Any]:
    """Identity, tuning, clock offset; refuse a sensor that cannot do this."""
    ident = get(url + "/api/identity")
    name = ident.get("hostname") or url
    center, rate = float(ident["center_freq_hz"]), float(ident["sample_rate_hz"])
    lo, hi = center - rate / 2, center + rate / 2
    problems = []
    if not ident.get("active"):
        problems.append("sensor is not active")
    missing = [f for f in seq_freqs if not lo < f < hi]
    if missing:
        problems.append(
            f"tones {[f / 1e6 for f in missing]} MHz outside {lo / 1e6:.2f}-{hi / 1e6:.2f}"
        )
    if not (lo < SSM_BAND[0] and SSM_BAND[1] < hi):
        problems.append(
            f"SSM band 902.6-927.2 MHz not inside {lo / 1e6:.2f}-{hi / 1e6:.2f} (some hops unseen)"
        )
    hs = get(url + "/api/high-sensitivity")
    if hs.get("run"):
        problems.append(f"a high-sensitivity run is on: {hs['run'].get('id')}")
    # A sensor without high-sensitivity-only schedules would record the whole
    # window as IQ: probe with a body only such a sensor refuses for `capture`.
    code, body = http(
        "POST",
        url + "/api/schedules",
        {"start_ms": int((time.time() - 3600) * 1000), "duration_ms": 1000, "capture": "probe"},
    )
    if not (code == 400 and "capture" in str(body)):
        problems.append(
            "sensor does not support high-sensitivity-only schedules (RFObserver after 0.16.0b0)"
        )
    t0 = time.time()
    now_ms = get(url + "/api/schedules")["now_ms"]
    t1 = time.time()
    skew = now_ms / 1000 - (t0 + t1) / 2
    return {
        "url": url,
        "name": name,
        "version": ident.get("version"),
        "center_hz": center,
        "rate_hz": rate,
        "gain_db": ident.get("gain_db"),
        "clock_offset_s": round(skew, 4),
        "rtt_s": round(t1 - t0, 4),
        "problems": problems,
    }


def post_schedule(url: str, body: dict[str, Any]) -> None:
    code, resp = http("POST", url + "/api/schedules", body)
    if code not in (200, 201):
        raise RuntimeError(f"{url}: schedule {body['id']} refused ({code}): {resp}")
    for w in resp.get("warnings") or []:
        log(f"  {url}: {w}")


# ---------------------------------------------------------------- TX host


class Tx:
    """Runs commands on the TX host (ssh) or here."""

    def __init__(self, host: str | None, ssm_tx: str, workdir: str) -> None:
        self.host = host
        self.ssm_tx = ssm_tx
        self.dir = workdir

    def run(
        self, cmd: str, timeout: float, stdin: bytes | None = None
    ) -> subprocess.CompletedProcess[bytes]:
        argv = ["ssh", "-o", "BatchMode=yes", self.host, cmd] if self.host else ["bash", "-c", cmd]
        return subprocess.run(argv, input=stdin, capture_output=True, timeout=timeout)

    def check(self, cmd: str, timeout: float = 60.0, stdin: bytes | None = None) -> str:
        r = self.run(cmd, timeout, stdin)
        if r.returncode != 0:
            raise RuntimeError(
                f"TX host: {cmd!r} failed ({r.returncode}): {r.stderr.decode()[-800:]}"
            )
        return r.stdout.decode()

    def put(self, data: bytes, name: str) -> None:
        self.check(f"cat > {shlex.quote(self.dir + '/' + name)}", stdin=data)

    def clock_offset(self) -> float:
        """TX host clock minus ours, from ping-pongs inside one session (so the
        ssh handshake is not counted); the one with the shortest round trip."""
        prog = "import sys, time\nfor _ in sys.stdin: print(time.time(), flush=True)\n"
        cmd = f"python3 -u -c {shlex.quote(prog)}"
        argv = ["ssh", "-o", "BatchMode=yes", self.host, cmd] if self.host else ["bash", "-c", cmd]
        proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        assert proc.stdin is not None and proc.stdout is not None
        best = (math.inf, 0.0)
        for _ in range(8):
            t0 = time.time()
            proc.stdin.write("\n")
            proc.stdin.flush()
            remote = float(proc.stdout.readline())
            t1 = time.time()
            best = min(best, (t1 - t0, remote - (t0 + t1) / 2))
        proc.stdin.close()
        proc.wait(timeout=10)
        return best[1]


def expand(path: str) -> str:
    """A remote path for the shell: ~ stays unquoted so it expands there."""
    if path.startswith("~/"):
        return "~/" + shlex.quote(path[2:])
    return shlex.quote(path)


def stage(tx: Tx, seq: dict[str, Any], waveforms: Path, gains: list[float]) -> None:
    tx.check(f"mkdir -p {shlex.quote(tx.dir)}")
    files = sorted({s["file"] for s in seq["segments"] if "file" in s})
    for f in files:
        log(f"  copying {f} to the TX host")
        tx.put((waveforms / f).read_bytes(), f)
    tx.put(json.dumps(seq, indent=1).encode(), "tone_steps.json")
    py = str(Path(tx.ssm_tx).parent / "python")
    for g in gains:
        # One epoch, every hop logged (DEBUG) with its time, at this gain.
        script = (
            "import os, yaml, ssm_tx\n"
            "p = os.path.join(os.path.dirname(ssm_tx.__file__), 'ssm_config.yaml')\n"
            "c = yaml.safe_load(open(p))\n"
            f"c['rf']['tx_gain'] = {g!r}\n"
            "c['hopping']['epoch_limit'] = 1\n"
            "c['hopping']['continuous'] = True\n"
            "c['logging']['level'] = 'DEBUG'\n"
            "c['logging']['log_transmissions'] = True\n"
            f"c['logging']['log_file'] = 'epoch_g{g:g}.log'\n"
            f"yaml.safe_dump(c, open('epoch_g{g:g}.yaml', 'w'))\n"
        )
        tx.check(f"cd {shlex.quote(tx.dir)} && PYTHONPATH= {expand(py)} -", stdin=script.encode())


def ssm_log_times(text: str) -> list[tuple[float, str]]:
    """(UTC time, message) of each ssm-tx log line (asctime, run with TZ=UTC)."""
    out = []
    for line in text.splitlines():
        try:
            stamp, msg = line[:23], line.split(" - ", 2)[2]
            t = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S,%f").replace(tzinfo=timezone.utc)
        except (ValueError, IndexError):
            continue
        out.append((t.timestamp(), msg))
    return out


def run_tones(tx: Tx, center: float, rate: float, t0: float, timeout: float) -> dict[str, Any]:
    start = datetime.fromtimestamp(t0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cmd = (
        f"cd {shlex.quote(tx.dir)} && TZ=UTC PYTHONPATH= {expand(tx.ssm_tx)} --no-zms "
        f"--iq-sequence tone_steps.json --center-freq {center:g} --sample-rate {rate:g} "
        f"--utc-start {start} > tones.log 2>&1; echo rc=$?; cat tones.log"
    )
    out = tx.run(cmd, timeout).stdout.decode(errors="replace")
    lines = ssm_log_times(out)
    segs = [t for t, m in lines if m.startswith("Starting ")]
    done = [m for _, m in lines if m.startswith("Sequence complete")]
    rc = out.split("rc=", 1)[1].split()[0] if "rc=" in out else "?"
    return {
        "rc": rc,
        "first_segment_log": segs[0] if segs else None,
        "complete": done[:1],
        "tail": out[-600:],
    }


def run_epoch(tx: Tx, gain: float, timeout: float) -> dict[str, Any]:
    power = gain + EIRP_AT_ZERO_GAIN
    cmd = (
        f"cd {shlex.quote(tx.dir)} && TZ=UTC PYTHONPATH= {expand(tx.ssm_tx)} --no-zms "
        f"--config epoch_g{gain:g}.yaml --power {power:g} > epoch_g{gain:g}.out 2>&1; "
        f"echo rc=$?; cat epoch_g{gain:g}.log"
    )
    out = tx.run(cmd, timeout).stdout.decode(errors="replace")
    rc = out.split("rc=", 1)[1].split()[0] if "rc=" in out else "?"
    hops = []
    start = end = None
    for t, m in ssm_log_times(out):
        if m.startswith("Starting transmission"):
            start = t
        elif m.startswith("Epoch 1 completed"):
            end = t
        elif m.startswith("CH ") and "MHz" in m:
            # "CH 12 |  906.20 MHz | TX:  27.1ms | ..." logged as send() returns,
            # TX ms after the pulse was handed to the radio.
            parts = m.split("|")
            mhz = float(parts[1].split()[0])
            sent = float(parts[2].split()[1].rstrip("ms")) / 1000
            hops.append({"t_log": t, "t_air": t - sent, "freq_hz": mhz * 1e6})
    return {"gain_db": gain, "power_dbm": power, "rc": rc, "start": start, "end": end, "hops": hops}


# ---------------------------------------------------------------- checks


def tone_steps(seq: dict[str, Any], t0: float) -> list[dict[str, Any]]:
    """Each waveform segment's (start, end, power) from the sequence's start."""
    steps, t = [], t0
    for _ in range(int(seq.get("repeat", 1))):
        for s in seq["segments"]:
            dur = float(s.get("dur", s.get("blank", 0.0)))
            if "file" in s:
                steps.append({"start": t, "end": t + dur, "power_dbm": s.get("power_dbm")})
            t += dur
    return steps


def check_tones(
    rows: list[dict[str, Any]], steps: list[dict[str, Any]], min_steps: int, tol_db: float
) -> dict[str, Any]:
    out: dict[str, Any] = {"freqs": {}, "ok": True}
    for f in TONE_FREQS:
        per = [None] * len(steps)
        extra = 0
        for r in rows:
            a = json.loads(r["attribution"])["frames"][0]
            if abs(float(a["tone_hz"]) - f) > 1e3:
                continue
            ts = parse_iso(r["start_time"])
            te = ts + float(a["duration_ms"]) / 1000
            best, best_ov = None, 0.0
            for i, s in enumerate(steps):
                ov = min(te, s["end"]) - max(ts, s["start"])
                if ov > best_ov:
                    best, best_ov = i, ov
            if best is None or best_ov < 0.5 * min(
                te - ts, steps[best]["end"] - steps[best]["start"]
            ):
                extra += 1
                continue
            row = {
                "level_db": a["level_db"],
                "snr_db": a["snr_db"],
                "duration_ms": a["duration_ms"],
                "offset_ms": round((ts - steps[best]["start"]) * 1000, 1),
            }
            if per[best] is None or row["snr_db"] > per[best]["snr_db"]:
                per[best] = row
        found = [i for i, p in enumerate(per) if p is not None]
        linearity = []
        for i, j in zip(found, found[1:], strict=False):
            planned = steps[i]["power_dbm"] - steps[j]["power_dbm"]
            measured = per[i]["level_db"] - per[j]["level_db"]
            linearity.append(
                {
                    "steps": [i + 1, j + 1],
                    "planned_db": round(planned, 1),
                    "measured_db": round(measured, 1),
                    "error_db": round(measured - planned, 1),
                    "ok": abs(measured - planned) <= tol_db,
                }
            )
        ok = len(found) >= min_steps
        out["ok"] &= ok
        out["freqs"][f"{f / 1e6:.2f}"] = {
            "steps_found": len(found),
            "steps": len(steps),
            "ok": ok,
            "unmatched_rows": extra,
            "per_step": per,
            "linearity": linearity,
        }
    return out


def check_ssm(
    rows: list[dict[str, Any]], epochs: list[dict[str, Any]], min_frac: float
) -> dict[str, Any]:
    used: set[int] = set()
    out: dict[str, Any] = {"epochs": [], "ok": True}
    for ep in epochs:
        hit, offsets, levels, srcs = 0, [], [], {}
        for h in ep["hops"]:
            match = None
            for r in rows:
                if (
                    r["id"] in used
                    or abs(float(r["center_freq_hz"]) - h["freq_hz"]) > HOP_FREQ_TOL_HZ
                ):
                    continue
                dt = parse_iso(r["start_time"]) - h["t_air"]
                if abs(dt) <= HOP_TIME_TOL_S and (match is None or abs(dt) < abs(match[1])):
                    match = (r, dt)
            if match is None:
                continue
            r, dt = match
            used.add(r["id"])
            hit += 1
            offsets.append(dt)
            levels.append(float(r["peak_power_db"]))
            src = (json.loads(r["attribution"]).get("frames") or [{}])[0].get("src_id")
            srcs[str(src)] = srcs.get(str(src), 0) + 1
        n = len(ep["hops"])
        ok = n > 0 and hit >= min_frac * n
        out["ok"] &= ok
        out["epochs"].append(
            {
                "gain_db": ep["gain_db"],
                "tx_start": iso(ep["start"]) if ep["start"] else None,
                "tx_end": iso(ep["end"]) if ep["end"] else None,
                "hops_sent": n,
                "hops_decoded": hit,
                "ok": ok,
                "src_ids": srcs,
                "median_offset_ms": round(statistics.median(offsets) * 1000, 1)
                if offsets
                else None,
                "median_peak_db": round(statistics.median(levels), 1) if levels else None,
            }
        )
    lv = [e["median_peak_db"] for e in out["epochs"]]
    if len(lv) == 2 and None not in lv:
        out["level_step_db"] = {
            "planned": round(epochs[0]["gain_db"] - epochs[1]["gain_db"], 1),
            "measured": round(lv[0] - lv[1], 1),
        }
    out["ssm_rows_not_ours"] = len([r for r in rows if r["id"] not in used])
    return out


def check_sensor(
    sensor: dict[str, Any],
    sched_id: str,
    steps: list[dict[str, Any]],
    epochs: list[dict[str, Any]],
    args: argparse.Namespace,
    win: tuple[float, float],
) -> dict[str, Any]:
    url = sensor["url"]
    sched = get(f"{url}/api/schedules/{sched_id}")
    runs = sched.get("runs") or []
    run = runs[0] if runs else {}
    hs_id = (run.get("info") or {}).get("hs_run")
    res: dict[str, Any] = {
        "sensor": sensor["name"],
        "schedule_run": {k: run.get(k) for k in ("status", "reason")},
        "hs_run": hs_id,
    }
    summary = {}
    if hs_id:
        summary = (get(f"{url}/api/high-sensitivity/{urllib.parse.quote(hs_id)}") or {}).get(
            "summary"
        ) or {}
    res["overflow_events"] = summary.get("overflow_events")
    res["hs_detections"] = summary.get("detections")
    tones = get(
        f"{url}/api/detections.json", limit=2000, model="Tone", since=iso(win[0]), until=iso(win[1])
    )
    res["tones"] = check_tones(
        tones["detections"], steps, args.min_tone_steps, args.linearity_tol_db
    )
    ssm = get(
        f"{url}/api/detections.json", limit=5000, model="SSM", since=iso(win[0]), until=iso(win[1])
    )
    res["ssm"] = check_ssm(ssm["detections"], epochs, args.min_ssm_frac)
    res["ok"] = bool(run.get("status") == "done" and res["tones"]["ok"] and res["ssm"]["ok"])
    return res


def print_result(r: dict[str, Any]) -> None:
    print(f"\n=== {r['sensor']}: {'PASS' if r['ok'] else 'FAIL'}")
    sr = r["schedule_run"]
    print(
        f"  HS run {r['hs_run']}: {sr['status']}"
        f"{' (' + sr['reason'] + ')' if sr.get('reason') else ''}, "
        f"{r['hs_detections']} detections, overflow events {r['overflow_events']}"
    )
    for f, t in r["tones"]["freqs"].items():
        print(
            f"  tone {f} MHz: {t['steps_found']}/{t['steps']} steps "
            f"({'ok' if t['ok'] else 'FAIL'}), "
            f"{t['unmatched_rows']} rows outside the steps"
        )
        for i, p in enumerate(t["per_step"]):
            if p:
                print(
                    f"    step {i + 1}: level {p['level_db']:7.1f} dB  SNR {p['snr_db']:5.1f} dB  "
                    f"{p['duration_ms']:6.1f} ms  start {p['offset_ms']:+6.1f} ms"
                )
            else:
                print(f"    step {i + 1}: not found")
        for li in t["linearity"]:
            flag = "" if li["ok"] else "  <-- off"
            print(
                f"    steps {li['steps'][0]}->{li['steps'][1]}: planned {li['planned_db']:5.1f}  "
                f"measured {li['measured_db']:5.1f} dB{flag}"
            )
    for e in r["ssm"]["epochs"]:
        print(
            f"  SSM epoch at {e['gain_db']:g} dB gain ({e['tx_start']} to {e['tx_end']}): "
            f"{e['hops_decoded']}/{e['hops_sent']} hops decoded ({'ok' if e['ok'] else 'FAIL'}), "
            f"median level {e['median_peak_db']} dB, median offset {e['median_offset_ms']} ms, "
            f"src {e['src_ids']}"
        )
    if "level_step_db" in r["ssm"]:
        s = r["ssm"]["level_step_db"]
        print(f"  SSM level step: planned {s['planned']} dB, measured {s['measured']} dB")
    print(
        f"  SSM rows not matched to a hop (TX images, other SSMs): {r['ssm']['ssm_rows_not_ours']}"
    )


# ---------------------------------------------------------------- main


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--sensor", action="append", required=True, help="sensor base URL (repeat for several)"
    )
    p.add_argument("--tx-host", help="ssh target running ssm-tx (default: this host)")
    p.add_argument(
        "--ssm-tx", default="~/GitHub/ssm-tx/.venv/bin/ssm-tx", help="ssm-tx command on the TX host"
    )
    p.add_argument(
        "--workdir", default="~/daily-check", help="run folder on the TX host (a subfolder per run)"
    )
    p.add_argument("--sequence", required=True, type=Path, help="tone step sequence JSON")
    p.add_argument(
        "--waveforms",
        type=Path,
        help="folder with the sequence's waveform files (default: beside it)",
    )
    p.add_argument("--center-freq", type=float, default=915e6, help="tone sequence centre (Hz)")
    p.add_argument(
        "--tone-offset-db", type=float, default=0.0, help="shift every tone step by this (dB)"
    )
    p.add_argument(
        "--ssm-gains",
        type=float,
        nargs=2,
        default=[89.8, 74.8],
        metavar="DB",
        help="TX gain of the two SSM epochs (dB; EIRP = gain - 81.8)",
    )
    p.add_argument(
        "--lead",
        type=float,
        default=30.0,
        help="seconds from now to the tones (radio init, schedules)",
    )
    p.add_argument(
        "--hs-pre",
        type=float,
        default=3.0,
        help="high sensitivity starts this long before the tones (s)",
    )
    p.add_argument(
        "--hs-seconds",
        type=float,
        default=90.0,
        help="length of the high-sensitivity run (s, at most 300)",
    )
    p.add_argument(
        "--capture-ms", type=int, default=2000, help="IQ capture of the tones (ms; 0: none)"
    )
    p.add_argument(
        "--min-tone-steps", type=int, default=5, help="steps each tone must show to pass"
    )
    p.add_argument(
        "--min-ssm-frac",
        type=float,
        default=0.5,
        help="fraction of hops each epoch must decode to pass",
    )
    p.add_argument(
        "--linearity-tol-db", type=float, default=2.0, help="step size error to flag (dB)"
    )
    p.add_argument("--report", type=Path, help="write the full result as JSON here")
    p.add_argument(
        "--dry-run", action="store_true", help="check and plan only; transmit and schedule nothing"
    )
    args = p.parse_args()

    seq = json.loads(args.sequence.read_text())
    waveforms = args.waveforms or args.sequence.parent
    rate = float(seq["sample_rate"])
    for i, s in enumerate(seq["segments"]):
        if "power_dbm" in s:
            power = round(float(s["power_dbm"]) + args.tone_offset_db, 2)
            g = power - EIRP_AT_ZERO_GAIN
            if g > MAX_GAIN_DB:
                sys.exit(f"tone step {power} dBm needs {g:.1f} dB gain, above {MAX_GAIN_DB}")
            if g < 0:
                # Below the radio's range: keep its time as a blank.
                print(f"tone step {power} dBm needs {g:.1f} dB gain: sent as a blank")
                seq["segments"][i] = {"blank": s["dur"]}
            else:
                s["power_dbm"] = power
    for g in args.ssm_gains:
        if not 0 <= g <= MAX_GAIN_DB:
            sys.exit(f"--ssm-gains {g} is outside 0-{MAX_GAIN_DB} dB")
    if not 0 < args.hs_seconds <= 300:
        sys.exit("--hs-seconds must be 1 to 300")
    seq["waveform_dir"] = "."
    seq.pop("_comment", None)
    for s in seq["segments"]:
        s.pop("_gain_db", None)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tx = Tx(args.tx_host, args.ssm_tx, f"{args.workdir.rstrip('/')}/{stamp}")
    if tx.dir.startswith("~/"):
        tx.dir = tx.check("echo $HOME").strip() + tx.dir[1:]

    log("preflight")
    sensors = [preflight_sensor(u.rstrip("/"), list(TONE_FREQS)) for u in args.sensor]
    for s in sensors:
        log(
            f"  {s['name']} {s['version']}: {s['center_hz'] / 1e6:g} MHz, "
            f"{s['rate_hz'] / 1e6:g} MS/s, "
            f"gain {s['gain_db']}, clock {s['clock_offset_s'] * 1000:+.0f} ms"
        )
        for pr in s["problems"]:
            log(f"  {s['name']}: {pr}")
    tx_skew = tx.clock_offset()
    log(f"  TX host {args.tx_host or 'local'}: clock {tx_skew * 1000:+.0f} ms")
    if any(s["problems"] for s in sensors):
        log("refusing to run: fix the sensor problems above")
        return 2

    t0 = math.ceil(time.time() + args.lead)
    steps = tone_steps(seq, t0)
    hs_start = t0 - args.hs_pre
    hs_id = f"daily-{stamp}-hs"
    log(
        f"plan: tones at {iso(t0)} ({len(steps)} steps, "
        f"{steps[0]['power_dbm']} to {steps[-1]['power_dbm']} dBm), "
        f"then SSM epochs at {args.ssm_gains[0]:g} and {args.ssm_gains[1]:g} dB gain; "
        f"high sensitivity {iso(hs_start)} for {args.hs_seconds:g} s"
    )
    if args.dry_run:
        return 0

    log("staging the TX host")
    stage(tx, seq, waveforms, args.ssm_gains)
    for s in sensors:
        post_schedule(
            s["url"],
            {
                "id": hs_id,
                "start_ms": int(hs_start * 1000),
                "duration_ms": int(args.hs_seconds * 1000),
                "high_sensitivity": True,
                "capture": False,
                "label": f"Daily check {stamp}",
            },
        )
        if args.capture_ms:
            post_schedule(
                s["url"],
                {
                    "id": f"daily-{stamp}-iq",
                    "start_ms": int(t0 * 1000) - 250,
                    "duration_ms": args.capture_ms,
                    "label": f"Daily check tones {stamp}",
                },
            )
    log(f"scheduled {hs_id} on {len(sensors)} sensor(s)")

    log("tones: ssm-tx waits for its --utc-start")
    tones = run_tones(tx, args.center_freq, rate, t0, timeout=args.lead + 60)
    log(
        f"  tones done (rc {tones['rc']}): "
        f"{tones['complete'][0] if tones['complete'] else tones['tail']}"
    )
    epochs = []
    for g in args.ssm_gains:
        log(f"SSM epoch at {g:g} dB gain ({g + EIRP_AT_ZERO_GAIN:g} dBm)")
        ep = run_epoch(tx, g, timeout=90)
        log(f"  rc {ep['rc']}, {len(ep['hops'])} hops logged")
        epochs.append(ep)
    hs_end = hs_start + args.hs_seconds
    last = max((ep["end"] or 0) for ep in epochs)
    if last > hs_end:
        log(
            f"WARNING: the last epoch ended {last - hs_end:.1f} s after high sensitivity "
            "did (raise --hs-seconds)"
        )
    wait = hs_end + 5 - time.time()
    if wait > 0:
        log(f"waiting {wait:.0f} s for the high-sensitivity run to end and its summary")
        time.sleep(wait)

    win = (hs_start - 1, max(hs_end, last) + 2)
    results = [check_sensor(s, hs_id, steps, epochs, args, win) for s in sensors]
    for r in results:
        print_result(r)
    report = {
        "stamp": stamp,
        "t0": iso(t0),
        "tx_host": args.tx_host,
        "tx_clock_offset_s": round(tx_skew, 4),
        "tone_offset_db": args.tone_offset_db,
        "sensors": sensors,
        "tones_tx": tones,
        "epochs_tx": [
            {k: v for k, v in e.items() if k != "hops"} | {"hops": len(e["hops"])} for e in epochs
        ],
        "results": results,
    }
    if args.report:
        args.report.write_text(json.dumps(report, indent=1, default=str))
        log(f"report: {args.report}")
    ok = all(r["ok"] for r in results)
    print(f"\nDaily check: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
