# OTA Burst Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans for the CODE tasks (1-4). Tasks 5-7 are live-hardware runbooks executed interactively, not subagent work. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Validate that the real field sensor (Jetson + B200mini running RFObserver) detects the burst set transmitted over the air by the workstation B200mini, with timing-independent ground truth via a frequency barcode.

**Architecture:** Reusable code tooling (JSON detections endpoint, barcode helper, TX harness, validation script) built and tested first; then live hardware bring-up, calibration, and the run. TX waveforms reuse the exact comb generator the simulated matrix uses.

**Tech Stack:** Python 3.10+ (Jetson) / 3.11 (workstation venv), UHD 4.9 (`uhd` python), numpy, FastAPI, the existing RFObserver pipeline. Two Ettus B200mini.

## Global Constraints

- Prefix Python commands with `PYTHONPATH=`; ruff-clean (`ruff check src/ tests/`, `ruff format --check`); mypy covers `src/` only. No emojis; no `Co-Authored-By: Claude`.
- **Hardware:** TX = workstation B200mini (serial 321D126), antenna port **TX/RX**. RX = Jetson B200mini, antenna port **RX2** (RFObserver's receiver uses `set_rx_antenna("RX2")`). OTA.
- **Band:** 915 MHz center, 915 ISM (902-928 MHz), usable offset **±12 MHz**. Keep TX power low / range short (Part 15 ISM).
- **Sensor config (real mode):** `RFOBS_MOCK_RECEIVER=false`, `RFOBS_SENSOR_ACTIVE=true`, `RFOBS_FREQUENCY_START=RFOBS_FREQUENCY_END=915000000`, `RFOBS_BANDWIDTH=28000000`, `RFOBS_NUM_FFT_BINS=2048`, `RFOBS_WEB_PORT=8888`, `RFOBS_GAIN=<Phase C>`.
- **Jetson:** `ocollaco@192.168.97.153`, dev checkout `~/GitHub/RFObserver` (`.venv`), `git pull` to update; passwordless sudo.
- **Burst set:** occupied BW ∈ {50k, 150k, 500k, 2M, 20M} Hz; duration ∈ {1.3, 2.7, 10.24, 83.2, 393.1} ms.
- **Barcode:** each combo transmitted at a distinct assigned center offset; `(center, bandwidth)` identifies the combo. TX and validator import the SAME `barcode_offset(bw, dur)`.

## File Structure

- `src/rfobserver/web/routes/api.py` — add `GET /api/detections.json` (JSON sibling of the HTML fragment).
- `tools/ota_common.py` (new) — `BURST_BWS`, `BURST_DURATIONS`, `barcode_offset(bw_hz, dur_ms)`, `all_combos()`; imports the comb generator from `tests.integration._synth`.
- `tools/ota_tx.py` (new) — UHD transmitter + schedule writer; `--dry-run` skips UHD.
- `tools/ota_validate.py` (new) — pull JSON detections, match by barcode, report; matching logic in an importable function.
- `tests/unit/test_ota_common.py`, `tests/unit/test_ota_validate.py` (new) — pure-logic tests.
- `tests/unit/test_web_routes.py` — add a test for the JSON endpoint.

Tests import tooling via `sys.path.insert(0, "tools")` (mirrors how integration tests import `_synth`).

---

### Task 1: JSON detections endpoint

**Files:**
- Modify: `src/rfobserver/web/routes/api.py`
- Test: `tests/unit/test_web_routes.py`

**Interfaces:**
- Produces: `GET /api/detections.json` → `{"detections": [ {center_freq_hz, bandwidth_hz, duration_ms, peak_power_db, start_time, stop_time, sdr_center_freq_hz, sample_rate_hz, ...}, ... ]}` (rows from `query_detections`, JSON-serializable).

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_web_routes.py` (mirror the existing detections-fragment test setup — reuse its client/db fixture):

```python
def test_detections_json_returns_rows(self, client_with_db_detections):
    client = client_with_db_detections  # fixture that seeds >=1 detection
    resp = client.get("/api/detections.json")
    assert resp.status_code == 200
    body = resp.json()
    assert "detections" in body
    assert isinstance(body["detections"], list)
    if body["detections"]:
        d = body["detections"][0]
        assert "center_freq_hz" in d and "bandwidth_hz" in d and "duration_ms" in d
```

(If no seeded-detection fixture exists, seed one via the test DB like the existing detections tests do; match their pattern.)

- [ ] **Step 2: Run it, verify it fails (404 / no route).**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_web_routes.py -k detections_json -q`
Expected: FAIL.

- [ ] **Step 3: Implement the endpoint**

In `src/rfobserver/web/routes/api.py`, add (uses the same `_get_db(request)` + `query_detections` as the HTML fragment; keep `limit` and the SDR filter params for parity):

```python
@router.get("/detections.json")
async def detections_json(
    request: Request,
    limit: int = 200,
    sdr_center: str | None = None,
    sample_rate: str | None = None,
) -> dict[str, list[dict]]:
    """JSON detections for external tooling (the OTA validator).

    Sibling of the HTML /detections fragment; returns rows as JSON so
    non-browser clients don't have to scrape HTML.
    """
    db = _get_db(request)
    if db is None:
        return {"detections": []}
    rows = await db.query_detections(
        limit=limit,
        sdr_center_freq=_opt_float(sdr_center),
        sample_rate=_opt_float(sample_rate),
    )
    return {"detections": [dict(r) for r in rows]}
```

Confirm `query_detections` rows are JSON-serializable (datetimes → check the row dict; if `start_time`/`stop_time` are datetimes, coerce with `str(...)`; FastAPI's default encoder handles datetime, so returning the dict is fine).

- [ ] **Step 4: Run test → PASS.** `PYTHONPATH= .venv/bin/pytest tests/unit/test_web_routes.py -k detections_json -q`

- [ ] **Step 5: Lint + full unit suite.** `ruff check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/ && PYTHONPATH= .venv/bin/pytest tests/unit/ -q`

- [ ] **Step 6: Commit.** `git add src/rfobserver/web/routes/api.py tests/unit/test_web_routes.py && git commit -m "api: add JSON /api/detections.json for external tooling"`

---

### Task 2: Frequency-barcode helper + burst set

**Files:**
- Create: `tools/ota_common.py`
- Test: `tests/unit/test_ota_common.py`

**Interfaces:**
- Produces:
  ```python
  BURST_BWS = [50_000, 150_000, 500_000, 2_000_000, 20_000_000]
  BURST_DURATIONS_MS = [1.3, 2.7, 10.24, 83.2, 393.1]
  CENTER_HZ = 915_000_000
  USABLE_HALF_HZ = 12_000_000
  def barcode_offset(bw_hz: float, duration_ms: float) -> float
  def all_combos() -> list[tuple[int, float, float]]   # (bw, dur_ms, offset_hz)
  ```

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_ota_common.py`:

```python
import sys
sys.path.insert(0, "tools")
import ota_common as oc  # noqa: E402


def test_barcode_offsets_fit_the_band():
    for bw in oc.BURST_BWS:
        for dur in oc.BURST_DURATIONS_MS:
            off = oc.barcode_offset(bw, dur)
            # occupied band must stay within +/- USABLE_HALF of center
            assert abs(off) + bw / 2 <= oc.USABLE_HALF_HZ + 1.0, (bw, dur, off)


def test_same_bw_durations_have_distinct_centers():
    for bw in oc.BURST_BWS:
        offs = [oc.barcode_offset(bw, d) for d in oc.BURST_DURATIONS_MS]
        assert len(set(round(o) for o in offs)) == len(offs), bw


def test_combo_identity_is_unique_by_center_and_bw():
    # (round(center), bw) must be unique across all 25 combos
    keys = {(round(oc.CENTER_HZ + off), bw) for bw, _d, off in oc.all_combos()}
    assert len(keys) == len(oc.BURST_BWS) * len(oc.BURST_DURATIONS_MS)
```

- [ ] **Step 2: Run → fail (ImportError).** `PYTHONPATH= .venv/bin/pytest tests/unit/test_ota_common.py -q`

- [ ] **Step 3: Implement `tools/ota_common.py`**

```python
"""Shared definitions for the OTA burst-validation tooling (TX + validator).

The frequency barcode: each (bandwidth, duration) combo is transmitted at a
distinct center offset so a detection's (center, bandwidth) identifies exactly
which combo it was -- ground truth independent of timing/clock.
"""

from __future__ import annotations

BURST_BWS = [50_000, 150_000, 500_000, 2_000_000, 20_000_000]
BURST_DURATIONS_MS = [1.3, 2.7, 10.24, 83.2, 393.1]
CENTER_HZ = 915_000_000
USABLE_HALF_HZ = 12_000_000  # +/-12 MHz of 915 -> stays in 902-928 ISM


def barcode_offset(bw_hz: float, duration_ms: float) -> float:
    """Distinct center offset (Hz) for this combo.

    Within a bandwidth the 5 durations are spread evenly across the range the
    band allows for that width (max = USABLE_HALF - bw/2), giving each combo a
    distinct center; across bandwidths the occupied width differs. So
    (center, bandwidth) is unique for every combo.
    """
    max_off = max(0.0, USABLE_HALF_HZ - bw_hz / 2.0)
    n = len(BURST_DURATIONS_MS)
    i = BURST_DURATIONS_MS.index(duration_ms)
    if n == 1:
        return 0.0
    # spread i in [0, n-1] to fraction in [-1, +1]
    frac = (2.0 * i / (n - 1)) - 1.0
    return frac * max_off


def all_combos() -> list[tuple[int, float, float]]:
    """[(bw_hz, duration_ms, offset_hz), ...] for the full matrix."""
    return [
        (bw, dur, barcode_offset(bw, dur))
        for bw in BURST_BWS
        for dur in BURST_DURATIONS_MS
    ]
```

- [ ] **Step 4: Run → PASS.** `PYTHONPATH= .venv/bin/pytest tests/unit/test_ota_common.py -q`

- [ ] **Step 5: Lint + commit.** `ruff check tools/ tests/ && git add tools/ota_common.py tests/unit/test_ota_common.py && git commit -m "tools: frequency-barcode offsets + burst set for OTA validation"`

---

### Task 3: TX harness

**Files:**
- Create: `tools/ota_tx.py`

**Interfaces:**
- Consumes: `ota_common` (barcode + combos), `tests.integration._synth.make_iq_with_wideband_burst`.
- Produces: transmits each combo at `CENTER_HZ` with its barcode offset baked into the IQ, writes `schedule.json`. `--dry-run` generates IQ + schedule without UHD (for offline check).

- [ ] **Step 1: Implement `tools/ota_tx.py`**

Key points (no unit test for the UHD path; `--dry-run` is the offline check):
- Import the comb generator; for each combo generate a **pure comb burst** (call `make_iq_with_wideband_burst` with `noise_stddev=0.0`, `burst_start_sec=0`, `duration_sec=dur`, `burst_offset_hz=barcode_offset(...)`, `num_bins=2048`), then normalize peak to ~0.7 for the DAC.
- TX via `uhd.usrp.MultiUSRP()`: `set_tx_rate(28e6)`, `set_tx_freq(tune_request(915e6))`, `set_tx_gain(args.tx_gain)`, `set_tx_antenna("TX/RX")`; stream the burst samples, then `args.gap` seconds of zeros (or just idle) between combos.
- `--subset` flag runs a representative subset (e.g. BW {150k, 2M, 20M} x dur {2.7, 83.2}); default full 25.
- Write `schedule.json`: `[{index, tx_wallclock, bw_hz, duration_ms, offset_hz, center_hz, tx_gain}]`.

```python
"""Transmit the OTA burst set (frequency barcode) via the workstation B200mini.

Usage:
  PYTHONPATH= .venv/bin/python tools/ota_tx.py --tx-gain 60 --gap 3 --subset
  PYTHONPATH= .venv/bin/python tools/ota_tx.py --dry-run   # no radio; writes schedule
"""

from __future__ import annotations

import argparse
import json
import sys
import time

import numpy as np

sys.path.insert(0, "tools")
sys.path.insert(0, ".")  # for tests.integration._synth
import ota_common as oc  # noqa: E402
from tests.integration._synth import make_iq_with_wideband_burst  # noqa: E402

TX_RATE = 28_000_000
SUBSET = [(150_000, 2.7), (2_000_000, 2.7), (20_000_000, 2.7),
          (150_000, 83.2), (2_000_000, 83.2), (20_000_000, 83.2)]


def make_tx_burst(bw_hz: float, dur_ms: float, offset_hz: float) -> np.ndarray:
    iq = make_iq_with_wideband_burst(
        duration_sec=dur_ms / 1000.0,
        sample_rate_hz=TX_RATE,
        burst_start_sec=0.0,
        burst_duration_sec=dur_ms / 1000.0,
        burst_bw_hz=bw_hz,
        burst_offset_hz=offset_hz,
        num_bins=2048,
        per_tone_amp=0.05,   # high enough to hit the generator's 0.8 peak cap
        noise_stddev=0.0,    # clean carrier; the OTA channel + RX add noise
    )
    peak = float(np.max(np.abs(iq))) or 1.0
    return (iq * (0.7 / peak)).astype(np.complex64)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tx-gain", type=float, default=60.0)
    ap.add_argument("--gap", type=float, default=3.0)
    ap.add_argument("--subset", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--schedule", default="ota_schedule.json")
    args = ap.parse_args()

    combos = SUBSET if args.subset else [(bw, d) for bw in oc.BURST_BWS for d in oc.BURST_DURATIONS_MS]

    usrp = None
    streamer = None
    if not args.dry_run:
        import uhd
        usrp = uhd.usrp.MultiUSRP()
        usrp.set_tx_rate(TX_RATE, 0)
        usrp.set_tx_freq(uhd.libpyuhd.types.tune_request(oc.CENTER_HZ), 0)
        usrp.set_tx_gain(args.tx_gain, 0)
        usrp.set_tx_antenna("TX/RX", 0)
        st = uhd.usrp.StreamArgs("fc32", "fc32")
        streamer = usrp.get_tx_stream(st)

    schedule = []
    for i, (bw, dur) in enumerate(combos):
        off = oc.barcode_offset(bw, dur)
        burst = make_tx_burst(bw, dur, off)
        rec = {"index": i, "tx_wallclock": time.time(), "bw_hz": bw,
               "duration_ms": dur, "offset_hz": off, "center_hz": oc.CENTER_HZ + off,
               "tx_gain": args.tx_gain}
        schedule.append(rec)
        print(f"[{i}] bw={bw} dur={dur}ms center={rec['center_hz']/1e6:.3f}MHz")
        if not args.dry_run:
            import uhd
            md = uhd.types.TXMetadata()
            md.start_of_burst = True
            md.end_of_burst = True
            streamer.send(burst, md)  # blocks until sent
        time.sleep(args.gap)

    with open(args.schedule, "w") as f:
        json.dump(schedule, f, indent=2)
    print(f"wrote {args.schedule} ({len(schedule)} bursts)")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Offline check (no radio).** `PYTHONPATH= .venv/bin/python tools/ota_tx.py --dry-run --subset` → prints the 6 subset combos with distinct centers, writes `ota_schedule.json`. Confirm centers are distinct and within 903-927 MHz.

- [ ] **Step 3: Lint + commit.** `ruff check tools/ && git add tools/ota_tx.py && git commit -m "tools: OTA TX harness (barcode bursts via B200mini, --dry-run)"`

(Live transmit is exercised in Task 6/7, not here.)

---

### Task 4: Validation script

**Files:**
- Create: `tools/ota_validate.py`
- Test: `tests/unit/test_ota_validate.py`

**Interfaces:**
- Produces: `match_detections(schedule, detections, *, center_tol_hz, bw_rel_tol) -> list[MatchResult]` (pure, testable) + a CLI that fetches `http://<jetson>:8888/api/detections.json` and prints a report.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_ota_validate.py`:

```python
import sys
sys.path.insert(0, "tools")
import ota_validate as ov  # noqa: E402


def test_match_by_barcode_center_and_bw():
    schedule = [
        {"index": 0, "bw_hz": 500_000, "duration_ms": 10.24, "center_hz": 910_000_000},
        {"index": 1, "bw_hz": 2_000_000, "duration_ms": 83.2, "center_hz": 920_000_000},
    ]
    detections = [
        # matches combo 1 (center ~920M, bw ~2M)
        {"center_freq_hz": 920_010_000, "bandwidth_hz": 2_100_000, "duration_ms": 80.0, "peak_power_db": -40},
        # matches combo 0 (center ~910M, bw ~0.5M)
        {"center_freq_hz": 909_990_000, "bandwidth_hz": 520_000, "duration_ms": 10.5, "peak_power_db": -45},
        # ambient junk far away -> ignored
        {"center_freq_hz": 905_000_000, "bandwidth_hz": 30_000, "duration_ms": 1.0, "peak_power_db": -50},
    ]
    results = ov.match_detections(schedule, detections, center_tol_hz=100_000, bw_rel_tol=0.5)
    by_idx = {r["index"]: r for r in results}
    assert by_idx[0]["matched"] and abs(by_idx[0]["meas_duration_ms"] - 10.5) < 1e-6
    assert by_idx[1]["matched"] and abs(by_idx[1]["meas_bandwidth_hz"] - 2_100_000) < 1e-6


def test_unmatched_combo_reported():
    schedule = [{"index": 0, "bw_hz": 50_000, "duration_ms": 1.3, "center_hz": 926_000_000}]
    results = ov.match_detections(schedule, [], center_tol_hz=100_000, bw_rel_tol=0.5)
    assert results[0]["matched"] is False
```

- [ ] **Step 2: Run → fail (ImportError).**

- [ ] **Step 3: Implement `tools/ota_validate.py`**

```python
"""Validate OTA burst detections against the barcode schedule.

Usage:
  PYTHONPATH= .venv/bin/python tools/ota_validate.py \
      --schedule ota_schedule.json --jetson 192.168.97.153 --port 8888
"""

from __future__ import annotations

import argparse
import json
import urllib.request


def match_detections(schedule, detections, *, center_tol_hz, bw_rel_tol):
    """Match each scheduled combo to its detection by barcode (center + bw).

    A detection matches a combo if its center is within center_tol_hz of the
    combo's assigned center AND its bandwidth is within bw_rel_tol (relative)
    of the combo's bandwidth. The strongest (peak_power_db) qualifying
    detection wins. Timing is not used.
    """
    results = []
    for combo in schedule:
        c = combo["center_hz"]
        bw = combo["bw_hz"]
        cands = [
            d for d in detections
            if abs(d["center_freq_hz"] - c) <= center_tol_hz
            and abs(d["bandwidth_hz"] - bw) <= bw_rel_tol * bw + center_tol_hz
        ]
        if not cands:
            results.append({**_combo_fields(combo), "matched": False})
            continue
        best = max(cands, key=lambda d: d.get("peak_power_db", -1e9))
        results.append({
            **_combo_fields(combo),
            "matched": True,
            "meas_center_hz": best["center_freq_hz"],
            "meas_bandwidth_hz": best["bandwidth_hz"],
            "meas_duration_ms": best["duration_ms"],
        })
    return results


def _combo_fields(combo):
    return {"index": combo["index"], "bw_hz": combo["bw_hz"],
            "duration_ms": combo["duration_ms"], "center_hz": combo["center_hz"]}


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
    url = f"http://{args.jetson}:{args.port}/api/detections.json?limit=1000"
    with urllib.request.urlopen(url, timeout=15) as r:
        detections = json.load(r)["detections"]

    results = match_detections(schedule, detections,
                               center_tol_hz=args.center_tol_hz, bw_rel_tol=args.bw_rel_tol)
    n_match = sum(1 for r in results if r["matched"])
    print(f"{n_match}/{len(results)} combos detected\n")
    for r in results:
        if r["matched"]:
            de = abs(r["meas_duration_ms"] - r["duration_ms"])
            we = abs(r["meas_bandwidth_hz"] - r["bw_hz"])
            print(f"[{r['index']}] bw={r['bw_hz']} dur={r['duration_ms']}ms "
                  f"@ {r['center_hz']/1e6:.3f}MHz -> dur_err={de:.2f}ms "
                  f"bw_err={we/1e3:.0f}kHz meas_center={r['meas_center_hz']/1e6:.3f}")
        else:
            print(f"[{r['index']}] bw={r['bw_hz']} dur={r['duration_ms']}ms "
                  f"@ {r['center_hz']/1e6:.3f}MHz -> NOT DETECTED")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run → PASS.** `PYTHONPATH= .venv/bin/pytest tests/unit/test_ota_validate.py -q`

- [ ] **Step 5: Lint + commit.** `ruff check tools/ tests/ && git add tools/ota_validate.py tests/unit/test_ota_validate.py && git commit -m "tools: OTA detection validator (barcode matching + report)"`

---

### Task 5: Jetson real-SDR bring-up (live runbook)

**Not subagent work — run interactively against the Jetson.**

- [ ] **Step 1: Install UHD on the Jetson.**
  `ssh ocollaco@192.168.97.153 'sudo apt-get update && sudo apt-get install -y libuhd-dev uhd-host python3-uhd'`
  Then `sudo uhd_images_downloader` (B2xx FPGA/firmware). If the apt UHD is too old for the module firmware (recv errors / "please update"), fall back to a source build of matching UHD.
- [ ] **Step 2: Confirm enumeration + raw capture.**
  `ssh ... 'uhd_find_devices'` shows the B200mini; a short `uhd_rx_cfile -f 915e6 -r 28e6 -N 2800000 /tmp/x.dat` completes without continuous `O` (overflow).
- [ ] **Step 3: RFObserver real mode.** On the Jetson checkout, `git pull` (gets Tasks 1-4), then launch with the real-mode env (Global Constraints) — NOT mock. Confirm the web UI at `http://192.168.97.153:8888` shows a live 915 MHz spectrum with a sane noise floor and no receiver errors in the log.
- [ ] **Deliverable:** sensor receiving live at 915/28 MHz, JSON endpoint reachable (`curl`-free: fetch `/api/detections.json` from the workstation — empty list is fine).

### Task 6: Link calibration (live runbook)

- [ ] **Step 1:** With the sensor running, transmit a repeating mid burst: `PYTHONPATH= .venv/bin/python tools/ota_tx.py --subset --tx-gain 60 --gap 3` (or a loop of one combo).
- [ ] **Step 2:** Watch the sensor's live spectrum / detections. Adjust **TX gain** and the sensor's **RFOBS_GAIN** so the burst sits ~20-40 dB above the noise floor without ADC clipping (RFObserver IQ stats / clip indicator).
- [ ] **Step 3:** Record the working `(tx_gain, rx_gain)`; note the ambient 915 ISM activity (informs `--center-tol`/barcode offset choice).

### Task 7: Run + report (live runbook)

- [ ] **Step 1: Subset run.** `tools/ota_tx.py --subset --tx-gain <cal>` while the sensor runs; then `tools/ota_validate.py --schedule ota_schedule.json` → report. Iterate placement/gain until the subset is reliably detected.
- [ ] **Step 2: Full run.** Drop `--subset`; transmit all 25, validate, save `ota_schedule.json` + the fetched detections JSON + the report.
- [ ] **Step 3:** Summarize: per-combo detected? duration/center/bandwidth error vs transmitted, against OTA-realistic tolerances. Note any corners that fail (expected: narrowest/weakest) with the real-world reason.

---

## Self-Review Notes

- **Spec coverage:** JSON readout (Task 1), barcode (Task 2), TX harness reusing the comb generator (Task 3), barcode-matching validator (Task 4), Jetson real-SDR bring-up (Task 5), calibration (Task 6), subset-then-full run + report (Task 7). All spec phases mapped.
- **Testable-first:** Tasks 1-4 are pure code/logic with tests and run before any hardware. Tasks 5-7 are live runbooks with explicit deliverables/verification.
- **Type/name consistency:** `barcode_offset(bw_hz, duration_ms)` and `CENTER_HZ`/`USABLE_HALF_HZ` are defined in `ota_common` (Task 2) and imported by TX (Task 3) and validator CLI (Task 4). `match_detections(schedule, detections, *, center_tol_hz, bw_rel_tol)` signature matches its test. Detection field names (`center_freq_hz`, `bandwidth_hz`, `duration_ms`, `peak_power_db`) match `query_detections` / the JSON endpoint.
- **Calibration constants** (`tx_gain`, `rx_gain`, tolerances) are found live in Tasks 6-7 — intended, not placeholders.
