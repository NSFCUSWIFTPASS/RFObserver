# Record-a-replay + Re-detect-any-capture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn a raw IQ file into an inspectable full-resolution capture by recording its replay, and let any capture's detections be re-computed on its stored `.psd` grid at chosen thresholds (no re-record). Reuse the existing replay + recording + viewer; minimal new code.

**Architecture:** One shared helper `write_sidecar_from_grid(sc16, config)` runs the batch `detect_bursts` on a capture's stored `.psd` grid and writes the detections sidecar. It is reused by (a) a `POST /captures/redetect/{filename}` endpoint (tune any capture) and (b) the replay record-stop path (build the sidecar for a replay recording, whose detections are not in the DB). A small opt-in lifts the `replay_mode` manual-record gate so a replay can be recorded.

**Tech Stack:** FastAPI, numpy memmap, vanilla JS. Spec: `docs/superpowers/specs/2026-08-18-analyze-capture-redetect-design.md`.

## Global Constraints
- Prefix every Python command with `PYTHONPATH=` and use `.venv/bin`. ruff is global (`ruff check` + `ruff format --check` both pass).
- No emojis, no em-dashes, no "Co-Authored-By" anywhere.
- **CI lint type-checks without numpy installed** (only ruff/mypy/pydantic). Keep declared return types concrete (annotate any `ndarray.tobytes()`/`.tolist()` results) so mypy's `no-any-return` does not fire in CI.
- Replay must still NOT write to the DB / NATS / ZMS. Recording during replay is an explicit opt-in that only writes the capture files (`.sc16`/`.psd`/`.json`/`.detections.json`); its sidecar comes from the grid, not the DB.
- Detection sets reflect the batch `detect_bursts` (whole-grid), matching `waterfall_plot.py` (`threshold_high_db=40`, median floor).
- All CLAUDE.md checks stay green (ruff, ruff format, mypy `src/rfobserver/`, unit, integration@NATS:4222).

## Interfaces (existing, verified)
- `storage/psd_grid.py`: `grid_paths(sc16)`, `load_grid(sc16) -> (mm ndarray (rows,num_bins), meta) | None`. `meta` keys: `rows, num_bins, time_resolution_s, freq_axis (list), center_freq_hz, bandwidth_hz, grid_min, grid_max`.
- `processing/spectral.py`: `PSDGridResult(grid, time_axis, freq_axis, ffts_per_slice, total_ffts)`.
- `processing/burst.py`: `BurstDetectionConfig(threshold_high_db=10.0, threshold_low_ratio=0.6, min_duration_sec=0.001, merge_freq_bins=5, merge_time_sec=0.003, noise_floor_percentile=50.0)`; `detect_bursts(psd_grid, config=None, center_freq_hz=0.0, capture_time=None) -> BurstDetectionResult(bursts=[BurstFingerprint])`; each `BurstFingerprint` has `start_time, stop_time, center_freq_hz, peak_freq_hz, bandwidth_hz, peak_power_db, duration_ms`.
- `storage/detections_sidecar.py`: `sidecar_path(sc16)`, `_read_json`, `build_sidecar_payload(sc16, db)`, `write_sidecar(sc16, db)`; sidecar schema `{capture_start_time, time_resolution_s, center_freq_hz, sample_rate_hz, gain_db, detections:[{start_time, stop_time, center_freq_hz, bandwidth_hz, peak_freq_hz, peak_power_db, duration_ms, row_start, row_stop}]}`.
- `pipeline/streaming.py`: `StreamingProcessor.__init__(..., replay_mode=False)`; `start_recording` (gated `if self._replay_mode: return`); `_deferred_sidecar(sc16_path, grace)` awaits `sleep(grace)` then `write_sidecar(sc16, self._db)`.

---

### Task 1: Shared helper `write_sidecar_from_grid`

**Files:** `src/rfobserver/storage/detections_sidecar.py`. Test: `tests/unit/test_detections_sidecar.py` (extend).

**Interfaces produced:**
- `def build_sidecar_from_grid(sc16_path: Path, config: BurstDetectionConfig) -> dict[str, Any]` (pure: grid -> payload).
- `def write_sidecar_from_grid(sc16_path: Path, config: BurstDetectionConfig) -> dict[str, Any]` (builds + writes the active sidecar; returns payload).

- [ ] **Step 1: Write the failing test.** Append to `tests/unit/test_detections_sidecar.py`:
```python
def test_write_sidecar_from_grid_threshold_changes_detections(tmp_path):
    import numpy as np

    from rfobserver.processing.burst import BurstDetectionConfig
    from rfobserver.storage import detections_sidecar as ds
    from rfobserver.storage import psd_grid

    rows, num_bins = 200, 64
    tres = 0.001
    sc16 = tmp_path / "cap.sc16"
    sc16.write_bytes(b"\x00" * 8)
    # capture meta
    start = "2026-08-18T00:00:00+00:00"
    sc16.with_suffix(".json").write_text(json.dumps(
        {"start_time": start, "duration_sec": rows * tres,
         "center_freq_hz": 915e6, "sample_rate_hz": 26e6, "gain_db": 35}))
    # synthetic grid: flat -100 dB noise + one strong burst (rows 50..70, bins 30..33)
    grid = np.full((rows, num_bins), -100.0, dtype=np.float32)
    grid[50:71, 30:34] = -40.0
    raw_path, meta_path = psd_grid.grid_paths(sc16)
    grid.tofile(raw_path)
    freq_axis = (np.arange(num_bins) - num_bins / 2) * (26e6 / num_bins)
    psd_grid.write_meta(meta_path, rows=rows, num_bins=num_bins, time_resolution_s=tres,
                        center_freq_hz=915_000_000, bandwidth_hz=26_000_000,
                        freq_axis=freq_axis, grid_min=-100.0, grid_max=-40.0,
                        cal_offset_db=None)

    hi = ds.write_sidecar_from_grid(sc16, BurstDetectionConfig(threshold_high_db=50.0))
    lo = ds.write_sidecar_from_grid(sc16, BurstDetectionConfig(threshold_high_db=20.0))
    # A 60 dB burst over a -100 floor: threshold 50 catches it, 20 catches it too;
    # threshold above the burst height (e.g. 70) catches nothing.
    none = ds.write_sidecar_from_grid(sc16, BurstDetectionConfig(threshold_high_db=70.0))
    assert len(lo["detections"]) >= 1
    assert len(none["detections"]) == 0
    d = lo["detections"][0]
    assert 45 <= d["row_start"] <= 55 and 68 <= d["row_stop"] <= 75  # burst rows ~50..70
    assert set(d) >= {"start_time", "stop_time", "center_freq_hz", "bandwidth_hz",
                      "peak_freq_hz", "peak_power_db", "duration_ms", "row_start", "row_stop"}
    # the active sidecar was written
    assert ds.sidecar_path(sc16).exists()
```
- [ ] **Step 2: Run to verify failure.** `PYTHONPATH= .venv/bin/pytest tests/unit/test_detections_sidecar.py -x -q -k from_grid` -> FAIL (function absent).
- [ ] **Step 3: Implement.** Add to `detections_sidecar.py` (imports: `from datetime import datetime`; numpy; `from rfobserver.storage import psd_grid`; `from rfobserver.processing.spectral import PSDGridResult`; `from rfobserver.processing.burst import detect_bursts, BurstDetectionConfig`):
```python
def build_sidecar_from_grid(sc16_path: Path, config: BurstDetectionConfig) -> dict[str, Any]:
    import numpy as np

    meta_cap = _read_json(_base(sc16_path).with_suffix(".json")) or {}
    loaded = psd_grid.load_grid(sc16_path)
    start_iso = meta_cap.get("start_time")
    out: dict[str, Any] = {
        "capture_start_time": start_iso,
        "time_resolution_s": None,
        "center_freq_hz": meta_cap.get("center_freq_hz"),
        "sample_rate_hz": meta_cap.get("sample_rate_hz"),
        "gain_db": meta_cap.get("gain_db"),
        "detections": [],
    }
    if loaded is None or not start_iso:
        return out
    grid, gmeta = loaded
    tres = float(gmeta.get("time_resolution_s") or 0.0)
    rows = int(gmeta.get("rows") or grid.shape[0])
    out["time_resolution_s"] = tres or None
    if tres <= 0 or rows <= 0:
        return out
    freq_axis = np.asarray(gmeta.get("freq_axis", []), dtype=np.float64)
    center = float(gmeta.get("center_freq_hz") or 0.0)
    psd = PSDGridResult(
        grid=np.asarray(grid), time_axis=np.arange(rows, dtype=np.float64) * tres,
        freq_axis=freq_axis, ffts_per_slice=0, total_ffts=rows,
    )
    cap_start = datetime.fromisoformat(start_iso)
    result = detect_bursts(psd, config, center, cap_start)
    dets: list[dict[str, Any]] = []
    for b in result.bursts:
        t0 = (b.start_time - cap_start).total_seconds()
        t1 = (b.stop_time - cap_start).total_seconds()
        row_start = max(0, min(rows, int(round(t0 / tres))))
        row_stop = max(0, min(rows, int(round(t1 / tres))))
        dets.append({
            "start_time": b.start_time.isoformat(), "stop_time": b.stop_time.isoformat(),
            "center_freq_hz": b.center_freq_hz, "bandwidth_hz": b.bandwidth_hz,
            "peak_freq_hz": b.peak_freq_hz, "peak_power_db": b.peak_power_db,
            "duration_ms": b.duration_ms, "row_start": row_start, "row_stop": row_stop,
        })
    out["detections"] = dets
    return out


def write_sidecar_from_grid(sc16_path: Path, config: BurstDetectionConfig) -> dict[str, Any]:
    payload = build_sidecar_from_grid(sc16_path, config)
    sidecar_path(sc16_path).write_text(json.dumps(payload, indent=2))
    return payload
```
Add a small `_base(sc16_path)` helper (strip `.sc16`) if one is not already present; otherwise reuse the existing base-stripping used by `sidecar_path`/`build_sidecar_payload`.
- [ ] **Step 4: Run to verify pass.** `PYTHONPATH= .venv/bin/pytest tests/unit/test_detections_sidecar.py -x -q` -> PASS.
- [ ] **Step 5: Lint + commit.**
```bash
ruff check src/ tests/ && ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/
git add src/rfobserver/storage/detections_sidecar.py tests/unit/test_detections_sidecar.py
git commit -m "detections_sidecar: build/write sidecar from a stored PSD grid via detect_bursts"
```

---

### Task 2: `POST /captures/redetect/{filename}`

**Files:** `src/rfobserver/web/routes/captures.py`. Test: `tests/unit/test_captures_redetect.py` (create).

**Interfaces:**
- Consumes: `write_sidecar_from_grid` (Task 1), `_validate_filename`, `_get_storage`, `BurstDetectionConfig`, `request.app.state.settings` (for `BURST_*` defaults + `merge_freq_bins`).
- Produces: `POST /captures/redetect/{filename}` returning the new sidecar payload.

- [ ] **Step 1: Write the failing test.** Create `tests/unit/test_captures_redetect.py`: seed a grid capture (as in Task 1's test, under `settings.STORAGE_PATH`), then via `TestClient`:
```python
def test_redetect_rewrites_sidecar(...):
    # seed cap.sc16 + .json + .psd + .psd.json with a synthetic burst under STORAGE_PATH
    r = client.post("/captures/redetect/cap.sc16", json={"threshold_high_db": 20.0})
    assert r.status_code == 200
    assert len(r.json()["detections"]) >= 1
    r2 = client.post("/captures/redetect/cap.sc16", json={"threshold_high_db": 70.0})
    assert r2.json()["detections"] == []

def test_redetect_404_without_grid(...):
    # seed only cap.sc16 + .json (no .psd)
    assert client.post("/captures/redetect/cap.sc16", json={}).status_code == 404
```
- [ ] **Step 2: Run -> fail** (route absent).
- [ ] **Step 3: Implement.** In `captures.py`:
```python
@router.post("/redetect/{filename}")
async def capture_redetect(request: Request, filename: str) -> dict[str, Any]:
    from rfobserver.processing.burst import BurstDetectionConfig
    from rfobserver.storage import detections_sidecar as ds
    from rfobserver.storage import psd_grid

    storage = _get_storage(request)
    base = filename.replace(".sc16", "").replace(".json", "").replace(".detections", "")
    sc16 = _validate_filename(base + ".sc16", storage)
    if psd_grid.load_grid(sc16) is None:
        raise HTTPException(status_code=404, detail="No PSD grid for this capture")
    s = request.app.state.settings
    body = await request.json() if await _has_body(request) else {}
    cfg = BurstDetectionConfig(
        threshold_high_db=float(body.get("threshold_high_db", s.BURST_THRESHOLD_HIGH_DB)),
        threshold_low_ratio=float(body.get("threshold_low_ratio", s.BURST_THRESHOLD_LOW_RATIO)),
        noise_floor_percentile=float(body.get("noise_floor_percentile", s.BURST_NOISE_FLOOR_PERCENTILE)),
        merge_time_sec=float(body.get("merge_time_ms", s.BURST_MERGE_TIME_MS)) / 1000.0,
        merge_freq_bins=int(body.get("merge_freq_bins", s.BURST_MERGE_FREQ_BINS)),
        min_duration_sec=float(body.get("min_duration_ms", 1.0)) / 1000.0,
    )
    return ds.write_sidecar_from_grid(sc16, cfg)
```
Use the request-JSON pattern already in `captures.py`/`api.py` (a try/except around `await request.json()` returning `{}` on empty, instead of the `_has_body` sketch) so an empty body uses all defaults.
- [ ] **Step 4: Run -> pass.** `PYTHONPATH= .venv/bin/pytest tests/unit/test_captures_redetect.py -x -q`; full unit.
- [ ] **Step 5: Lint + commit.** `captures: POST /captures/redetect to re-run detection on a capture's stored grid`.

---

### Task 3: Record during replay (opt-in) + grid-based record-stop sidecar

**Files:** `src/rfobserver/pipeline/streaming.py`, `src/rfobserver/web/routes/api.py`. Test: `tests/unit/test_streaming_replay_mode.py` (extend), `tests/unit/test_replay_routes.py` (extend).

**Interfaces produced:**
- `StreamingProcessor.set_replay_recording(on: bool)` + `self._replay_record`.
- `POST /api/replay/record {on: bool}`.

- [ ] **Step 1: Write the failing tests.**
  - In `test_streaming_replay_mode.py`: with `replay_mode=True`, `start_recording()` is inert (state stays idle) UNTIL `set_replay_recording(True)`, after which `start_recording()` sets state to `recording`; `arm_trigger()` stays inert regardless. (Mirror the existing replay-mode fixture.)
  - In `test_streaming_replay_mode.py`: assert `_deferred_sidecar` uses the grid path under replay: monkeypatch `rfobserver.storage.detections_sidecar.write_sidecar_from_grid` and `.write_sidecar`; with `replay_mode=True`, run `await proc._deferred_sidecar(sc16, 0)` and assert `write_sidecar_from_grid` was called and `write_sidecar` (DB) was not. (Construct the processor with `replay_mode=True`; `self._db` may be a MagicMock.)
  - In `test_replay_routes.py`: `POST /api/replay/record {on: true}` returns 409 when no replay is active (fake supervisor `replay_status()` None).
- [ ] **Step 2: Run -> fail.**
- [ ] **Step 3: Implement streaming.py.**
  - `__init__`: `self._replay_record = False`. Add `def set_replay_recording(self, on: bool) -> None: self._replay_record = bool(on)`.
  - `start_recording`: change the gate to `if self._replay_mode and not self._replay_record: return` (arm_trigger / _check_trigger_and_record / _begin_recording keep their plain `if self._replay_mode: return`).
  - `_deferred_sidecar`: branch on replay:
    ```python
    async def _deferred_sidecar(self, sc16_path: Path, grace: float) -> None:
        from rfobserver.processing.burst import BurstDetectionConfig
        from rfobserver.storage.detections_sidecar import write_sidecar, write_sidecar_from_grid
        try:
            await asyncio.sleep(grace)
            if self._replay_mode:
                s = self._settings
                cfg = BurstDetectionConfig(
                    threshold_high_db=s.BURST_THRESHOLD_HIGH_DB,
                    threshold_low_ratio=s.BURST_THRESHOLD_LOW_RATIO,
                    noise_floor_percentile=s.BURST_NOISE_FLOOR_PERCENTILE,
                    merge_time_sec=s.BURST_MERGE_TIME_MS / 1000.0,
                    merge_freq_bins=s.BURST_MERGE_FREQ_BINS,
                )
                write_sidecar_from_grid(sc16_path, cfg)
            else:
                await write_sidecar(sc16_path, self._db)
        except Exception:
            logger.exception("Deferred sidecar write failed for %s", sc16_path.name)
    ```
    (Confirm the exact current `_deferred_sidecar` body/imports before editing; keep the existing grace + try/except shape.)
- [ ] **Step 4: Implement the endpoint (api.py).**
```python
@router.post("/replay/record")
async def replay_record(request: Request) -> dict[str, Any]:
    supervisor = getattr(request.app.state, "supervisor", None)
    if supervisor is None or supervisor.replay_status() is None:
        raise HTTPException(status_code=409, detail="No active replay")
    proc = getattr(request.app.state, "processor", None)
    if proc is None:
        raise HTTPException(status_code=409, detail="No active processor")
    body = await request.json()
    on = bool(body.get("on", False))
    if on:
        proc.set_replay_recording(True)
        proc.start_recording()
    else:
        proc.stop_recording()
        proc.set_replay_recording(False)
    return proc.recording_status()  # reuse the existing recording-status method
```
(Verify the processor's stop method name (`stop_recording`) and a status method; reuse whatever `/api/recording/stop` uses.)
- [ ] **Step 5: Run -> pass** (both test files + full unit); ruff/mypy.
- [ ] **Step 6: Commit.** `replay: opt-in record-during-replay + grid-based record-stop sidecar; POST /api/replay/record`.

---

### Task 4: UI - Record toggle in the replay banner + Re-detect panel in the viewer

**Files:** `src/rfobserver/web/templates/dashboard.html`, `src/rfobserver/web/templates/captures.html`, `src/rfobserver/web/static/style.css`. Static/manual verify.

- [ ] **Step 1: Replay banner Record toggle (`dashboard.html`).** In `#replay-banner`, add a Record / Stop-record button that POSTs `/api/replay/record {on}` (toggle state from the heartbeat `recording` field, which is already broadcast and handled). Show a small recording indicator (reuse existing recording-state styling). Do not alter non-replay behavior.
- [ ] **Step 2: Viewer Re-detect panel (`captures.html`).** In the capture detail/viewer, add a Re-detect panel: number inputs `threshold_high_db` (default from the loaded sidecar's `params` if present else 40), `threshold_low_ratio` (0.6), `noise_floor_percentile` (50), `merge_time_ms` (30), and a Re-detect button -> `POST /captures/redetect/{filename}` with those fields -> on success, reload `/captures/detections/{filename}` and refresh the overlay + Detections table in place (reuse the existing `viewerDetections` load + `drawDetectionOverlay()` + `buildDetectionsTable()`). Show server error text on non-200.
- [ ] **Step 3: Styles (`style.css`).** Add a `.redetect-panel` (Apple-style, reuse existing input/button classes) and the banner Record-button state. No emojis/em-dashes.
- [ ] **Step 4: Static + manual verify.** `node --check` on the edited script blocks. Then (controller does the live browser run): record the SSM replay -> open the capture -> scrub bursts at 0.2 ms -> Re-detect at thr 40 vs 20 and confirm the overlay/table change. Confirm the live DB is not written during the replay recording.
- [ ] **Step 5: Commit.** `ui: replay Record toggle + capture Re-detect panel`.

---

### Task 5: Full verification + finish
- [ ] `ruff check src/ tests/ && ruff format --check src/ tests/`; `PYTHONPATH= .venv/bin/mypy src/rfobserver/`; `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`; `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q`.
- [ ] Also run mypy in a CI-like env (no numpy) to catch `no-any-return`: `python3.11 -m venv /tmp/ci_mypy && /tmp/ci_mypy/bin/pip install -q ruff mypy pydantic pydantic-settings && /tmp/ci_mypy/bin/mypy src/rfobserver/`.
- [ ] Live smoke: on this box, replay + record the SSM `.dat`, open the capture, re-detect at threshold 40 (should reproduce the ~9-hop, ~82 ms pattern), then a higher threshold (fewer/narrower). Then nano-super smoke of the mock flow. Do NOT touch HCRO; give the user redeploy/test steps.
- [ ] superpowers:finishing-a-development-branch.

## Self-Review
- Spec coverage: shared helper -> Task 1; re-detect endpoint -> Task 2; record-during-replay + grid record-stop sidecar + endpoint -> Task 3; UI -> Task 4; verification -> Task 5.
- Types consistent: `write_sidecar_from_grid(sc16, BurstDetectionConfig)` defined in Task 1, consumed by Tasks 2 and 3; `set_replay_recording`/`_replay_record` defined and used within Task 3; sidecar schema matches `build_sidecar_payload`.
- No placeholders; code shown for each implementation step. Re-detect overwrites the single sidecar (per spec). CI-mypy-without-numpy guard called out in Global Constraints + Task 5.
