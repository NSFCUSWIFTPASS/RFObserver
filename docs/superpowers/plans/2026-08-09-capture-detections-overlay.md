# Capture Detections Overlay + Sidecar + Faster Scrolling - Implementation Plan

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Faster PSD scrolling (preload 2 windows ahead), accurate persisted detection times, an independently-loadable `<base>.detections.json` sidecar per capture, detection boxes overlaid on the capture waterfall, and a per-capture "Detections" table with click-to-scroll.

**Tech:** FastAPI, aiosqlite, numpy, vanilla JS canvas. Env: prefix Python with `PYTHONPATH=`, use `.venv/bin`. ruff global. No emojis, no em-dashes, no Co-Authored-By.

## Global Constraints
- No DB schema change. Reuse `detections.start_time`/`stop_time` (made accurate in Task 2).
- Sidecar is `<base>.detections.json` next to the `.sc16` (companion like `.psd`/`.json`).
- Shared sidecar helper lives in `storage/` so both `pipeline/streaming.py` and
  `web/routes/captures.py` import it (no web<-pipeline coupling).
- All CLAUDE.md checks stay green (ruff, mypy, unit, integration@NATS:4222).

---

### Task 1: Preload 2 windows ahead in scroll direction

**Files:** `src/rfobserver/web/routes/captures.py` (`capture_psd_ws`), `src/rfobserver/web/templates/captures.html` (`onScroll`/`ensureVisible`). Test: `tests/unit/test_captures_ws.py`.

- [ ] **Step 1 (test):** extend `test_captures_ws.py`: connect, send `{start:0,count:2,max_bins:512}`, collect all binary frames for ~0.5 s, assert the served `start` values include `0`, `2` (next), and `4` (2-ahead) for a capture with >=6 rows (prev `-2` is out of range at start). Decode headers with `struct.unpack("<iii", ...)`.
- [ ] **Step 2:** run -> fail (only 0,2,-2 pushed today).
- [ ] **Step 3 (server):** in `capture_psd_ws` (`captures.py`, the block that currently does `serve(start)`, `serve(start+count)`, `serve(start-count)`), add `await serve(start + 2 * count, count, max_bins, have, skip_have=True)`. Order: requested, +count, +2*count, -count. Keep bounds + `have` dedup.
- [ ] **Step 4 (client):** in `captures.html`, track scroll direction in `onScroll` (module var `lastFirstVisibleRow`; `dir = sign(firstVisibleRow - lastFirstVisibleRow)`). After the existing `ensureVisible(firstVisibleRow)`, when `dir >= 0` also request the next 2 pages beyond the visible span; when `dir < 0` request 1 page before. Reuse the existing WS-request path (build `have` from `pageCache` keys, guard with `pageLoading`, fall back to `fetchPageRaw` when the WS is closed). Do not double-fetch pages already in `pageCache`/`pageLoading`.
- [ ] **Step 5:** run `PYTHONPATH= .venv/bin/pytest tests/unit/test_captures_ws.py -x -q`; ruff/mypy. Commit: `captures: preload 2 PSD windows ahead in scroll direction`.

---

### Task 2: Accurate persisted detection times

**Files:** `src/rfobserver/processing/rolling_burst.py` (`_to_fingerprint`). Test: `tests/unit/test_rolling_burst.py`.

- [ ] **Step 1 (test):** add a test that feeds the detector so a burst ENDS several evals before it is emitted (burst present in early chunks, then feed more noise-only chunks so it stops growing and is emitted later). Assert the emitted fingerprint's `stop_time` is close to `detection_timestamp - (rows_from_newest_to_abs_end)*tres` and NOT `detection_timestamp` (i.e. the stop lags the emission by the abs-row gap). Simplest concrete assertion: emit a burst that ended ~K rows before the newest row, and assert `(emit_now - stop_time).total_seconds() ~= K*tres` (within one chunk), whereas the OLD code gave ~0. Reuse the module's grid/feed helpers.
- [ ] **Step 2:** run -> fail against current `now - duration` stamping.
- [ ] **Step 3 (impl):** rewrite the time stamping in `_to_fingerprint`:
    ```python
    now = datetime.now(timezone.utc)
    tres = self._time_resolution_s
    start_time = now - timedelta(seconds=(self._total_rows_written - t.abs_start) * tres)
    stop_time = now - timedelta(seconds=(self._total_rows_written - t.abs_end) * tres)
    n_rows = t.abs_end - t.abs_start
    duration_sec = n_rows * tres
    ```
    Keep `duration_ms=duration_sec*1000`, `peak_freq_hz`, `center_freq_hz`, `bandwidth_hz`, `peak_power_db`, `detection_timestamp=now` unchanged.
- [ ] **Step 4:** run `PYTHONPATH= .venv/bin/pytest tests/unit/test_rolling_burst.py tests/integration/test_replay_reference.py -x -q` (replay still passes; it asserts duration/peak/count, not absolute times). Full unit suite.
- [ ] **Step 5:** ruff/mypy. Commit: `rolling_burst: stamp accurate burst start/stop from absolute-row position (not emission time)`.

---

### Task 3: query_detections `until` + sidecar helper (`storage/detections_sidecar.py`)

**Files:** `src/rfobserver/storage/database.py` (`query_detections`); new `src/rfobserver/storage/detections_sidecar.py`. Test: `tests/unit/test_database.py`, `tests/unit/test_detections_sidecar.py` (new).

**Interfaces produced:**
- `query_detections(..., until: datetime | None = None)` -> adds `start_time < until`.
- `detections_sidecar.sidecar_path(sc16_path: Path) -> Path` = `<base>.detections.json`.
- `async def build_sidecar_payload(sc16_path, storage_path, db) -> dict` (reads capture `.json` + `.psd.json` meta, queries the DB window+tuning, returns the sidecar dict) and `async def write_sidecar(sc16_path, storage_path, db) -> dict` (builds + writes the file, returns the dict).

- [ ] **Step 1 (tests):**
  - `test_database.py`: seed detections at t-old/t-mid/t-new; `query_detections(since=a, until=b)` returns only rows with `a <= start_time < b` (half-open, mirrors `since`).
  - `test_detections_sidecar.py`: create a temp storage dir with `<base>.json` (capture meta: `start_time`, `duration_sec`, `center_freq_hz`, `sample_rate_hz`, `gain_db`) and `<base>.psd.json` (`time_resolution_s`); seed matching + non-matching detections in a temp DB; call `write_sidecar` and assert the JSON has `capture_start_time`, `time_resolution_s`, tuning, and a `detections` list containing only the in-window+matching-tuning detections with correct `row_start`/`row_stop` (`round((det_start-cap_start)/tres)`).
- [ ] **Step 2:** run -> fail.
- [ ] **Step 3 (impl):**
  - `database.py`: add `until` param to `query_detections`; in the WHERE builder, after the `since` block: `if until is not None: conditions.append("start_time < ?"); params.append(until.isoformat())`.
  - `storage/detections_sidecar.py`:
    ```python
    import json
    from datetime import datetime, timedelta, timezone
    from pathlib import Path
    from typing import Any

    def sidecar_path(sc16_path: Path) -> Path:
        return sc16_path.with_suffix("").with_suffix(".detections.json")  # <base>.detections.json

    def _read_json(p: Path) -> dict[str, Any] | None:
        try:
            return json.loads(p.read_text())
        except Exception:
            return None

    async def build_sidecar_payload(sc16_path: Path, db: Any) -> dict[str, Any]:
        meta = _read_json(sc16_path.with_suffix(".json")) or {}
        from rfobserver.storage import psd_grid
        _raw, meta_path = psd_grid.grid_paths(sc16_path)
        psd_meta = _read_json(meta_path) or {}
        tres = float(psd_meta.get("time_resolution_s", 0.0)) or None
        start_iso = meta.get("start_time")
        dur = float(meta.get("duration_sec", 0.0))
        center = meta.get("center_freq_hz"); sr = meta.get("sample_rate_hz"); gain = meta.get("gain_db")
        out: dict[str, Any] = {
            "capture_start_time": start_iso, "time_resolution_s": tres,
            "center_freq_hz": center, "sample_rate_hz": sr, "gain_db": gain,
            "detections": [],
        }
        if not (start_iso and tres and dur):
            return out
        start = datetime.fromisoformat(start_iso)
        rows = await db.query_detections(
            since=start, until=start + timedelta(seconds=dur),
            sdr_center_freq=center, sample_rate=sr, gain=gain, limit=100000,
        )
        cap_start = start.timestamp()
        det = []
        for r in rows:
            ds = datetime.fromisoformat(r["start_time"]).timestamp()
            dp = datetime.fromisoformat(r["stop_time"]).timestamp()
            det.append({
                "start_time": r["start_time"], "stop_time": r["stop_time"],
                "center_freq_hz": r["center_freq_hz"], "bandwidth_hz": r["bandwidth_hz"],
                "peak_freq_hz": r.get("peak_freq_hz"), "peak_power_db": r["peak_power_db"],
                "duration_ms": r["duration_ms"],
                "row_start": int(round((ds - cap_start) / tres)),
                "row_stop": int(round((dp - cap_start) / tres)),
            })
        out["detections"] = det
        return out

    async def write_sidecar(sc16_path: Path, db: Any) -> dict[str, Any]:
        payload = await build_sidecar_payload(sc16_path, db)
        sidecar_path(sc16_path).write_text(json.dumps(payload, indent=2))
        return payload
    ```
    (Confirm `query_detections` accepts the exact kwarg names `sdr_center_freq`/`sample_rate`/`gain` per database.py; adjust if different. `query_detections` rows are dicts via `aiosqlite.Row`.)
- [ ] **Step 4:** run the two test files; ruff/mypy. Commit: `storage: query_detections until-bound + detections sidecar builder`.

---

### Task 4: Generate sidecar at record-stop (grace) + `GET /captures/detections/{filename}`

**Files:** `src/rfobserver/pipeline/streaming.py` (schedule deferred write), `src/rfobserver/config.py` (grace const), `src/rfobserver/web/routes/captures.py` (endpoint). Test: `tests/unit/test_captures_ws.py` or a captures route test.

- [ ] **Step 1 (test):** in a captures route test (TestClient + seeded `<base>.sc16`/`.json`/`.psd.json` + DB detections via `app.state.database`), `GET /captures/detections/<file>`: when no sidecar exists and the capture `start_time` is old (> grace), it lazily generates + returns `{detections:[...]}` with the in-window rows and writes the file; a second call reads the file. When the capture is very recent (< grace) and no file, returns `{"detections": [], "pending": true}`.
- [ ] **Step 2:** run -> fail (route absent).
- [ ] **Step 3 (config):** `config.py` add `DETECTIONS_SIDECAR_GRACE_SEC: float = 3.0`.
- [ ] **Step 4 (streaming):** at the end of `_end_recording` (after `_write_recording_metadata`, `streaming.py:792`), schedule the deferred sidecar write on the event loop (this method runs off-loop; `self._loop` + `self._db` exist):
    ```python
    if self._loop is not None and self._db is not None:
        sc16 = self._storage.storage_path / base_name
        grace = self._settings.DETECTIONS_SIDECAR_GRACE_SEC
        self._loop.call_soon_threadsafe(
            lambda: asyncio.ensure_future(self._deferred_sidecar(sc16, grace))
        )
    ```
    Add the coroutine method:
    ```python
    async def _deferred_sidecar(self, sc16_path, grace: float) -> None:
        from rfobserver.storage.detections_sidecar import write_sidecar
        try:
            await asyncio.sleep(grace)
            await write_sidecar(sc16_path, self._db)
        except Exception:
            logger.exception("Detections sidecar write failed for %s", sc16_path.name)
    ```
    (Verify `self._loop`/`self._db` attribute names against streaming.py; both are used elsewhere.)
- [ ] **Step 5 (endpoint):** in `captures.py` add:
    ```python
    @router.get("/detections/{filename}")
    async def capture_detections(request: Request, filename: str) -> dict[str, Any]:
        from datetime import datetime, timezone
        from rfobserver.storage import detections_sidecar as ds
        storage = _get_storage(request)
        base = filename.replace(".sc16", "").replace(".json", "").replace(".detections", "")
        sc16 = _validate_filename(base + ".sc16", storage)
        sp = ds.sidecar_path(sc16)
        if sp.exists():
            return json.loads(sp.read_text())
        db = getattr(request.app.state, "database", None)
        # decide pending vs lazy-generate based on capture age
        meta = ds._read_json(sc16.with_suffix(".json")) or {}
        grace = request.app.state.settings.DETECTIONS_SIDECAR_GRACE_SEC
        start_iso = meta.get("start_time"); dur = float(meta.get("duration_sec", 0.0))
        too_new = False
        if start_iso:
            end = datetime.fromisoformat(start_iso).timestamp() + dur
            too_new = (datetime.now(timezone.utc).timestamp() - end) < grace
        if db is None or too_new:
            return {"detections": [], "pending": True}
        return await ds.write_sidecar(sc16, db)
    ```
    (Match `_get_storage`/`_validate_filename` usage already in captures.py; import `json` if not present.)
- [ ] **Step 6:** run the captures route test + full unit; ruff/mypy. Commit: `captures: detections sidecar at record-stop (grace) + GET /captures/detections with lazy fallback`.

---

### Task 5: Detection overlay on the capture waterfall (`captures.html`)

**Files:** `src/rfobserver/web/templates/captures.html`. Manual verify.

- [ ] **Step 1:** add an overlay `<canvas id="viewer-wf-overlay">` absolutely positioned over `#viewer-wf` inside `.viewer-wf-spacer`'s sticky/scroll area (same 920 x viewH, `pointer-events:none`, `position:absolute; top:0; left:0`). Match the existing waterfall canvas placement so it aligns 1:1.
- [ ] **Step 2:** on viewer open (where `/captures/detail` is fetched and `viewerTimeRes`/`viewerFreqs` are set), also `fetch("/captures/detections/" + encodeURIComponent(filename))` and store `viewerDetections = resp.detections || []` and `viewerDetPending = resp.pending`.
- [ ] **Step 3:** add `drawDetectionOverlay()`: clear the overlay; for each det with `row_start`/`row_stop`, compute `yTop = row_start - firstVisibleRow`, `yBot = row_stop - firstVisibleRow`; skip if `yBot < 0 || yTop > viewH`; `xLo = ((det.center_freq_hz - det.bandwidth_hz/2) - viewerFreqs[0]) / (viewerFreqs[viewerFreqs.length-1] - viewerFreqs[0]) * 920`, `xHi` likewise; draw `strokeRect`/translucent `fillRect` (red, like the dashboard `drawBurstOverlay`); if `det === selectedDetection` use a brighter stroke. Call `drawDetectionOverlay()` at the end of `renderWaterfall()` and in `onScroll` after repaint.
- [ ] **Step 4 (manual):** mock pipeline, record a capture with mock bursts (or use an existing capture with a sidecar), open it: boxes appear at the right frequency columns and scroll in lockstep with the waterfall. Static-verify if no browser.
- [ ] **Step 5:** commit `captures: overlay detection boxes on the IQ waterfall from the sidecar`.

---

### Task 6: "Detections" table for the capture + click-to-scroll (`captures.html`)

**Files:** `src/rfobserver/web/templates/captures.html`. Manual verify.

- [ ] **Step 1:** add a "Detections" section (heading "Detections") near the viewer with a table reusing the Recent-Detections column layout/CSS (Time, Freq, Bandwidth, Duration, Peak). Populate client-side from `viewerDetections`: Time = `(row_start * viewerTimeRes).toFixed(3)+"s"` (capture-relative) or the absolute `start_time`; Freq = `peak_freq_hz ?? center_freq_hz` in MHz; Bandwidth in MHz; Duration ms; Peak dB. Empty/`pending` state message when none.
- [ ] **Step 2:** each `<tr>` gets `data-row-start` (+ store the det object). Click handler: `selectedDetection = det; scrollEl.scrollTop = Math.max(0, det.row_start - Math.floor(viewH/2)); ` sync the slider (`slider.value = det.row_start; slider.dispatchEvent(new Event("input"))` if that is the existing pattern), then `renderWaterfall()` + `drawDetectionOverlay()` so the box highlights. Mark the selected row (`is-selected` class).
- [ ] **Step 3:** rebuild the table when a new capture is opened (clear on capture switch, like the PSD viewer teardown).
- [ ] **Step 4 (manual):** open a capture, click a detection row -> waterfall scrolls to it and the box highlights. Static-verify if no browser.
- [ ] **Step 5:** commit `captures: per-capture Detections table with click-to-scroll`.

---

### Task 7: Full verification + finish
- [ ] `ruff check src/ tests/ && ruff format --check src/ tests/`; `PYTHONPATH= .venv/bin/mypy src/rfobserver/`; `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`; `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q`.
- [ ] Live smoke on the LOCAL test Jetson (nano-super) per prior practice: mock pipeline, confirm captures page loads detections + overlay + table (or static-verify). Do NOT touch HCRO; give the user redeploy/test steps.
- [ ] superpowers:finishing-a-development-branch.

## Self-Review
- Sidecar helper in `storage/` (no web<-pipeline coupling); reused by streaming + endpoint.
- Accurate times reuse existing columns (no migration). Overlay/table load only from the sidecar (independent of the live DB). Preload dedups via existing `have`/`pageCache`.
