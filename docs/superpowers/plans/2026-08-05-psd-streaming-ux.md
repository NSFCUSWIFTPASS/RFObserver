# PSD Streaming UX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`).

**Goal:** (1) hard-disable the High-Res burst-tuning threshold panel, (2) gate dashboard live-PSD streaming on visibility, (3) add a binary WebSocket path with server push-ahead for the captures spectrogram.

**Architecture:** Server changes in `web/websocket.py` (per-subscriber PSD gating) and `web/routes/captures.py` (shared PSD-slice helpers + a binary WS endpoint). Client changes in `templates/dashboard.html` (const gate + visibility → set_view) and `templates/captures.html` (WS range client with binary parse + push-ahead cache fill). HTTP PSD endpoint retained as fallback.

**Tech Stack:** FastAPI (WebSocket), numpy (float32 memmap slices), vanilla JS (canvas, WebSocket, DataView/Float32Array). Env: prefix Python commands with `PYTHONPATH=`; use `.venv/bin`. ruff is global.

## Global Constraints

- No emojis, no em-dashes anywhere (code/UI/docs). No "Co-Authored-By" in commits. Apple-style UI.
- Every Python command prefixed with `PYTHONPATH=`; `.venv/bin/...`.
- All CLAUDE.md checks stay green: `ruff check src/ tests/`, `ruff format --check src/ tests/`, `PYTHONPATH= .venv/bin/mypy src/rfobserver/`, `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`, `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q` (needs NATS :4222).
- The existing HTTP `GET /captures/psd/{filename}` JSON response must remain byte-for-byte identical after the helper refactor.
- Binary PSD frame format (little-endian): 12-byte header `int32 start, int32 count, int32 num_bins`, then `count*num_bins` float32, C-order row-major.
- Captures router is mounted at prefix `/captures`, so a `@router.websocket("/ws/psd/{filename}")` is reached at `/captures/ws/psd/{filename}`.

---

### Task 1: Hard-disable the burst-tuning panel (keep code)

**Files:** Modify `src/rfobserver/web/templates/dashboard.html:107-110`.

- [ ] **Step 1: Add the gate.** Immediately above `const burstTuningWrap = ...` (line 107) add:
```js
    // Burst-tuning threshold panel is intentionally hidden. Flip to true to restore
    // the draggable high/low threshold display; all its draw/drag code is retained.
    const SHOW_BURST_TUNING = false;
```
Change `applyBurstTuningVisibility()` body (line 109) to:
```js
        burstTuningWrap.style.display =
            (SHOW_BURST_TUNING && highRes && showBursts) ? "" : "none";
```

- [ ] **Step 2: Verify no other show path.** Grep `burst-tuning-wrap` / `burstTuningWrap.style.display` in dashboard.html; confirm `applyBurstTuningVisibility` is the only assignment to `.style.display` for that element. If another exists, route it through the same const.

- [ ] **Step 3: Manual check.** Run the mock pipeline
  `PYTHONPATH= RFOBS_MOCK_RECEIVER=true RFOBS_WEB_PORT=8888 .venv/bin/rfobserver run`,
  open `/`, enable High Res + Bursts: the burst-tuning panel must NOT appear; burst
  rectangles on the waterfall must still render. (If no browser automation available,
  static-verify the two edits and that `updateBurstOverlay` is untouched.)

- [ ] **Step 4: Commit** — `git add src/rfobserver/web/templates/dashboard.html && git commit -m "dashboard: hard-disable burst-tuning threshold panel (keep code behind const)"`.

---

### Task 2: Gate dashboard live-PSD on visibility

**Files:** Modify `src/rfobserver/web/websocket.py`; modify `src/rfobserver/web/templates/dashboard.html`. Test: `tests/unit/test_websocket.py` (new).

**Interfaces:**
- Produces: `_Subscriber.wants_psd: bool = True`; `set_view` message handling; `publish()` skips `type=="psd"` for `wants_psd=False`; `has_high_res_subscribers()` counts only `high_res and wants_psd`.

- [ ] **Step 1: Write failing tests** in `tests/unit/test_websocket.py`:
```python
import asyncio
import pytest
from rfobserver.web.websocket import LiveBroadcast


@pytest.mark.asyncio
async def test_wants_psd_gates_only_psd_messages():
    b = LiveBroadcast()
    sub = b.subscribe()
    sub.wants_psd = False
    await b.publish({"type": "heartbeat"})
    await b.publish({"type": "psd", "powers": [1.0]})
    got = []
    while not sub.queue.empty():
        got.append(sub.queue.get_nowait())
    assert [m["type"] for m in got] == ["heartbeat"]  # psd dropped


@pytest.mark.asyncio
async def test_wants_psd_true_receives_psd():
    b = LiveBroadcast()
    sub = b.subscribe()  # default wants_psd True
    await b.publish({"type": "psd", "powers": [1.0]})
    assert sub.queue.get_nowait()["type"] == "psd"


def test_has_high_res_counts_only_viewing():
    b = LiveBroadcast()
    s = b.subscribe()
    s.high_res = True
    s.wants_psd = False
    assert b.has_high_res_subscribers() is False
    s.wants_psd = True
    assert b.has_high_res_subscribers() is True
```

- [ ] **Step 2: Run, expect fail** — `PYTHONPATH= .venv/bin/pytest tests/unit/test_websocket.py -x -q` (AttributeError `wants_psd` / psd not dropped).

- [ ] **Step 3: Implement server** in `websocket.py`:
  - `_Subscriber.__slots__` add `"wants_psd"`; in `__init__` set `self.wants_psd = True`.
  - `has_high_res_subscribers`: `return any(s.high_res and s.wants_psd for s in self._subscribers)`.
  - `publish`: after popping `grid_rows`, when `data.get("type") == "psd"`, skip subscribers with `not sub.wants_psd`. Concretely, inside the per-sub loop:
    ```python
    is_psd = data.get("type") == "psd"
    for sub in list(self._subscribers):
        if is_psd and not sub.wants_psd:
            continue
        ...
    ```
  - `recv_loop`: add branch `elif msg.get("type") == "set_view": sub.wants_psd = bool(msg.get("psd_visible", True))`.

- [ ] **Step 4: Run tests → PASS.**

- [ ] **Step 5: Implement client** in `dashboard.html`:
  - Add state `let psdVisible = true;` near `highRes`.
  - Add `function sendView() { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({type:"set_view", psd_visible: psdVisible})); }`.
  - `function setPsdVisible(v){ if (v === psdVisible) return; psdVisible = v; sendView(); }`.
  - `document.addEventListener("visibilitychange", () => setPsdVisible(document.visibilityState === "visible" && psdInView));` where `psdInView` is the observer's last value (track it in a module var, default true).
  - IntersectionObserver on the PSD card container (the element wrapping the PSD canvas; pick the nearest stable `.card` / `#psd-...` container — verify the id/class in dashboard.html and use it):
    ```js
    let psdInView = true;
    const psdCard = document.getElementById("psd-card") || psdCanvas.closest(".card");
    if (psdCard && "IntersectionObserver" in window) {
        new IntersectionObserver((entries) => {
            psdInView = entries[0].isIntersecting;
            setPsdVisible(psdInView && document.visibilityState === "visible");
        }, {threshold: 0.01}).observe(psdCard);
    }
    ```
  - In `ws.onopen` (near the existing `if (highRes) ...set_mode`), also `sendView();`.

- [ ] **Step 6: Manual check.** Mock pipeline on :8888; open devtools Network/WS: switching to another tab (tab hidden) or scrolling the PSD card out of view stops `psd` frames (heartbeat continues); returning resumes. (Static-verify if no browser.)

- [ ] **Step 7: Commit** — `git commit -am "dashboard/ws: stream live PSD only while the PSD card is visible (set_view gating)"`.

---

### Task 3: Captures spectrogram over binary WebSocket (server push-ahead)

**Files:** Modify `src/rfobserver/web/routes/captures.py`; modify `src/rfobserver/web/templates/captures.html`. Test: `tests/unit/test_captures_ws.py` (new); keep the existing HTTP captures-PSD test green.

**Interfaces:**
- Produces: `_open_psd(sc16_path) -> tuple[grid, dict] | None`; `_slice_psd(grid, freq_axis, num_bins, start, count, max_bins) -> tuple[np.ndarray, np.ndarray, int]` (contiguous C-order float32, downsampled freq axis, ds num_bins); `_psd_frame_bytes(start, sliced) -> bytes`; WS route `/captures/ws/psd/{filename}`.

- [ ] **Step 1: Write failing unit tests** in `tests/unit/test_captures_ws.py` (seed an `.npz` + `.sc16` under `settings.STORAGE_PATH` exactly like the existing captures-PSD test in `tests/unit/test_web_routes.py`; a small grid e.g. 6 rows x 8 bins with known values):
```python
import struct
import numpy as np
from fastapi.testclient import TestClient
# ... build app = create_app(settings); seed npz with grid/freq_axis/time_resolution_s/
#     center_freq_hz/bandwidth_hz (+ optional cal_offset_db); write a .sc16 stub.

def test_slice_psd_contiguous_float32_and_downsample():
    from rfobserver.web.routes.captures import _slice_psd
    grid = np.arange(6 * 8, dtype=np.float32).reshape(6, 8)
    fa = np.arange(8, dtype=np.float64)
    sliced, ds_fa, ds_n = _slice_psd(grid, fa, 8, start=1, count=3, max_bins=4)
    assert sliced.dtype == np.float32 and sliced.flags["C_CONTIGUOUS"]
    assert sliced.shape == (3, 4) and ds_n == 4  # 8 bins -> 4 via factor-2 mean

def test_psd_frame_bytes_header_and_len():
    from rfobserver.web.routes.captures import _psd_frame_bytes
    sliced = np.zeros((3, 4), dtype=np.float32)
    buf = _psd_frame_bytes(7, sliced)
    start, count, num_bins = struct.unpack("<iii", buf[:12])
    assert (start, count, num_bins) == (7, 3, 4)
    assert len(buf) - 12 == 3 * 4 * 4

def test_ws_streams_meta_then_binary_window(client, seeded_filename):
    with client.websocket_connect(f"/captures/ws/psd/{seeded_filename}") as ws:
        meta = ws.receive_json()
        assert meta["type"] == "meta" and meta["total_rows"] >= 1
        ws.send_json({"start": 0, "count": 2, "max_bins": 512})
        frame = ws.receive_bytes()
        start, count, num_bins = struct.unpack("<iii", frame[:12])
        assert start == 0 and count >= 1
        rows = np.frombuffer(frame[12:], dtype="<f4").reshape(count, num_bins)
        # equals the HTTP endpoint's grid for the same range
        http = client.get(f"/captures/psd/{seeded_filename}?start=0&count=2&max_bins=512").json()
        assert np.allclose(rows, np.array(http["grid"], dtype=np.float32))
```
(If TestClient binary-then-json ordering is fiddly, receive frames in a loop and classify by type: bytes vs dict.)

- [ ] **Step 2: Run, expect fail** (helpers/route absent).

- [ ] **Step 3: Refactor helpers in `captures.py`.** Extract from `capture_psd` (lines 115-161) `_open_psd(sc16_path)` (returns `(grid, meta_dict)` or None; meta_dict keys: freq_axis(np), time_resolution_s, center_freq_hz, bandwidth_hz, total_rows, num_bins, grid_min, grid_max, cal_offset_db) and `_slice_psd(grid, freq_axis, num_bins, start, count, max_bins)` (row slice + the exact bin-downsample block at 156-161; return `np.ascontiguousarray(sliced, dtype=np.float32)`, ds freq_axis, ds num_bins). Rewrite `capture_psd` to call them and build the SAME JSON dict (grid=`sliced.tolist()`, count=`end-start`, etc.). Add `_psd_frame_bytes(start, sliced)`: `struct.pack("<iii", int(start), int(sliced.shape[0]), int(sliced.shape[1])) + np.ascontiguousarray(sliced, dtype="<f4").tobytes()`.

- [ ] **Step 4: Add the WS endpoint** `@router.websocket("/ws/psd/{filename}")` async `capture_psd_ws(websocket, filename)`:
  - Resolve storage from `websocket.app.state`; validate filename; `opened = _open_psd(sc16_path)`; if None: `await websocket.close(code=1011); return`.
  - `await websocket.accept()`; send meta JSON: `{"type":"meta","freq_axis": ds_freq_axis_for_default_max_bins.tolist(), "time_resolution_s":..., "total_rows":..., "num_bins": ds_num_bins, "grid_min":..., "grid_max":..., "cal_offset_db":..., "center_freq_hz":..., "bandwidth_hz":...}`. (Compute the ds axis with `_slice_psd(grid, freq_axis, num_bins, 0, 0, max_bins_default=512)` or a dedicated axis-downsample; keep num_bins consistent with data frames.)
  - Loop `receive_json()` -> `{start,count,max_bins}`. Define `async def serve(s):` that clamps `s` to `[0,total_rows)`, computes the window via `_slice_psd`, and `await websocket.send_bytes(_psd_frame_bytes(s, sliced))` (skip if empty). Serve the requested `start`, then push-ahead `start+count` and `start-count` (bounded to grid, skip negatives / >=total_rows). Honor an optional `have` list in the request to skip already-cached neighbor starts.
  - Wrap in `try/except WebSocketDisconnect: pass` + `except Exception: logger.exception(...)`, mirroring `modules.py:audio_websocket`.

- [ ] **Step 5: Run unit tests → PASS.** Also `PYTHONPATH= .venv/bin/pytest tests/unit/test_web_routes.py -x -q` (HTTP JSON unchanged).

- [ ] **Step 6: Client in `captures.html`.** Reuse the existing render/cache path (`pageCache` keyed by page-aligned `start`, `PAGE`, `ensureVisible`, `renderWaterfall`):
  - On viewer open for a capture with PSD, open `psdWs = new WebSocket("ws://"+location.host+"/captures/ws/psd/"+encodeURIComponent(viewerFilename))`; `psdWs.binaryType = "arraybuffer"`. On `message`: if `typeof ev.data === "string"` parse the meta JSON and store fields exactly where the HTTP meta probe stored them (freq axis, total_rows, time_resolution_s, grid_min/max, cal_offset_db, center/bw); else parse binary: `const dv = new DataView(ev.data); start=dv.getInt32(0,true); count=dv.getInt32(4,true); nb=dv.getInt32(8,true); const rows = new Float32Array(ev.data, 12);` slice into `count` rows of `nb`, store into `pageCache` keyed by `start` (align to PAGE), run the existing LRU eviction, then if `[start,start+count)` intersects the viewport call the existing repaint.
  - Change `fetchPageRaw(start)` to: if `psdWs` open, `psdWs.send(JSON.stringify({start, count: PAGE, max_bins: 512, have: <cached page starts>}))` and return (the frame arrives async and fills the cache); else fall back to the existing HTTP GET. Keep a `pageLoading` guard so a page is not requested twice.
  - The server already pushes neighbors, so `ensureVisible` naturally gets look-ahead; no extra client prefetch needed. Close `psdWs` on viewer close / capture switch (add to the existing teardown).
  - Keep the HTTP first-frame probe as a fallback if `meta` has not arrived when the first render is needed.

- [ ] **Step 7: Manual check.** Mock pipeline; record or use an existing capture with PSD; open it on `/captures`; scroll fast: rows should fill without the HTTP round-trip stalls, and scrolling into not-yet-seen regions should already be populated (push-ahead). Confirm HTTP fallback still works by temporarily blocking the WS. (Static-verify if no browser.)

- [ ] **Step 8: Commit** — `git commit -am "captures: binary WebSocket PSD streaming with server push-ahead (HTTP kept as fallback)"`.

---

### Task 4: Full verification

- [ ] **Step 1:** `ruff check src/ tests/ && ruff format --check src/ tests/`
- [ ] **Step 2:** `PYTHONPATH= .venv/bin/mypy src/rfobserver/`
- [ ] **Step 3:** `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`
- [ ] **Step 4:** `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q`
- [ ] **Step 5:** If all green, use superpowers:finishing-a-development-branch.

## Self-Review (author)

- Spec coverage: Task 1 = burst panel const; Task 2 = dashboard visibility gating (server+client+tests); Task 3 = captures binary WS (helpers+endpoint+client+tests). All spec sections covered.
- Placeholders: none; server Python and framing are complete code; JS steps give exact protocol, cache keys, and insertion points (the large existing JS is edited in place, not reproduced wholesale).
- Type consistency: `wants_psd` (bool), binary header `<iii` and `<f4` payload used identically in `_psd_frame_bytes`, the WS endpoint, the client `DataView`, and the tests.
