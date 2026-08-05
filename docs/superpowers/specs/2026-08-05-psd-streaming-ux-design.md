# PSD streaming UX: disable burst-tuning panel, WS captures spectrogram, gated dashboard PSD

## Problem / goals

Three independent web-UI changes around PSD (power spectral density) streaming:

1. **Hide the burst-tuning threshold panel** shown in High-Res mode. Keep the code
   (draw + drag handlers) but hard-disable the display.
2. **Stream the captures-page spectrogram PSD over a WebSocket** (in addition to the
   existing paged HTTP endpoint), with the server pushing adjacent pages ahead of the
   scroll so the user never waits while scrolling. Binary Float32 payloads.
3. **Only stream dashboard live PSD while it is actually being viewed** — a
   backgrounded tab or a scrolled-away PSD card must cost no PSD bandwidth.

## Current state (established by code read)

- Dashboard opens `ws://<host>/ws/live` unconditionally on load (`dashboard.html:97,992`);
  server `websocket_endpoint` (`websocket.py:57`) with per-subscriber `high_res` flag.
  Publisher `StreamingProcessor._result_consumer_loop` (`streaming.py:1223-1317`) sends
  a `type:"psd"` message every chunk (averaged when no high-res subscriber, per-chunk +
  `bursts` when a high-res subscriber exists). No visibility gating exists.
- Burst-tuning panel `#burst-tuning-wrap` (`dashboard.html:43`) is shown via
  `applyBurstTuningVisibility()` (`dashboard.html:108-110`) when `highRes && showBursts`.
- Captures spectrogram fetches paged PSD over HTTP
  `GET /captures/psd/{filename}?start&count&max_bins` (`captures.html:174`,
  `captures.py:100`), returning a JSON `grid` window (<=500 rows x <=512 bins) plus meta.
  Client is already virtualized: `pageCache` LRU (`MAX_PAGES=48`), `PAGE=500`,
  scroll -> `onScroll` -> `ensureVisible` -> `fetchPageRaw` (`captures.html:106-110,277-373`).
- `drawPSD` (`shared-charts.js:29`) draws an optional "Burst Hi" line only when
  `triggerLevel != null`; both callers already pass `null` (untouched by this work).

## Design

### Task 1 - hard-disable burst-tuning panel (dashboard.html)

Add a module-level `const SHOW_BURST_TUNING = false;` near the other dashboard consts and
change `applyBurstTuningVisibility()` to:

```js
burstTuningWrap.style.display = (SHOW_BURST_TUNING && highRes && showBursts) ? "" : "none";
```

Nothing else changes: the panel's draw code (`drawBurstTuning`, ~`dashboard.html:495-610`),
its drag handlers, and the separate waterfall burst-rectangle overlay
(`updateBurstOverlay`) all stay live. Flipping the const back to `true` fully restores it.

### Task 2 - captures spectrogram over WebSocket (binary + server push-ahead)

**Server** (`routes/captures.py`):
- Refactor the grid-load + slice + bin-downsample logic out of `capture_psd` into two
  helpers so HTTP and WS share one implementation:
  - `_open_psd(sc16_path) -> tuple[grid, meta_dict] | None` (new/legacy load, returns the
    memmap grid + a dict: freq_axis, time_resolution_s, center_freq_hz, bandwidth_hz,
    total_rows, num_bins, grid_min, grid_max, cal_offset_db).
  - `_slice_psd(grid, freq_axis, num_bins, start, count, max_bins) -> tuple[np.ndarray, np.ndarray, int]`
    returns `(sliced_f32_contiguous, ds_freq_axis, ds_num_bins)` (contiguous float32,
    C-order, ready for `.tobytes()`).
  `capture_psd` (HTTP) keeps its exact JSON response, now built via these helpers.
- New endpoint `@router.websocket("/ws/psd/{filename}")` -> `capture_psd_ws`:
  - On connect: validate filename + load grid (close 1011 if absent). Send ONE JSON "meta"
    frame: `{type:"meta", freq_axis, time_resolution_s, total_rows, num_bins, grid_min,
    grid_max, cal_offset_db, center_freq_hz, bandwidth_hz}` (num_bins here is the
    downsample-capped value used for data frames).
  - Recv loop: client sends JSON `{start, count, max_bins}` range requests.
  - For each request, serve that window, THEN proactively serve the next window and the
    previous window (push-ahead), skipping any the client already has (client includes an
    optional `have: [startA, startB, ...]` hint, or the server just always pushes the two
    neighbors and the client dedups by `start`). Keep pushes bounded (at most the
    requested window + 2 neighbors per request).
  - Each data frame is a single **binary** WS message: a 12-byte little-endian header
    `int32 start, int32 count, int32 num_bins` followed by `count*num_bins` little-endian
    float32 (C-order, row-major). Helper `_psd_frame_bytes(start, sliced) -> bytes`.
  - Guard against slow clients / backpressure with a bounded send and `WebSocketDisconnect`
    handling mirroring `modules.py` audio WS.

**Client** (`captures.html`):
- When a capture with PSD is opened, open `ws://<host>/captures/ws/psd/<filename>`; on the
  "meta" frame store the same fields the HTTP path stored (freq axis, total_rows, etc.) so
  the render path is unchanged.
- Replace `fetchPageRaw`'s HTTP GET with a WS range request when the socket is open; on a
  binary frame, parse the 12-byte header + Float32Array, write rows into `pageCache` keyed
  by `start` (same cache the renderer reads), evict via the existing LRU, then repaint if
  the arrived page intersects the viewport. Push-ahead frames simply populate the cache.
- Keep the HTTP path as the fallback when the WS is not open/errored (and for the very
  first metadata probe if the socket has not yet delivered "meta"). Close the WS when the
  viewer closes / a different capture is selected.

### Task 3 - gate dashboard PSD on visibility

**Client** (`dashboard.html`):
- Track `psdVisible` from BOTH `document.visibilitychange` (tab hidden -> false) AND an
  `IntersectionObserver` on the PSD card container (scrolled out of view -> false).
- On any change, if the socket is open send `{type:"set_view", psd_visible: <bool>}`.
  Send the current value on `ws.onopen` too (alongside the existing high_res re-send).
- Default `psdVisible = true` (page loads with PSD in view).

**Server** (`websocket.py` + `streaming.py`):
- `_Subscriber` gains `wants_psd: bool = True`. `recv_loop` handles
  `type:"set_view"` -> `sub.wants_psd = bool(msg.get("psd_visible", True))`.
- `publish()`: for a message with `type == "psd"`, skip subscribers whose `wants_psd` is
  False (they still receive non-psd messages such as `heartbeat`). Non-psd messages
  unaffected.
- `has_high_res_subscribers()` -> count only subscribers with `high_res and wants_psd`, so
  the publisher drops to the cheaper averaged path (or, if no one is viewing, still
  computes it but nobody receives it; acceptable — the expensive per-chunk high-res work is
  what we gate). Keep the heartbeat flowing so the client still knows the link is alive.

## Testing

- **Task 1:** no logic to unit-test (a display const). Manual: High Res on -> panel stays
  hidden; burst rectangles overlay still works; toggling the const to true restores panel.
- **Task 2 (unit, `tests/unit/test_web_routes.py` or a new `test_captures_ws.py`):**
  - `_slice_psd` returns contiguous float32 with correct shape and bin-downsample
    (num_bins>max_bins path), matching the HTTP JSON `grid` values.
  - `_psd_frame_bytes` header decodes to (start, count, num_bins) and payload length ==
    count*num_bins*4.
  - WS endpoint (FastAPI `TestClient.websocket_connect`): connect to a seeded capture,
    receive a "meta" JSON frame, send a range request, receive the window binary frame
    followed by the push-ahead neighbor frame(s); assert headers/starts and that decoded
    rows equal the HTTP endpoint's `grid` for the same range. Missing-capture -> close.
  - HTTP `capture_psd` still returns the identical JSON after the helper refactor
    (regression: keep/extend the existing captures PSD test).
- **Task 3 (unit, `tests/unit/test_websocket.py` — add if absent):**
  - A subscriber with `wants_psd=False` receives a `heartbeat` publish but NOT a `psd`
    publish; with `wants_psd=True` receives both.
  - `set_view` recv toggles `wants_psd`; `has_high_res_subscribers()` returns False when the
    only high-res subscriber has `wants_psd=False`.
- Full CI per CLAUDE.md (ruff check + format, mypy, unit, integration).

## Files

- `src/rfobserver/web/templates/dashboard.html` - Task 1 const + visibility gate (Task 3
  client: IntersectionObserver, visibilitychange, set_view send).
- `src/rfobserver/web/websocket.py` - Task 3 `wants_psd`, `set_view`, publish/high-res gating.
- `src/rfobserver/web/routes/captures.py` - Task 2 helpers + WS endpoint.
- `src/rfobserver/web/templates/captures.html` - Task 2 WS client + binary parse + push-ahead cache fill.
- `tests/unit/test_captures_ws.py` (new), `tests/unit/test_websocket.py` (new/extend),
  existing captures-PSD HTTP test (regression).

## Out of scope

- Changing the PSD storage format, time resolution, or the dashboard waterfall rendering.
- Converting the dashboard live PSD to binary (stays JSON; only captures playback goes binary).
- Auth/rate-limiting on the new WS beyond the disconnect handling the audio WS already uses.
