# Capture detections: sidecar, waterfall overlay, table + faster PSD scrolling

## Goal

On the IQ-captures page: (0) make the PSD waterfall load faster while scrolling by
preloading further ahead; (1) persist each capture's detections as an independently
loadable sidecar file; (2) overlay detection bounding boxes on the capture's waterfall;
(3) show a per-capture "Detections" table (reusing the Recent-Detections look) where
clicking a row scrolls the waterfall to that detection. Detection time placement must be
accurate.

## Background (from code audit)

- Capture companions per `.sc16`: `<base>.json` (capture meta: absolute `start_time`,
  `duration_sec`, `center_freq_hz`, `sample_rate_hz`, `gain_db`), `<base>.psd` +
  `<base>.psd.json` (PSD grid: `rows`, `num_bins`, `time_resolution_s`, `freq_axis`, ...).
  Meta is exposed by `GET /captures/detail/{filename}`.
- Captures waterfall (`captures.html`) is virtualized, **1 px per row**, row 0 = capture
  start, `time = row * time_resolution_s`; `firstVisibleRow = round(scrollEl.scrollTop)`.
  PSD streams over `capture_psd_ws` (binary frames + `have`-dedup); server pushes
  requested + next + prev windows today.
- Detections table is an HTMX HTML fragment `GET /api/detections` (DB-backed, dashboard).
- Detections are stored with UTC-isoformat `start_time`/`stop_time`, `center_freq_hz`,
  `bandwidth_hz`, `peak_freq_hz`, `peak_power_db`, `duration_ms`, `sdr_center_freq_hz`,
  `sample_rate_hz`, `gain_db`. Same system UTC clock as captures.
- **Time-accuracy issue:** `rolling_burst._to_fingerprint` stamps `start_time = now -
  duration` at emission, lagging true signal time by up to ~one eval-interval (~0.4 s at
  the field config). Fixed in Part 1 below.

## Design

### Part 0 - Preload 2 windows ahead in scroll direction (fast scrolling)

- **Server** `capture_psd_ws.serve` (`routes/captures.py`): in addition to the requested
  window and `-count` (prev), push **`+count` and `+2*count`** (two ahead). Keep the
  `have`-dedup + bounds guard. At most ~4 frames per request.
- **Client** (`captures.html`): in `onScroll`, track scroll direction (compare new vs
  previous `firstVisibleRow`); when scrolling forward, also enqueue the **next 2 pages**
  beyond the visible range (and 1 behind when scrolling back), via the existing WS request
  path (dedup by `pageCache`/`pageLoading` + `have`). Falls back to HTTP when the WS is
  closed.

### Part 1 - Accurate persisted detection times (no schema change)

Fix `processing/rolling_burst.py::_to_fingerprint`: the tracker holds each burst in
absolute row coordinates (`abs_start`/`abs_end` vs `_total_rows_written`). Anchor the
newest processed row's wall-clock to `now` (emission time, which happens synchronously
just after the newest chunk is appended) and offset back by the burst's absolute-row
distance:
```python
now = datetime.now(timezone.utc)
tres = self._time_resolution_s
start_time = now - timedelta(seconds=(self._total_rows_written - t.abs_start) * tres)
stop_time  = now - timedelta(seconds=(self._total_rows_written - t.abs_end) * tres)
```
`duration_ms`/`peak_freq_hz`/etc unchanged; `detection_timestamp=now`. This removes the
emission-latency error so the stored `start_time`/`stop_time` reflect the burst's true
position. It is independent of the dashboard live WS overlay (which builds its own
window-relative times from `rolling_detector.last_detection`), so that path is untouched.

### Part 2 - `<base>.detections.json` sidecar (independent association)

- **Path helper** (e.g. `storage/psd_grid.py` alongside `grid_paths`, or a small
  `captures` helper): `<base>.detections.json` next to the `.sc16`.
- **`query_detections` gains an `until` upper bound** (`storage/database.py`): today it has
  only `since`. Add `until: datetime | None` -> `start_time < until` in the WHERE builder
  (mirrors `since`). Used to scope a capture's `[start, start+duration]` window.
- **Writer** `write_detections_sidecar(sc16_path, meta, db)`:
  1. Read capture `start_time` (abs), `duration_sec`, `center_freq_hz`, `sample_rate_hz`,
     `gain_db` from the capture `.json` meta; `time_resolution_s` from the PSD meta.
  2. `dets = query_detections(since=start, until=start+duration, sdr_center=center,
     sample_rate=sr, gain=gain, limit=large)`.
  3. Write JSON:
     ```json
     {"capture_start_time": "...Z", "time_resolution_s": 0.0002,
      "center_freq_hz": ..., "sample_rate_hz": ..., "gain_db": ...,
      "detections": [{"start_time","stop_time","center_freq_hz","bandwidth_hz",
        "peak_freq_hz","peak_power_db","duration_ms",
        "row_start": <int>, "row_stop": <int>}]}
     ```
     `row_start = round((det_start - capture_start)/time_resolution_s)`, `row_stop` from
     stop. Precomputed rows make the file self-contained for future tooling.
- **Generate at record-stop + grace, lazy fallback:**
  - In the streaming recording-stop path (off the event loop, as recording-stop already
    runs), schedule an asyncio task that after a grace delay
    (`DETECTIONS_SIDECAR_GRACE_SEC`, default ~ 2x the rolling-detection window duration,
    e.g. 3 s) calls the writer, so trailing/late-emitted detections are included. Guard
    with try/except; never affect recording.
  - **`GET /captures/detections/{filename}`** (`routes/captures.py`): if the sidecar file
    exists, return it; else if the capture is older than the grace period, lazily generate
    it (writer) and return; else return `{"detections": []}` with a `pending: true` flag.
    This covers pre-existing captures and robustness.

### Part 3 - Overlay boxes on the capture waterfall

`captures.html`: add an absolutely-positioned overlay canvas over `#viewer-wf` (same
920 x viewH size). On viewer open, fetch `GET /captures/detections/<filename>` and keep
the detection list. A `drawDetectionOverlay()` runs after each `renderWaterfall` (and on
scroll): for each detection,
`yTop = row_start - firstVisibleRow`, `yBot = row_stop - firstVisibleRow` (clip to
viewport), `xLo/xHi` from `center +/- bandwidth/2` mapped across `viewerFreqs[0]..[N-1]`
onto width 920 (mirroring the dashboard's `drawBurstOverlay`). Draw a stroked+translucent
rect; highlight the selected one. Skip boxes fully outside the viewport.

### Part 4 - "Detections" table for the selected capture + click-to-scroll

`captures.html`: render a table titled **"Detections"** from the sidecar JSON (reuse the
Recent-Detections column layout + CSS: time, freq, bandwidth, duration, peak). Build it
client-side so it stays independent of the DB. Each row carries the detection's
`row_start` + `center_freq_hz`. Clicking a row:
`scrollEl.scrollTop = row_start - Math.floor(viewH/2)` (centers it), syncs the slider, and
sets the selected detection so `drawDetectionOverlay` highlights its box. Empty state when
no detections (or `pending`).

## Testing

- **Part 0:** unit-level assert `capture_psd_ws` pushes `start`, `start+count`,
  `start+2*count`, `start-count` for a seeded capture (extend `test_captures_ws.py`).
  Client scroll-prefetch verified manually.
- **Part 1 (`tests/unit/test_rolling_burst.py`):** feed windows where a burst ends several
  evals before it is emitted (stops growing, then more chunks arrive); assert the emitted
  `start_time`/`stop_time` reflect the burst's absolute-row position (offset from newest
  row), NOT `now - duration`. Existing rolling-burst + replay tests still pass.
- **Part 2 (`tests/unit/test_database.py` + a captures test):** `query_detections(until=)`
  bounds correctly (half-open, mirrors `since`). `write_detections_sidecar` writes the JSON
  with correct window filtering + `row_start/row_stop` for seeded detections + capture meta.
  `GET /captures/detections/{filename}` returns the sidecar (present) and lazy-generates
  when missing + old; `pending` when too new.
- **Parts 3-4:** no unit tests (canvas/DOM); manual verification on the captures page
  (mock pipeline): boxes align with waterfall energy; clicking a table row scrolls+highlights.
- Full CI per CLAUDE.md.

## Files

- `src/rfobserver/processing/rolling_burst.py` - accurate `_to_fingerprint` times.
- `src/rfobserver/storage/database.py` - `query_detections(until=...)`.
- `src/rfobserver/storage/psd_grid.py` (or a captures helper) - sidecar path.
- `src/rfobserver/pipeline/streaming.py` - schedule sidecar write at record-stop + grace;
  `config.py` - `DETECTIONS_SIDECAR_GRACE_SEC`.
- `src/rfobserver/web/routes/captures.py` - `write_detections_sidecar`,
  `GET /captures/detections/{filename}`, extended `capture_psd_ws` push-ahead.
- `src/rfobserver/web/templates/captures.html` - scroll prefetch, overlay canvas + draw,
  "Detections" table + click-to-scroll.
- Tests: `test_rolling_burst.py`, `test_database.py`, `test_captures_ws.py` (+ captures sidecar test).

## Out of scope

- Changing detection generation beyond the time-stamp fix. No new DB columns.
- Editing/deleting detections from the captures page (read-only overlay + table).
