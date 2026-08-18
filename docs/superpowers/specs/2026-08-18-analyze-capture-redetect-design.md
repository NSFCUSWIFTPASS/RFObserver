# Record a replay + re-run detection on any capture (threshold tuning)

## Goal

Let a raw IQ file (e.g. the December SSM/FHSS `.dat`) become a full-resolution,
inspectable capture, and let a capture's detections be re-computed at different
thresholds without re-recording — so bursts can be studied at ~0.2 ms/row in the
existing Captures viewer and the detection threshold tuned in place. Reuse the
existing replay + recording + viewer machinery; keep the new code minimal.

## Approach (reuse what exists)

The replay already plays a file through the live pipeline, and the pipeline's
*recording* path already builds and persists the full-resolution `.psd` grid. So:

1. **Record the replay once** -> a normal capture (`.sc16` + `.psd` + `.json`),
   turning the raw `.dat` into an inspectable capture (IQ copied once).
2. **Re-run detection on a capture's stored `.psd` grid** to (re)write its
   detections sidecar at chosen thresholds -- reusing IQ+PSD, no re-record. This
   works on **any** existing capture with a `.psd` grid, field recordings included.

One shared new helper does the detection pass; the rest is small wiring.

## Background (from code audit)

- **Recording writes a full-resolution grid.** `_end_recording`
  (`pipeline/streaming.py:733`) finalizes `<base>.psd` (float32 memmap, every PSD
  row at `PSD_TIME_RESOLUTION_MS`, default 0.2 ms) + `<base>.psd.json`
  (`rows`, `num_bins`, `time_resolution_s`, `freq_axis`, `grid_min/max`,
  `center_freq_hz`, `bandwidth_hz`) via `storage/psd_grid.py::write_meta`, and
  schedules `_deferred_sidecar` -> `detections_sidecar.write_sidecar(sc16, db)`
  (DB-sourced).
- **Replay recording is currently gated off.** `start_recording` / `arm_trigger`
  / `_begin_recording` / `_check_trigger_and_record` early-return when
  `self._replay_mode` (the Task-3 hardening). Recording during replay needs an
  opt-in that lifts the manual-record gate for a deliberate replay-record.
- **The batch detector runs on a whole grid.**
  `processing/burst.py::detect_bursts(psd_grid: PSDGridResult, config:
  BurstDetectionConfig | None, center_freq_hz, capture_time)` returns
  `BurstDetectionResult` (bursts with `start_time`/`stop_time` = `capture_time +
  t`, `center_freq_hz`, `peak_freq_hz`, `bandwidth_hz`, `peak_power_db`,
  `duration_ms`). This is the whole-grid method the validated
  `waterfall_plot.py` uses; `--burst-threshold 40` == `threshold_high_db=40`
  (see [[reference_ssm_waterfall_plot_params]]).
- **Grid IO.** `storage/psd_grid.py::grid_paths(sc16)` and `load_grid(sc16) ->
  (mm ndarray (rows,num_bins), meta)`. `PSDGridResult` (`processing/spectral.py`)
  fields: `grid`, `time_axis`, `freq_axis`, `ffts_per_slice`, `total_ffts`
  (detect_bursts uses only `grid`/`time_axis`/`freq_axis`).
- **Sidecar schema + helpers.** `storage/detections_sidecar.py`: `sidecar_path`,
  `build_sidecar_payload(sc16, db)`, `write_sidecar(sc16, db)`; schema
  `{capture_start_time, time_resolution_s, center_freq_hz, sample_rate_hz,
  gain_db, detections:[{start_time, stop_time, center_freq_hz, bandwidth_hz,
  peak_freq_hz, peak_power_db, duration_ms, row_start, row_stop}]}`.
- **Endpoints/allowlist** from the replay feature: `/api/replay/{start,stop,speed}`,
  `REPLAY_SOURCE_DIR` + `_resolve_replay_path`, and the replay banner in
  `dashboard.html`. The Captures viewer + overlay + Detections table +
  click-to-scroll are already built (`captures.html`).

## Design

### Part 1 - Shared helper: `write_sidecar_from_grid` (`storage/detections_sidecar.py`)

`async def build_sidecar_from_grid(sc16_path, config: BurstDetectionConfig) ->
dict` and `def write_sidecar_from_grid(sc16_path, config) -> dict` (sync file
write; detect is CPU, no awaits needed):

1. `load_grid(sc16_path)` -> `(mm, meta)`; if absent, return an empty payload /
   raise a clear error the callers turn into 404.
2. Reconstruct `PSDGridResult(grid=mm, time_axis=arange(rows)*tres,
   freq_axis=np.asarray(meta["freq_axis"]), ffts_per_slice=0, total_ffts=rows)`
   where `tres = meta["time_resolution_s"]`.
3. Read the capture `.json` for `start_time` (-> `capture_start`),
   `center_freq_hz`, `sample_rate_hz`, `gain_db`. `center_freq_hz` for
   `detect_bursts` comes from `meta["center_freq_hz"]`.
4. `result = detect_bursts(psd_grid, config, center_freq_hz, capture_time=capture_start)`.
5. For each burst, `row_start = round((b.start_time - capture_start).total_seconds()
   / tres)`, `row_stop = round((b.stop_time - capture_start).total_seconds() /
   tres)`, both clamped to `[0, rows]` (exact grid rows -- no effective-tres
   ambiguity). Build the same sidecar dict schema as `build_sidecar_payload`.
6. Write to `sidecar_path(sc16_path)` (overwrites the active detections sidecar).

This one helper is reused by both callers below. It is threshold-only work on the
stored grid: fast, no IQ reprocessing.

### Part 2 - Re-detect endpoint (`web/routes/captures.py`)

`POST /captures/redetect/{filename}` body `{threshold_high_db, threshold_low_ratio,
noise_floor_percentile, merge_time_ms, min_duration_ms}` (all optional, default to
the settings' `BURST_*`): resolve `<base>` via `_validate_filename`; 404 if the
`.sc16` or its `.psd` grid is missing. Build a `BurstDetectionConfig` from the body
(map `merge_time_ms`/`min_duration_ms` -> seconds; `merge_freq_bins` from settings)
and call `write_sidecar_from_grid`. Return the new payload. Works on any capture
with a grid (replay-recorded or field). This is the tuning mechanism.

### Part 3 - Record during replay (`pipeline/streaming.py` + replay endpoints)

- Add `self._replay_record = False` and a setter `set_replay_recording(bool)` on
  `StreamingProcessor`. Change the manual-record gate: `start_recording` proceeds
  when `not self._replay_mode or self._replay_record` (arm/trigger stay gated off;
  only explicit manual record is allowed during replay).
- In `_end_recording`'s deferred sidecar (`_deferred_sidecar`): when
  `self._replay_mode` (a replay recording; the DB has no replay detections),
  build the sidecar from the recorded grid at the current burst config
  (`write_sidecar_from_grid`) instead of `write_sidecar(sc16, db)`. Non-replay
  recordings are unchanged (DB path).
- Endpoint `POST /api/replay/record` body `{on: bool}`: when `on`, set
  `processor.set_replay_recording(True)` then `processor.start_recording()`; when
  `off`, `processor.stop_recording()` (which triggers `_end_recording` + the
  grid-based sidecar) and clear the flag. 409 if no replay is active. Returns the
  recording state.

### Part 4 - UI (`dashboard.html` + `captures.html`)

- **Replay banner** (`dashboard.html`): add a **Record / Stop-record** toggle that
  POSTs `/api/replay/record {on}`. Show a small recording indicator + the current
  recording bytes/duration from the existing heartbeat `recording` field (already
  broadcast). After stop, the new capture appears on the Captures page.
- **Capture viewer** (`captures.html`): add a **Re-detect** panel (burst-hi dB,
  low ratio, floor %, merge ms inputs prefilled from the capture's current
  sidecar/config + a button) -> `POST /captures/redetect/{filename}` -> reload the
  detections and refresh the overlay + Detections table in place. Reuse the
  existing overlay/table/click-to-scroll.

### Part 5 - Reuse (no new code)

Captures list/detail/PSD/detections endpoints, the `.psd` viewer, overlay,
Detections table, click-to-scroll, `.sc16` recording, `.psd` grid write, and
`detect_bursts` are all existing. The `.sc16` is copied once by the normal
recorder (accepted); re-detect never re-copies.

## Testing

- `tests/unit/test_detections_sidecar.py` (extend): `write_sidecar_from_grid` on a
  seeded `<base>.psd`/`.psd.json` (+ `.json`) with a synthetic burst: a high
  `threshold_high_db` yields fewer/narrower detections than a low one; the burst's
  `row_start/row_stop` land on its grid rows; schema matches `build_sidecar_payload`.
  Missing grid -> clear empty/error handled.
- `tests/unit/test_captures_redetect.py`: `POST /captures/redetect/{filename}` on a
  seeded grid capture rewrites the sidecar and returns the detections; 404 when the
  grid is absent; params map to `BurstDetectionConfig`.
- `tests/unit/test_streaming_replay_mode.py` (extend): with `replay_mode=True` and
  `set_replay_recording(True)`, `start_recording()` proceeds (state -> recording),
  while `arm_trigger()` stays inert; and the replay-record `_deferred_sidecar` uses
  the grid path (assert `write_sidecar_from_grid` is called, DB `insert_detection`
  still not).
- Route test for `POST /api/replay/record` (409 when idle; toggles state).
- UI: manual/browser -- record the SSM replay -> open the capture -> scrub bursts
  @0.2 ms -> re-detect at thr 40 vs 35, overlay updates. Local Jetson smoke; do NOT
  touch HCRO.
- Full CI per CLAUDE.md (ruff, ruff format, mypy, unit, integration@NATS:4222; note
  the CI lint job type-checks without numpy -- keep return types concrete).

## Files

- `src/rfobserver/storage/detections_sidecar.py` - `build_sidecar_from_grid` +
  `write_sidecar_from_grid` (the shared helper).
- `src/rfobserver/web/routes/captures.py` - `POST /captures/redetect/{filename}`.
- `src/rfobserver/pipeline/streaming.py` - `_replay_record` flag +
  `set_replay_recording`; gate change in `start_recording`; grid-based sidecar in
  `_deferred_sidecar` for replay recordings.
- `src/rfobserver/web/routes/api.py` - `POST /api/replay/record`.
- `src/rfobserver/web/templates/dashboard.html` - Record toggle in the replay banner.
- `src/rfobserver/web/templates/captures.html` - Re-detect panel in the viewer.
- Tests: extend `test_detections_sidecar.py`, `test_streaming_replay_mode.py`; add
  `test_captures_redetect.py` + the `/api/replay/record` route test.

## Out of scope

- A separate analyze/batch grid builder or `/api/analyze` (recording builds the grid).
- Multiple named detection sets + selector (re-detect overwrites the one sidecar;
  to compare, record/keep separate captures).
- Skipping the `.sc16` copy (accepted: normal recorder copies it once).
- Changing grid params (fft bins / time-res) per capture (that is a re-record).

## Notes / risks

- A replay recording copies the source IQ into `.sc16` once (~2 GB for the 20 s
  SSM file) plus the `.psd` grid (~800 MB at 2048 bins / 0.2 ms). Deliberate and
  user-triggered; watch the Jetson NVMe.
- Detection sets reflect the batch `detect_bursts` (whole-grid), matching
  `waterfall_plot.py` -- the live rolling detector is a separate path.
- Re-detect overwrites the capture's single detections sidecar; the previous
  detection result is not retained (re-run to change it back).
