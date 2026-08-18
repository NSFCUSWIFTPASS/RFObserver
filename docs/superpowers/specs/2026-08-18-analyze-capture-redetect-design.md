# Analyze a capture to a re-detectable, full-resolution capture (threshold tuning)

## Goal

Turn a raw IQ file (e.g. the December SSM/FHSS `.dat`) into a viewable capture
whose PSD spectrogram is stored at full internal resolution, so each burst can be
inspected at ~0.2 ms/row in the existing Captures viewer, and whose detections can
be re-computed on that stored grid at different thresholds without reprocessing the
IQ. This gives a fast tune-and-inspect loop: the expensive spectrogram is built
once; re-tuning is a cheap detection pass, and multiple labeled detection sets can
be compared on the same spectrogram.

## Background (from code audit)

- **Recorded `.psd` grids are full resolution.** Recording appends every PSD row
  at `PSD_TIME_RESOLUTION_MS` (default 0.2 ms -> 5000 rows/s), written to
  `<base>.psd` (float32 memmap) + `<base>.psd.json` (`rows`, `num_bins`,
  `time_resolution_s`, `freq_axis`, `grid_min/max`, `center_freq_hz`,
  `bandwidth_hz`). The Captures viewer renders this 1px/row (virtualized, paged)
  with the detection overlay + click-to-scroll (built in the
  2026-08-09 capture-detections-overlay feature).
- **The live/replay waterfall is much coarser** (normal = `DURATION_SEC`-averaged;
  high-res = per-dwell-chunk into a bounded display buffer), so it is unsuitable
  for per-burst inspection. That is why we persist a grid instead.
- **The batch detector runs on a whole grid.**
  `processing/burst.py::detect_bursts(psd_grid: PSDGridResult, config:
  BurstDetectionConfig | None, center_freq_hz: float, capture_time)` returns a
  `BurstDetectionResult`. `BurstDetectionConfig` fields: `threshold_high_db`,
  `threshold_low_ratio` (T_L = T_H*ratio), `noise_floor_percentile` (default 50 =
  median), `min_duration_sec`, `merge_freq_bins`, `merge_time_sec`. This is the
  same whole-grid method the validated `gr-modules/iq-processing/waterfall_plot.py`
  reference uses; `--burst-threshold 40` there == `threshold_high_db=40`.
- **Grid IO.** `storage/psd_grid.py::grid_paths(sc16_path)` -> `(<base>.psd,
  <base>.psd.json)`; a reader memmaps the raw grid + parses meta. A writer for a
  full grid is needed (mirror the recording write path).
- **Captures list globs `*.sc16`** (`web/routes/captures.py::captures_list`), and
  the detail/PSD/detections endpoints resolve `<base>` from the `.sc16` name.
- **Raw-file plumbing exists** from the replay feature: `sigmf_reader.load_raw`,
  `parse_capture_filename`, the `REPLAY_SOURCE_DIR` allowlist +
  `_resolve_replay_path`.
- **Detections sidecar schema** (`storage/detections_sidecar.py`):
  `{capture_start_time, time_resolution_s, center_freq_hz, sample_rate_hz,
  gain_db, detections: [{start_time, stop_time, center_freq_hz, bandwidth_hz,
  peak_freq_hz, peak_power_db, duration_ms, row_start, row_stop}]}`.

## Decisions (from brainstorming)

1. **Analyze-to-capture, one-shot, unpaced** (not a live-replay recording).
2. Detection params come from **explicit fields on the Analyze panel**, stamped
   into the output; grid params (`num_fft_bins`, `time_resolution_ms`) in an
   advanced area.
3. **Shared grid + swappable detection sets**: build the spectrogram once; re-tune
   with a cheap detection pass on the stored grid.
4. **Multiple named detection sets per capture + a selector** in the viewer.
5. **`<base>.sc16` is a symlink to the source `.dat`** (zero copy); the list and
   viewer work unchanged.

## Design

### Part 1 - Grid + analyze builder (`storage/analyze.py`, new)

`async def analyze_capture(source_path, *, sample_rate_hz, center_freq_hz,
gain_db, datatype, num_fft_bins, time_resolution_ms, detection: BurstDetectionConfig,
label: str, storage: LocalStorage) -> str` (returns the capture base name):

- **Capture identity = (source basename, num_fft_bins, time_resolution_ms).** Base
  name is a deterministic slug, e.g. `<sourceslug>__fft<N>_tres<ms>` (sanitized).
  Same grid params -> same base -> the grid is reused; only a detection set is
  (re)written. Different grid params -> a new capture.
- If the grid does not exist yet:
  1. `cap = load_raw(source_path, datatype=datatype, sample_rate_hz=,
     center_freq_hz=)` (memmap).
  2. Compute the PSD grid over the whole file at `(num_fft_bins,
     time_resolution_ms)`: chunk the memmap, per `slice = sample_rate *
     tres/1000` samples take an `num_fft_bins`-point windowed FFT ->
     power-dB row; assemble rows. Reuse the pipeline's PSD compute
     (`processing/spectral`) so the spectrogram matches the live/replay path.
     Stream rows to `<base>.psd` (bounded RAM, like disk-mode recording).
  3. Write `<base>.psd.json` (`rows`, `num_bins`, `time_resolution_s =
     time_resolution_ms/1000`, `freq_axis`, `grid_min/max`, `center_freq_hz`,
     `bandwidth_hz = sample_rate_hz`).
  4. Write `<base>.json` capture meta: `file`, `format: "sc16"`, `sample_rate_hz`,
     `center_freq_hz`, `bandwidth_hz`, `gain_db`, `start_time` (from the source
     filename's timestamp if parseable, else file mtime, UTC iso),
     `duration_sec = rows * time_resolution_s`, `source_path`, `analyzed: true`,
     `num_fft_bins`, `time_resolution_ms`.
  5. Symlink `<base>.sc16 -> source_path` (a raw ci16_le `.dat` is byte-compatible
     with `.sc16`; if a symlink already exists, leave it).
- Always: run `redetect(base, detection, label, storage)` (Part 2) to write the
  detection set for this analyze call.

### Part 2 - Re-detect on the stored grid (`storage/analyze.py`)

`async def redetect(base, detection: BurstDetectionConfig, label, storage) ->
dict`:
- Load the stored grid via `psd_grid` reader -> ndarray `(rows, num_bins)` +
  `freq_axis` + `time_resolution_s`; wrap in a `PSDGridResult` (grid, freq_axis,
  `time_axis = arange(rows)*tres`, `time_resolution`).
- `result = detect_bursts(psd_grid, detection, center_freq_hz)`.
- Map each burst to grid rows directly from its time-axis indices (row_start/
  row_stop are exact grid rows - no time-based mapping, so none of the
  effective-tres ambiguity of DB-sourced sidecars). Build the sidecar detections
  list (same schema + `peak_freq_hz` from `detect_bursts`).
- Write the set to `<base>.detset.<labelslug>.json` with an extra
  `{"label": ..., "params": {threshold_high_db, threshold_low_ratio,
  noise_floor_percentile, merge_time_ms, merge_freq_bins, min_duration_ms}}`
  block, and copy it to `<base>.detections.json` (the "active" set, so the
  existing `GET /captures/detections/{filename}` and any tooling keep working).
- Label defaults to a param slug, e.g. `thr40_floor50_merge30`, when not provided.

### Part 3 - Detection-set API (`web/routes/captures.py`)

- `GET /captures/detection-sets/{filename}` -> `[{label, params, count, active}]`
  (glob `<base>.detset.*.json`).
- Extend `GET /captures/detections/{filename}` with optional `?set=<label>`:
  return that set's file; default (no `set`) returns `<base>.detections.json` as
  today.

### Part 4 - Endpoints (`web/routes/api.py` or a small `analyze` router)

- `POST /api/analyze` body `{path, sample_rate_hz, center_freq_hz, gain_db,
  datatype, num_fft_bins, time_resolution_ms, threshold_high_db,
  threshold_low_ratio, noise_floor_percentile, merge_time_ms, min_duration_ms,
  label}`. Resolve `path` via `_resolve_replay_path` (allowlist reused). Build a
  `BurstDetectionConfig` from the detection params, call `analyze_capture`, return
  `{capture: <base>.sc16, set: <label>}`. `sample_rate_hz <= 0` -> 400 (as replay).
- `POST /captures/redetect/{filename}` body `{threshold_high_db, ...,
  label}`: resolve `<base>`, call `redetect`, return the set. 404 if the capture
  or its grid is missing.

### Part 5 - UI (`web/templates/captures.html`)

- Rename the existing raw-file panel to **"Analyze a raw file"**: keep the path
  input + auto-parsed tuning (center/rate/gain/datatype via
  `parse_capture_filename`), add detection-param inputs (burst-hi dB, low ratio,
  floor %, merge ms), an advanced `<details>` area (fft bins default 2048,
  time-res ms default 0.2), and a label. **Analyze** -> `POST /api/analyze` ->
  select + open the returned capture.
- Capture viewer: add a **re-detect panel** (the detection-param inputs + label +
  **Re-detect**) that `POST /captures/redetect/{filename}`, then reloads the
  detection set and refreshes the overlay + Detections table in place; and a
  **detection-set `<select>`** populated from `GET /captures/detection-sets` that,
  on change, loads `GET /captures/detections/{filename}?set=<label>` and re-draws
  the overlay + table. Reuse the existing overlay/table/click-to-scroll.

### Part 6 - Grid-only captures via symlink

`<base>.sc16` is a symlink to the source `.dat`, so `captures_list` (globs
`*.sc16`), detail, PSD, and detections endpoints work unchanged. If the symlink is
broken (source moved), the viewer still renders from `<base>.psd`; the list shows
the entry (guard the `.sc16.stat()` / size read so a broken link does not 500).

## Testing

- `tests/unit/test_analyze.py`: `analyze_capture` on a small synthetic raw file
  writes `<base>.psd`/`.psd.json`/`.json` + the `.sc16` symlink + an initial
  detection set; `rows == round(duration/tres)`; grid reused when called again
  with the same grid params (grid file mtime unchanged, new detection set added).
- `redetect` on a stored grid with a synthetic burst: a high `threshold_high_db`
  yields fewer/narrower detections than a low one; `row_start/row_stop` land on
  the burst's grid rows; the `params` block is stamped.
- `test_captures_detection_sets`: `GET /captures/detection-sets` lists sets;
  `GET /captures/detections?set=` returns the chosen set; unknown set -> 404 or
  falls back with a clear response.
- `POST /api/analyze` (allowlist reused; 400 on bad rate) and
  `POST /captures/redetect` route tests with a seeded grid.
- UI: manual browser verification (analyze the SSM `.dat`, scrub bursts at 0.2 ms,
  re-detect at thr 40 vs 35, switch sets). Local Jetson smoke; do NOT touch HCRO.
- Full CI per CLAUDE.md (ruff, ruff format, mypy, unit, integration@NATS:4222).

## Files

- `src/rfobserver/storage/analyze.py` (new) - `analyze_capture`, `redetect`,
  grid builder, set IO.
- `src/rfobserver/storage/psd_grid.py` - add a full-grid writer if not reusable
  from the recording path.
- `src/rfobserver/web/routes/api.py` (or new `web/routes/analyze.py`) -
  `/api/analyze`, `/captures/redetect/{filename}`.
- `src/rfobserver/web/routes/captures.py` - `/captures/detection-sets`,
  `?set=` on `/captures/detections`, broken-symlink guard in the list.
- `src/rfobserver/web/templates/captures.html` - Analyze panel + viewer re-detect
  panel + detection-set selector.
- Tests: `test_analyze.py`, `test_captures_detection_sets.py`, route tests.

## Out of scope

- Live-replay recording (superseded by analyze-to-capture for inspection).
- Editing/deleting detection sets from the UI beyond creating them (a set is
  overwritten when re-detected with the same label).
- Re-tuning grid params in the viewer (changing fft/tres makes a new capture via
  Analyze, by design).
- Copying the IQ into the capture (symlink only).

## Notes / risks

- A full grid is ~`rows * num_fft_bins * 4` bytes (~800 MB for 20 s at 2048 bins /
  0.2 ms). One grid per (source, grid params); detection sets are tiny. Watch the
  Jetson NVMe; document that lowering `time_resolution_ms` resolution or bins
  shrinks it.
- Analyze is CPU-bound (whole-file FFTs) but unpaced, so seconds-to-a-minute for
  the 20 s file; runs on the server host (may be the Jetson).
- `detect_bursts` (batch, whole-grid) is the reference method; the live rolling
  detector is a separate path. Detection sets reflect the batch detector, matching
  `waterfall_plot.py`. See [[reference_ssm_waterfall_plot_params]] for the
  validated params (`threshold_high_db=40`, median floor, merge 30 ms).
