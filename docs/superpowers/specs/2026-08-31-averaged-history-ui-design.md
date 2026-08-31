# Averaged-history UI: datetime range selector + waterfall + stats

- **Date:** 2026-08-31
- **Status:** Approved for planning
- **Scope of this cut:** a new `/averaged/` page that shows the historical averaged
  windows (stats + PSD) over a user-selected datetime range, with last-day /
  last-2-days / last-week presets, a captures-style selector line on the
  waterfall, and server-side time-bucket aggregation so a week fits in a ~1.2 MB
  payload. No live-view changes, no HCRO work.

## Problem

`avg_windows` now collects every DURATION_SEC-averaged window (stats forever,
PSD blobs for the last `DB_RETENTION_DAYS` days), and `/api/averaged*` exposes
individual windows. There is no way to *see* the historical averaged data over a
range: no start/stop datetime selection, no waterfall of the averaged PSD, no
stats-over-time plot.

At defaults (DURATION_SEC=0.5, 2048 bins) the table grows ~172,800 rows/day:
~1.4 GB/day of PSD blobs. A week is ~1.2M windows / ~9.9 GB of blobs. Any
"view the whole week" UI must compress hard: it cannot transfer or render
1.2M x 2048 values.

## Decisions (locked with the user on 2026-08-31)

- **Separate `/averaged/` page** (new nav link) that reuses the dashboard look;
  the live dashboard and its WebSocket wiring are untouched.
- **The waterfall is the compressed view**: a fixed-height canvas (~600 px,
  1 px per row) where each row is one *time bucket* of the selected range.
  The range selector is the zoom control (week -> ~17 min/bucket, day ->
  ~2.4 min/bucket, sub-hour -> individual windows).
- **Bucket PSD = per-bin mean** across the bucket's windows (same "averaged
  window" semantics; a bucket is just a longer average). `pwr_max` (max of
  window maxes) rides in the per-bucket stats, not the waterfall.
- **On-demand aggregation + in-memory LRU cache.** First load of a full week
  reads ~10 GB of blobs (~5-10 s on the Jetson NVMe, shown as a
  "Computing aggregate..." state); the three presets share cache keys, so
  repeat navigation is instant. A pipeline-maintained pre-aggregated table is a
  follow-up if real usage shows the read cost matters.
- **Binary delivery, no JSON fallback.** One-shot binary HTTP response
  (`application/octet-stream`): 16-byte header + 48-byte meta + float32 PSD
  rows + float64 per-bucket stats. ~1.2 MB for a week vs ~10 MB of JSON.
  (Not a WebSocket: the captures WS earns its complexity from progressive
  multi-request paging; here the response is computed once.)
- **Stats timeline is blob-independent** so it works after retention prunes
  PSD, including ranges older than `DB_RETENTION_DAYS`.
- **Frequency axis stays server-side metadata.** The axis is uniform
  (`freq_start_hz + i * freq_step_hz`); bin-downsampling keeps it uniform, so
  the response carries just the two scalars and the client rebuilds the axis.

## Data volume math

| Range | Windows | Raw PSD payload | Bucketed (600 rows x 512 bins) |
|---|---|---|---|
| Last day | ~173k | ~1.4 GB | ~1.2 MB |
| Last 2 days | ~345k | ~2.8 GB | ~1.2 MB |
| Last week | ~1.2M | ~9.9 GB | ~1.2 MB |

Bucket granularity: day -> ~144 s/row; week -> ~1008 s/row. The bin axis is
downsampled 2048 -> 512 via the reshape-mean already used by
`captures.py:_slice_psd` (the 920 px canvas cannot use more than 920 bins).

## Aggregation semantics

- Buckets form a fixed grid: `bucket_sec = (until - since) / max_rows`,
  bucket i covers `[since + i*bucket_sec, since + (i+1)*bucket_sec)`. Windows
  are bucketed by their `start_time`. Empty buckets (sensor off, or a gap in a
  sweep) render as dark rows with `count = 0`.
- Bucket PSD: mean of the per-bin means of the bucket's windows. Windows whose
  PSD blob was pruned by retention contribute **no PSD** (the bucket's PSD row
  is NaN-only -> dark row, hint "PSD pruned by retention") but **do** count
  toward the scalar stats.
- Bucket scalar stats: `pwr_avg`/`pwr_median`/`pwr_std`/`kurtosis` = mean of
  window values; `pwr_max` = max of window maxes (keeps bursts visible in the
  stats chart).
- Tuning: windows may span multiple SDR centers under a sweep. The waterfall
  requires an effective tuning (the page filters on it, defaulting to the most
  recent config); the frequency axis comes from the first window in the range.
  The scalar stats endpoint accepts "all" tunings (no axis needed).

## Binary response format (`GET /api/averaged/waterfall`)

All little-endian, `application/octet-stream`:

```
Header (16 bytes):  struct "<4i"
    magic           = 0x52464F42 ("RFOB")
    version         = 1
    bucket_count
    num_bins        (after 2048 -> max_bins downsampling)
Meta (48 bytes):    struct "<6d"
    bucket_sec      (seconds per row)
    min_db          (color-scale floor over all returned powers)
    max_db          (color-scale ceiling)
    total_windows   (windows with PSD in the range)
    freq_start_hz   (downsampled axis start)
    freq_step_hz    (downsampled axis step)
PSD rows:           bucket_count x num_bins float32, row-major;
                    NaN = empty or pruned bucket
Per-bucket stats:   bucket_count x struct "<7d"
    start_epoch     (unix seconds, float64)
    pwr_avg, pwr_max, pwr_median, pwr_std, kurtosis
    count           (windows in the bucket, float64)
```

The client rebuilds the axis as `freq_start_hz + i*freq_step_hz` for
`i in range(num_bins)` and maps a time to a row via
`floor((t - since) / bucket_sec)`.

## API (all under `api.py`; page route in a new `averaged.py`)

- `GET /api/averaged/waterfall` - required `since`/`until` (ISO 8601),
  optional `sdr_center`/`sample_rate`/`gain`/`max_rows` (default 600, clamp
  1..2000)/`max_bins` (default 512, clamp 2..2048). Binary body per above.
  Errors: 400 when `since >= until` or parsing fails; 503 when no DB.
- `GET /api/averaged/stats` - same params (+`max_points`, default 600), JSON
  `{"bucket_sec":..,"points":[{start_time,count,pwr_avg,pwr_max,pwr_median,
  pwr_std,kurtosis}],"min_pwr":..,"max_pwr":..}`. Reads only light columns, so
  it works over any range regardless of blob retention.
- `GET /api/averaged/configs` - JSON `{"configs":[{sdr_center_freq_hz,
  sample_rate_hz,gain_db}...],"latest":{...}|null}` from `avg_windows`
  (distinct tunings + the most recent) for the filter dropdown default.
- `GET /api/detections.json` - add optional `since`/`until` query params
  (already has tuning filters) so the range's detections load for the table and
  the waterfall overlay.
- `GET /averaged/` - the page (HTML template), mounted from a new
  `web/routes/averaged.py` (mirrors `history.py`).
- Waterfall responses are cached in an in-memory LRU (max ~8 entries) keyed by
  (since, until, tuning, max_rows, max_bins) in the web layer.

## Database layer (`SensorDatabase`)

- `query_avg_waterfall(*, since, until, sdr_center_freq=None, sample_rate=None,
  gain=None, max_rows=600, max_bins=512) -> dict` - pages through the range's
  windows (`fetchmany`), decodes each BLOB via `np.frombuffer(..., "<f4")`,
  bin-downsamples with the `_slice_psd` reshape-mean when `num_bins > max_bins`,
  accumulates per-bucket sums/counts, tracks the global min/max, and returns
  the header/meta/rows/stats arrays plus the downsampled axis scalars. Rows
  with a NULL blob contribute stats only. Returns arrays ready for the binary
  packer (never `np.ndarray.tolist()` on the PSD - keep it concrete for mypy).
- `query_avg_stats(*, since, until, sdr_center_freq=None, sample_rate=None,
  gain=None, max_points=600) -> dict` - same bucketing over the light columns
  only (no blob reads), so it is O(range) in rows and works after pruning.
- `avg_window_configs() -> dict` - distinct tuning configs + the latest.

## UI (`/averaged/`)

Reuses `shared-charts.js` (`powerToColor`, `renderWaterfallRow`, `drawPSD`)
and the captures.html interaction patterns, minus virtualization (the whole
range fits on screen, so the slider spans the full waterfall):

1. Toolbar card: `datetime-local` start/stop inputs, preset buttons
   (Last Day / Last 2 Days / Last Week), SDR tuning filter (populated from
   `/api/averaged/configs`, default = latest config), Apply, and a persistent
   hint "PSD retained N days; stats and detections kept indefinitely".
2. Stats timeline card: `pwr_avg` and `pwr_max` lines over the range (canvas,
   dashboard timeseries pattern), fed by `/api/averaged/stats`.
3. Waterfall card: fixed-height canvas (~600 px, 1 px/bucket) rendered from the
   binary payload; empty/pruned rows are dark. Slider + horizontal selector
   line (`drawHighlight` pattern) scoped to the full range; clicking a row
   selects it. Detection overlay boxes on top (`drawDetectionOverlay` math:
   time -> row via `bucket_sec`, frequency -> x via the rebuilt axis).
4. PSD card: `drawPSD` of the selected bucket's mean powers with crosshair
   tooltip; when the bucket has no PSD (empty/pruned) show a placeholder.
5. Selected-bucket stats card: count, span, pwr_avg/max/median/std/kurtosis.
6. Detections card: table (time/freq/BW/duration/peak) for the range via
   `/api/detections.json`.

New files: `templates/averaged.html`, `static/averaged.js` (all waterfall/PSD
rendering uses the shared functions), nav link added to `base.html`.

## Retention interaction

PSD blobs are nulled after `DB_RETENTION_DAYS` (7). "Last week" is therefore
the maximum range with PSD; older ranges still show the stats timeline and
detections (permanent data). The page surfaces this in the hint and per-bucket
"PSD pruned" placeholders. This is expected behavior, not an error.

## Testing

**Unit (`tests/unit/test_database.py`):**
- `query_avg_waterfall`: buckets windows by time (counts per bucket), mean PSD
  per bucket, `max_rows` cap, bin downsampling (num_bins > max_bins), pruned
  (NULL blob) windows counted in stats but absent from PSD, min/max scale,
  frequency axis scalars.
- `query_avg_stats`: scalar aggregation, pruned windows still counted, tuning
  filter, empty range.
- `avg_window_configs`: distinct configs + latest.

**Integration (`tests/integration/test_web_integration.py`):**
- `/api/averaged/waterfall` returns a parseable binary body (struct unpack),
  header/meta/rows/stats consistent with the seeded windows; 400 on bad range.
- `/api/averaged/stats` and `/api/averaged/configs` JSON shapes.
- `/api/detections.json` `since`/`until` scoping.
- `/averaged/` renders.

**Deployment (`nano-super`, no HCRO):** run the mock pipeline, open the page,
exercise Last Day / Last 2 Days / Last Week presets against real accrued rows,
confirm the selector line moves the PSD chart, and confirm a pruned (old)
bucket shows the dark/pruned state while the stats chart still renders.

## Out of scope (follow-up)

- Zoom-by-drag on the waterfall (the range selector is the zoom for now).
- Peak-hold (per-bin max) overlay on the waterfall.
- Pipeline-maintained pre-aggregated per-minute table.
- A history-page time-range viewer for *detections* (this cut is averaged
  windows; detections appear only as the range's table + overlay).
