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
- **The waterfall is the compressed view**: a fixed-height canvas (~600 px)
  where each row is one window (when few) or one time bucket (when many). The
  range selector is the zoom control (week -> ~17 min/row, day -> ~2.4
  min/row, sub-hour -> individual windows, no averaging).
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

## Binary response format v2 (`GET /api/averaged/waterfall`)

All little-endian, `application/octet-stream`:

```
Header (16 bytes):  struct "<4i"
    magic           = 0x52464F42 ("RFOB")
    version         = 2
    row_count       (windows in raw mode, buckets in aggregated mode)
    num_bins        (after 2048 -> max_bins downsampling)
Meta (48 bytes):    struct "<6d"
    bucket_sec      (span / max_rows; reference grid, also each bucket's span)
    min_db          (color-scale floor over all returned powers)
    max_db          (color-scale ceiling)
    total_windows   (windows with PSD in the range)
    freq_start_hz   (downsampled axis start)
    freq_step_hz    (downsampled axis step)
PSD rows:           row_count x num_bins float32, row-major;
                    NaN = no PSD (empty or pruned)
Per-row stats:      row_count x struct "<8d"
    start_epoch     (unix seconds, float64)
    duration_sec    (real window duration in raw mode; bucket_sec aggregated)
    count           (windows covered; 1 in raw mode)
    pwr_avg, pwr_max, pwr_median, pwr_std, kurtosis
```

The client rebuilds the axis as `freq_start_hz + i*freq_step_hz` for
`i in range(num_bins)` and renders each row at its own time span
`[start_epoch, start_epoch + duration_sec]` mapped onto the canvas by
`y(t) = floor((t - since) / span * height)`; gaps between rows stay dark. This
single time-positioned renderer handles both modes.

## Aggregation semantics (adaptive)

- The server **only averages when the data outnumbers what can be displayed**:
  - `window_count <= max_rows` -> **raw mode**: every window is its own row
    (its real `start_epoch`/`duration_sec`, its own PSD and stats). A 60 s
    range with ~100 windows renders ~100 stretched rows, not 600 sparse ones.
  - `window_count > max_rows` -> **aggregated mode**: `max_rows` time buckets
    of `bucket_sec = span / max_rows`; per-bin mean PSD, scalar stats
    mean-of-means except `pwr_max` = max-of-maxes (keeps transient bursts
    visible). Week view ~17 min/bucket, day ~2.4 min/bucket.
- A `COUNT(*)` over the range picks the mode before any blob reads.
- `query_avg_stats` follows the same rule (one point per window when the count
  fits `max_points`, else buckets) so the stats chart never averages short
  ranges either.
- Windows whose PSD blob was pruned by retention contribute stats but no PSD
  (their row is all-NaN, rendered dark).
- Tuning: windows may span multiple SDR centers under a sweep. The waterfall
  requires an effective tuning (the page filters on it, defaulting to the most
  recent config); the frequency axis comes from the first window in the range.
  The scalar stats endpoint accepts "all" tunings (no axis needed).

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
  gain=None, max_rows=600, max_bins=512) -> dict` - counts the range first and
  returns **raw mode** (one row per window: its `start_epoch`, `duration_sec`,
  own stats, own PSD) when the count fits `max_rows`, else pages through the
  windows (`fetchmany`), decodes each BLOB via `np.frombuffer(..., "<f4")`,
  bin-downsamples with the `_slice_psd` reshape-mean when `num_bins > max_bins`,
  accumulates per-bucket sums/counts, tracks the global min/max, and returns
  the header/meta/rows/stats arrays plus the downsampled axis scalars and a
  `mode` (0 raw / 1 aggregated). Rows with a NULL blob contribute stats only.
  Returns arrays ready for the binary packer (never `np.ndarray.tolist()` on
  the PSD - keep it concrete for mypy).
- `query_avg_stats(*, since, until, sdr_center_freq=None, sample_rate=None,
  gain=None, max_points=600) -> dict` - same adaptive rule over the light
  columns only (no blob reads), so it is O(range) in rows and works after
  pruning.
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
3. Waterfall card: fixed-height canvas (~600 px) rendered from the binary
   payload; each row is drawn at its own time span (windows stretch in raw
   mode, buckets tile 1 px in aggregated mode); empty/pruned rows and gaps are
   dark. Slider + horizontal selector line (`drawHighlight` pattern) scoped to
   the full range; clicking a pixel selects the row whose span contains it.
   Detection overlay boxes on top (`drawDetectionOverlay` math: time -> pixel
   via the span mapping, frequency -> x via the rebuilt axis).
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

## Addendum (2026-08-31, live/"Now" mode + usability pass)

Locked with the user after the first cut shipped:

- **Default is a Grafana-style "Now" mode**: the range end tracks the current
  time and the range is re-fetched every 2 s. The next poll is scheduled only
  after the previous load finishes, so a slow long-range aggregate never
  stacks overlapping requests. Polling pauses while the tab is hidden and
  resumes (with an immediate refresh) on return.
- **Default range is Last 15 minutes**, not Last Day — a short sliding window
  keeps per-poll blob reads small (the server-side LRU never hits on a sliding
  window, so day/week polls are full re-aggregations; acceptable at the
  load+2 s effective cadence but not something to default to).
- **Selection survives refreshes**: the selected row follows the newest
  window until the user clicks/drags to an older row, which is then kept by
  `start_epoch` across polls. Follow mode picks the newest bucket **with
  windows** — the grid's last bucket can still be empty while its span is
  only starting.
- **Axes added**: waterfall frequency labels (start/center/end, below the
  canvas) and time labels (5 ticks down the left edge, drawn on the overlay
  canvas), plus time ticks on the stats chart. Detections card header shows
  the in-range count.

## Addendum 2 (2026-08-31, Grafana-style one-line bar + time picker)

Reworked the range controls after the user shared Grafana screenshots:

- **One-line bar**: tuning selects (SDR center / sample rate / gain) on the
  left; a time-picker button showing the current range label, a refresh
  button, and the Now toggle on the right. No always-visible datetime inputs.
- **The time-picker button opens a dropdown panel** (Grafana layout): left
  column = "Absolute time range" with From/To `datetime-local` inputs and
  "Apply time range" (fixed range, Now off, label becomes
  `MM/DD HH:MM → MM/DD HH:MM`); right column = quick ranges 5m/15m/30m/1h/
  3h/6h/12h/24h/2d/7d (sliding windows, Now on, label = "Last …"). The
  retention hint moved into the panel. Closes on selection, outside click,
  or Escape; opening snapshots the current window into the inputs (polls
  never clobber edits).
- Quick-range clicks while already live reload immediately on the new span;
  the button label updates in the same handler (a stale-label bug the
  puppeteer test caught).
- The dropdown escapes its card via `overflow: visible` on the range card
  (cards are `overflow: hidden` for header-radius clipping) and an explicit
  `[hidden] { display: none }` rule, since `display: flex` on the panel
  would otherwise override the `hidden` attribute.

Puppeteer coverage (`tests/ui/puppeteer_avg_history.js`) asserts: Now on by
default with the "Last 15 minutes" label, the Updated clock advancing across
polls, picker open/close (button, outside click), populated snapshot inputs,
active quick-range highlight, absolute Apply flipping the label to the
absolute form and Now off, quick ranges re-enabling Now with the expected
bucket granularity, live follow selecting a non-empty bucket, plus the
waterfall/PSD pixel, axis-label, slider, and click-selection checks.

## Addendum 3 (2026-08-31, loading spinner + full-width Grafana layout + rotated waterfall)

After the user shared a full-dashboard Grafana screenshot:

- **Stale indicator**: any user action that changes the range or tuning
  (quick range, absolute Apply, refresh, Now on, tuning select) sets a
  `stale` flag: a spinning circle appears next to the range button and the
  three chart panels dim (`.avg-panel.stale`) until the load finishes. The
  flag clears on success; a failed load clears it only when Now is off (the
  error goes to the status line), so live mode keeps spinning while it
  retries. The 2 s background polls never set it, so live updates don't
  flash. The spinner uses `visibility` (space always reserved) so the bar
  doesn't shift.
- **Full-width page**: the averaged page opts out of the 960 px reading
  column via `.content:has(.avg-picker-card) { max-width: none }`. Charts
  size to their cards (CSS `clamp()` heights: stats/PSD ~26vh, waterfall
  ~52vh); `fitCanvases()` matches each canvas's backing store to the
  displayed size on boot and on window resize (debounced 150 ms) and
  re-renders. Time-axis tick density adapts to width (W/200, clamped 4-10).
- **Waterfall rotated 90°**: X is now TIME with exactly the stats chart's
  span and pixel width, so the two are time-correlated (a gap or event lines
  up vertically); Y is frequency, low at the bottom (PSD orientation). The
  selector is a vertical band+line; clicking a time column selects it.
  Frequency labels (max/mid/min) are pill-drawn on the overlay's left edge,
  time ticks along the bottom; the old HTML freq-axis strip is gone. Axis
  labels gain a date prefix for day-and-longer spans and seconds for
  sub-10-minute spans. Empty buckets (`count == 0`) are left dark so data
  gaps read as gaps instead of noise-floor blue. Detection markers are
  vertical lines spanning the band at reduced alpha (0.45) so dense clusters
  brighten without a solid smear.
- **Tuning selects apply immediately** (change → spinner + reload);
  previously they only took effect on the next range change.

Puppeteer coverage grew: spinner off after first load, full-width assertion
(`max-width: none`, waterfall > 1000 px at 1440 viewport), rotation
assertion (canvas wider than tall), stats/waterfall width equality (shared
time axis), overlay opacity sample (canvas-drawn axis labels), click
selecting an *earlier* time column, spinner-on-then-off (with dimmed panels)
around absolute Apply, each quick range, and a tuning-select change.
