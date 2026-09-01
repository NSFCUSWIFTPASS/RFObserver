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

## Addendum 4 (2026-08-31, two-column correlated layout + one-line title bar)

- **Two-column chart grid** (`.avg-grid`, explicit `grid-template-areas`):
  left column = Power Over Time + waterfall (time-correlated as before),
  right column = PSD at Selected Time + Kurtosis Over Time. Both columns are
  `1fr`, so all four charts share the same pixel width and every time axis
  aligns. Below 900 px the grid stacks (power, waterfall, PSD, kurtosis).
  The detections card stays full-width below. The page also gained real
  16 px gaps (the cards were previously flush, separated only by shadow).
- **Kurtosis chart**: same stats-points source as the power chart
  (blob-independent, works beyond PSD retention), orange trace matching the
  dashboard's kurtosis color (#ff9f0a), auto-scaled Y with 0.5 minimum pad.
  The card uses flex fill so its canvas stretches to the waterfall row's
  height instead of leaving whitespace.
- **Selection marker everywhere**: the selected bucket's start time is drawn
  as a vertical white line on the power and kurtosis charts too
  (`drawSelectionMarker`), and `selectRow` re-renders all three time charts,
  so clicking a waterfall column or dragging the slider moves the marker
  across every panel.
- **One-line title bar**: the big `page-header` block is gone; "Averaged
  History" is a compact inline title at the left of the control bar, next to
  the tuning selects. The "Updated …" timestamp moved from the status row
  into the bar's right end. The status row now holds only the
  windows/buckets status text.

Puppeteer coverage grew: title/Updated in the bar (and no `.page-header`),
half-column waterfall width, equal power/waterfall and PSD/kurtosis widths,
kurtosis fill height, kurtosis trace pixels, and selection-marker tracking —
a pixel scan finds the white marker column on the power/kurtosis charts and
asserts its X fraction tracks follow-latest (~1.0) and a waterfall click at
25 % width (~0.25 ± 0.06).

## Addendum 5 (2026-09-01, manual display scale, DB-persisted)

- **Per-chart Scale inputs in each panel header** (revised the same day from
  a single dropdown in the control bar): every chart header — Power, PSD,
  Waterfall, Kurtosis — carries its own `Scale: [low] [high]` number inputs
  (right-aligned in the `.card-header`). Empty input = that bound
  auto-scales from the data. A `change` (Enter/blur) validates the whole set
  (numbers, low < high per pair; invalid inputs get a red border +
  tooltip), saves immediately, and re-renders; clearing both bounds returns
  the chart to auto. The PSD pair (`psd_lo/psd_hi`) is independent of the
  waterfall pair (`wf_lo/wf_hi`) — both auto-scale from the same waterfall
  meta range by default, so the correlation holds until overridden.
- **Persisted server-side in the SQLite config table** (the same key/value
  store the pipeline config uses): one JSON document under the `ui_prefs`
  key, exposed via `GET/PUT /api/ui-prefs`. PUT validates keys
  (wf_lo/wf_hi/psd_lo/psd_hi/pwr_lo/pwr_hi/kurt_lo/kurt_hi), rejects
  non-finite/bool values and inverted pairs with 400, and replaces the
  whole document. Server-side storage means the scale is shared by every
  browser viewing the instance and survives restarts. (Rejected
  alternative: localStorage, which is per-browser.)
- **Rendering**: manual bounds override per side — waterfall colors
  (`wfRange()`), PSD Y (`psdRange()`), power and kurtosis Y
  (`chartRange()`, which never lets the range invert against the data). The
  waterfall legend shows the effective bounds. Values outside the range
  clamp at the chart edges.
- **Bug found by this round's puppeteer run**: `drawSelectionMarker` could
  land off-canvas when the selected bucket started in the last half-pixel
  of the time axis (`Math.round(x) + 0.5 == W`), making the marker
  invisible in raw mode with a fresh database. Now clamped to the last
  pixel column. The marker-tracking assertions were also made
  data-density-proof: they compare against the selected bucket's time, not
  the click position (a click in a data gap selects the nearest earlier
  row).

Puppeteer coverage grew: default Auto state, panel open, Apply → legend
shows the manual bounds, persistence across a full page reload, power-trace
re-scale (pixel-count drop with an out-of-data range), inverted-bounds
rejection (panel stays open with an error), and reset to Auto. Python
integration tests cover the endpoint roundtrip (including the config-table
write and full-replace semantics) and the 400 validations.

## Addendum 6 (2026-09-01, absolute-anchored bucket grid)

Live "Now" ranges slide every poll, but aggregated buckets were anchored to
the range's `since`: `bucket_sec = span / max_rows`, bucket *i* covering
`[since + i*bucket_sec, ...)`. Each poll shifted the whole grid, so a narrow
peak kept changing buckets — sharing its bucket with a varying set of noise
windows, or straddling a boundary one poll and sitting centered the next —
and visibly flickered in and out on the waterfall and power/kurtosis
timelines (most noticeable on the 3 h+ presets where buckets are wide).

Both aggregations (`_waterfall_aggregated`, `_stats_aggregated`) now anchor
the grid to absolute epoch multiples of `bucket_sec` (grafana-style). A peak
stays in the same absolute bucket until it ages out of the range; only the
partial edge buckets change membership as the window slides. The grid is
`max_rows` or `max_rows + 1` buckets (edge buckets partial); the client is
unchanged because it already maps every bucket by its absolute
`start_epoch`. Regression tests: the same sliding range queried at two poll
times yields identical boundaries and an identical peak bucket (waterfall
and stats).

## Addendum 7 (2026-09-01, single power trace + drag-to-zoom)

The power-over-time chart drew two traces (window **avg** in blue, window
**max** in red). Two curves read as two quantities; the chart now draws the
avg trace only (the per-bucket max is still in the bucket-stats row). The
auto-scale range follows the avg values.

Grafana-style **drag-to-zoom** on the time-domain charts (power, kurtosis,
waterfall): pressing the mouse and dragging a horizontal band paints a
selection rect, and on release the whole page zooms to that absolute range —
`activePreset` clears, Now turns off, and the range label flips to the
absolute `from → to` form. Zoom chains compose (each zoom re-anchors on the
current range); a quick range or the Now button returns to live polling. A
sub-8 px drag is treated as a plain click, preserving the waterfall's
click-to-select-a-time-column behavior; zooms under 5 s are ignored. The
band is painted on the chart itself for the line charts and on the
transparent overlay canvas for the waterfall (`renderWfOverlay` redraws the
detections + axes under it without re-rendering the data pixels). The PSD
chart is frequency-domain and is excluded. Puppeteer coverage: the zoomed
span matches the dragged fraction, Now turns off, a plain waterfall click
still selects, and the power chart has a single trace.

## Addendum 8 (2026-09-01, range back/forward history)

`<` and `>` buttons flank the time-range button and walk an undo/redo stack
of range selections, so a drag-zoom (or any range change) can be stepped
back out of and redone. Every user range change — quick range, absolute
Apply, drag-zoom, Now — pushes a snapshot of the previous range
(`sinceMs/untilMs/spanMs/live/activePreset`) onto the back stack and clears
the forward stack; the buttons pop one stack onto the other and are
disabled when their stack is empty. Restoring a live snapshot re-anchors
the sliding window on now (setLive/pollTick); restoring an absolute
snapshot reapplies its exact frozen window. Stack depth is capped at 50.
Tuning-select changes and manual refreshes reload but do not push history.
Puppeteer coverage: buttons start disabled, a zoom enables back, back
restores the live "Last 15 minutes" window with Now on, forward redoes the
exact zoomed absolute window with Now off.

## Addendum 9 (2026-09-01, landing Dashboard rename + color themes)

The averaged-history page is now the landing **Dashboard**: it is served at
`/` (the legacy `/averaged/` URL keeps working, same handler) and leads the
navbar. The former dashboard (live spectrogram) moved to `/live/` and is
labeled **Live**, second in the navbar. File names were deliberately kept —
`averaged.js`/`averaged.html`/`averaged.py` match the `/api/averaged/*`
endpoints and `avg_windows` tables, and `dashboard.*` stays the live view's
group; renaming files would diverge them from the API/DB naming for no
clarity gain.

**Color theme (Auto/Light/Dark, default Auto).** A picker at the right end
of the navbar selects the theme. The choice lives in the same `ui_prefs`
config document as the chart scales (`{"scale": {...}, "theme": "..."}`) —
`PUT /api/ui-prefs` now *merges* the provided keys instead of replacing the
document, so a theme change keeps the stored scale and vice versa (unknown
theme values are rejected with 400). Page routes resolve the stored theme
per render (`web/uiprefs.py::ui_theme`) and stamp it into
`<html data-theme>` plus the picker's `selected` option, so the first paint
is already correct with no client-side flash. `theme.js` applies a change
instantly via the attribute and PUTs it for persistence. CSS keys off the
attribute: `:root` holds the light variables, `:root[data-theme="dark"]`
pins the dark set, and a `prefers-color-scheme: dark` media query applies
the same set to `data-theme="auto"`; `color-scheme` is set per theme so
native inputs/selects/scrollbars match. The chart canvases keep their dark
panel look in both themes (Grafana-style), so no chart JS is theme-aware.
Puppeteer coverage: navbar order/labels, `/` landing page, `/live/` title,
picker default, immediate apply, persistence across reload, scale doc
preserved, Auto resolving to the OS theme.
