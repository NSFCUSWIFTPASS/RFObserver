# Dashboard peak finder design

Date: 2026-09-21.

**Goal:** let an operator jump straight to the strongest events in recent
history, without scrolling a month of waterfall. A new control on the Dashboard
toolbar finds the top N power peaks over a chosen lookback and opens a window
of a chosen width centred on each one.

**Architecture:** a per-minute rollup table summarises `avg_windows` so the
search is cheap at any lookback; a new read-only endpoint ranks rollup rows and
applies a separation rule; the Dashboard treats a chosen peak as an ordinary
absolute time range, so the existing render path is untouched.

**Tech stack:** SQLite (aiosqlite), FastAPI, vanilla JS (`averaged.js`).

## Global constraints

- Python >= 3.10 clean. The Jetsons run 3.10; CI covers 3.10, 3.11 and 3.12.
- No emojis and no em-dashes in code, UI strings, comments or docs.
- The lint CI job installs only ruff, mypy, pydantic and pydantic-settings, so
  any subclass of a third-party class needs `# type: ignore[misc,unused-ignore]`.
- Heavy read queries go through the read-only DB connection and a semaphore, and
  return 499 when the client disconnects, matching the existing Dashboard
  endpoints.
- The peak search must not add work to the per-window insert path, which runs at
  about 2 inserts per second on the field sensor.

## Non-goals

- No new view mode. The Dashboard never renders a discontinuous time axis.
- No age-based IQ pruning, no free-space handling, no row-level retention for
  `avg_windows`. Those belong to the storage budgeting project (see
  Assumptions).
- No change to how peaks are detected in the signal sense. This ranks stored
  per-window statistics; it is not a detector.

## UI

The control sits at the left of the existing `.avg-time` toolbar, before the
back arrow and the range button.

```
idle     [ Peaks v ] [<] [ Last 15 minutes v ] [>] [refresh] [Now]
active   [< Peak 3/10 >] [<] [ Sep 19 02:59 - 03:29 v ] [>] [refresh] [Now]
```

Clicking `Peaks` opens a popover built like the existing `#avg-picker`:

```
Find peaks
  Look back   [3 days] [7 days] [2 weeks] [1 month]
  Window      [15 min] [30 min] [1 hour] [3 hours]
  Show top    [5] [10] [15] [20]
  Rank by     [Peak power] [Above noise] [Band average]
  ------------------------------------------------
   1  Sep 19  03:14:22   -22.4 dBm  ########
   2  Sep 17  21:02:07   -28.1 dBm  #####
   3  Sep 20  11:47:55   -30.6 dBm  ####
   4  Sep 12  06:33:10   -31.2 dBm  ####    stats only
  ------------------------------------------------
  searched 7 days in 0.04 s
```

Defaults: lookback 7 days, window 30 min, count 10, metric Peak power.

Behaviour:

- The popover searches on open and on any control change, showing a spinner
  inside itself. The Dashboard behind it is not disturbed, so a slow search
  never blanks the view being looked at.
- Picking a row sets the range to `peak_time +/- window/2`, exits Now mode,
  pushes the previous range onto the existing back stack, closes the popover,
  and loads through the normal path.
- The arrows step to the adjacent peak using the client-cached list, with no
  refetch.
- Peak mode clears when a preset is clicked, a custom range is applied, or Now
  is pressed, because the range no longer corresponds to a peak.
- A peak older than the PSD retention cutoff is tagged `stats only`: its stats
  plot will render but its waterfall will be empty, because
  `prune_avg_psd_blobs` has nulled those blobs. The response carries the cutoff
  so the tag appears before the row is clicked.
- If the rollup backfill has not yet reached the start of the chosen lookback,
  the footer says how far back the search actually covered.
- Window widths reuse the existing preset keys (`15m`, `30m`, `1h`, `3h`), so
  the range label and the "min/row" status line need no new cases.

## Data model

New table, added to the module-level `SCHEMA` string that `connect()` applies
with `executescript`, so it is created alongside the others on any existing
database without a migration step:

```sql
CREATE TABLE IF NOT EXISTS avg_minutes (
    minute_start        TEXT NOT NULL,   -- 'YYYY-MM-DDTHH:MM', UTC
    sdr_center_freq_hz  REAL NOT NULL,
    n                   INTEGER NOT NULL,
    sample_rate_hz      REAL,
    gain_db             REAL,
    pwr_max             REAL,            -- max(pwr_max) over the minute
    pwr_snr             REAL,            -- max(pwr_max - pwr_median)
    pwr_avg             REAL,            -- max(pwr_avg)
    peak_max_time       TEXT,            -- start_time of the pwr_max winner
    peak_snr_time       TEXT,
    peak_avg_time       TEXT,
    PRIMARY KEY (minute_start, sdr_center_freq_hz)
);
```

`minute_start` is the first 16 characters of the window's `start_time`, so it
sorts lexicographically exactly as `avg_windows.start_time` does and needs no
date parsing to range-filter.

`sample_rate_hz` and `gain_db` hold the winning window's values and are used
only to answer the Dashboard's sample-rate and gain filters. This is an
approximation: those two do not vary within a minute on a real sensor, and any
imprecision affects only which minutes are listed, never what is displayed,
because clicking a peak issues an ordinary exact query against `avg_windows`.

Size at 2 rows/s: about 1.05M rows over two years, roughly 250 MB including the
primary key. It is deleted alongside the `avg_windows` rows it summarises, once
row-level retention exists.

## Rollup maintenance

A task in `pipeline/app.py`, started next to `_cleanup_loop`, running every
`PEAKS_ROLLUP_INTERVAL_SEC` (default 60.0; 0 disables it).

- State lives in the existing `config` key-value table (`get_config` /
  `set_config`, `database.py:1383-1400`): `rollup_newest` (the last fully closed
  minute rolled forward) and `rollup_oldest` (how far back the backfill has
  reached). Storing it in the DB rather than in memory is what makes the
  backfill resumable across restarts.
- **Forward pass:** read the light columns for windows between `rollup_newest`
  and the last closed minute, group by minute and centre frequency in Python,
  compute the three maxima and their argmax timestamps, and upsert. About 120
  rows per tick.
- **Backfill pass:** when `rollup_oldest` is later than the oldest window, walk
  backwards a chunk at a time through the existing keyset pager, upserting as it
  goes and advancing `rollup_oldest` after each chunk. Newest-first so recent
  history is searchable immediately. Resumable across restarts; yields between
  chunks so it never pins the WAL or starves the pipeline.
- Upserts are idempotent (`ON CONFLICT ... DO UPDATE` taking the larger value
  and its timestamp), so a re-run over an already-rolled range is harmless and
  a late-arriving window can only raise a maximum.

## API

`GET /api/averaged/peaks`

| param | type | notes |
|---|---|---|
| `since`, `until` | ISO datetime | required, validated by the existing `_parse_range` |
| `window_sec` | int | one of 900, 1800, 3600, 10800 |
| `count` | int | 1 to 20 |
| `metric` | str | `pwr_max`, `pwr_snr` or `pwr_avg` |
| `sdr_center`, `sample_rate`, `gain` | float | optional, same filters as the other endpoints |

Response:

```json
{
  "metric": "pwr_max",
  "window_sec": 1800,
  "peaks": [
    {"rank": 1, "peak_time": "2026-09-19T03:14:22.108+00:00",
     "since": "2026-09-19T02:59:22.108+00:00",
     "until": "2026-09-19T03:29:22.108+00:00",
     "value": -22.4, "pwr_max": -22.4, "pwr_snr": 29.6, "pwr_avg": -48.1,
     "psd_available": true}
  ],
  "covered_since": "2026-08-22T00:00+00:00",
  "psd_cutoff": "2026-08-22T11:04:00+00:00",
  "truncated": false
}
```

`covered_since` is the later of the requested `since` and `rollup_oldest`.
`truncated` is true when the separation rule ran out of candidates before
reaching `count`, which means one long event dominated the range.

Gated by a semaphore and returning 499 on client disconnect, like the waterfall
and stats endpoints. Cached in a small LRU keyed on the parameters with `until`
quantised to the minute, so reopening the popover is free.

## Selection algorithm

```
rows = SELECT minute_start, <metric> AS v, <peak time col>, pwr_max, pwr_snr, pwr_avg
         FROM avg_minutes
        WHERE minute_start >= ? AND minute_start < ?   -- + tuning filters
          AND v IS NOT NULL
        ORDER BY v DESC
        LIMIT 2000

selected = []
for row in rows:                       # already in descending order
    t = row.peak_time
    if all(abs(t - s.peak_time) >= window_sec for s in selected):
        selected.append(row)
        if len(selected) == count:
            break
truncated = len(selected) < count and len(rows) == 2000
```

The separation rule is what stops the top 10 being ten samples of one burst.
Centring on `peak_time` rather than on the minute boundary is what makes the
window actually contain the event. The upper bound of each window is clamped to
now.

`LIMIT 2000` at minute granularity covers 33 hours of contiguous domination
before `truncated` can trip, on a table where scanning a whole month is 43k
rows.

## Retention interaction

Two independent horizons meet in this list and the UI must not blur them:

- `DB_RETENTION_DAYS` (30 on the field sensor) bounds **PSD blobs**. A peak
  older than this has no waterfall, only stats. Tagged `stats only`.
- Stats rows are intended to live about **2 years** (not yet enforced, see
  Assumptions). The rollup covers whatever stats exist, so a 1-month lookback
  is well inside both horizons.

## Measurements

Taken on this workstation against a synthetic month at the field rate
(3.7M windows). The Jetson is several times slower; ratios are what matter.

| approach | month-long search | storage over 2 years |
|---|---|---|
| plain top-K over `avg_windows` | 0.96 s | none, but no separation guarantee |
| covering index on `avg_windows` | 0.20 s | 9.9 GB |
| `GROUP BY strftime(...)` bucket | 1.51 s | none |
| `GROUP BY substr(...)` bucket | 1.00-1.15 s | none |
| **minute rollup (chosen)** | **< 0.05 s** | **0.25 GB** |

Row costs with blobs nulled, measured over 2M rows: row 147 B, `idx_time` 38 B,
`idx_center_time` 45 B, candidate peaks index 79 B, total 308 B/row, which is
38.9 GB for two years of stats at 2 rows/s.

## Measured and REJECTED (do not retry)

- **Covering index on `avg_windows`** `(start_time, pwr_max, pwr_median,
  pwr_avg, sdr_center_freq_hz, sample_rate_hz, gain_db)`. Fast enough at 0.20 s
  per month, but 79 B/row is 9.9 GB over two years of stats, roughly 96% of
  which is never read because the search never looks back more than a month. It
  also needs a multi-minute `CREATE INDEX` on the existing 30 GB field DB, which
  blocks writers on that table while it runs. Rejected once stats retention was
  set at 2 years; it was the recommended option before that.
- **Bucketing in SQL** (`GROUP BY` over `strftime('%s', start_time)/W` or over
  `substr(start_time, 1, 13)`). Both are 5 to 7 times slower than a plain
  top-K, at 1.0 to 1.5 s per month. The cost is the `GROUP BY` temp B-tree, not
  the date parsing: replacing `strftime` with `substr` saved only 0.4 s. Do not
  reach for prefix tricks expecting them to be cheap.
- **Plain top-K with greedy separation and no summary table.** Works and is
  cheap (0.28 s at `LIMIT 20000`), but a single contiguous event longer than
  about 2.8 hours can crowd out the whole list, and it cannot search beyond
  roughly a month without degrading.
- **Stitched multi-window waterfall** (all N windows in one canvas with breaks).
  Rejected on design, not performance: it forces a segmented time model through
  the row-to-pixel mapping, the cursor readout, the min/row status line and the
  detections overlay, to show something the jump list shows without touching any
  of them.

## Measurement traps

- `avg_windows.start_time` is TEXT, so a range filter is a string comparison and
  any date arithmetic in SQL costs a per-row function call over millions of rows.
- The blob column dominates the row. A query that avoids `psd_powers` is still
  slow if it makes SQLite fetch rows, because the rows are spread thinly across
  a large file. Only an index-only scan or a summary table avoids that.
- SQLite returns the argmax row's other columns from a bare `MAX()` with
  `GROUP BY`, but only for one aggregate per query. Three metrics would need
  three queries, which is why the rollup computes argmax in Python instead.
- Benchmarks here ran with 1 KB blobs to keep the synthetic DB at 5 GB. Real
  blobs are 8 KB, so anything that touches table rows is worse in the field than
  these numbers suggest, while index-only and rollup paths are unaffected.

## Assumptions and dependencies

- **Row-level retention for `avg_windows` does not exist.** Rows are never
  deleted today, only their blobs nulled. The 2-year intent is recorded here but
  implemented in the storage budgeting project, which also owns deleting the
  matching `avg_minutes` rows.
- Field sensor layout, to be re-verified before deploying: OS on a 114 GB SD
  card, data on a 916 GB SSD at `/mnt/ssd`, 239 GB free, `ARCHIVE_MAX_GB=600`,
  `DB_RETENTION_DAYS=30`. Projected steady state is about 42 GB of PSD blobs,
  about 29 GB of stats rows and indexes after two years, plus 600 GB of archive.
- Nothing in the app measures free disk space, and an ENOSPC during a recording
  is absorbed silently. This feature does not make that worse, but it is the
  reason the storage project follows.

## Testing

- **Selection unit tests:** clustered candidates yield spread peaks; fewer
  candidates than requested; exact ties; empty range; window centring and the
  clamp at now; `truncated` set only when candidates ran out.
- **Rollup unit tests:** a minute's maxima and argmax timestamps; idempotent
  re-run; a late-arriving window raising a maximum; watermark advance and
  resume after a simulated restart.
- **DB tests:** on a seeded database, rollup output matches a brute-force scan
  of `avg_windows` for all three metrics; range filtering on the lexicographic
  `minute_start` returns the same set as filtering on `start_time`.
- **Route tests:** parameter validation (bad `metric`, `count` out of range,
  `window_sec` not in the allowed set, inverted range), the cache key, and the
  client-disconnect path.
- **UI test:** extend `tests/ui/puppeteer_avg_history.js` with open, list
  renders, pick sets the range and loads, arrows step without refetching, and
  peak mode clears on preset click.
- Full check set before every commit: ruff, ruff format, mypy locally and in a
  lint-only venv, unit tests on 3.10 and 3.11, integration tests with NATS on
  :4222.

## Decided, previously open

- **No "jump to the loudest IQ capture" shortcut.** Landing on a peak already
  shows any IQ capture inside that window through the Dashboard's existing
  overlay, so a separate control adds a surface without adding reach.
- **`PEAKS_ROLLUP_INTERVAL_SEC` stays environment-only.** It is a maintenance
  cadence with a sound default; a config-page control would have to be explained
  to every operator who will never change it.
- **An empty lookback shows an empty list and says so.** Never widen the search
  beyond what was asked for. Partial history is a separate case and is already
  covered by `covered_since`, which reports how far back the search reached.
  Silently returning peaks from outside the requested range would make the list
  untrustworthy, which is worse than returning nothing.

## Open, not yet answered

- The one-time backfill cost on the real 30 GB field database is estimated, not
  measured. It should be timed on a copy before this is deployed, since it is a
  full pass over the table.
