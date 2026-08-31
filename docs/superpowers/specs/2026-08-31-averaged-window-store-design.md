# Averaged-window store: persist averaged PSD/stats/flags to SQLite

- **Date:** 2026-08-31
- **Status:** Approved for planning
- **Scope of this cut:** storage + query API + detection association. No viewer UI.

## Problem

The averaged data RFObserver sends to OpenZMS every processing window (averaged
PSD, averaged IQ statistics, and interference/violation flags) is built in
memory, POSTed to the DST, and otherwise not kept. SQLite stores only individual
burst `detections`, `tone_checks`, and `config`. There is a dormant `stats` table
with an `insert_stats` method that has zero callers.

We want a local, queryable history of the averaged windows so an operator can:

1. Select a datetime start/stop range and look at the averaged data over that span.
2. Know exactly which time span each averaged record covers (start + duration).
3. Associate burst detections with the averaged window they fall inside.

## Decisions (locked during brainstorming)

- **Store full-fidelity PSD.** All `NUM_FFT_BINS` averaged powers per window, as a
  compact little-endian float32 BLOB (~8 KB at 2048 bins). Not downsampled.
- **Persist always while the pipeline runs** (live, not replay), independent of
  whether OpenZMS/NATS are enabled. Local history must not depend on an external
  sink being on.
- **Association is a time-range + tuning join at query time** (no foreign key).
- **New purpose-built table** `avg_windows`; the generic dormant `stats` table is
  left untouched (it stays unused; not removed in this cut).
- **Stored values are raw dBFS**, consistent with the rest of the system
  ("published and stored data stay raw"); calibration/scale is display-only.
- **Retention is blob-only.** The PSD blob is ~98% of each window's storage and is
  the only expensive data; the cheap stats row, `detections`, and `tone_checks`
  are kept permanently. Retention nulls `psd_powers`/`violations` after
  `DB_RETENTION_DAYS` instead of deleting rows (locked with the user on
  2026-08-31: Option A nullable column, prune to NULL; never prune tone_checks;
  reuse `DB_RETENTION_DAYS`).

## Data available at the persistence point

In `streaming.py`, `_publish_processed(avg_powers, result, iq_stats)` is the point
where one `DURATION_SEC`-averaged window is finalized. `_run_tone_check` already
runs there and persists to SQLite unconditionally (it is NOT gated by ZMS/NATS),
which is the model to follow. Available per window:

| Field | Source | Notes |
|---|---|---|
| start_time | `datetime.now(timezone.utc)` at finalize | window start, UTC ISO |
| duration_sec | `settings.DURATION_SEC` | window length |
| sdr_center_freq_hz | `result.center_freq_hz` | per-dwell center (varies under sweep) |
| sample_rate_hz | `settings.BANDWIDTH` | matches detection `sample_rate_hz` |
| gain_db | `settings.GAIN` | matches detection `gain_db` |
| num_bins | `result.summary_psd.num_bins` | |
| freq_start_hz, freq_step_hz | derived from `result.summary_psd.frequencies` | FFT axis is uniform; store start+step, reconstruct on read (avoids a second ~8 KB blob) |
| pwr_avg/max/median/std, kurtosis | `iq_stats` (IQStatistics) | averaged stats |
| psd_powers | `avg_powers` | float32 BLOB |

### Flags: honest current state

The live streaming path does **not** currently compute `interference` or per-bin
`violations` (the known PSDProcessor gap: direct OpenZMS submission bypasses the
zms-monitor PSDProcessor; see the project's violation-flags TODO). The submit path
calls `submit_observation(envelope)` with `interference` defaulting to `False` and
`violations=None`.

Therefore this cut adds the columns (`interference INTEGER NULL`, `violations BLOB
NULL`) and writes whatever is available (NULL today). When the PSDProcessor gap is
closed, the same insert path populates them with no schema migration. We do NOT
fabricate flag values.

## Schema: `avg_windows`

```sql
CREATE TABLE IF NOT EXISTS avg_windows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time TEXT NOT NULL,
    duration_sec REAL NOT NULL,
    sdr_center_freq_hz REAL NOT NULL,
    sample_rate_hz REAL NOT NULL,
    gain_db REAL,
    num_bins INTEGER NOT NULL,
    freq_start_hz REAL NOT NULL,
    freq_step_hz REAL NOT NULL,
    pwr_avg REAL,
    pwr_max REAL,
    pwr_median REAL,
    pwr_std REAL,
    kurtosis REAL,
    interference INTEGER,          -- nullable bool flag (NULL until PSDProcessor gap closed)
    psd_powers BLOB,               -- num_bins little-endian float32; NULL when retention pruned it
    violations BLOB,               -- reserved: per-bin flags, NULL for now
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_avg_windows_time ON avg_windows(start_time);
CREATE INDEX IF NOT EXISTS idx_avg_windows_center_time
    ON avg_windows(sdr_center_freq_hz, start_time);
```

- Added to the `SCHEMA` executescript block (fresh DBs). Existing DBs get it
  created on next `connect()`.
- `psd_powers` is nullable so retention can evict the blob in place. DBs created
  before this decision (with `BLOB NOT NULL`) are upgraded by a table-rebuild
  migration in `connect()` (`_migrate_avg_windows_psd_nullable`), which copies the
  data, drops the old table, renames the new one, and recreates the indexes.
- Retention: `prune_avg_psd_blobs(days)` does
  `UPDATE avg_windows SET psd_powers = NULL, violations = NULL
   WHERE start_time < ? AND psd_powers IS NOT NULL`
  using the same cutoff and `DB_RETENTION_DAYS` as before. It returns how many
  blobs were nulled. **No rows are ever deleted by retention**: detections,
  stats, and tone_checks are kept permanently (measured: ~157 B/window stats row
  and ~250 B/detection are effectively free; the ~8 KB PSD blob is the only
  costly data).

### BLOB encoding

```python
import numpy as np
blob = np.asarray(avg_powers, dtype="<f4").tobytes()          # encode
powers = np.frombuffer(blob, dtype="<f4").tolist()            # decode
```

float32 is lossless enough for dBFS PSD display (values ~ -140..0 dB; float32 gives
> 6 significant digits). Round-trip is asserted in tests to within 1e-3 dB.

## DB API (`SensorDatabase`)

- `insert_avg_window(*, start_time, duration_sec, sdr_center_freq_hz,
  sample_rate_hz, gain_db, num_bins, freq_start_hz, freq_step_hz, pwr_avg,
  pwr_max, pwr_median, pwr_std, kurtosis, powers, interference=None,
  violations=None) -> None` — encodes `powers` to the BLOB, single INSERT + commit.
- `query_avg_windows(*, since=None, until=None, sdr_center_freq=None,
  sample_rate=None, gain=None, limit=500) -> list[dict]` — metadata + stats + flags,
  **no** `psd_powers`/`violations` blobs (keeps range lists light), newest first.
  Reuses the same SDR-context WHERE fragments as `query_detections`
  (`_sdr_conditions`), and `start_time` range like the detection `since`/`until`.
- `get_avg_window(window_id) -> dict | None` — one full record; decodes
  `psd_powers` to a `powers` list and reconstructs `frequencies` as
  `freq_start_hz + i * freq_step_hz` for `i in range(num_bins)`. When retention
  has pruned the blob, `powers` is `null` but the stats row and the
  (metadata-derived) `frequencies` are still returned.

Association reuses the existing `query_detections(since, until, sdr_center_freq,
sample_rate, gain)` verbatim: detections for window R are those with
`since = R.start_time`, `until = R.start_time + R.duration_sec`, matching tuning.
The range predicate is on the detection's **`start_time`** (the burst start), which
is exactly the field `query_detections` already ranges on via `since`/`until` — so
a burst is associated with the window it started in. A thin helper
`detections_for_window(window_row)` composes this.

## Persistence wiring (`streaming.py`)

Add `_persist_avg_window(avg_powers, result, iq_stats)` and call it from
`_publish_processed` at the same site as `_run_tone_check`, guarded by
`if not self._replay_mode`. It derives `freq_start_hz`/`freq_step_hz` from
`result.summary_psd.frequencies` (first element and first delta), reads
`gain`/`sample_rate` from settings, and calls `insert_avg_window`. Wrapped in
`try/except` with `logger.exception` so a persistence failure never disrupts the
processing loop (same defensive pattern as the tone-check insert). It runs whether
or not ZMS/NATS are attached, because `_publish_processed`/`_run_tone_check` are
not behind the ZMS/NATS gate that `_maybe_submit` uses.

## Web API (`routes/api.py`)

- `GET /api/averaged` — query params `since`, `until` (ISO 8601), `sdr_center`,
  `sample_rate`, `gain`, `limit`. Returns JSON list of window rows (ids + metadata
  + stats + flags, no PSD). This is the datetime start/stop selection surface.
- `GET /api/averaged/{window_id}` — full record incl. decoded `powers` +
  reconstructed `frequencies`. 404 when missing.
- `GET /api/averaged/{window_id}/detections` — detections whose timestamp falls in
  `[start, start+duration)` for that window's tuning (the association), as JSON.

All three are JSON REST (not htmx fragments) since there is no viewer UI in this
cut; the follow-up UI consumes them.

## Testing

**Unit (`tests/unit/test_database.py` or extend existing):**
- BLOB round-trip: encode→decode preserves values within 1e-3.
- `insert_avg_window` then `query_avg_windows` returns the row; `since`/`until` and
  tuning filters scope correctly; blobs absent from the light query.
- `get_avg_window` decodes powers and reconstructs `frequencies` from start+step
  (length == num_bins, endpoints correct).
- Association: given a window and detections at timestamps inside and outside
  `[start, start+duration)`, only the inside ones (matching tuning) are returned.
- Retention: `prune_avg_psd_blobs` nulls the PSD blob of windows older than the
  cutoff and counts them; the stats rows, detections, stats, and tone_checks all
  survive (verified per table); a pruned window returns `powers: null` with
  stats + frequency axis intact; a second pass reports 0.
- Migration: a DB created with the original `psd_powers BLOB NOT NULL` schema is
  rebuilt on `connect()` so the column is nullable, existing rows and blobs
  survive, and pruning then works.

**Unit (`tests/unit/test_web_routes.py`):**
- `/api/averaged` range + filter query returns inserted windows.
- `/api/averaged/{id}` returns full PSD; unknown id → 404.
- `/api/averaged/{id}/detections` returns the associated detections.

**Integration (`tests/integration/`):**
- Run the mock pipeline with ZMS and NATS **disabled**; assert `avg_windows` rows
  accrue over a few windows (proves persistence is independent of the sinks).
- A detection produced during a window is returned by that window's
  `/detections` endpoint.

## Storage/volume note

At defaults (DURATION_SEC=0.5 → ~2 windows/s, 2048 bins) the PSD blob is ~8 KB, so
~1.4 GB/day. Retention nulls blobs older than `DB_RETENTION_DAYS` (7 → ~10 GB
steady state on the Jetson NVMe; SQLite reuses the freed pages, so the file stops
growing once the window is filled). The permanently-kept data is cheap:
~157 B/window stats (~23-27 MB/day ≈ 8-10 GB/year) and ~250 B/detection
(negligible at realistic burst rates; even 10k/day ≈ 0.9 GB/year). Under a
frequency sweep there is one row per dwell/window. This is the reason for float32
BLOB over JSON text (which would be ~3x larger), for storing the frequency axis
as start+step rather than a second array, and for making the blob the *only*
evictable data.

## Out of scope (follow-up)

- History-page time-range viewer UI: averaged-PSD waterfall over the selected
  window, stats-over-time plots, and detections overlaid. Built against real data
  once this cut is collecting it.
- Closing the PSDProcessor violation-flags gap so `interference`/`violations` are
  actually computed and stored non-NULL.

## Open, not yet decided

- Whether to eventually add a coarse per-window "any violation" summary column for
  fast filtering once the flags are computed. Deferred until the gap is closed.
