# Tone check (antenna / environment diagnostic)

Date: 2026-07-22

## Goal

A built-in tone test: point the sensor at a band, transmit a CW tone at a known
frequency, and have RFObserver report each averaging interval whether it sees
the tone >= a threshold above the noise floor. Sweeping the sensor's center
across bands (e.g. 915 MHz vs 2.4 GHz) and watching where the tone appears
confirms the antenna's band and the link characteristics.

## Decisions (from brainstorming)

- **Absolute** tone frequency (`TONE_CHECK_FREQ_HZ`), converted to a bin via
  `freq - center`; out-of-band is recorded, not an error.
- **Persist results to a DB table** (`tone_checks`) so tests survive restarts and
  can be queried historically.
- Evaluated once per `DURATION_SEC` (the averaging interval) on the averaged PSD.
- Threshold configurable, default **10 dB**. Noise floor = 10th percentile of the
  averaged PSD (matches the codebase's `compute_noise_floor` convention).

## Config (`src/rfobserver/config.py`, `.env`-persistable)

- `TONE_CHECK_ENABLED: bool = False`
- `TONE_CHECK_FREQ_HZ: float = 0.0`  (absolute Hz)
- `TONE_CHECK_THRESHOLD_DB: float = 10.0`

## DB (`src/rfobserver/storage/database.py`)

New table + index (added to `SCHEMA`, created idempotently in `connect()`):

```sql
CREATE TABLE IF NOT EXISTS tone_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    tone_freq_hz REAL NOT NULL,
    sdr_center_freq_hz REAL NOT NULL,
    in_band INTEGER NOT NULL,
    tone_power_db REAL,
    noise_floor_db REAL,
    snr_db REAL,
    detected INTEGER NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_tone_checks_time ON tone_checks(timestamp);
```

Methods:
- `insert_tone_check(*, timestamp, tone_freq_hz, sdr_center_freq_hz, in_band, tone_power_db, noise_floor_db, snr_db, detected)`
- `query_tone_checks(limit=200)` -> list of dict rows, newest first.

## Evaluation (`src/rfobserver/pipeline/streaming.py`)

In the consumer loop's `DURATION_SEC` flush (where `avg` averaged PSD is
computed), when `TONE_CHECK_ENABLED`:

1. Map `TONE_CHECK_FREQ_HZ` to an offset from `result.center_freq_hz`; find the
   nearest bin in `result.summary_psd.frequencies`. If the offset is outside the
   captured span, record `in_band=False`, `detected=False` (still logged so the
   user sees "tone out of band").
2. `tone_power_db` = max of `avg` over a small window (+/- 2 bins) around the
   tone bin. `noise_floor_db` = 10th percentile of `avg`. `snr_db = tone_power -
   noise_floor`. `detected = in_band and snr_db >= TONE_CHECK_THRESHOLD_DB`.
3. `await self._db.insert_tone_check(...)` and `logger.info(...)` a one-line
   summary each interval.

A helper `evaluate_tone_check(avg, frequencies, center_hz, tone_freq_hz, threshold_db)`
-> dict is pure and unit-tested; the loop just persists + logs its result.

## API (`src/rfobserver/web/routes/api.py`)

- `GET /api/tone-check` -> `{enabled, freq_hz, threshold_db, results: [...]}`
  (config from `app.state.settings`, results from `query_tone_checks`).
- `POST /api/tone-check` `{enabled?, freq_hz?, threshold_db?}` -> updates the
  settings (`object.__setattr__`) and persists to `.env` (reuse `_persist_settings`);
  no pipeline reconfigure (the check only reads the existing PSD). Returns the
  updated config.

## Tests

- `tests/unit/test_database.py`: insert + query `tone_checks` round-trip.
- New `tests/unit/test_tone_check.py`: `evaluate_tone_check` -- detects a tone
  above threshold, rejects below, flags out-of-band.
- `tests/unit/test_web_routes.py`: `GET`/`POST /api/tone-check` (config echo,
  results shape, persist called).
- Full CI suite green.

## Verification

Unit + integration suites green. Manual (later, on the Jetson): enable via API,
transmit a CW, confirm `detected=true` rows accumulate at the right center; sweep
the sensor center to 2.4 GHz to test the antenna-band hypothesis.
