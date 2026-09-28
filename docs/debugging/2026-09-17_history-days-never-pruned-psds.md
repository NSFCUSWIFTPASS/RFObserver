# "History Days" on the Config page never controlled PSD retention

Date: 2026-09-17. Field sensor `rf-nano-002@rfnano`, running main at a788dbf.

## The question

> In the config of that deployed sensor, I've set history days to 30, but it
> seems to be pruning PSDs earlier. The history days is specifically for PSDs
> correct and not IQ? The IQ pruning should be done based on archive max, is
> that how it's implemented?

## The answer

The intent was right, the wiring was not. The Config page's "History Days"
field wrote `HISTORY_DAYS`, and **nothing in the codebase ever read
`HISTORY_DAYS`**. PSD pruning is driven by a different setting,
`DB_RETENTION_DAYS` (default 7), so the sensor kept aging PSD blobs out at 7
days while the page showed 30 and reported the change applied.

IQ is as expected: bounded only by `ARCHIVE_MAX_GB`, by size, not by age.

## How each retention path actually works

| What | Setting | Mechanism |
|---|---|---|
| PSD blobs of averaged windows | `DB_RETENTION_DAYS` (days) | `AveragedDatabase.prune_avg_psd_blobs` NULLs `psd_powers` and `violations` for rows with `start_time` older than the cutoff. The cheap stats row survives, as do `detections` and `tone_checks`. Runs at startup then every `DB_CLEANUP_INTERVAL_SEC` (3600 s); `0` disables the loop. |
| IQ captures | `ARCHIVE_MAX_GB` (size) | `LocalStorage.enforce_cap` FIFO-evicts the oldest `auto/*.sc16` while over the cap. Manual captures are never evicted, the newest capture is never evicted, and companion files (`.json`, `.detections.json`) go with the capture. |
| `HISTORY_DAYS` | none | Dead. Written by the page, read by nobody. |

The cleanup loop reads `settings.DB_RETENTION_DAYS` on every pass, so a change
takes effect at the next pass without a restart (worst case one hour).

## Procedure that produced it

1. `grep -rn "HISTORY_DAYS" src/` -- three hits: the field definition, the
   Config-page form mapping, the template. No reader. That alone settles it.
2. `grep -rn "DB_RETENTION_DAYS" src/` -- `pipeline/app.py` gates and drives
   `_db_cleanup_loop`, which calls `prune_avg_psd_blobs`. This is the live path.
3. Read `prune_avg_psd_blobs` (`storage/database.py`) to confirm what it
   deletes: PSD blobs only, stats rows kept.
4. Read `LocalStorage.enforce_cap` (`storage/local.py`) to confirm IQ is
   size-capped, auto-only, newest-preserved.

## The fix

- `config.py`: `history_days` on the page now maps to `DB_RETENTION_DAYS`.
- `config.py` (settings): a `model_validator(mode="after")` carries a stored
  `HISTORY_DAYS` into `DB_RETENTION_DAYS` when `DB_RETENTION_DAYS` was not set
  explicitly, so a deployment whose `.env` already holds the old key starts
  honouring the number it was given instead of reverting to 7.
- `config.html`: the field is relabelled "PSD History (days)", reads
  `settings.DB_RETENTION_DAYS`, and both storage fields carry one line saying
  what they bound (PSD data by age; IQ captures by size, manual kept).
- `HISTORY_DAYS` is kept as a deprecated field purely so the carry-over above
  can read it.

Tests: `tests/unit/test_config.py` (default 7, legacy carry-over, explicit
`DB_RETENTION_DAYS` wins), `tests/unit/test_web_routes.py`
(`history_days` POST lands on `DB_RETENTION_DAYS` and persists to `.env`).

## Traps

- **The page reports success either way.** `/config/apply` returns
  `{"changed": ["HISTORY_DAYS"]}` and persists it to `.env`, so both the UI and
  the file agreed the setting had been applied. Only the absence of a reader
  shows otherwise. When a setting "does not work", grep for its readers before
  investigating the mechanism it is supposed to drive.
- **`tests/integration/test_config_live.py` asserted the broken behaviour.**
  Its field table checked `history_days` against `HISTORY_DAYS`, so the dead
  wiring was covered by a green test. Round-trip tests that assert
  "the value reached some attribute" do not prove the attribute is used.
- The persistence writer rewrites `.env` from every non-default field, so after
  the fix a migrated sensor keeps both `RFOBS_HISTORY_DAYS=30` and
  `RFOBS_DB_RETENTION_DAYS=30`. Harmless, and the explicit key wins from then on.

## Immediate workaround (no upgrade needed)

Set `RFOBS_DB_RETENTION_DAYS=30` in the sensor's `.env` and restart the service.

## Open, not yet answered

- PSD blobs already nulled between day 7 and day 30 on that sensor are gone;
  only the stats rows remain for that period.
- No age-based IQ pruning exists. Whether one is wanted (for example, drop
  `auto/` captures older than N days even when under the cap) was not decided.
