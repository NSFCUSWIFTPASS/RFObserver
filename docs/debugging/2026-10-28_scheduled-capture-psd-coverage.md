# Scheduled captures: waterfall (PSD grid) covers only part of the window

## Question (2026-10-28)

On a mock sensor (`RFOBS_MOCK_RECEIVER=true`, 56 MS/s, this workstation,
branch `feat/scheduled-captures`), why does a scheduled run's `.psd` grid start
millions of samples into a 300 ms (16.8 M sample) file, with no dropped chunks
logged and `grid_rows_dropped` 0?

First seen: run `t1` grid `rows 560, start_sample_offset 11063591` of 16.8 M
samples. After the review fixes (a50c8b7), 6 runs on two mock sensors: offsets
597 237 to 6 195 082 samples, rows 1035 to 1640 of 1640.

## Answer

PSD results reach the dispatch thread out of order (processing workers finish
chunks in parallel). The engine anchored a run's grid on the first rows to
arrive and dropped any rows that arrived later for earlier positions as
"already placed". When a later chunk's rows came first, the grid started
there. Fixed by anchoring row 0 at the run's first row boundary (from the
chunk the run began in), holding early rows until the ones before them arrive,
and filling rows that never come (zero fill, or a dropped chunk) at close.

## Procedure

1. Mock run with two overlapping schedules, then per-run `.psd.json` rows and
   `start_sample_offset` against the `.sc16` sample count. Isolates whether
   rows are missing at the head, the tail or throughout. Result: at the head.
2. `TIMING recv#` lines in the mock's `run.log` around the run (`dropped`,
   `handoff_dropped`). Controls for processing loss. Result: `dropped=0`,
   `handoff_dropped=0/0`: no chunk was dropped, so the rows were produced.
3. Added `grid_rows_dropped` (rows refused by the writer's byte budget) to
   the run metadata and re-ran. Controls for writer budget drops. Result: 0.
4. Single-schedule run: full coverage (1639 rows). Two-schedule run right
   after: full coverage. Not reproduced at that point.
5. After the review fix that placed rows by file index (a50c8b7), a repeating
   fleet schedule on two mocks: 6 runs, 5 started late. Reproducible again.
6. Unit test feeding the engine grids for chunks 300, 100, 200 (out of order):
   `offset_samples` came out 200 instead of 0 (test
   `test_grid_rows_arriving_out_of_order_keep_their_place`). Confirms the
   mechanism without the pipeline.
7. Fix, then the same fleet run: 6 of 6 runs with 1639 to 1640 rows starting
   within one row of the file start.

## Evidence

Before (a50c8b7, mock-a / mock-b, 300 ms runs at 56 MS/s, slice 10240):

```
mock8890 0 recorded -5 16800000 psd rows 1528 off 1146773 covers 16793493 0
mock8890 1 recorded -7 16800000 psd rows 1582 off 597237 covers 16796917 0
mock8890 2 recorded -5 16800000 psd rows 1035 off 6195082 covers 16793482 0
mock8891 0 recorded 1 16800000 psd rows 1141 off 5108221 covers 16792061 0
mock8891 1 recorded 1 16800000 psd rows 1170 off 4813528 covers 16794328 0
mock8891 2 recorded -9 16800000 psd rows 1640 off 986 covers 16794586 0
```

After:

```
mock8890 0 recorded 3 16800000 rows 1639 off 8353 covers 16791713 dropped 0
mock8890 1 recorded 2 16800000 rows 1640 off 3783 covers 16797383 dropped 0
mock8890 2 recorded -9 16800000 rows 1639 off 8623 covers 16791983 dropped 0
mock8891 0 recorded -2 16800000 rows 1640 off 4286 covers 16797886 dropped 0
mock8891 1 recorded -1 16800000 rows 1640 off 5485 covers 16799085 dropped 0
mock8891 2 recorded 5 16800000 rows 1640 off 833 covers 16794433 dropped 0
```

## Measured and REJECTED (do not retry)

- Processing dropped the chunks: rejected, `dropped=0 handoff_dropped=0/0`
  over the run.
- Writer byte budget refused the rows: rejected, `grid_rows_dropped` 0 on
  every affected run.
- Rows for the ring part of a start read back from the ring: not this case;
  these starts were in the future (the run began in the chunk holding the
  start), and the gap was millions of samples, not up to `TRIGGER_PRE_SEC`.

## Measurement traps

- Steps 3 and 4 did not reproduce it, which made it look like a one-off at
  startup. Out-of-order arrival depends on worker timing, so a clean run
  proves nothing; the unit test with a fixed order is the reliable check.
- With 1 s repeats the same fleet run also showed the fourth run `missed`
  (`busy`): runs waiting up to 2 s for their PSD rows counted toward the
  two-concurrent-runs limit. Separate bug, fixed with this one (only runs
  still recording IQ count).

## Open

- Rows for the part of a run read back from the ring (start up to
  `TRIGGER_PRE_SEC` in the past) never arrive: those chunks were processed
  before the run existed. The waterfall then starts up to that much late;
  the IQ is complete.
- Not yet checked on a Jetson with the CUDA PSD backend, where worker timing
  differs.
