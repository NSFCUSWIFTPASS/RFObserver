# Dashboard first load takes seconds

## 1. The question

Date: 2026-10-02. HCRO sensor (rfnano, Jetson, v0.10.0b0 at 9897c77, 26 Msps,
DURATION_SEC 1.0, 2048 bins, about 8.9 GB DB), reached over the VPN from the workstation.
Reported: "the dashboard still takes couple seconds to show the data, can we not cache
it?", then "Can we not speedup the first Dashboard load?". The user also asked for stored
coarser averages (2 to 10 min) for the 30 min to 24 h views. Two constraints from the
user: no backfill of past data, and running accumulators reset each period.

## 2. The answer

Before the page asked for any data, it waited about 4 s:
- `GET /api/averaged/configs` took **3.35 s**: a `SELECT DISTINCT` over every averaged
  window, to fill the tuning dropdowns;
- then `GET /api/ui-prefs` took 0.6 s.

After that:
- **Short ranges:** the time is the download. A 15 min waterfall was 330 KB at about
  190 KB/s.
- **Long ranges:** the time is the server folding every window in Python (24 h: 9.7 s to
  the first byte).

## 3. Procedure

1. `curl` timings from the workstation for each request the page makes, split into time
   to first byte (server work plus round trip) and total (plus transfer), for 15 min, 1 h,
   6 h and 24 h. This separates server time from link time.
2. Re-encoded a real HCRO 15 min waterfall body offline. This compares wire sizes without
   touching the sensor.
3. Profiled the aggregation on a local 20,000-window, 2048-bin DB with cProfile, and
   timed it against the previous code (`git show HEAD:...database.py`, imported side by
   side). This isolates the folding from SQLite reads.
4. Timed the boot requests (`/api/averaged/configs`, `/api/ui-prefs`), since they run
   before any data request.

## 4. Evidence

HCRO, before (time to first byte / total, gzipped size):

```
15m  waterfall 0.71/2.46 s 333 KB   stats 0.57/0.85   detections 0.33/0.59   iq 0.49/0.56
1h   waterfall 1.15/2.94 s 304 KB   stats 0.47/0.73
6h   waterfall 4.24/5.75 s 269 KB   stats 0.94/1.23
24h  waterfall 9.67/11.10 s 282 KB  stats 1.19/1.45
averaged/configs ttfb 3.35 s        ui-prefs ttfb 0.59 s
```

Waterfall encodings, real HCRO 15 min body (601 x 512):

```
int16 0.01 dB (v3)   gz 330,017 B
uint8 0.035 dB       gz 164,095 B
int16 0.1 dB (v4)    gz  98,020 B
```

Aggregation, local, 20,000 windows x 2048 bins to 600 x 512:

```
old (per-window Python loop)                 440 ms
numpy per chunk, mean(axis=2) downsample     264 ms   (reduce: 102 ms of it)
numpy per chunk, strided-slice downsample    205 ms   (SQLite scan alone: 88 ms)
```

Tiers, local, 24 h of 1 s windows x 2048 bins (insert with tiers: 189 us/window):

```
range 23h50m   tiered (10 s rows, 150 s buckets)   183 ms   572 rows
               raw (vectorized)                    732 ms   477 rows
stats timeline tiered                               16 ms
```

nano-super (Orin Nano, Python 3.10), 6 h of 1 s windows x 2048 bins, 540 rows:

```
tiered (10 s rows, 40 s buckets)   187 ms
raw (vectorized)                   805 ms      (HCRO before: 4.24 s to first byte)
insert, plain                      755-772 us/window
insert, with tiers                1324-1373 us/window  (accumulator alone ~125 us/window)
```

Browser cache, mock pipeline on localhost: the cached copy painted 78 ms after
navigation; the first request after it was a tail (6 KB, against 15 KB for a full load).

## 5. Changes

- **Waterfall v4:** int16 rows in 0.1 dB steps (3.4x smaller on the wire).
- **Vectorized folding:** the waterfall and stats aggregation run per chunk in numpy
  (`storage/avg_fold.py`). This also stops a long-range load from holding the GIL for
  seconds while the receiver captures.
- **Stored tiers:** `avg_tiers` at 10 s, 1 min and 10 min (`storage/psd_tiers.py`).
  - The writer keeps running sums per period and tuning, writes a row when the period
    ends, writes the open periods at shutdown, and merges a split period after a
    restart.
  - No backfill: each tier records the epoch from which it is complete, and only ranges
    starting after it use the tier.
- **Tuning configs:** kept by the writer under the config key `avg_window_configs`,
  seeded once on a separate connection. No per-request scan.
- **Boot data in the page:** the Dashboard HTML embeds the configs and prefs, so the
  page's first requests are the data.
- **Browser cache:** IndexedDB keeps the last load of each live preset and tuning
  (6 kept, up to 1 h old). Reopening paints it at once, then fetches the tail, or
  reloads in the background when the copy is older than 5 min.

## 6. Measured and REJECTED (do not retry)

- **uint8 rows:** 164 KB against 98 KB for int16 at 0.1 dB, and coarser.
- **Byte-shuffling the int16 rows:** 87 KB against 98 KB. An 11% gain, not worth a custom
  decoder.
- **`mean(axis=2)` for the bin downsample:** 100 ms of 264. Strided slice sums are the
  same result, several times faster.

## 7. Measurement traps

- A 23h50m range picks the 10 s tier (its 1 min buckets would be 180 s, above 1.25 x
  143 s). An exact 24 h preset picks the 1 min tier. Bench the presets, not arbitrary
  spans.
- `curl` totals include the VPN's transfer time. Use the time to first byte for server
  work.

## 8. Open, not yet answered

- Not yet measured on HCRO after deploy. Expect `/` to be near the page's own time, a 15
  min waterfall around 0.7 s plus about 0.5 s transfer, and 24 h fast only once the
  1 min tier covers the last 24 h, that is a day after deploy.
- A crash (no clean shutdown) loses the open period of each tier, up to 10 min. The
  stored row for that period then undercounts, and nothing detects it.
- The 1 h range still folds 3,600 raw windows per load (no tier is fine enough for its
  6 s buckets).
