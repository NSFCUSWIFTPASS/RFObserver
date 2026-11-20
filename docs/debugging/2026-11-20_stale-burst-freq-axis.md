# Burst frequencies scaled by the old sample rate after a live bandwidth change

## The question (2026-11-20)

Lab daily-check run 1 (tools/daily_check.py): sensor `ubuntu` (.177, B200mini
321D126, RFObserver c8e970a, 0.16.0b0) retuned live with `POST /config/apply`
from 100 MHz / 56 MS/s to 915 MHz / 26 MS/s; TX nano-super (ssm-tx 7afd734),
two SSM epochs. Tone rows were right (9/9 steps, correct frequencies), but the
SSM epochs decoded **0/83 hops** each. Burst rows in the epochs were reported at
**890-941 MHz**, outside the 902-928 MHz the sensor was tuned to, while their
rows said `sample_rate_hz` 26 MS/s.

## The answer

The burst thread's `RollingBurstDetector` was built from a stale 56 MS/s PSD
grid after the reconfigure, so it kept the 56 MS/s frequency axis: every burst
is placed at 915 + (f - 915) x 56/26. Isolation then cuts the wrong frequency
and the SSM decoder never sees a hop. A restart (or any later rebuild of the
detector from a fresh grid) clears it.

Mechanism (src/rfobserver/pipeline/streaming.py, burst loop): a generation
change sets `rolling_detector = None` ("re-init on next grid"), and the next
grid taken from `_burst_queue` builds it with `freq_axis=psd_grid.freq_axis`.
Grids queued before the reconfigure are still at the old rate; the stale-grid
check only compares the bin count, and both rates use 2048 bins, so a stale
grid passes and seeds the detector. Nothing rebuilds it afterwards (the
generation does not change again). The tone detector is unaffected because it
reads each result's own `freq_axis`.

## Procedure

1. Run 1 of the daily check: tone rows right, SSM 0/83. Tones and bursts come
   from the same PSD grids, so the receiver and tuning were right; the fault is
   in the burst path.
2. All rows during epoch 1 (not only SSM): 1098 rows, all `attempted`, none
   decoded; centres 887-943 MHz = 915 +/- 28 MHz, i.e. the 56 MS/s span.
3. Hop log against the strong long rows, in order (both lists are the same
   hops, one slot apart): ratio (f_row - 915) / (f_hop - 915):

   | hop (ssm-tx log) | row | ratio |
   |---|---|---|
   | 916.70 | 918.650 | 2.147 |
   | 921.20 | 928.344 | 2.152 |
   | 912.50 | 909.613 | 2.155 |
   | 916.40 | 918.021 | 2.158 |
   | 919.40 | 924.475 | 2.153 |
   | 907.10 | 897.979 | 2.155 |

   56/26 = 2.154.
4. Restart the service (pipeline built at 26 MS/s): rows back in 903.8-920.4
   MHz; run 2 decoded 59/83 and 82/83 hops.
5. Reproduction on the same box: `POST /config/apply {"bandwidth": 56000000}`,
   20 s, then `{"bandwidth": 26000000}`. The next minute: 22 rows at 26 MS/s,
   6 of them outside 902-928 MHz (889.6 and 937.8 MHz among them). Restarted.

## Evidence

Run 1 epoch-1 rows (excerpt, `sdr_center_freq_hz` 915000000, `sample_rate_hz`
26000000 on every row):

```
17:19:03.934 890.227 MHz  2133 kHz  77.2 ms  -57.4 dB  attempted, not decoded
17:19:03.730 937.436 MHz  1066 kHz   1.3 ms  -94.3 dB  attempted, not decoded
17:19:03.726 941.059 MHz   164 kHz   3.5 ms  -87.9 dB  attempted, not decoded
```

Reproduction after 26 -> 56 -> 26 MS/s:

```
22 rows, rate {26000000.0}, freq range 889.58 - 937.82 MHz, outside 902-928: 6
```

## Measured and rejected (do not retry)

- The radio still running at 56 MS/s: rejected. The averaged windows after the
  retune have `freq_start_hz` 902000000 and `freq_step_hz` 12695.3 (26e6/2048),
  and the tone rows land on 914.85 / 915.10 MHz.
- A matching tolerance problem in the check script: rejected. Matching the hop
  list with the 56/26 ratio applied still gave only 16/83, because the rows'
  times were also off from the assumed air time; the frequency error alone is
  2x and visible on every strong row.

## Measurement traps

- The first look at detections "since restart" used `limit=300` with no
  `since`, so it mixed rows from before the restart (887-942 MHz) with good
  ones; filter by time.
- Burst rows' `start_time` is processing time: `_to_fingerprint` stamps
  `datetime.now()` minus the row offset, so rows trail the air by the pipeline
  latency (0.45-0.73 s median per epoch here, 0.5-0.9 s spread). Tone rows use
  the capture clock (+17 to +28 ms). Do not match bursts to transmissions with
  a tight time window.

## Open, not yet answered

- Fix not written. Candidate: rebuild the detector when a grid's `freq_axis`
  differs from the one it was built with (or skip grids whose axis does not
  match the current `BANDWIDTH`), with a unit test feeding a stale grid after a
  generation bump.
- Whether a centre-only retune (sweep mode, schedule-driven retune) has the
  same staleness for the centre: the detector takes its centre from settings,
  not from the grid's `freq_hz`; not checked.
- The HCRO sensors are not retuned in bandwidth in normal use, so field data is
  probably unaffected; not checked against their history.
