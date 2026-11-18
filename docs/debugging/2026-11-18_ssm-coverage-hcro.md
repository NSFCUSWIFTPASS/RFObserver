# SSM attribution coverage across the HCRO sensors (2026-11-06 captures)

## The question

2026-11-18: "I did some SSM captures transmitting out of two SSMs and scheduled captured it on all
the sensors, I wanted to see the attribution coverage on all the sensors and if we can improve it."
Then: "I might change the bursts thresholds down to 15dB, we might also need to consider when closer
transmitter raise the entire local frequency floor around the burst frequency which cause those
bursts to be detected as much wider than they are."

Hardware: HCRO hcro-jet-001..006 (10.1.42.31-.36; NNE60, E450, W230, NbE540, N830, N1000), B200
class radios at 26 MS/s centred on 915 MHz, gain 35, RFObserver 0.15.0b0. SSM1 is colocated with
N1000 (.36), SSM3 with NbE540 (.34). Settings on all: `BURST_THRESHOLD_HIGH_DB` 30 (.33: 40),
`ISOLATION_SNR_DB` 13, `ISOLATION_MAX_PER_SEC` 20, decoders `ssm,silverspring` (read 2026-11-18;
the Nov 6 values were not recorded).

Captures: five 5 s scheduled captures on all six sensors, 2026-11-06 21:48:00 (`SSM1_neg12`),
21:49:00 (`SSM1_0`), 21:55:00 (`SSM1_12`), 22:53:00 and 23:11:30 (no label). Every file has 0
zero-filled samples, so the files line up by sample index across sensors.

Symptom numbers (live database):
- In the labelled windows only .36 labelled SSM: 21/21, 9/33 and 26/54 SSM-length (~82 ms) bursts.
  The other five sensors logged no SSM-length burst in any window.
- 21:20-21:36 (SSM3, no capture): .34 labelled 330 of 817 hops (40 %); 482 never tried. .36 labelled
  277/310 (89 %).

## The answer

1. **Near the transmitter, the per-second isolation budget is the loss.** Each hop also appears as
   copies (±2 MHz at about -23 dB, ±9.5 MHz at about -30 dB, the image 15-18 MHz away at -20 to -25
   dB), each still 30-40 dB over the floor. Four hops a second with about 7 rows each exceed
   `ISOLATION_MAX_PER_SEC` 20, so real hops go untried, and the copies that are tried get labelled
   SSM too (overcount). A copy gate (`ISOLATION_COPY_DB` 15) fixes both.
2. **Near the transmitter, reported bandwidth is 4-75x the real width** (432 kHz-7.8 MHz against
   102-190 kHz): the region grows through the strong signal's skirt above the low threshold. A
   depth limit (`BURST_GROW_DEPTH_DB` 25) fixes it. Decoding is not affected by the width.
3. **Far from the transmitter, the 30 dB threshold is the loss.** At 15 dB, with the cap lifted,
   .34 and .35 detect 19/19 and 17/19 of SSM1_12's hops and label 17 each. With the cap at 20, 15 dB
   labels fewer hops than 30 dB, because .34 then produces 430-800 candidates a second. Not fixed
   here; see "Open".

## Procedure

1. **Live database per sensor**, `GET /api/detections.json?since=&until=&limit=20000` for the test
   evening, and `GET /api/detections/hourly`: which sensors labelled SSM, when, and how many
   SSM-length (76-88 ms) bursts went untried (`attribution` null). This separates "never detected"
   from "detected, not tried" from "tried, not decoded".
2. **Hop grouping on .34** (rows starting within 15 ms of each other): rows per hop and each row's
   frequency offset and level relative to the hop's strongest row. This exposed the copies.
3. **Download** all 30 captures: `GET /captures/download/<base>.sc16` (and `.json`) from each sensor
   into `~/rfobs-data/hcro-ssm-2026-11-06/` (about 5 MB/s over the VPN, shared, so not
   parallelised).
4. **Ground truth from the IQ, without the detector** (`truth.py`): 1 ms frames of 256-bin FFTs; a
   hop is a 60-100 ms run where the strongest bin stays more than 25 dB over its median. True
   width: the hop-averaged 2048-bin spectrum, contiguous span within 20 dB of its peak. The sensor
   with the most hops in a window is the reference; files line up by sample.
5. **Decoder in isolation**: the first four hops of .36 SSM1_neg12 isolated at 100 kHz, 660 kHz and
   2 MHz declared width and decoded (`dec1.py`). Separates decoding from detection.
6. **Replay with sample positions** (`ssm_eval.py`): `run_replay` with isolation and attribution on,
   decoders `ssm` only, rtl_433 stubbed with `/bin/true`, and `RollingBurstDetector.feed` wrapped to
   record each burst's `start_sample`/`stop_sample` by `burst_id`. The rows then join to the truth
   hops by sample range (overlap over 50 % of the hop) and frequency (peak within 150 kHz, or the
   band covers the hop).
7. **Variants** per capture: A 30 dB cap 20 (deployed), B 15 dB cap 20, C 15 dB cap 20 with both
   fixes, D 30 dB cap 20 with both fixes, D1/D2 each fix alone (.36 only), E 15 dB uncapped (the
   detection ceiling). Scored by `score.py`.

## Evidence

Truth hops per capture (reference sensor in brackets): SSM1_neg12 21 (.36), SSM1_0 20 (.36),
SSM1_12 19 (.36), 22:53 none on any sensor. 23:11:30: 2-5 bursts of 60-98 ms on every sensor, not
labelled SSM by any variant, uncapped included, so not SSM; excluded below.

.36 (N1000, beside SSM1), hops labelled / SSM labels on no hop / median reported width:

```
            SSM1_neg12 (21)      SSM1_0 (20)          SSM1_12 (19, true width 190 kHz)
A deployed  21  0   787 kHz      14   9  2774 kHz     13  33  7770 kHz
D1 depth    21  0   127 kHz      13  11   178 kHz     12  33   216 kHz
D2 copy     21  0   787 kHz      19   3  2774 kHz     18   5  7770 kHz
D both      21  0   127 kHz      19   4   178 kHz     18   6   216 kHz
```

Copies gated in D: 0, 64, 91. With the depth limit alone each hop is still one row, but its copies
are no longer swallowed by the hop's wide skirt and come out as rows of their own (136 -> 145 and
148 -> 169 rows), which costs budget; the copy gate takes it back.

Far sensors, SSM1_12 hops detected / labelled:

```
          A 30 dB cap 20   B 15 dB cap 20   E 15 dB uncapped   rows at 15 dB (5 s)
.34       0 / 0            19 / 4           19 / 17            2148
.35       0 / 0            17 / 4           17 / 17             205
.33       0 / 0             4 / 0            4 / 3              355
```

Of .34's 2148 candidates at 15 dB, 21 last 60 ms or more; of .35's 205, 21.

All 390 hop-sensor pairs (6 sensors x 65 truth hops):

```
A  detected  85  labelled 48  SSM labels on no hop 42
B  detected 146  labelled 47                         7
C  detected 146  labelled 38                         4
D  detected  85  labelled 58                        10
E  detected 146  labelled 108                       21
```

Decoder in isolation: every hop decoded `0200574952470001` with 0 bit errors at 100 kHz, 660 kHz and
2 MHz declared width.

## Measured and REJECTED (do not retry)

- **"The inflated bandwidth stops the SSM decoder."** Hops isolated at 100 kHz and at 2 MHz both
  decode with 0 bit errors. The width matters for the bandwidth reported (and for merging neighbours),
  not for labelling.
- **"The copy gate helps at 15 dB with the cap at 20."** C labels 38 against B's 47. At 15 dB the
  budget goes to hundreds of short candidates a second, not to copies.
- **"Lower the threshold to 15 dB as it is."** With the cap at 20, B labels 47 against A's 48 while
  emitting 4-20x the rows.

## Measurement traps hit

- **`python -I` ignores `PYTHONPATH`.** The first replays ran the venv's editable install, which
  points at the main checkout, then on an old docs branch without the SSM decoder, and every one
  reported "0 SSM". The scripts now put `RFOBS_SRC` on `sys.path` themselves. Check with a capture
  whose answer is known (.36 SSM1_neg12: 21/21 live) before trusting a 0.
- **Replay rows carry replay wall-clock times**, not capture times (also in
  2026-11-05_ssm3-not-attributed.md). Sample positions come from the wrapped `feed`.
- **The schedules API showed most runs as `missed / not_running`**, yet every sensor has a file for
  every window. The capture list (`/api/iq-captures?since=&until=`) is the record to trust.
- **A whole-band row (about 26 MHz wide) "covers" any hop** and counts as a detection in the scorer
  (.32 at 15 dB). Those single matches on .32 are not SSM detections.
- **.35 has no `.detections.json` sidecars**; the download reports them as failed. Optional.

## What was done

- `BURST_GROW_DEPTH_DB` (default 25): a burst's extent counts only cells within this many dB of its
  own peak (`processing/burst.py`).
- `ISOLATION_COPY_DB` (default 15): the isolation gate skips a candidate this many dB under another
  in the same batch that starts within 5 ms and lasts within 20 % of it, counted as `copies` inside
  `gated_out` (`pipeline/isolation.py`).

## Open, not yet answered

- **15 dB needs a different budget rule.** A duration-first priority (try bursts of 20 ms or more
  first) would spend .34's 20 a second on the 21 long candidates instead of the 2148 short ones. Not
  built or measured.
- Which SSM was on at 22:53 and 23:11:30: no SSM hop is in either capture. SSM3 (by NbE540) was not
  captured; its live coverage on .34 (40 %) is the copy-budget case above, not re-measured here.
- At SSM1_12 (+12 dB) .36's reported width stays 8 MHz with the depth limit at 15 dB (C): cells
  within 25 dB of a very strong peak span MHz (front-end distortion near the transmitter). Not
  investigated.
- `ISOLATION_COPY_DB` compares bursts only within one isolation batch; a copy that completes in a
  different evaluation than its hop is not caught. Not measured.
