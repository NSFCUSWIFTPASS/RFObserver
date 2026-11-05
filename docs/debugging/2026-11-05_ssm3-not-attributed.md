# SSM3 transmissions at HCRO were not attributed

## The question

2026-11-05: "Is SSM attribution on by default? I did run SSM transmissions and they aren't
attributed." The sensors were HCRO hcro-jet-001 and 003-006 (10.1.42.31, .33-.36), USRP B200
class radios at 26 MS/s centred on 915 MHz with gain 35 dB. All ran 0.13.0b0 (the first release
with the SSM decoder) except .31, which ran 0.12.0b0. .32 was down. The SSM3 transmitter sent
DBPSK hops of about 83 ms each, one about every 240 ms, across 902-928 MHz.

Symptom: no detection carried `model "SSM"` on any sensor. Live detections did include
81-83 ms bursts (5811 on .34 in 50 minutes, 15 on .35, 104 on .36), all with `attribution: null`.

## The answer

Two causes:

1. **Attribution was off** on .34, .35 and .36 (`features.attribution false`,
   `burst_isolation false`). The SSM decoder runs inside attribution, so no burst ever reached
   it. .33 had attribution on but detected nothing after 19:59 UTC. .31 has no SSM decoder in
   0.12.0b0.
2. **On the far sensors, detection is the limit, not the decoder.** At .35 and .36 most SSM3
   hops are 12-30 dB over the noise floor per bin, below `BURST_THRESHOLD_HIGH_DB` 30. Only 4-6
   of the 19 hops cross it. Every hop that is detected decodes with 0 bit errors.

## Procedure

1. **Sensor state**, read-only `GET /api/identity` on each sensor: version and `features`
   (isolation, attribution). This isolates "decoder not running" from "decoder failing".
2. **Live detections**, `GET /api/detections.json?limit=20000`: count bursts lasting 60-100 ms
   (the SSM frame is 83 ms) and look at their `attribution` field. `null` means the burst was
   never attempted; `{"attempted": true, "decoded": false}` means a decoder ran and failed.
3. **Reference capture.** The user scheduled a 5 s capture, `SSM3`, on all sensors at
   2026-11-05T21:56:45Z (schedule e1e25ee6c75e). It was downloaded from .34, .35 and .36 with
   `GET /captures/download/<name>` to `~/rfobs-captures/2026-11-05_SSM3/`. All three started
   within 20 ns of the requested instant on time1.hcro.org, so the captures line up in time.
4. **Offline replay with the decoder on**: `run_replay` on each capture with
   `ISOLATION_ENABLED`, `ATTRIBUTION_ENABLED`, `ISOLATION_MAX_PER_SEC 1000`, and
   `ATTRIBUTION_RTL433_PATH /bin/true`. rtl_433 is stubbed, so only SSM labels are under test.
   This shows what the sensors would have labelled with attribution on.
5. **Hop SNR straight from the IQ**, independent of the detector. The hops were found on .34
   (the strongest sensor) as runs of 60 ms or more where the peak bin is more than 35 dB over its
   median, using 256-bin FFTs averaged over 1 ms. At the same times and frequencies, SNR was
   measured on all three sensors, first with that 101.6 kHz / 1 ms grid and then with 2048-bin
   rows. The 2048-bin rows match the pipeline's grid, so they can be compared directly with the
   30 dB threshold. Each sensor's own threshold was read from its `/config/` page.

## Evidence

Identity (`features`), 2026-11-05 about 21:53Z:

```
hcro-jet-001 NNE60  0.12.0b0  isolation True  attribution True
hcro-jet-003 W230   0.13.0b0  isolation True  attribution True   (newest detection 19:59:42Z)
hcro-jet-004 NbE540 0.13.0b0  isolation False attribution False
hcro-jet-005 N830   0.13.0b0  isolation False attribution False
hcro-jet-006 N1000  0.13.0b0  isolation False attribution False
```

Replay of the 5 s SSM3 captures (rtl_433 stubbed):

```
.34  detections 245  SSM 86  not_decoded 159   main hops (peak > -50 dB) 20, labelled SSM 19
     other 70-95 ms bursts 74, labelled SSM 54   (the +-9.2 MHz copies and image)
     src_id 0200574952470001 x84, fae92a4be7fee93c x1 (preamble_errors 6), 0200576952470001 x1
.35  detections 88   SSM 3   all src 0200574952470001, bit_errors 0, peak about -98 dB
.36  detections 74   SSM 3   all src 0200574952470001, bit_errors 0, peak about -100 dB
```

Hop SNR on 2048-bin rows (dB over the per-bin median), the 19 hops in order:

```
.34 max-pixel   64 68 69 68 67 70 67 67 68 67 68 69 69 63 68 67 68 65 70   >= 30 dB: 19
.36 max-pixel   30 30 28 29 19 27 20 25 30 31 27 33 33 18 29 29 29 28 30   >= 30 dB: 6
.36 row median  27 26 25 26 14 23 16 21 26 28 24 30 29 13 26 26 27 24 27
.35 max-pixel   21 33 25 33 28 27 21 20 26 30 30 25 18 20 21 31 29 23 29   >= 30 dB: 4
.35 row median  17 29 22 29 24 23 16 15 22 26 26 21 12 16 16 28 26 19 25
```

`burst_threshold_high_db` was 30.0 on .34, .35 and .36.

## Measured and REJECTED (do not retry)

- **"The SSM decoder fails on the HCRO hops."** The replay labelled 19 of the 20 main hops on
  .34, and every detected hop on .35 and .36, with 0 preamble errors and 0 or 1-2 bit errors.
  The decoder is not the problem.
- **"OpenZMS or the fleet hides the labels."** The detections rows themselves had
  `attribution: null`, so nothing was attempted on the sensor.

## Measurement traps hit

- **Replay detection times are replay wall-clock time**, not capture time. Times inside the
  capture were taken from the IQ (step 5), not from replay rows.
- **The first replay attempt pointed at `.venv/bin/python`.** On this workstation the venv is
  `.venv310`. The run printed "No such file" for each capture and exited 0, so read the output,
  not just the exit code.
- **The main checkout was on a docs branch older than the SSM decoder.** The replay was run from
  a worktree of main with `PYTHONPATH=<worktree>/src`.
- **.35 has no `.detections.json` for this capture**, so downloading it failed. That file is
  optional.

## What was done

- 2026-11-05, 788ca83: per-decoder switches (`RFOBS_ATTRIBUTION_DECODERS`, a checklist under
  Attribution on the Config page). The SSM decoder now runs when rtl_433 is not installed.

## Open, not yet answered

- Turn Attribution on for .34, .35 and .36 and update .31. This is for the user, on the field
  sensors.
- Lower `BURST_THRESHOLD_HIGH_DB` on .35 and .36 (about 20 dB would catch most hops), at the cost
  of more noise bursts. Not tried.
- Why .33 stopped detecting after 19:59Z while its API stayed up. Not looked at.
- Why .31's `/captures/list` returned an empty body and then timed out. Not looked at.
- Two labels carried a MAC that is not the transmitter's (`fae92a4be7fee93c`,
  `0200576952470001`), because a label needs only the preamble to match. Requiring the MAC to
  match would remove them.
- About 4.3 SSM labels per real hop near the transmitter (the copies and image). Still open, as
  it was in 2026-11-05_ssm-waveform-dbpsk.md.
- One main hop on .34 (926.5 MHz, -48 dB) was not labelled. Not looked at.
