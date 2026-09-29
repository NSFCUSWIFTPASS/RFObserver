# Burst isolation and rtl_433 attribution: acceptance on the workstation and nano-super

## 1. The question

Date: 2026-09-28. Asked by the user: "We also also run the full 26Msps files that the
bursts were derived from so we know the full pipeline works". It is checked against the
spec's success criteria (`docs/superpowers/specs/2026-09-28-burst-isolation-streaming-design.md`):

> with the switches on, a replayed wideband SSN capture on nano-super yields
> SilverSpring-Mesh / protocol 383 on its strong bursts, isolated bursts are saved and
> handed to modules, the live B200mini pipeline keeps its latency and drops nothing
> because of the stage, and every burst the gate picks ends in exactly one reported state.

Build: branch `feat/rtl433-burst-attribution`, HEAD `ea82ffd` (the final fix wave).

Hardware:
- Workstation: x86_64, Python 3.11 venv, rtl_433 at `~/rtl_433_build/build/src/rtl_433`.
- nano-super: "NVIDIA Jetson Orin Nano Developer Kit", L4T R36.5.0, kernel 5.15.185-tegra,
  6 cores, 7607 MB RAM, Python 3.10 (aarch64). Power mode **15W; MAXN unavailable
  (non-Super device tree, the firmware caps it)**, so nvpmodel was not touched. B200mini
  serial 322750B, antenna RX2, gain 40 dB.

Captures: `/mnt/storage/ssn-wide/feb4_19-39-48_915MHz_26Msps_cf32.dat` and
`feb5_05-29-58_915MHz_26Msps_cf32.dat`. Each is raw cf32_le, 26 Msps, 915 MHz center,
1,248,000,000 bytes = 156 M samples = 6.0 s. The nano-super copies have the same md5
(`4d7512f6...`, `328ddae3...`).

## 2. The answer

Yes: the full pipeline works on both hosts. Both full captures, replayed through the
offline harness and through the UI replay path (`POST /api/replay/start`), decode
SilverSpring-Mesh / 383 at 919.43 MHz (feb4) and at 917.03 / 916.99, 913.4 (x3) and
seven more channels (feb5). 904.73 MHz decodes as the `ssnmesh` flex decoder on the
first pass and as 383 on later passes. Every picked burst ends in exactly one state, and
the replay writes SigMF files and `attribution.jsonl` but no DB rows. The nano-super
results match the workstation's burst for burst.

Live on nano-super, the stage costs nothing measurable. Steady-state latency (`excess_ms`)
p50 is 110.6 ms with the switches off and 110.6 ms with them on. There were 0 overflows
in either mode. Dropped chunks happened only in the first 2 s after start (2 with the
switches off, 5 and 6 with them on). RSS went up by about 130 MB and peak RSS by
230 MB, inside the guard's 780 MB budget. The M-5 concern (the read_range copy under
the ring lock) did not show: the longest read_range was 4.4 ms, and ring writes looked
the same in both modes.

Two things are not settled. One live burst out of 95 ended `iq_expired` although it was
only 345 ms old, and this was not reproduced in 143 bursts on a rerun (F3). All the
on-air decodes are `ssnmesh` flex decodes with no CRC; none is protocol 383 (F6).

## 3. The procedure

The probes, in the order they were run, and what each one isolates:

1. **Offline harness, workstation** (done before this task, in `final-fix-report.md`).
   `run_replay(..., datatype="cf32_le", overrides={"ATTRIBUTION_ENABLED": True})` is
   lossless: it is not paced and the receiver waits for detection. This isolates the
   DSP, isolation and attribution chain from any real-time pressure.
2. **UI replay, workstation.** The server ran from a scratch dir (cwd, `.env`,
   STORAGE_PATH and DB_PATH all in the scratchpad) with `RFOBS_ATTRIBUTION_ENABLED=true`,
   `RFOBS_MOCK_RECEIVER=true`, `RFOBS_SENSOR_ACTIVE=false`,
   `RFOBS_REPLAY_SOURCE_DIR=/mnt/storage/ssn-wide`, on port 8888. Each capture was replayed
   with body `{"path", "sample_rate_hz": 26e6, "center_freq_hz": 915e6, "datatype": "cf32_le", "speed": 1.0}`.
   This adds real-time pacing, the drop-on-overflow receiver, the replay-only output
   routing (`bursts/replay-<stem>/`) and the no-DB rule. The API hardcodes `loop=True`
   (see F2), so each run was stopped after one or more passes. Rows were split into
   passes by `start_time_ms` relative to the time of the start request. Checks:
   - `/api/health` isolation counts;
   - `attribution.jsonl`;
   - every `.sigmf-meta` loaded with `sigmf.sigmffile.fromfile` (sigmf 1.13.0);
   - the `detections` row count before and after the replay.
3. **Unit suite and e2e on nano-super.** The branch was shipped as a git bundle and
   fetched into a detached worktree `~/rfobs-attrib`. No ref or branch was created and
   nothing was pushed. It ran with `PYTHONPATH=~/rfobs-attrib/src`, and
   `rfobserver.__file__` was confirmed to be `/home/ocollaco/rfobs-attrib/src/rfobserver/__init__.py`.
   This isolates Python 3.10 / aarch64 portability. The e2e test's decodes were printed
   through a wrapper around the test body. sigmf is not in the Jetson venv, so it was
   installed with `pip --target ~/rfobs-attr-val/pylib` (with jsonschema) and put on the
   path only for the tests that need it. The venv itself was not modified.
4. **Offline harness, nano-super** (the scratchpad `full_replay.py`, unchanged). This
   isolates the 6-core aarch64 box for the lossless path. It prints `isolation_status()`
   just before the stage stops, which is the value `result["isolation"]` carries.
5. **UI replay, nano-super.** Same as probe 2, with the real UHD build (sensor inactive)
   and captures in `~/rfobs-attr-val`.
6. **Live, nano-super.** B200mini, 915 MHz, 26 Msps, streaming (STEP 0), `SENSOR_ACTIVE=true`,
   scratch STORAGE_PATH and DB_PATH, port 8888. Three runs of 10 minutes each. Sampling
   began 20 s after the server came up.
   - `live_off`: ISOLATION and ATTRIBUTION both false. This is the baseline.
   - `live_on`: ATTRIBUTION_ENABLED=true, which turns isolation on as well.
   - `live_on2`: a repeat of `live_on`, adding a log line for every `read_range` that
     returns None, to chase F3. A browser was on the Live page from about 21:48:30 to
     21:49:00 for the screenshot check.

   Instrumentation for the live runs came from a wrapper runner (`val_run.py`). It
   monkeypatches, and no product code was changed:
   - `CircularBuffer.write`: this time includes any wait for the ring lock, so a
     read_range copy that holds the lock would show here as a slow write (M-5).
   - `CircularBuffer.read_range`: duration and size.
   - `_LoopHandoff.submit`: the per-chunk `latency_ms`, which is the value broadcast as
     `excess_ms`.

   Every 60 s it logged a `VALTRACE` line with those numbers plus VmRSS and VmHWM. Also
   every 60 s, `poll.py` read `/api/health` and checked both invariants:
   - `received == gated_out + queue_full + picked`
   - `picked - (isolated + iq_expired + too_long + error) == 0`

   Dropped chunks and overflows come from the pipeline's own `TIMING recv#` log lines
   and from `/api/health` `pipeline.overflow_events`.

## 4. Evidence

### 4.1 Offline harness (lossless)

Workstation, from `final-fix-report.md` (final tree):
```
feb4 FINAL_STATUS counts {"received": 2, "picked": 2, "isolated": 2, "attr_decoded": 1, "attr_not_decoded": 1, "attr_dropped": 0}
     919.431 SilverSpring-Mesh 383 -46.0
feb5 FINAL_STATUS counts {"received": 36, "picked": 35, "isolated": 35, "attr_not_decoded": 22, "attr_decoded": 13, "gated_out": 1, "attr_dropped": 0}
```

nano-super, the same script:
```
feb4 FINAL_STATUS {"enabled": true, "attribution": true, "rtl433": "/home/ocollaco/rtl_433_build/build/src/rtl_433", "ring_sec": 1.5, "disabled_reason": null, "counts": {"received": 2, "picked": 2, "isolated": 2, "attr_decoded": 1, "attr_not_decoded": 1, "attr_dropped": 0}}
ELAPSED 8.0
feb5 FINAL_STATUS {"enabled": true, "attribution": true, "rtl433": "/home/ocollaco/rtl_433_build/build/src/rtl_433", "ring_sec": 1.5, "disabled_reason": null, "counts": {"received": 36, "picked": 35, "isolated": 35, "attr_not_decoded": 21, "attr_decoded": 14, "gated_out": 1, "attr_dropped": 0}}
ELAPSED 10.7
```
nano-super decodes (peak MHz, model, protocol, peak dB):
```
feb4  919.431 SilverSpring-Mesh 383 -46.0
feb5  904.729 ssnmesh None -71.9        913.73  SilverSpring-Mesh 383 -69.5
      911.331 SilverSpring-Mesh 383 -74.7  916.13 ssnmesh None -67.6
      912.194 SilverSpring-Mesh 383 -69.0  916.993 SilverSpring-Mesh 383 -64.2
      913.4   SilverSpring-Mesh 383 -71.8  917.031 SilverSpring-Mesh 383 -63.7
      913.4   SilverSpring-Mesh 383 -71.9  920.332 SilverSpring-Mesh 383 -70.1
      913.4   SilverSpring-Mesh 383 -72.0  920.903 ssnmesh None -73.1
      922.122 SilverSpring-Mesh 383 -66.9  922.998 SilverSpring-Mesh 383 -69.2
```
iq_expired is 0 on both hosts. nano-super decoded 14: the same 11 protocol-383 decodes
plus 3 ssnmesh, including the 920.903 flex decode that the workstation's final run did
not get (it got it in an earlier run; see §6).

### 4.2 UI replay (paced, drop-on-overflow)

Workstation feb4 (stopped during pass 2):
```
{"received": 4, "picked": 4, "isolated": 4, "attr_decoded": 2, "attr_not_decoded": 2, "attr_dropped": 0}
919.430664 MHz  SilverSpring-Mesh 383 decoded   snr 67.8   (pass 1)  freq_low 902.0 / freq_high 927.99 MHz
926.032    MHz  not_decoded                     snr 51.1   (pass 1)
919.430664 MHz  SilverSpring-Mesh 383 decoded   snr 67.9   (pass 2)
926.032    MHz  not_decoded                     snr 51.7   (pass 2)
frame: "model": "SilverSpring-Mesh", "src_id": "00135003004b9712", "channel": 57, "len": 111, "mic": "CRC"
```

nano-super feb4 (identical):
```
  1.024   919.431 bw=25.987 snr= 67.8 decoded      SilverSpring-Mesh 383
  1.335   926.032 bw= 0.927 snr= 51.1 not_decoded  None None
  8.525   919.431 bw=25.987 snr= 67.9 decoded      SilverSpring-Mesh 383
  8.916   926.032 bw= 1.231 snr= 51.7 not_decoded  None None
```

feb5 pass 1, which is **identical row for row on both hosts** (columns: seconds from the
start request on nano-super, peak MHz, detected bandwidth, SNR, outcome):
```
  0.558   918.771 bw=18.611 snr= 57.9 not_decoded  None None
  1.393   911.331 bw= 0.216 snr= 39.2 decoded      SilverSpring-Mesh 383
  1.540   921.830 bw= 0.127 snr= 33.1 not_decoded  None None
  1.697   919.672 bw=15.882 snr= 57.8 not_decoded  None None
  1.707   919.723 bw=15.666 snr= 58.6 not_decoded  None None
  5.856   920.903 bw= 0.216 snr= 39.9 decoded      ssnmesh None
  5.863   909.173 bw= 0.127 snr= 33.5 not_decoded  None None
  5.871   909.173 bw= 0.127 snr= 32.7 not_decoded  None None
  5.875   920.929 bw= 0.203 snr= 39.9 not_decoded  None None
  5.884   920.332 bw= 0.292 snr= 42.8 decoded      SilverSpring-Mesh 383
  5.925   909.528 bw= 0.241 snr= 39.6 not_decoded  None None
  5.945   909.528 bw= 0.203 snr= 38.8 not_decoded  None None
  5.964   909.528 bw= 0.203 snr= 39.6 not_decoded  None None
  5.969   904.729 bw= 0.394 snr= 41.6 not_decoded  None None
  5.977   904.729 bw= 0.368 snr= 41.6 decoded      ssnmesh None
  6.144   916.130 bw= 0.279 snr= 45.8 decoded      ssnmesh None
  6.218   902.927 bw= 0.470 snr= 45.1 not_decoded  None None
  6.324   913.400 bw= 0.305 snr= 41.9 decoded      SilverSpring-Mesh 383
  6.332   913.400 bw= 0.254 snr= 41.7 decoded      SilverSpring-Mesh 383
  6.352   913.400 bw= 0.457 snr= 41.7 decoded      SilverSpring-Mesh 383
  6.518   911.572 bw= 0.114 snr= 31.1 not_decoded  None None
  6.942   913.730 bw= 0.355 snr= 44.4 not_decoded  None None
  6.959   913.730 bw= 0.343 snr= 44.1 decoded      SilverSpring-Mesh 383
  7.014   917.031 bw= 0.330 snr= 49.3 decoded      SilverSpring-Mesh 383
  7.021   916.993 bw= 0.419 snr= 48.8 decoded      SilverSpring-Mesh 383
  7.242   919.101 bw= 0.495 snr= 44.0 not_decoded  None None
  7.360   915.521 bw= 0.292 snr= 47.4 not_decoded  None None
  7.508   922.122 bw= 0.292 snr= 46.3 decoded      SilverSpring-Mesh 383
  7.820   913.096 bw= 0.432 snr= 43.5 not_decoded  None None
  7.948   922.998 bw= 0.305 snr= 44.3 decoded      SilverSpring-Mesh 383
  7.968   922.998 bw= 0.419 snr= 44.2 not_decoded  None None
  7.986   922.998 bw= 0.254 snr= 44.2 not_decoded  None None
  7.991   912.194 bw= 0.229 snr= 44.8 decoded      SilverSpring-Mesh 383
  8.040   907.129 bw= 0.254 snr= 47.8 not_decoded  None None
  8.088   913.730 bw= 0.254 snr= 43.9 not_decoded  None None
```
That is 35 rows (35 picked, as offline), with 14 decoded: 11 x 383 and 3 x ssnmesh.
Pass 2 is also identical on both hosts, and it differs from pass 1 in the same way on
both:
- 920.929 decodes as 383, and 920.903 is not decoded;
- 904.704 decodes as 383;
- 916.104 decodes as 383 (it was ssnmesh in pass 1);
- 916.993 is not decoded, while 916.968 decodes.

Final health snapshots (taken just before `/replay/stop`):
```
workstation feb5 (about 3 passes): {"received": 110, "picked": 105, "isolated": 103, "attr_not_decoded": 59, "attr_decoded": 44, "gated_out": 5, "attr_dropped": 0}
   attribution.jsonl rows: 105; sigmf-meta 105, sigmf-data 105
nano-super feb5 (2+ passes):       {"received": 78, "picked": 75, "isolated": 75, "attr_not_decoded": 45, "attr_decoded": 30, "gated_out": 3, "attr_dropped": 0}
   attribution.jsonl rows: 75; sigmf-meta 75, sigmf-data 75
```
On the workstation, 2 of the 105 picked bursts were in flight when the snapshot was
taken. All 105 reached `attribution.jsonl` as `isolated`, so the invariant holds once the
stage is idle (see §6).

SigMF load, sigmf 1.13.0 (a sample; every file loaded):
```
9a6a9192 sigmf 1.13.0 ci16_le 1600000 919430664.0625 n 23100 complex64 ann []
d34c6809 sigmf 1.13.0 ci16_le 1600000 926032226.5625 n 22784 complex64 ann []
08384875 sigmf 1.13.0 ci16_le 1600000 912194335.9375 n 11757 complex64 ann []
```
DB: `detections` was 0 before and 0 after all four UI replays on both hosts.

Pacing (workstation server log): the IQ per chunk is 39.4 ms, but chunks were delivered
every 43 to 49 ms:
```
15:16:47,853 TIMING recv#50: recv=46.1ms dropped=0 (IQ=39.4ms) handoff_dropped=0/0 ovf=0 lost=0
15:16:50,115 TIMING recv#100: recv=42.4ms dropped=0 (IQ=39.4ms) ...
PROC chunk#100: process=54.2ms latency=104.6ms (IQ=39.4ms)
```
50 chunks (1.97 s of IQ) took about 2.27 s. nano-super: pass period 7.5 s for a 6.0 s
file. See F1.

### 4.3 Unit and e2e on nano-super

```
FAILED tests/unit/test_burst_archive.py::test_save_writes_a_loadable_sigmf_pair
1 failed, 861 passed, 3 warnings in 93.54s
>       import sigmf  # the official library
E       ModuleNotFoundError: No module named 'sigmf'
```
With sigmf from the scratch target dir on the path: `7 passed` for that file.
```
test_isolation_attribution_e2e.py::test_ssn_bursts_decode_through_the_streaming_pipeline PASSED
test_isolation_replay_lookback.py::test_lossless_replay_isolates_every_picked_burst_at_the_default_lookback PASSED
2 passed in 13.06s
```
e2e decodes (from the wrapper):
```
ISOLATION counts {'received': 3, 'picked': 3, 'isolated': 3, 'attr_decoded': 2, 'attr_not_decoded': 1, 'attr_dropped': 0}
DET 913.4   None None
DET 917.901 SilverSpring-Mesh 383
DET 919.405 SilverSpring-Mesh 383
```
This matches the known behaviour of the 913.4 fixture (Task 7 minor: pedestal from the
synthetic construction). The three `~/ssn_bursts` fixtures on nano-super are byte
identical to the workstation's (md5 `cd2011d7...`, `558b47cc...`, `79164b39...`).

### 4.4 Live, nano-super, 915 MHz / 26 Msps

Chunk: `chunk=1024000 samples (39.4 ms), 3 PSD workers`. Ring:
- off: `pre-trigger=1.00s (26000000 samples)`, which is 104 MB;
- on: `pre-trigger=1.50s (39000000 samples)`, which is 156 MB.

Per-minute VALTRACE (latency = `excess_ms`; wr = ring write including lock wait; rr =
read_range):

| run | minute | lat p50 | lat p99 | lat max | wr mean | wr max | wr>5ms | rr n | rr max ms | rr max samples | RSS MB | HWM MB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| off | 1 (startup) | 110.7 | 190.9 | 343.0 | 0.82 | 24.1 | 6 | 0 | - | - | 534 | 534 |
| off | 2-10 range | 110.4-110.6 | 134.3-135.2 | 140.0-163.5 | 0.75-0.78 | 2.9-9.5 | 0-1 | 0 | - | - | 495-543 | 537-543 |
| on | 1 (startup) | 110.8 | 175.3 | 461.0 | 0.84 | 27.2 | 8 | 7 | 1.3 | 1,476,160 | 627 | 670 |
| on | 2-10 range | 110.5-110.8 | 130.5-139.9 | 144.3-181.7 | 0.76-0.78 | 2.7-6.7 | 0-1 | 6-12 | 0.5-4.4 | up to 4,153,920 | 598-705 | 744-769 |
| on2 | 1 (startup) | 110.7 | 218.7 | 444.3 | 0.84 | 18.4 | 11 | 10 | 1.2 | 1,476,160 | 635 | 684 |
| on2 | 10 | 110.4 | 134.6 | 151.6 | 0.76 | 3.8 | 0 | 9 | 0.6 | 339,520 | 700 | 777 |

Raw lines (first and last of `live_on`):
```
VALTRACE lat_n=1433 lat_p50=110.8 lat_p99=175.3 lat_max=461.0 wr_n=1440 wr_mean=0.84 wr_max=27.2 wr_over5=8 wr_over20=1 rr_n=7 rr_mean=0.66 rr_max=1.3 rr_max_samples=1476160 rss_mb=627 hwm_mb=670 ring_caps=[39000000] ...
VALTRACE lat_n=1524 lat_p50=110.6 lat_p99=138.9 lat_max=181.7 wr_n=1525 wr_mean=0.76 wr_max=6.7 wr_over5=1 wr_over20=0 rr_n=9 rr_mean=0.76 rr_max=3.0 rr_max_samples=4153920 rss_mb=663 hwm_mb=769 ring_caps=[39000000] all_wr_max=27.2 all_rr_max=4.4 all_lat_max=461.0 all_wr_over20=1
```

Dropped chunks: every drop happened in the first 50 chunks (the first 2 s), and the
counter never moved after that in any run:
```
live_off  21:22:55,334 TIMING recv#50: recv=38.5ms dropped=2 (IQ=39.4ms) handoff_dropped=0/0 ovf=0 lost=0
live_on   21:33:34,942 TIMING recv#50: recv=38.6ms dropped=5 (IQ=39.4ms) handoff_dropped=0/0 ovf=0 lost=0
live_on2  21:45:50,055 TIMING recv#50: recv=38.5ms dropped=6 (IQ=39.4ms) handoff_dropped=0/0 ovf=0 lost=0
last line of each run: recv#15900 (live_off, live_on) / recv#15750 (live_on2): dropped unchanged, ovf=0 lost=0
```
`/api/health` `overflow_events` was 0 in all 33 samples.

`/api/health` per minute, `live_on` (`inv` is the received invariant, followed by picked
minus the terminal states):
```
21:33:54 0 {"received": 1, "picked": 1, "isolated": 1, "attr_not_decoded": 1} inv True 0 rss 621
21:34:54 1 {"received": 11, "picked": 11, "isolated": 11, "attr_not_decoded": 10, "attr_decoded": 1} inv True 0 rss 626
21:35:54 2 {"received": 23, "picked": 23, "isolated": 23, ...} inv True 0 rss 648
21:36:54 3 {"received": 33, "picked": 33, "isolated": 33, ...} inv True 0 rss 662
21:37:54 4 {"received": 40, "picked": 40, "isolated": 40, ...} inv True 0 rss 662
21:38:54 5 {"received": 49, "picked": 49, "isolated": 49, ...} inv True 0 rss 598
21:39:54 6 {"received": 54, "picked": 54, "isolated": 54, ...} inv True 0 rss 662
21:40:54 7 {"received": 63, "picked": 63, "isolated": 63, ...} inv True 0 rss 662
21:41:54 8 {"received": 75, "picked": 75, "isolated": 75, ...} inv True 0 rss 705
21:42:55 9 {"received": 86, "picked": 86, "isolated": 86, "attr_not_decoded": 84, "attr_decoded": 2} inv True 0 rss 705
21:43:55 10 {"received": 95, "picked": 95, "isolated": 94, "attr_not_decoded": 91, "attr_decoded": 3, "iq_expired": 1, "attr_dropped": 0} inv True 0 rss 674
```
`live_on2`: all 11 samples `inv True 0`, ending at
`{"received": 143, "picked": 143, "isolated": 143, "attr_not_decoded": 133, "attr_decoded": 10}`.
There were no `VALNONE` lines, so no read_range returned None in that run.
`live_off`: `iso_enabled False`, `ring_sec 1.0`, empty counts.

In both modes, gated_out and queue_full were 0 live and attr_dropped was 0.

On-air decodes (all `ssnmesh`, the `-X` flex pass, and none is protocol 383):
```
live_on   917.603 -75.8 dB 16.3 ms {814 bits}; 905.199 -68.5 dB 6.7 ms {343}; 921.602 -67.6 dB 12.6 ms {615}
live_on2  912.804, 907.599, 926.400, 920.002, 923.595, 910.404, 909.604, 915.203 (each 36.4-36.6 ms, about every 20 s, -66.6 to -74.7 dB),
          916.003 (27.4 ms), 907.205 (12.6 ms)
```
In `live_on`, the DB had 95 detections and 94 with attribution. The one without is the
`iq_expired` burst (F3). In `live_off` there were 92 detections and 0 with attribution.
`bursts/20260928/` in `live_on` held 94 SigMF pairs (9.6 MB).

Dashboard: the Live page waterfall showed an `ssnmesh` label at the top right (on2,
about 21:48:40, with High Res off). A screenshot was taken in the session, but it was
not saved to disk.

## 5. Measured and REJECTED (do not retry)

- **"The stage's read_range copy under the ring lock stalls the receiver" (M-5).** Rejected.
  The longest read_range over 30 minutes was 4.4 ms, for 4,153,920 samples (a 155 ms
  burst plus guard). Steady-state ring writes (the time includes waiting for the lock)
  averaged 0.76 to 0.78 ms both on and off. The worst steady-state write was 9.5 ms
  with the switches off and 6.7 ms with them on. There were 0 overflows. Do not
  re-measure this at 26 Msps. At 56 Msps the copy doubles, which is still far below the
  39 ms chunk.
- **"Isolation raises pipeline latency".** Rejected at 26 Msps. The steady p50 is
  110.4 to 110.8 ms in both modes, and p99 is 130 to 140 ms in both.
- **"The 6-core box outruns detection and expires bursts in lossless replay".** Rejected:
  iq_expired is 0 on nano-super offline, and 35 of 35 picked bursts are isolated on feb5.
- **"The Jetson decodes differently from the workstation".** Rejected. UI pass 1 and
  pass 2 are row-for-row identical, and offline gives the same 11 protocol-383 decodes.

## 6. Measurement traps

- **A health snapshot during activity is not a check of the invariant.** The workstation
  feb5 snapshot shows `picked 105, isolated 103` with no other terminal state, because 2
  bursts were being processed. Check the invariant only at 1-minute polls (where it
  held every time) or after the stage is idle. The jsonl had all 105 rows.
- **The brief's invariant differs from the code's.** The brief writes "picked =
  isolated + iq_expired + too_long + queue_full + error". The code counts queue_full
  before picking: `received = gated_out + queue_full + picked` and
  `picked = isolated + iq_expired + too_long + error`. This record checks the code's
  form.
- **UI replay loops.** `/api/replay/start` always passes `loop=True`, so the counts
  include later passes. Split by `start_time_ms` against the start time. Pass boundaries
  are not at 6.0 s intervals (F1).
- **The ssnmesh flex decode of 920.903 MHz varies.** The workstation offline run got 13
  (without it) in one run and 14 in another; nano-super offline and both UI pass-1 runs
  got it. The protocol-383 set is stable. Compare 383 counts, not attr_decoded.
- **The first minute of every live run carries startup costs**: p99 175 to 219 ms, a
  max of 343 to 461 ms, and write spikes up to 27 ms, in both modes. Compare minutes 2
  to 10.
- **Pytest's default basetemp** (`/tmp/pytest-of-ocollaco`) was used by the first Jetson
  unit run. This run's `pytest-10` was deleted afterwards and `pytest-current` was
  repointed to `pytest-9`. Pytest's own 3-dir rotation may have removed an older
  `pytest-7`. Later runs used `--basetemp` under the scratch dir.

## 7. Findings (not fixed here; product code unchanged)

- **F1 (Minor): paced replay runs slower than real time.** `FileReplayReceiver.recv_chunk`
  converts the chunk and then sleeps a full chunk duration (`time.sleep(n / (fs * speed))`),
  so the conversion time adds to every period. Measured: 0.86x real time on the
  workstation (43 to 49 ms per 39.4 ms chunk) and 0.80x on nano-super (a 7.5 s pass for
  6.0 s of IQ). Wall-clock burst timestamps in replay are stretched to match. This does
  not hurt isolation: it gives the stage more wall time, not less.
- **F2 (Minor, known as M-2): UI replay cannot be single-pass.** `loop=True` is hardcoded
  in `replay_start`. Each pass re-isolates and re-decodes the same bursts.
  Loop-to-loop decode differences are deterministic and the same on both hosts, which
  points to the burst boundaries changing when the loop splice shifts the chunk grid.
- **F3 (Open): one live `iq_expired` for a young burst.** DB row 87, start
  21:43:03.479, stop +9.1 ms, detected 21:43:03.824 (345 ms later), 925.994 MHz,
  76 kHz, -87.7 dB. It was one of 3 bursts in one batch ("Detected 3 bursts" at
  21:43:03,849). The ring holds 1.5 s, so the samples could not have been overwritten
  unless the positions were wrong. `isolate_burst` returns `"iq_expired"` whenever
  `read_range` returns None. That covers both "overwritten" (`start < oldest`) and "not
  yet written" (`stop + guard > total_written`), so the state name cannot tell the two
  apart. The rerun (`live_on2`, 143 bursts, None-read logging) did not reproduce it.
  The cause is not determined.
- **F4 (Observation): the strongest bursts are detected as very wide.**
  - feb4 919.43 MHz is detected as 902.0 to 927.99 MHz (25.99 MHz wide).
  - feb5 has 918.77 / 919.67 / 919.72 MHz at 15.7 to 18.6 MHz wide, SNR 58.
  - Isolation cuts at the peak frequency at the tier rate (1.6 Msps here), and feb4
    still decodes.
  - The feb5 wide ones do not decode, except one pass-3 row (919.723 as 383). The same
    happens offline.
  - Whether these are splatter from strong SSN bursts or something else was not checked.
- **F5 (Minor, test hygiene): the unit test needs sigmf.**
  `tests/unit/test_burst_archive.py::test_save_writes_a_loadable_sigmf_pair` imports
  `sigmf` unconditionally and fails on a host without it (nano-super's venv). It passes
  when sigmf is present.
- **F6 (Observation): on-air decodes are flex only.** There were 13 on-air
  `ssnmesh` decodes (3 + 10) and no protocol-383 decode. In `live_on2`, eight were 36.4
  to 36.6 ms bursts hopping about every 20 s. Flex decodes have no CRC, so these are
  not confirmed SSN.
- **F7 (Observation): startup drops are slightly higher with the stage on.** 5 and 6
  chunks were dropped with the switches on, against 2 with them off. All of them were in
  the first 2 s and none came later. The likely cause is the larger ring allocation and
  the stage starting up, but this was not isolated.

## 8. Corrections

- The controller's note said nano-super's `~/ssn_bursts` was missing
  `burst_feb5_917MHz_56dB.cs16`. It was present, and all three fixtures matched the
  workstation's md5, so nothing was copied and the e2e test used its fixed path.
- Spec §Acceptance says "nano-super, MAXN" and "baseline (about 270 ms)". The run was at
  15W because MAXN is unavailable. The measured 26 Msps baseline is 110.6 ms p50, so the
  270 ms figure does not apply to this configuration.

## 9. Open, not yet answered

- The cause of F3's live `iq_expired`: overwritten, not yet written, or a stale position.
  Next probe: log `start - oldest` and `end - total_written` for every None read (the
  `VALNONE` wrapper does this) over a longer live run, or split the state into two
  counters.
- Latency and lookback at 56 Msps under load (spec open item): not measured. This run was
  26 Msps only.
- MAXN behaviour: not possible on this board.
- Whether the wide (15 to 26 MHz) detections in F4 are real signals or detector artefacts
  around strong bursts.
- Whether the live 36.4 ms, about 20 s periodic flex decodes (F6) are SSN at all.
- Why the 920.903 MHz flex decode comes and goes offline on the workstation. It was
  stable in every nano-super and UI pass-1 run here.

## Cleanup

nano-super was left as found:
- `~/rfobs-attrib` (worktree) and `~/rfobs-attr-val` (captures, bundle, scratch runs,
  pylib) were removed, followed by `git worktree prune`;
- no branch or ref was created, and no process is running;
- `~/GitHub/RFObserver` is still on `feat/averaged-window-store` with `stash@{0}`
  intact;
- `~/rfobs-replay-data`, `~/rfobs-stall`, `~/rfobs-stalltest` and `~/ssn_bursts` were
  untouched;
- nvpmodel is still 15W.

On the workstation, the bundle was deleted and no server is left on 8888.

## CORRECTION / follow-up 2026-09-28: F3 root cause

This section is appended; sections 1 to 9 above are unchanged. It overturns section 7's
line "The ring holds 1.5 s, so the samples could not have been overwritten unless the
positions were wrong" and closes section 9's first open item for the mechanism, not for
the live instance (see "Open" below).

### Question

Why did the F3 burst (DB row 87, 925.994 MHz, 9.1 ms, detected 345 ms after its stop,
one of 3 in its batch) end `iq_expired` on nano-super (Jetson Orin Nano, 15W, 6 cores,
26 Msps, 1.5 s ring), and how is it fixed? Build: `feat/rtl433-burst-attribution` at
`942899d`.

### Answer

The positions were right; the samples really were overwritten. `IsolationStage.process_batch`
handled a batch's picked bursts strongest first and read each one from the ring just
before its own DSP. `channelize_to_cs16` cost about 4.5 s per second of burst IQ on
nano-super (734 ms for a 160 ms burst), so the stronger bursts' DSP (about 1.5 s here,
by the controller's analysis of the batch) let the receiver overwrite the weakest burst
before its turn. More generally the stage saturated once picked burst airtime passed
about 20% of wall time.

Fixed in three commits plus one follow-up:

- `7223330` Snapshot first: after the gate, every picked ring burst's raw samples are
  copied before any DSP (bounded at 256 MB of raw ring data per batch; beyond that,
  bursts are read just before their DSP and one INFO line is logged per batch).
  `processing/isolate.py` now has `burst_read_range` (the range, guard and truncation)
  and `isolate_samples` (the DSP); `isolate_burst` composes them and behaves as before.
- `f1ec477` FFT channelizer: overlap-save frequency-domain channelization (keep the
  output band's bins around the offset, raised-cosine taper over the outer 5% of the
  band, inverse FFT, restore one continuous mix's phase, remove the sub-bin residual).
  Output length is `ceil(n * up / down)`, identical to resample_poly's.
- `d03f004` Diagnosable expiry: one WARNING per expired ring burst with the reason
  (`overwritten`, `unwritten` or `nopos`), the read range, the ring's
  `[oldest, total_written)` right after the failed read and the age from `stop_sample`
  to `total_written`. The counters `iq_expired_overwritten`, `iq_expired_unwritten` and
  `iq_expired_nopos` sit alongside `iq_expired` in health and on the Config page.
  `CircularBuffer.bounds()` returns both ends under the lock.
- `19986d2` Channelizer follow-up: 32768-sample blocks, transformed 16 at a time straight
  from the input (no burst-sized padded copy). The RAM guard now budgets 3 complex64
  copies (measured peak 1.4) plus the snapshot, instead of 6 copies.

### Procedure

1. Reproduce the mechanism deterministically:
   `test_a_weak_burst_is_not_evicted_by_the_dsp_of_stronger_ones` in
   `tests/unit/test_isolation_stage.py`. A 400k-sample ring holds 3 picked bursts; a
   patched `channelize_to_cs16` writes 50k samples into the ring per call (the receiver
   running during DSP). The weakest, oldest burst's read starts at 16k, so it is gone
   after one DSP call. This controls for positions: they are exact by construction.
2. Run that test against the untouched `942899d` source (`git archive 942899d src`,
   `PYTHONPATH` pointed at it): RED.
3. Benchmark the channelizer alone and the read plus isolate path on nano-super, old
   (`942899d`) against new (`19986d2`), in fresh processes, each case once per process,
   two runs.
4. Replay both full captures through the offline harness (`full_replay.py`) before and
   after, and compare every burst's final state and decode by peak frequency, centre
   frequency and power.
5. Compare the FFT channelizer against the resample_poly path in band (FSK test signal)
   and measure tone spurs, alias rejection and output length (unit tests).

### Evidence

RED on the old code (step 2):

```
E       AssertionError: assert [('b0', 'isol...'iq_expired')] == [('b0', 'isol..., 'isolated')]
E         At index 2 diff: ('b2', 'iq_expired') != ('b2', 'isolated')
1 failed, 27 deselected in 0.33s
```

nano-super, 15W, `bench_isolate.py` (`channelize_to_cs16` on complex64, offset 1.2 MHz,
26 Msps), second run of each (the first run matched within 3 ms):

```
== BEFORE (942899d)                         == AFTER (19986d2)
dur=10ms rate=1000000 isolate=53ms          dur=10ms rate=1000000 isolate=12ms
dur=10ms rate=1600000 isolate=49ms          dur=10ms rate=1600000 isolate=8ms
dur=50ms rate=1000000 isolate=239ms         dur=50ms rate=1000000 isolate=33ms
dur=50ms rate=1600000 isolate=235ms         dur=50ms rate=1600000 isolate=29ms
dur=160ms rate=1000000 isolate=733ms        dur=160ms rate=1000000 isolate=90ms
dur=160ms rate=1600000 isolate=732ms        dur=160ms rate=1600000 isolate=86ms
dur=500ms rate=1000000 isolate=2259ms       dur=500ms rate=1000000 isolate=262ms
dur=500ms rate=1600000 isolate=2249ms       dur=500ms rate=1600000 isolate=259ms
```

That is 8.1x to 8.5x at 160 ms and 8.6x to 8.7x at 500 ms. The stage's full per-burst
cost (int32 read, `iq_to_complex`, channelize, pack; `isolate_burst` from an int32
array):

```
== STAGE read+isolate BEFORE                 == STAGE read+isolate AFTER
dur=10ms rate=1000000 read+isolate=77ms      dur=10ms rate=1000000 read+isolate=21ms
dur=10ms rate=1600000 read+isolate=73ms      dur=10ms rate=1600000 read+isolate=16ms
dur=50ms rate=1000000 read+isolate=269ms     dur=50ms rate=1000000 read+isolate=54ms
dur=50ms rate=1600000 read+isolate=264ms     dur=50ms rate=1600000 read+isolate=47ms
dur=160ms rate=1000000 read+isolate=803ms    dur=160ms rate=1000000 read+isolate=121ms
dur=160ms rate=1600000 read+isolate=787ms    dur=160ms rate=1600000 read+isolate=124ms
dur=500ms rate=1000000 read+isolate=2426ms   dur=500ms rate=1000000 read+isolate=366ms
dur=500ms rate=1600000 read+isolate=2405ms   dur=500ms rate=1600000 read+isolate=372ms
```

So the stage now spends about 0.75 s per second of burst IQ, not about 4.8 s. The
int32 to complex64 conversion is now roughly a third of the per-burst cost.

Full captures on the workstation (offline harness, default 1.5 s lookback), counts:

```
feb4 before: received 2, picked 2, isolated 2, attr_decoded 1, attr_not_decoded 1
feb4 after:  received 2, picked 2, isolated 2, attr_decoded 1, attr_not_decoded 1
feb5 before: received 36, picked 35, isolated 35, gated_out 1, attr_decoded 14, attr_not_decoded 21, iq_expired 0
feb5 after:  received 36, picked 35, isolated 35, gated_out 1, attr_decoded 14, attr_not_decoded 21, iq_expired 0
```

Decodes (peak MHz, centre MHz, power dB, model, protocol):

```
feb4 before and after (identical):
   (919.431, 914.994, -46.05, 'SilverSpring-Mesh', 383)
feb5 before:                                              feb5 after:
   (904.729, 904.774, -71.867, 'ssnmesh', None)              (904.729, 904.774, -71.867, 'ssnmesh', None)
                                                             (904.729, 904.787, -71.865, 'ssnmesh', None)   gained
   (911.331, 911.299, -74.671, 'SilverSpring-Mesh', 383)     same
   (912.194, 912.201, -69.003, 'SilverSpring-Mesh', 383)     same
   (913.4,   913.312, -71.967, 'SilverSpring-Mesh', 383)     same
   (913.4,   913.388, -71.805, 'SilverSpring-Mesh', 383)     same
   (913.4,   913.413, -71.924, 'SilverSpring-Mesh', 383)     same
   (913.73,  913.737, -69.465, 'SilverSpring-Mesh', 383)     same
   (916.13,  916.13,  -67.631, 'ssnmesh', None)              same
   (916.993, 917.0,   -64.246, 'SilverSpring-Mesh', 383)     same
   (917.031, 917.006, -63.732, 'SilverSpring-Mesh', 383)     same
   (920.332, 920.326, -70.108, 'SilverSpring-Mesh', 383)     same
   (920.903, 920.897, -73.101, 'ssnmesh', None)              lost (not decoded)
   (922.122, 922.128, -66.851, 'SilverSpring-Mesh', 383)     same
   (922.998, 923.036, -69.245, 'SilverSpring-Mesh', 383)     same
```

All 11 protocol-383 decodes on feb5 and the feb4 919.43 MHz decode are unchanged. The
only differences are two `-X ssnmesh` flex decodes (no CRC): 904.787 gained and 920.903
lost. Before and after were each run 3 times on feb5 and every run matched its own
kind burst for burst, so the difference is the channelizer's and not run-to-run noise.
The SSN e2e test (`tests/integration/test_isolation_attribution_e2e.py`, default
lookback) passes.

Channelizer quality (unit tests in `tests/unit/test_channelize.py`):
- In band against the resample_poly path, FSK at 20 kbaud with +-50 kHz deviation:
  -58.2 dB (26 to 1.6 Msps), -54.7 dB (26 to 1.0 Msps, band wrapping past -fs/2),
  -58.6 dB (56 to 1.6 Msps), -53.3 dB (2 to 1.6 Msps). The test asserts < -40 dB,
  because the old test's <= 2 LSB bitwise match cannot hold with a different low-pass
  filter.
- Tones at offset, including non-bin-aligned, wrapping, odd-rate and upsampling cases:
  land on DC, worst spur -108 to -143 dB (asserted < -40 dB), and phase continuous
  across block edges (phase std < 2e-7 rad).
- An equal-power tone 1.0 MHz from the burst (it would alias to -600 kHz at 1.6 Msps)
  stays 40 dB down.

Memory, one 0.5 s burst at 26 Msps (`ru_maxrss` over baseline, in complex64 copies of
the burst): resample_poly path 2.34, one-shot FFT 3.85, first block version (padded
copy, 64 blocks per call) 2.8, final version 1.39.

Unit suite on nano-super (`PYTHONPATH=/tmp/f3b/src`, clone of `19986d2`):
`883 passed, 3 warnings in 93.57s`. Workstation: ruff, format, mypy (also clean in the
CI-like lint venv), 883 unit passed, integration 110 passed and 10 skipped (NATS).

### Measured and REJECTED (do not retry)

- **`scipy.fft` with `workers=-1`.** The task suggested it. On nano-super it gave no
  speedup: 500 ms burst 230 to 242 ms with `workers=1`, 222 to 249 ms with 2, 251 to
  269 ms with 3 and 239 to 250 ms with -1. The transforms are memory-bound. The FFTs run
  on one thread, so they do not compete with the receiver.
- **One FFT of the whole burst** (the first version): 3.3x on the workstation and 3.4x
  on nano-super (500 ms burst 678 to 700 ms). A single 13 M point transform is memory-
  bound. It survives only as the fallback for sample rates whose ratio to the tier rate
  does not reduce to a short block (for example 26,000,007 Hz), where its output rate is
  off by less than 1 / output length (12 ppm for the 50 ms unit-test tone, 25 ppm for a
  500k-sample burst).
- **Larger blocks.** On nano-super, 65536-sample blocks with a 128-sample edge cost
  98 to 112 ms (160 ms burst) and 284 to 320 ms (500 ms burst), against 86 to 90 and
  259 to 266 ms for 32768 / 64. 131072 was slower still (121 / 362 to 381 ms). The
  factor of 13 in 26 Msps / 1.6 Msps (65) makes every block length 13-smooth:
  66560-point FFTs cost 16.4 ns per sample against 11.1 for 65536.
- **A wider taper to raise decode counts** (not adopted, recorded as open below). Same
  harness, feb5:

  ```
  taper 0.02: attr_decoded 13, protocol 383 x11, 920.903 flex lost
  taper 0.05: attr_decoded 14, protocol 383 x11 (shipped)
  taper 0.10: attr_decoded 14, protocol 383 x13 (904.774 and 916.13 become 383)
  taper 0.20: attr_decoded 14, protocol 383 x14 (904.787 also 383); feb4 unchanged
  taper 0.30: attr_decoded 13, protocol 383 x12
  ```

  A 20% roll-off narrows the flat passband to +-480 kHz of the 1.6 MHz band, which
  keeps out noise. It was not adopted: it is tuned on one capture, it also changes the
  1.0 Msps default-decoder tier, and the task asked for a gentle taper.

### Measurement traps

- `bench_isolate.py` makes one call per size in a fresh process, so its "after" numbers
  are 30 to 40% higher than a loop that runs the old channelizer first (my `cmp.py`
  showed 69 to 82 ms and 225 to 250 ms for the first FFT version). The old code's
  allocations warm the allocator. The table above uses `bench_isolate.py` for both
  sides, which matches the live stage (one burst at a time, fresh buffers).
- nano-super timings drift by 10 to 20% between sweeps of the same code (for example
  283 against 339 ms for the same 500 ms case). Compare within one sweep only.
- Comparing the block path against the one-shot FFT path gives about -19 dB, not a
  bug: the one-shot fallback's output rate is approximate, so the time axes drift apart.
  Compare against resample_poly instead.
- `sumdec.py` first keyed bursts on (peak MHz, power to 2 decimals). Two 904.729 MHz
  bursts collided and it reported a false swap. The key now includes the centre
  frequency and 3 decimals.

### Open, not yet answered

- A live re-run on nano-super to confirm no young `iq_expired` under load, and, if any
  expire, which sub-counter it lands in. This fix has not run live.
- Whether F3's live batch really held two long bursts. The 1.5 s of competing DSP is the
  controller's analysis; the live log did not record the other two bursts' durations.
  The mechanism is reproduced deterministically, but this instance is inferred.
- The taper sweep above: a 10 to 20% roll-off decodes 2 to 3 more protocol-383 bursts
  on feb5. It needs a check on more captures and on the 1.0 Msps tier before adopting.
- The 920.903 MHz flex decode (section 9 already lists it as flaky offline) is lost
  with every taper tried (0.02 to 0.30). It has no CRC, and it was not investigated
  further.
- Bursts past the 256 MB snapshot budget are still read lazily, so they can still
  expire. At 26 Msps that needs a batch of more than about 5 bursts of 0.5 s (52 MB
  each) whose DSP ahead of them outlasts the ring. This was not tested live.

### Cleanup (this follow-up)

On nano-super, `/tmp/f3`, `/tmp/f3b`, `/tmp/iso.bundle`, `/tmp/f3b.bundle`,
`/tmp/bench_isolate.py` and `/tmp/f3b_bench_stage.py` were removed. No process was left
running. `~/GitHub/RFObserver` is still on `feat/averaged-window-store` with
`stash@{0}` intact and a clean status. `~/rfobs-*` was not touched. The unit suite's
`tmp_path` directories went into the existing `/tmp/pytest-of-ocollaco`, which pytest
rotates.

## F3 live confirmation (2026-09-28, after 41d6545)

This section is appended; everything above is unchanged. It answers the first item of
"Open, not yet answered" in the F3 root-cause section (the fix had not run live).

### Question

Does the F3 fix (snapshot-first reads, FFT channelizer, expiry sub-counters, one expiry
WARNING per batch, lazy bursts read first) hold on the live B200mini, with no young
`iq_expired` under load, and if any burst expires, which sub-counter does it land in?
Build: `feat/rtl433-burst-attribution` at `41d6545`. Hardware: nano-super, 15W (MAXN
unavailable, nvpmodel not touched), B200mini serial 322750B, 915 MHz, 26 Msps.

### Answer

Yes. In 3 live runs of 10 minutes each (343 picked bursts in total), `iq_expired` was
0, and so were all three sub-counters (`overwritten`, `unwritten`, `nopos`). There was
no expiry WARNING and no failed read_range. Both invariants held and the sub-counters
summed to `iq_expired` in all 33 health samples. Latency is unchanged from the
pre-fix live runs: steady-state (minutes 2 to 10) `excess_ms` p50 was 110.5 to 110.8 ms
and p99 was 134.2 to 141.8 ms. There were 0 overflows. Dropped chunks were 3, 4 and 3,
all in the first 50 chunks. Peak RSS was 698 MB, below the earlier 777 MB.

The stage keeps up: no batch waited in the queue for more than 2 ms, and the slowest
batch (10 bursts, run 3) took 564 ms. Most of the lookback is now spent before the
stage sees a burst, not in it: bursts are 0.53 to 1.28 s past their stop sample when
their batch starts (see Open).

### Procedure

1. Shipped the committed branch as a git bundle (`/tmp/f3live.bundle`) and cloned it
   to `~/rfobs-f3live/repo` (a separate clone, so `~/GitHub/RFObserver` gained no ref).
   The server ran with `PYTHONPATH=~/rfobs-f3live/repo/src` and the existing
   `~/GitHub/RFObserver/.venv`. Each server log starts with
   `rfobserver from /home/ocollaco/rfobs-f3live/repo/src/rfobserver/__init__.py`.
2. Instrumentation came from the section 3 wrapper (`val_run.py`, same `VALTRACE`,
   `VALNONE` and latency hooks), which calls `rfobserver.cli.main` with `run`. One hook
   was added, `VALBATCH`, wrapping `IsolationStage.submit` and `process_batch`. Per
   batch it logs:
   - `qwait_ms`: time from submit to the start of processing, which isolates stage
     backlog;
   - `age_at_start_ms`: `total_written - stop_sample` for each candidate at the start
     of the batch, which is how far into the 1.5 s ring each burst already is when the
     stage first touches it;
   - `proc_ms`: the whole batch, gate plus snapshot plus DSP;
   - burst durations and final states.
   No product code was changed.
3. The env was the same as section 3's `live_on`: `RFOBS_SENSOR_ACTIVE=true`,
   `RFOBS_FREQUENCY_START=915000000`, `RFOBS_FREQUENCY_END=915000000`,
   `RFOBS_FREQUENCY_STEP=0`, `RFOBS_BANDWIDTH=26000000`,
   `RFOBS_ATTRIBUTION_ENABLED=true`, port 8888, with STORAGE_PATH and DB_PATH under
   `~/rfobs-f3live/<run>/storage`. The runs were:
   - `run1` and `run2`: the settings above;
   - `run3`: the same plus `RFOBS_ISOLATION_SNR_DB=8` (default 13), to raise the
     isolation load.
4. For each run: start the server, wait for `/api/health`, wait 20 s, then poll
   `/api/health` 11 times at 60 s. Each poll checked:
   - `received == gated_out + queue_full + picked`;
   - `picked - (isolated + iq_expired + too_long + error) == 0`;
   - `iq_expired_overwritten + iq_expired_unwritten + iq_expired_nopos == iq_expired`;
   - VmRSS and VmHWM from `/proc/<pid>/status`.
   Then `fuser -k 8888/tcp`, wait 10 s, and confirm no `val_run.py` is left.
5. Afterwards, grep each log for `iq_expired`, `WARNING`, `ERROR`, `Traceback`, the
   snapshot-budget INFO line and `VALNONE`, and read decodes from each run's DB.

### Evidence

Per run (steady = minutes 2 to 10; startup = minute 1):

| run | picked | isolated | iq_expired (ovw / unw / nopos) | gated_out, queue_full, too_long, error | attr_decoded | lat p50 steady | lat p99 steady | lat p99 / max startup | dropped | ovf | RSS range MB | HWM MB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| run1 | 108 | 108 | 0 (0/0/0) | 0, 0, 0, 0 | 7 | 110.6-110.8 | 134.2-139.7 | 164.5 / 367.8 | 3 | 0 | 570-666 | 668 |
| run2 | 99 | 99 | 0 (0/0/0) | 0, 0, 0, 0 | 8 | 110.5-110.7 | 134.6-139.8 | 207.0 / 414.2 | 4 | 0 | 614-656 | 664 |
| run3 (SNR 8) | 136 | 136 | 0 (0/0/0) | 0, 0, 0, 0 | 20 | 110.6-110.7 | 134.4-141.8 | 190.2 / 335.4 | 3 | 0 | 582-659 | 698 |

The sub-counters never appeared in `counts` (the stats object only lists counters that
have moved), so each one is 0. `sub_sum_ok` was True in every sample.

Final health sample of each run (`inv` is the received invariant, then picked minus the
terminal states, then the sub-counter sum check):
```
run1 23:56:37 10 {'received': 108, 'picked': 108, 'isolated': 108, 'attr_decoded': 7, 'attr_not_decoded': 101, 'attr_dropped': 0} inv True 0 sub True ovf 0 proc {'VmHWM': 668, 'VmRSS': 666}
run2 00:07:18 10 {'received': 99, 'picked': 99, 'isolated': 99, 'attr_decoded': 8, 'attr_not_decoded': 91, 'attr_dropped': 0} inv True 0 sub True ovf 0 proc {'VmHWM': 664, 'VmRSS': 656}
run3 00:17:59 10 {'received': 136, 'picked': 136, 'isolated': 136, 'attr_decoded': 20, 'attr_not_decoded': 116, 'attr_dropped': 0} inv True 0 sub True ovf 0 proc {'VmHWM': 698, 'VmRSS': 659}
```
All 33 samples: `inv True 0 sub True ovf 0`.

The `iq_expired` WARNING lines, verbatim: none. `grep 'iq_expired\|WARNING\|ERROR\|Traceback\|snapshot budget\|lagged'`
matched nothing in any of the three server logs, and there were 0 `VALNONE` lines, so no
read_range returned None.

Drops: the counter was set by recv#50 and never moved.
```
run1 23:46:17,603 TIMING recv#50: recv=38.4ms dropped=3 (IQ=39.4ms) handoff_dropped=0/0 ovf=0 lost=0
run1 23:56:41,848 TIMING recv#15900: recv=38.6ms dropped=3 (IQ=39.4ms) handoff_dropped=0/0 ovf=0 lost=0
run2 23:56:58,306 TIMING recv#50: recv=38.5ms dropped=4 (IQ=39.4ms) handoff_dropped=0/0 ovf=0 lost=0
run2 00:07:22,550 TIMING recv#15900: recv=38.6ms dropped=4 (IQ=39.4ms) handoff_dropped=0/0 ovf=0 lost=0
run3 00:07:39,257 TIMING recv#50: recv=38.6ms dropped=3 (IQ=39.4ms) handoff_dropped=0/0 ovf=0 lost=0
run3 00:18:03,505 TIMING recv#15900: recv=38.3ms dropped=3 (IQ=39.4ms) handoff_dropped=0/0 ovf=0 lost=0
```

Last VALTRACE of each run (the `all_*` fields cover the whole run):
```
run1 VALTRACE lat_n=1525 lat_p50=110.6 lat_p99=135.2 lat_max=150.8 wr_n=1525 wr_mean=0.77 wr_max=4.4 wr_over5=0 wr_over20=0 rr_n=12 rr_mean=0.66 rr_max=2.8 rr_max_samples=4153920 rss_mb=666 hwm_mb=668 all_wr_max=8.6 all_rr_max=3.7 all_lat_max=367.8 all_wr_over20=0
run2 VALTRACE lat_n=1525 lat_p50=110.5 lat_p99=139.0 lat_max=160.5 wr_n=1525 wr_mean=0.76 wr_max=2.3 wr_over5=0 wr_over20=0 rr_n=8 rr_mean=0.45 rr_max=0.7 rr_max_samples=441920 rss_mb=634 hwm_mb=664 all_wr_max=12.8 all_rr_max=4.0 all_lat_max=414.2 all_wr_over20=0
run3 VALTRACE lat_n=1525 lat_p50=110.6 lat_p99=134.7 lat_max=156.4 wr_n=1525 wr_mean=0.77 wr_max=2.2 wr_over5=0 wr_over20=0 rr_n=16 rr_mean=0.84 rr_max=4.6 rr_max_samples=4153920 rss_mb=659 hwm_mb=698 all_wr_max=16.7 all_rr_max=15.3 all_lat_max=335.4 all_wr_over20=0
```
In run3 minute 3, the longest read_range was 15.3 ms, for 6,719,040 samples (the
254 ms burst). The worst ring write that minute was 9.6 ms, and there was no overflow.
The 16.7 ms write maximum was in minute 1 (startup).

Stage lag (`VALBATCH`, all batches):
```
run1 batches 71 picked/batch max 5  proc_ms p50 39 p99 165 max 165 | qwait_ms max 1 | age_at_start_ms p50 723 p99 1017 max 1282 | burst dur p50 9.1 max 156.0, sum 2.56 s
run2 batches 64 picked/batch max 3  proc_ms p50 42 p99 173 max 173 | qwait_ms max 1 | age_at_start_ms p50 728 p99 1266 max 1266 | burst dur p50 9.1 max 155.8, sum 2.23 s
run3 batches 78 picked/batch max 10 proc_ms p50 42 p99 564 max 564 | qwait_ms max 2 | age_at_start_ms p50 722 p99 1206 max 1271 | burst dur p50 9.1 max 254.4, sum 3.49 s
```
The heaviest batches, verbatim:
```
run3 VALBATCH n=10 picked=10 qwait_ms=0 proc_ms=564 age_at_start_ms=[782, 767, 752, 737, 722, 707, 692, 677, 661, 646] dur_ms=[209.1, 12.8, 12.8, 13.0, 12.8, 12.8, 13.0, 12.8, 12.8, 13.0] states={'isolated': 10}
run3 VALBATCH n=2 picked=2 qwait_ms=0 proc_ms=424 age_at_start_ms=[1077, 819] dur_ms=[118.5, 254.4] states={'isolated': 2}
run3 VALBATCH n=1 picked=1 qwait_ms=0 proc_ms=268 age_at_start_ms=[1129] dur_ms=[239.5] states={'isolated': 1}
run1 VALBATCH n=2 picked=2 qwait_ms=0 proc_ms=78 age_at_start_ms=[1282, 529] dur_ms=[36.6, 9.1] states={'isolated': 2}
run2 VALBATCH n=1 picked=1 qwait_ms=0 proc_ms=71 age_at_start_ms=[1266] dur_ms=[52.8] states={'isolated': 1}
```
The 10-burst batch is a sweep at 908.0 to 911.2 MHz in 0.4 MHz steps, 12.8 ms bursts
15 ms apart, plus one 209 ms burst. It is larger than F3's 3-burst batch, and every
burst was isolated. The read start (the stop age plus the duration plus the 2 ms guard)
was at most 1321 ms old at batch start (run1: 1282 + 36.6 + 2). In run3,
1129 + 239.5 + 2 = 1371 ms, which is 129 ms short of the 1.5 s ring. Snapshot-first
means that margin only has to cover the gate and the copy, not the DSP.

On-air decodes (DB rows with a model). All of them are `ssnmesh` flex decodes (no CRC),
and none is protocol 383, as in F6:
```
run1 (7): 916.803 -67.6 dB 6.7 ms; 927.594 -67.1 dB 27.8 ms; 923.595 -67.8 dB 16.3 ms; 904.006 -71.2 dB 6.7 ms;
          907.599 -71.7 dB 27.6 ms; 911.204 -73.3 dB 6.7 ms; 926.400 -64.2 dB 23.0 ms
run2 (8): 911.598 27.4 ms; 914.797 27.4 ms; 917.603 6.7 ms; 911.204 46.5 ms; 920.396 12.8 ms; 913.604 6.7 ms;
          904.806 46.3 ms; 913.997 12.6 ms (-67.1 to -72.5 dB)
run3 (20): 908.005 to 911.204 (9 of the 12.8 ms sweep bursts, -72.8 to -75.4 dB); 917.196 254.4 ms 6.74 MHz wide -67.1 dB;
          916.396; 903.600; 907.599; 912.804 (67.0 ms); 913.604; 913.197 (x2); 907.205; 916.803; 904.806
```
Detections: 111 / 99 / 137, each with attribution. SigMF metas under `bursts/`: 111 /
99 / 137.

### Measured and REJECTED (do not retry)

- **"The F3 young expiry recurs under live load after the fix."** Rejected: 0 of 343
  picked bursts expired, including a 10-burst batch (564 ms of DSP) and 118 to 254 ms
  bursts in run3.
- **"The FFT channelizer or the snapshot copy costs latency or RSS live."** Rejected:
  steady p50 was 110.5 to 110.8 ms (110.4 to 110.8 before) and p99 134.2 to 141.8 ms
  (130.5 to 139.9 before). Peak HWM was 698 MB against 769 to 777 MB before.

### Measurement traps

- **SNR 8 in run3 is not shown to be the cause of its higher load.** gated_out was 0 in
  all three runs, so the gate did not reject anything at 13 dB either. run3's extra
  bursts (136 against 99 and 108) and its 10-burst sweep may be on-air variation. The
  setting was applied (it is in the run3 env dump), but its effect was not isolated.
- **An absent counter means 0.** `iq_expired_*` do not appear in `counts` until they
  move, so a script that requires the key would report a false failure. poll.py
  treats a missing key as 0.
- **The run timestamps cross midnight.** run2 and run3 ended on 2026-09-29 (bursts under
  `bursts/20260929/`), although the section is dated by the build day.
- **The first minute still carries startup costs** (p99 164 to 207 ms, max 335 to
  414 ms), as in section 6. The steady-state figures exclude it.

### Open, not yet answered

- **Detection lag uses most of the lookback.** A burst is 0.53 to 1.28 s past its stop
  sample before its batch starts, although `excess_ms` is about 110 ms. The rest is
  upstream of the stage, presumably burst tracking and closure over later chunks, and
  it was not broken down here. The tightest read was 129 ms from being overwritten
  (run3). A longer burst, up to ISOLATION_MAX_BURST_SEC = 0.5 s, detected that late
  would not fit in the 1.5 s ring whatever the stage does. It would land in
  `iq_expired_overwritten`, and that counter is now visible if it happens.
- The lazy path (a batch over the 256 MB snapshot budget) was not hit live: no
  snapshot-budget INFO line. It is still untested live.
- F6 stands: 35 on-air decodes, all flex `ssnmesh`, none protocol 383.
- 56 Msps and MAXN: still not measured.

### Cleanup (this confirmation)

On nano-super, `~/rfobs-f3live` (the clone, scripts, run dirs, DBs and bursts) and
`/tmp/f3live.bundle` were removed. After `fuser -k`, `fuser 8888/tcp` finds nothing,
and no `rfobserver`, `val_run.py`, `poll.py` or `live.sh` process is running.
`~/GitHub/RFObserver` is still on `feat/averaged-window-store` with `stash@{0}` intact
and a clean status. `~/rfobs-replay-data`, `~/rfobs-stall` and `~/rfobs-stalltest` were
not touched, and nvpmodel is still 15W. On the workstation, the bundle was deleted. The
server logs and poll files are kept in the session scratchpad only.

## Open items as of 2026-09-28 (end of session, HEAD ff9f16b)

This list replaces section 9 as the current open list. Section 9 stays as written.
Resolved since section 9:
- F1: replay pacing now uses a deadline clock, and a 6.0 s capture plays in 6.03 s.
- F2: replay loop is now an option and off by default.
- F3: the live `iq_expired` burst is root-caused and fixed, with 0 expiries in 343 live
  bursts.
- F5: sigmf is installed on nano-super.
- The lookback default was raised to 2.0 s.

**Findings not yet investigated**
- **F4.** Strong bursts are detected 15 to 26 MHz wide (feb4 919.43 MHz: 902.0 to
  927.99 MHz). They could be splatter or a detector artefact. Their stored bandwidth is
  wrong, although isolation still cuts at the peak and feb4 decodes.
- **F6.** Every on-air decode is from the flex `ssnmesh` decoder, none is protocol 383:
  13 in the acceptance runs and 35 in the F3 live runs. Flex has no CRC, so these are
  not confirmed SSN.
- **Channelizer taper.** A 10 to 20% taper decoded 13 to 14 protocol-383 bursts on feb5
  against 11 with the 5% taper. This was tested on one capture only, so it was not
  adopted. It needs the full-capture set before any change.
- **Detection delay.** Bursts reach isolation 0.5 to 1.3 s after they start, although
  end-to-end latency is about 110 ms; the rolling detector emits a burst only once it has
  stopped growing. With 2.0 s of lookback the margin is about 0.6 s. Not broken down
  further. Watch `iq_expired_overwritten` in health.
- **The 920.903 MHz flex decode comes and goes.** It varies between offline runs on the
  workstation and between channelizer versions.

**Known limitations (parked, with a follow-up noted)**
- A finished single-pass UI replay keeps running until the operator clicks Stop; there
  is no auto-stop.
- A wedged stage worker (a hung filesystem during an archive write) can hang the offline
  `run_replay` until its 3600 s ceiling.
- A mid-run RAM-guard trip joins the isolation stage on the receiver thread for up to
  5 s, inside an existing reconfigure gap.
- Position-less streaming batches switch the rate gate to the monotonic clock and reset
  its window; this is rare.
- A looping UI replay decodes the same bursts again on every pass, when loop is turned
  on.
- Bursts beyond the 256 MB per-batch snapshot budget are read lazily and can still
  expire under extreme load.

**Not measured**
- Latency and lookback at 56 Msps under load. Everything here was 26 Msps.
- MAXN behaviour. Not possible on this board until it is reflashed with the Super device
  tree.
