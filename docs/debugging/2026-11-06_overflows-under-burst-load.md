# HCRO .34 and .36 overflow in bursts: per-burst Python work starves the receiver

## The question

2026-11-06, about 23:00 UTC. "On the fleet I noticed a lot of missed and partial, check." The
"missed" half was a schedule bug (see 2026-11-06_one-shot-runs-remarked-missed.md). The "partial"
half is real IQ loss. HCRO fleet, six Orin Nano Super sensors on 0.14.0b0, B205mini at 26 MS/s,
MAXN_SUPER, isolation and attribution on (decoders `ssm,silverspring`), burst threshold 30 dB.

Overflow counters since 21:08 UTC: .34 360 events (575M samples), .36 106 (206M); .32 1,
.33 and .35 1 to 5. Scheduled SSM1 runs: .34 partial (lost 1,992,823 of 130M), .36 partial
(lost 9,450,490 and 13,048,012).

## The answer

The overflows follow the burst detection rate. When the SSM transmitter runs, .34 and .36
detect 20 to 40 bursts a second (.33 detects about 1), and the per-burst Python work (burst
fingerprinting on the `burst` thread, result handling on the event loop) holds the GIL long
enough that the receiver thread cannot get back to `recv()` for 125 ms or more, so UHD
overflows. The same capture replayed on a Jetson with no SDR reproduces the stall signature;
the same replay with burst detection off does not cross the overflow threshold.

## The procedure

1. **Per-minute counts of `UHD overflow` and `Detected N bursts` in syslog** on .33, .34, .36
   (read-only SSH). Separates "the sensor overflows" from "the sensor overflows when bursts
   arrive". .33 is the control: same site, same transmitter, few detections.
2. **The `RECV STALL` breakdown around each overflow** (.36, 14:48-14:55 MST). `recv` 143 to
   187 ms against a 39.4 ms chunk, `work` 1 ms, `recording=idle`. Rules out recording and the
   receiver's own work; same shape as 2026-10-01_hcro-overflows-storage-scans.md section 12,
   where the cause was GIL holders elsewhere.
3. **Local paced replay** of .34's SSM capture (`32274A4-hcro-jet-004-20261105T215645000-sched-
   e1e25ee6c75e-0.sc16`, 5 s, looped) through `rfobserver run` with `/api/replay/start`, under
   `py-spy record --gil --threads -r 200 -d 90`, settings copied from .34's Config page. Then
   the same with `BURST_DETECTION_ENABLED`, `ISOLATION_ENABLED`, `ATTRIBUTION_ENABLED` false as
   the control. The replay receiver sleeps instead of calling UHD, so a `RECV STALL` there is
   the receiver thread waiting for the GIL, with no USB in the path.
4. **The same pair on nano-super** (Orin Nano Super, the sensors' hardware class), from a
   detached worktree of main 1fad272 at `~/rfo-gilrun`, capture in `~/ssm-replay`.

## Evidence

Burst load against overflows, MST on 2026-11-06:

```
            14:20-29          14:30-35          14:48-49          14:54-55
.33   bursts=372  ovf=0  bursts=497   ovf=1  bursts=91   ovf=1  bursts=113  ovf=0
.34   bursts=7621 ovf=120 bursts=13271 ovf=175 bursts=323 ovf=1  bursts=677  ovf=2
.36   bursts=4172 ovf=40 bursts=2264  ovf=1  bursts=1263 ovf=32 bursts=1048 ovf=23
```

.36 during the SSM1 runs:

```
14:48:57 RECV STALL: recv=164.0ms (chunk 39.4ms) work=7.4ms [ring=7.2 ...] recording=idle
14:48:57 UHD overflow (O): lost samples
14:49:03 RECV STALL: recv=34.6ms (chunk 39.4ms) work=140.0ms [ring=138.4 ...] recording=idle
14:49:08 Scheduled capture 466d68eb330d run 0: partial (...sched-466d68eb330d-0.sc16, 130000000 samples)
```

A 140 ms `ring` write (a 4 MB numpy copy) is the same wait landing after `recv()` instead of
inside it.

nano-super, 15 W, 90 s each:

```
                      GIL held   RECV STALL lines   recv>125ms   max recv
control (no bursts)       42%          12                0         116.7 ms
SSM replay                55%          38               12         243.8 ms

SSM replay, largest new GIL holders: burst _extract_fingerprints (burst.py:137) 4.8%,
MainThread +8% overall (result consumer, _put_nowait_drop_full)
```

Workstation (much faster cores), same pair: GIL held 15% without bursts, 39% with; 2 stalls,
neither past 45 ms. The workstation has headroom; the Orin does not.

## Measured and REJECTED (do not retry)

- **Recording or scheduled-capture writes.** Every stall on .36 says `recording=idle`; the
  stall runs continued at 14:49:09-27 after the run ended.
- **The receiver's own work.** `work` 0.8 to 7 ms in the `recv`-type stalls.
- **USB or the radio.** The replay receiver has neither and still stalls to 244 ms under burst
  load.
- **SSM decoding as the main cost.** On nano-super isolation fell behind (725 of 737 picked
  bursts `iq_expired`), so very little reached the decoder, and the stalls still happened.
  `decode_ssm` was 2.7% of GIL samples on the workstation.

## Measurement traps hit

- nano-super runs the 15 W profile and its nvpmodel.conf has no MAXN entry; HCRO runs
  MAXN_SUPER. Switching needs a system file change, not done. Its CPU clocks are lower, so the
  Jetson numbers are somewhat pessimistic.
- nano-super's own :8888 and :8889 instances ran during both runs (CPU, not GIL, competition),
  and at 15 W the control dropped 599 chunks (PSD workers behind), which also removes work. The
  comparison is still like for like.
- `py-spy --gil` records GIL holders only, not how long the receiver waited. The wait is
  inferred from `RECV STALL`.
- Replay mode writes attribution to a JSONL sink; sensors write to the database. The
  `attribution.py:187/195` frames in the workstation profile are that sink.

## Open, not yet answered

- No GIL profile on an HCRO sensor itself (needs py-spy and sudo on the sensor).
- Whether the fix below is enough on HCRO under a real SSM run (measured on nano-super only).
- Why .34 detects about 2x more bursts than .36 and 20x more than .33 (distance to the
  transmitter is the likely reason, unconfirmed).

## Fix: fingerprint extraction without find_objects (2026-11-06)

Per step of `detect_bursts` on one 4096x2048 window of the SSM capture, nano-super. A second
thread sleeping 1 ms at a time measures the longest single GIL hold ("rx late max"):

```
percentile      304.0 ms/call  rx late max   12.4 ms   (releases the GIL)
masks            21.7 ms/call  rx late max    5.6 ms
label            62.8 ms/call  rx late max   10.9 ms
unique            5.6 ms/call  rx late max   12.2 ms
find_objects     59.5 ms/call  rx late max   68.4 ms   <- one C call, GIL held throughout
extract_fp       81.8 ms/call  rx late max   55.5 ms   <- per-label loop
```

The window had 7,294 to 11,242 labels, of which 53 to 88 were valid. `find_objects` walks
the whole grid and builds a slice for every label, noise included. `_extract_fingerprints` now
takes the low-threshold mask, gathers only the cells of valid labels (`flatnonzero` of the
mask, 5.8 ms, against 35 ms for the label image), and measures every region at once with
`reduceat` (bounds, peak, first peak cell in row-major order). Output is identical to the old
code on the three SSM windows and on random busy grids (`test_fingerprints_match_reference_on_
busy_grids`); its longest GIL hold is 16 to 19 ms.

Replay on nano-super, 90 s, recv stalls over 100 ms:

```
                      stall lines  recv>125ms  max recv   pattern
control (no bursts)        12           0       116.7 ms  100-117 ms, spread out
SSM, before               38          12       243.8 ms  195-244 ms every ~5 s (each loop of the capture)
SSM, fix, run 1           33           4       133.5 ms  100-134 ms
SSM, fix, run 2           30           3       165.4 ms  100-165 ms, no 200 ms class
```

The 200 ms class tied to the bursts is gone. What remains is the same 100-130 ms band the
control shows on this box (15 W, two other RFObserver instances running).
