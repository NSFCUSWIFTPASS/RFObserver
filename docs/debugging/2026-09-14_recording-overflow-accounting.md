# Recordings report "0 dropped" while UHD overflows remove samples

Date: 2026-09-14. Issue 5 in `2026-09-14_stall-safety-net-hardware-validation.md`.

## The question

Why does a recording on nano-super say "0 dropped" when about 20% of its
samples are missing? The recording covered 30.1 s of wall time but holds only
24.1 s of samples: 5,409,536,000 B / 4 / 56e6 = 24.15 s.

Setup: nano-super, a Jetson Orin Nano at 15 W (non-Super flash, 1.51 GHz cap),
L4T R36.5, UHD 4.1, B200mini 322750B over USB 3, at 56 MS/s sc16.

## The answer

- `Receiver.recv_chunk()` (`capture/receiver.py`) sees each UHD overflow
  (`RXMetadataErrorCode.overflow`, the "O"). It logs a warning and keeps
  filling the buffer.
- The lost span is never measured, and nothing is passed to the caller, so
  the samples after the gap are joined straight onto the samples before it.
- The recording's `dropped_chunks` (`_recording_dropped`) counts only chunks
  dropped from the pipeline's own queue. It cannot see UHD overflow loss.
- The loss can be measured exactly. On the B200mini, every received packet
  carries a `time_spec`, and the gap before a packet is
  `time_spec.to_ticks(rate) - end_of_previous_samples_in_ticks`, in integer
  ticks. Float seconds are not precise enough at epoch device time; see
  REJECTED.

## Procedure

1. **Read the code path.** Receiver loop (`streaming.py` `_receiver_loop`),
   then `recv_chunk()`, then `_check_trigger_and_record(buf[:n])`, then
   `_finalize_recording()` and `_write_recording_metadata()`. Overflow handling
   is a log line only. `dropped_chunks` is incremented only where the chunk
   queue is full (`streaming.py:817`, `826`).
2. **`overflow_probe.py`** (in `2026-09-14_recording-overflow-accounting/`).
   This isolates UHD from the pipeline:
   - It uses the same stream setup as `Receiver`: `num_recv_frames=1024`,
     sc16/sc16, RX2, `start_cont` with `stream_now`, and 2,048,000-sample
     chunks.
   - Device time is set to 0 so the time_spec values are small.
   - It stalls the consumer 60 ms every 10 chunks to force overflows. A run
     with no stall is the control.
   - For every `recv()` that returns samples it compares the packet's
     `time_spec` with where the previous samples ended.
   - Check: the received count plus the gap samples must equal the device-time
     span multiplied by the rate.

## Evidence

```
$ overflow_probe.py 56e6 10 60 10      (forced stalls)
OOOOOOOOOOOOOOOOOOOOOOOO
rate=56.0MS/s wall=10.00s chunks=249 overflows=24 gap_events=24 gap_samples=50277824 received=509952000 no_ts=0
device_span_samples=560229824 received+gaps=560229824 mismatch=0 loss=8.97% wall_expected=560242651

$ overflow_probe.py 56e6 10 0 0        (control: no stall)
rate=56.0MS/s wall=10.02s chunks=274 overflows=0 gap_events=0 gap_samples=0 received=561152000 no_ts=0
device_span_samples=561152000 received+gaps=561152000 mismatch=0 loss=0.00% wall_expected=561169196
```

- Each overflow was exactly one gap: 24 overflows, 24 gap events.
- `time_spec` accounts for every sample (mismatch 0), and every packet had one
  (`no_ts=0`).
- The device span matches wall time within 0.002%.
- Receiving alone keeps up at 56 MS/s. The overflows in the field come from the
  pipeline's CPU load at 15 W, as the worker A/B showed.

Integer ticks at production device time (`overflow_probe_ticks.py`, 2026-09-15).
`Receiver.initialize()` sets device time to the host epoch (about 1.8e9 s),
so this probe does the same. It compares `time_spec.to_ticks(rate)` with
`get_real_secs()`:

```
$ overflow_probe_ticks.py 56e6 10 60 10
device_span_samples=561854326 received+gaps=561854326 mismatch=0 loss=8.87%
$ overflow_probe_ticks.py 56e6 6 0 0
rate=56.0MS/s wall=6.03s chunks=165 overflows=0 gap_events=0 ... max_float_err_samples=13
device_span_samples=337920000 received+gaps=337920000 mismatch=0 loss=0.00%
```

## Measured and REJECTED (do not retry)

- **Gap arithmetic in float seconds (`get_real_secs()`).** Rejected. At epoch
  device time a double resolves only about 2.4e-7 s. With no overflows at all,
  the float gap estimate was off by up to 13 samples per packet, which would
  log a false gap on nearly every packet. Use `to_ticks(rate)`, which is an
  exact integer.

- **"Count overflow events and multiply by a fixed size."** Rejected: one
  overflow loses a variable number of samples. The probe averaged 2.1 M
  samples per event, and that depends on how long the consumer stalled.
  `time_spec` gives the exact count.
- **"Sample shortfall against wall clock"** (the fallback suggested in the
  issue). Workable, since wall and device time agree within 0.002% here, but it
  is coarser than `time_spec`. It needs a monotonic start/stop pair, and it
  cannot say where in the file each gap falls.

## Measurement traps

- **The first probe set device time to 0.** Float arithmetic looked exact only
  because the timestamps were small. Production uses the host epoch; see the
  `to_ticks` evidence above.

- **Without load, receiving alone never overflows on this box.** A probe with
  no consumer stall reads 0%. Force the stall, or run the pipeline.
- **The "O" characters go to stderr from UHD**, not through Python logging.
  Count overflows with the metadata error code, not by grepping the log.

## Open, not yet answered

- Whether detections and PSD rows computed from sample counts drift in time
  across a gap. Timestamps are taken per chunk from the host clock, so the drift
  would be bounded by one chunk (36.6 ms), but this was not checked.
- Behaviour with an external time/ref source (`set_time_source("external")`).
  `time_spec` still applies, but this was not tested.

## Fix verification (branch D, a8fc78e, 2026-09-15)

nano-super, B200mini 322750B, 15 W, 56 MS/s, `start.sh` (watchdog on),
manual recording with the 30 s auto-stop:

```
Recording saved: 322750B-nano-super-20260915T144654.sc16 (5606144000 bytes, 30.1s, 0 dropped,
  0 grid rows dropped, 60 overflow gaps (332542744 samples lost))
.json: total=25.027s lost=5.938s (19.2%) time_span=30.966 duration=25.027 overflow_events=60
  gaps=60 truncated=False dropped_chunks=0 pre=1.0
  gaps sorted=True first=[[58119020, 5164785], [72453745, 1289764], [215814390, 10026378]]
  sum_lost_matches=True
/api/health pipeline: overflow_events 19 -> 85, overflow_lost_samples 23149053 -> 379397812
TIMING recv#1400: ... dropped=403 (IQ=36.6ms) handoff_dropped=0/0 ovf=85 lost=379397812
```

- **Consistency check.** The pre-roll is the full 1.0 s ring (56,000,000
  samples) with no gaps in it: the first gap is at 58.1 M. The post-trigger span
  is therefore 30.966 - 1.0 = 29.966 s, against the 30.0 s auto-stop, which is
  within one 36.6 ms chunk.
- **Before the fix**, the same kind of capture said "0 dropped" with 24.1 s of
  samples in a 30.1 s span.

## Measurement traps (added)

- **The "30.1s" in "Recording saved" is not the recording span.** It is
  measured at finalize, so it includes the 50 ms straggler sleep and the recctl
  hand-off. Check `time_span_sec` against the auto-stop setting (30.0 s) plus
  the pre-roll, not against that log figure.
- **TIMING `dropped=` counts chunks the PSD pipeline skipped**
  (processing-queue drops). It is unrelated to recording loss and to UHD
  overflows, which are `ovf=` and `lost=`.

## CORRECTION 2026-09-15: the a8fc78e consistency check hid a lost chunk

The check above called 29.966 s "within one chunk" of the 30.0 s auto-stop.
That tolerance was wrong in one direction. Auto-stop fires only after the
elapsed time reaches 30.0 s, so the accounted post-trigger span must be
**at least** 30.0 s.

The final branch review found the cause: a manual-start race.
- The web thread's pre-roll read, a 224 MB copy at 56 MS/s, takes about one
  chunk period.
- A chunk arriving in that window never reached the file, and no gap was
  recorded for it.
- The review reproduced this with `race_probe.py`, in the review scratchpad.

The fix (9b13160) runs a stream-position continuity check on every recording
write. A skipped stretch becomes a gap, an overlap is trimmed, and a chunk that
arrives after the state flip is written.

Re-run on 8bb5662 (same setup):

```
Recording saved: 322750B-nano-super-20260915T152406.sc16 (5597952000 bytes, 30.1s, 0 dropped,
  0 grid rows dropped, 63 overflow gaps (339249060 samples lost))
.json: total=24.991s lost=6.058s (19.5%) time_span=31.049 overflow_events=63 gaps=63
  first=[[31493265, 765927], [51974510, 804473], [60166795, 5752353]] sum_lost_matches=True
```

- The pre-roll is 56 M samples plus two gaps inside it (1.57 M samples,
  0.028 s), so its time span is 1.028 s.
- Post-trigger: 31.049 - 1.028 = **30.021 s**. That is at least 30.0 s and
  less than one chunk over, as expected.
