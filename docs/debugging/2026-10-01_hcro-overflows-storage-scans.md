# HCRO sensor overflows: storage directory scans starve the receiver

## 1. The question

Date: 2026-10-01. The user asked: "Deployed, check if the sensor is still dropping samples".
The HCRO field sensor `rfnano` (v0.10.0b0 at 9f874d9, B205mini, 26 Msps, DURATION_SEC
1.0, 2048 bins, CPU PSD backend, MAXN Super) loses IQ continuously.

| When | Overflow events | IQ lost |
|---|---|---|
| Before the UI deploy, 98 min | 1,239 | 2,370,466,363 samples |
| Before the UI deploy, 61 s sample | 13 | 1.02 s (1.7%) |
| After the UI deploy, 121 s sample | 24 | 2.10 s (1.74%) |

## 2. The answer

About 70% of the time any thread holds Python's GIL goes to pure-Python walks of the
capture storage directory (pathlib `glob`/`rglob`, with `exists()`/`stat()` per capture
and per companion file). They run constantly:

- the heartbeat runs one every 1 s, **on the event loop**;
- `enforce_cap` runs one after **every** recording;
- the storage governor runs one every 10 s.

Their cost grows with the number of captures on disk; auto/ holds 599.8 GB. The event loop
blocked in the heartbeat walk cannot consume chunk results, so `handoff_dropped` climbs by
about 12 a second, roughly 45% of the results. Threads holding the GIL in these loops delay
the receiver thread past UHD's buffer, which causes an overflow every 3 to 5 s, each losing
about 0.1 s of IQ. The UI changes and isolation/attribution are not the cause; both were
measured and rejected (section 5).

## 3. Procedure

1. **Overflow rate after the UI deploy:** two `/api/health` samples 120 s apart (counters
   `overflow_events` and `overflow_lost_samples`). The rate was unchanged, which rules out
   the old UI.
2. **The sensor's journal** (user-supplied `journalctl -u rfobserver -f`): TIMING, WORKER
   and PROC lines, and the overflow warnings. This separates three losses:
   - `ovf`/`lost`: USB overflow at the receiver;
   - `dropped`: the worker queue was full;
   - `handoff_dropped`: the event loop was not consuming results.
3. **A/B on isolation:** 120 s with isolation on, then 120 s off (toggled by the user in
   Config, and confirmed via `/api/health` `isolation.enabled`). It controls for the burst
   channelizer, the SigMF writes and rtl_433.
4. **`py-spy record --gil --threads`**, 30 s at 100 Hz, run by the user on the sensor. With
   `--gil`, only samples where a thread holds the GIL are recorded, so the flamegraph shows
   what keeps the receiver (and the event loop) from running Python.

## 4. Evidence

Journal excerpt (cumulative counters; the chunk is 39.4 ms of IQ):

```
20:58:13 TIMING recv#13350: recv=38.5ms dropped=242 (IQ=39.4ms) handoff_dropped=6025/0 ovf=122 lost=249640561
20:58:13 WARNING UHD overflow (O): lost samples
20:58:15 TIMING recv#13400: recv=27.5ms dropped=242 handoff_dropped=6034/0 ovf=123 lost=252328208
20:58:17 WARNING UHD overflow (O): lost samples
20:58:17 WORKER chunk#13450: convert=5.2ms psd=295.7ms stats=12.1ms total=313.6ms
20:58:21 WARNING UHD overflow (O): lost samples
20:58:27 TIMING recv#13700: recv=38.2ms dropped=246 handoff_dropped=6189/0 ovf=125 lost=257951666
```

- `handoff_dropped` grew by 164 in 14 s (about 11.7 a second, against about 25 results a
  second).
- `lost` grows by about 2.7M samples per overflow (about 0.1 s at 26 Msps).
- Normal worker time is 50 to 80 ms, with occasional spikes (313 ms).
- On the workstation and nano-super mock runs, `handoff_dropped` stayed at 0/0.

A/B on isolation:

```
isolation ON  (start ON):  over 121s 22 overflows (10.9/min), 1.50 s IQ lost (1.23%), bursts isolated +38
isolation OFF (start OFF): over 120s 27 overflows (13.5/min), 1.94 s IQ lost (1.61%), bursts isolated +0
```

py-spy, GIL holders (1,187 samples in 30 s):

```
31.51% MainThread (event loop)
  28.39% _heartbeat_loop (pipeline/app.py:486) -> rglob (pathlib)          # every 1 s, on the loop
27.72% recctl
  27.72% _end_recording -> _finalize_recording (streaming.py:2170)
         24.09% enforce_cap (storage/local.py:132) -> _size_or_zero        # after every recording
20.47% asyncio_2 (executor)
         10.95% + 9.44% storage sample (storage/local.py:246-253)          # governor, every 10 s
 6.07% psd_0 (a PSD worker)
```

## 5. Measured and REJECTED (do not retry)

- **"The web UI (old code) causes the overflows."** Rejected: the rate was unchanged after
  deploying the UI changes (1.7% before, 1.74% after).
- **"Burst isolation / rtl_433 attribution causes them."** Rejected: 1.23% with isolation
  on against 1.61% with it off.
- **"USB link speed."** Not supported: the loss is about 1.7%, not the most-of-the-stream
  loss a USB 2 link at 26 Msps sc16 (104 MB/s) would give. `lsusb -t` was not captured.

## 6. Measurement traps

- `/api/status` `capture_count` is the processor's **chunk** count (147,878 is about the
  uptime divided by 39.4 ms), not the number of capture files. An earlier version of this
  analysis read it as the file count. **Withdrawn:** the file count on the sensor is
  unknown; at the 14.5 MB recordings seen in the journal, 599.8 GB of auto/ would be
  roughly 40,000 captures.
- The scans scale with the files on disk, so a test box with few captures (nano-super, the
  workstation) cannot reproduce this. A reproduction needs a storage directory with tens of
  thousands of captures.
- `py-spy --gil` shows GIL holders only; a thread waiting for the GIL (the receiver) does
  not appear in that profile. The all-threads profile was not captured.

## 7. Open, not yet answered

- The exact receiver mechanism is not measured. The hypothesis is GIL contention: the
  receiver must reacquire the GIL after each blocking UHD call and wait behind threads
  running Python-heavy scans (switch interval 5 ms, several contenders). Stall logging in
  the receive loop would confirm it.
- The capture file count on the sensor.
- Why the sensor records so often: a recording finalizes every few seconds, and each one
  ran the full `enforce_cap` scan. It may be the power trigger level.
- The Captures page (`/captures/list`) stats and reads metadata for every capture and is
  reloaded each time the heartbeat's capture count changes, so with the page open it is a
  fourth scan.

## 8. Fix (2026-10-01, branch fix/capture-index)

`LocalStorage` keeps an in-memory capture index: each capture's path, mtime and footprint
(the sc16 plus its companions), with per-directory totals.

- It is built by one `os.scandir` pass when storage is opened, at startup and before
  streaming, off the event loop. The same happens when the storage path changes.
- After that it is kept current by tracking alone:
  - the recording finalize registers the new capture (stats only its files);
  - the deferred detections sidecar and the redetect route re-measure the capture;
  - eviction claims a victim under the lock before deleting it.
- These now read the index instead of walking the directories: `enforce_cap`,
  `evict_until_free`, the governor `sample`, the heartbeat capture count, and the
  Captures list. The list is paged at 200, and the deep link fetches older captures
  directly.
- There is no periodic scan. A file deleted outside RFObserver stays counted until
  eviction reaches it (a no-op delete that corrects the total) or until the next start.
  Disk safety still comes from the volume's real free space.

Benchmark on nano-super (15 W): 40,000 dummy captures, 200,000 files, warm cache. A
stand-in receiver thread reads 4 MB from /dev/zero in a loop (a GIL-releasing C call)
while the storage work runs at production cadence for 90 s:

```
== OLD (main)
heartbeat   n= 11 avg   7576.7 ms  max  11591.7 ms
sample      n=  3 avg  29923.4 ms  max  38425.6 ms
record+cap  n=  2 avg  29904.4 ms  max  30383.8 ms
receiver: reads 186669, p50 0.43 ms, p99 0.9 ms, max 169.1 ms, >50ms 28, >160ms 1
== NEW (capture index)
open (index build if new): 2.93 s
heartbeat   n= 90 avg      0.0 ms  max      0.1 ms
sample      n= 10 avg      1.0 ms  max      2.8 ms
record+cap  n= 23 avg      1.7 ms  max      2.7 ms
receiver: reads 245564, p50 0.37 ms, p99 0.4 ms, max 2.3 ms, >50ms 0, >160ms 0
```

With the old code, every storage operation took longer than its own interval, so the
scans ran back to back without pause. On the sensor that is the busy event loop and the
dropped results. The stand-in receiver has fewer competitors than the real pipeline,
so its single gap over 160 ms understates the effect on the real receiver.

To confirm after deploying: the overflow and `handoff_dropped` rates on HCRO, measured
over 2 minutes as in section 3.
