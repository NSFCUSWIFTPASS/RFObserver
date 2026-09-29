# Live per-burst protocol attribution via rtl_433

- **Date:** 2026-09-07
- **Status:** Approved for planning
- **Scope of this cut:** live continuous path only. A queue that channelizes
  detected bursts out of the in-memory IQ, an in-process rtl_433 worker on the
  sensor, and attribution written back onto the `detections` row. No batch pass
  over stored captures and no offline `/captures/attribute` route in this cut
  (both are natural follow-ons on the same primitives).
- **Companion recon:** `feature-reqs/rtl433-burst-attribution.md` (the offline /
  per-file framing). This spec is the live-queue variant chosen during
  brainstorming.

## Problem

RFObserver detects bursts and stores five-parameter fingerprints (time,
frequency, bandwidth, power) but says nothing about what a burst *is*. rtl_433
carries ~250 sub-GHz decoders plus, on master, a Silver Spring Networks / Itron
mesh FSK decoder (protocol 383) already validated against the HCRO 915 MHz
captures. Attaching a protocol (and, where the decoder parses them, fields) to
each burst turns the detection log from "energy was here" into "this endpoint
transmitted here" - directly useful for HCRO interference attribution.

The goal in this cut: as the live continuous loop detects bursts, run the
strongest ones through rtl_433 and record the result on the burst, without
disturbing the live capture cadence.

## Decisions (locked during brainstorming)

- **A queue is the core primitive.** It decouples fast live detection (one
  `DURATION_SEC` chunk at a time) from slow per-burst subprocess decode. The
  producer runs in the continuous processing path; a single background worker
  drains it.
- **rtl_433 runs in-process on the sensor Jetson**, not off-box. Justified by the
  measured budget below: the deployment target is the MAXN Super, which has ample
  headroom at 26 Msps. The recon's "not on the live hot path" caution was written
  against a 15 W board and does not bind here.
- **Backpressure = bounded queue, drop-and-keep-strongest.** The live loop never
  blocks. On a full queue the *weakest-power* item is evicted so the strongest
  bursts (most likely to decode) win the limited worker time. Best-effort
  attribution; some bursts go unattributed under load, which is acceptable.
- **Enqueue is gated, not everything.** Two filters at enqueue time:
  - **SNR gate:** skip bursts under ~12-15 dB over the chunk noise floor. Established
    against HCRO data - weak bursts never decode and only cost subprocess time.
  - **Top-N cap** per chunk by `peak_power_db` (recon used 40). One wide capture
    can hold hundreds of bursts; the cap bounds worst-case worker load.
- **Attribution attaches to the `detections` row by `burst_id`.** Three new
  nullable columns via the existing ALTER-TABLE migration path. No sidecar
  dependency, so it works for live un-recorded bursts. The worker merges with
  `UPDATE detections SET ... WHERE burst_id = ?`.
- **Off by default behind a config flag.** Attribution stays disabled until the
  rtl_433 binary is present and the flag is set, so the feature is inert on
  deployments without rtl_433.
- **Two-tier rate/protocol policy keyed on burst bandwidth** (resolved
  2026-09-07 - fixed table, not a per-band lookup; only protocol 383 is validated
  so far, so a richer scheme is premature):

  | Burst bandwidth | Target rate | rtl_433 pass |
  |---|---|---|
  | narrow ~100-200 kHz (2-FSK, ISM OOK) | 1.0 Msps | full default `-R` set |
  | SSN-mesh-class 2-FSK 100 kbit/s | 1.6 Msps | `-R 383` (+ flex `-X`) |

- **Multiple frames per burst window -> JSON array on the one burst row**
  (resolved 2026-09-07). A burst window can decode into several rtl_433 frames
  (the HCRO data had a poll + a data frame in one channel); all go into that
  detection's `attribution` JSON as an array. Preserves the one-burst =
  one-detections-row model and the `burst_id` merge key; no synthetic per-frame
  ids.

- **Mark every attempt, three-state** (resolved 2026-09-07). On a gated burst
  that is decoded but matches nothing, still write `attribution` with
  `{"attempted": true, "decoded": false, "at": ...}` and leave `model` /
  `protocol_id` null. This lets the operator distinguish "not attributed yet"
  (all null) from "attributed as nothing" (attempted, not decoded) from
  "attributed" (model set).

- **Sensor runs at BOTH 26 and 56 Msps** (resolved 2026-09-07). The channelizer
  computes the resample ratio dynamically from the live `sample_rate_hz`, so the
  feature works at either operating point. 56 Msps is the tighter budget (see
  below); the drop-strongest cap is what keeps the worker safe there.

## Measured budget (this is why in-process-on-Jetson is viable)

All measured 2026-09-07. Full method in the session; key numbers:

- **Deployment target = `nano-super` @192.168.97.153**, Orin Nano Super, 6 cores.
  MAXN_SUPER enabled this session (nvpmodel symlink repointed to the Super conf;
  old target saved at `/etc/nvpmodel.conf.prev-target-preMAXN`). It now has a
  B200mini attached. Caveat: CPU still caps at 1510 MHz under MAXN - the Super
  ~1.7 GHz boost appears to need a reboot; numbers below are the conservative
  1510 MHz case.
- **Pipeline compute scaling** (measured on-box, single-thread): 26 Msps costs
  0.589x the 56 Msps compute per chunk. PSD + IQ-stats scale ~linearly (0.51x);
  burst detection is fixed cost (runs on a rate-independent 2500x2048 grid).
- **Pipeline load at 26 Msps ~= 2.9 of 6 cores** (from the scaling above,
  cross-checked against the live 15 W .177 box which runs 4.17 cores at 56 Msps).
- **rtl_433 decode cost on aarch64/MAXN, including fork+exec+model-load:**
  ~5.0 ms/burst for `-R 383` alone, ~6.4 ms for the full ~250-decoder set,
  so ~11 ms/burst worst case for the two-pass policy.
- **Worst-case worker load** (pathological: 40 gated bursts every 0.5 s chunk,
  both passes each) = 40 x 11 ms / 0.5 s ~= 0.9 core. Typical load after the SNR
  gate is a fraction of that.
- **Net at 26 Msps: ~3.8 of 6 cores** in the pathological case, well under half a
  core typically. The feature fits with margin.
- **At 56 Msps** the pipeline is ~4.2 cores (measured on the live .177 box; the
  MAXN box runs the same 1510 MHz clock so the core-count is comparable). Same
  worst-case worker ~0.9 core -> **~5.1 of 6**. Tighter but still fits, and the
  drop-strongest cap bounds it; typical load leaves comfortable room.

The three known-good fixtures (`burst_feb4_919MHz_75dB.cs16`,
`burst_feb5_917MHz_56dB.cs16`, `burst_feb5_913MHz_47dB.cs16`) decode with valid
CRC on the aarch64 build.

## Data available at the persistence point

In `pipeline/continuous.py`, at the point `detect_bursts` returns, the
`_ProcessResult` still carries the raw `iq_bytes` for the chunk alongside the
`bursts` list and `center_freq_hz`. This is the one window in which the IQ that
produced a burst is in hand - in free-run continuous mode `iq_bytes` is discarded
after processing unless a recording was triggered. So the producer must
channelize the burst slice out of that in-memory IQ *here*, not from a file that
will not exist.

Each `BurstFingerprint` carries what the channelizer needs: `peak_freq_hz`,
`center_freq_hz`, `bandwidth_hz`, `peak_power_db`, `start_time`, `stop_time`, and
the `burst_id` used to key the DB row.

## Architecture

### Producer (continuous processing path)

After `detect_bursts`, for each burst that clears the SNR gate, of the top N by
`peak_power_db`:

1. Slice the burst's sample range out of the chunk IQ (with a small guard).
2. Channelize: frequency-shift `peak_freq_hz - sdr_center` to DC, low-pass to
   `bandwidth_hz`, resample to the target rate (`scipy.signal.resample_poly`, ratio
   computed dynamically from the live `sample_rate_hz`, gcd-reduced from
   `target/source`; 56 Msps -> 1.6 Msps reduces to `resample_poly(1, 35)`,
   26 Msps -> 1.6 Msps to `resample_poly(4, 65)`).
3. Convert to interleaved int16 (`.cs16`).
4. Enqueue `(burst_id, cs16_bytes, target_rate, peak_power_db)` on a bounded
   queue. On a full queue, evict the lowest `peak_power_db` item (drop-strongest).

The DSP in steps 1-3 is ported from the sibling `gr-modules/iq-processing/
ssn_scan.py`; it is not yet in this repo.

### Consumer (single background worker task)

Drains the queue and for each item:

1. Write the `.cs16` to a temp file (or feed via stdin/`-r -`).
2. Run rtl_433 per the rate/protocol policy, `-F json`, via
   `asyncio.create_subprocess_exec` (never blocks the loop).
3. Parse stdout JSON. On a decode, `UPDATE detections SET model=?,
   protocol_id=?, attribution=? WHERE burst_id=?`. On no decode, either leave the
   row untouched or record an explicit "attempted, no match" marker (see Open
   items).

rtl_433 discovery mirrors `ssn_scan.find_rtl()`: `$RTL433`, a known build path,
then `PATH`. On the target the binary is `~/rtl_433_build/build/src/rtl_433`.

### Storage (`detections` table)

Three new nullable columns via the existing `PRAGMA table_info` + `ALTER TABLE`
migration in `database.py`:

- `model TEXT` - rtl_433 device model string (e.g. "SilverSpring-Mesh")
- `protocol_id INTEGER` - rtl_433 protocol number (383 for SSN)
- `attribution TEXT` - JSON. On decode: an array of frame objects (device/src id,
  RF channel, message type, CRC-valid, ...) plus a decode timestamp. On a gated
  burst that decoded to nothing: `{"attempted": true, "decoded": false, "at": ...}`
  with `model`/`protocol_id` left null (three-state, see Decisions).

`burst_id TEXT UNIQUE` already exists and is the merge key. `query_detections`
returns `SELECT *`, so the new columns surface to the API and the captures UI
overlay with minimal change.

## Data flow

```
continuous loop chunk
  -> detect_bursts -> [BurstFingerprint...]        (iq_bytes still in hand)
  -> SNR gate + top-N by peak_power_db
  -> channelize slice -> .cs16 bytes
  -> bounded queue (drop-strongest on overflow)
        |
        v  (background worker)
  rtl_433 -s <rate> -F json  ->  parse JSON
  -> UPDATE detections SET model,protocol_id,attribution WHERE burst_id
  -> surfaces via query_detections / UI overlay
```

## Error handling

- **Queue full:** evict weakest, never block. Count drops for a stat/log.
- **rtl_433 missing or flag off:** producer does not enqueue; feature inert.
- **rtl_433 nonzero exit / timeout:** log, drop the item, do not retry (a decode
  either works on the samples or does not). A per-decode wall-clock timeout guards
  against a hung subprocess.
- **No decode:** expected for most bursts; not an error. Still writes the
  attempted-marker JSON (three-state) rather than leaving the row untouched.
- **DB update for a since-pruned/absent burst_id:** `UPDATE` affects 0 rows, no-op.

## Testing

- **Unit - channelize:** feed a synthetic tone at a known offset, assert it lands
  at DC at the target rate and the output length matches the resample ratio.
- **Unit - decode step (fixtures):** run the aarch64/x86 rtl_433 against the three
  `~/ssn_bursts/*.cs16` fixtures, assert model "SilverSpring-Mesh", protocol 383,
  CRC valid. This is the smallest reproducer, independent of channelization.
  Skip-if rtl_433 not found.
- **Unit - backpressure:** fill the bounded queue past capacity, assert the
  lowest-power items are the ones dropped and the highest-power retained.
- **Unit - merge:** insert a detection, run the merge, assert the row's
  `model`/`protocol_id`/`attribution` populate and `query_detections` returns them.
- **Integration - end-to-end (guarded):** channelize one fixture-equivalent burst
  through the producer path and confirm the worker writes attribution to the row.

## Deployment notes

- rtl_433 must be the master build (protocol 383 = `src/devices/
  silver_spring_mesh.c`), configured file-only:
  `-DENABLE_RTLSDR=OFF -DENABLE_SOAPYSDR=OFF -DENABLE_OPENSSL=OFF`. Built and
  verified on the target this session at `~/rtl_433_build/build/src/rtl_433`.
- Fixtures kept at `~/ssn_bursts/` on the target as regression inputs.
- MAXN persists per the nvpmodel change, but the Super conf's boot default is 25W
  (mode 1); pin MAXN at boot if the full profile is wanted after a reboot, and
  reboot once to realize the ~1.7 GHz Super CPU clock.

## Resolved (2026-09-07)

The four items open at first draft are now decided and folded into Decisions
above: (1) fixed two-tier rate/protocol table keyed on bandwidth; (2) multiple
frames stored as a JSON array on the single burst row; (3) every attempt marked
(three-state: null / attempted-not-decoded / attributed); (4) the sensor runs at
both 26 and 56 Msps, so the channelizer derives the resample ratio dynamically
and the budget covers both (56 Msps is the tighter case, ~5.1 of 6 cores worst).

Nothing remains open at the design level; the next step is an implementation
plan.
