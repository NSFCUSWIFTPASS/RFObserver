# Burst Isolation and Attribution in the Streaming Pipeline: Design

Date: 2026-09-28
Branch: `feat/rtl433-burst-attribution` (main merged in at 25ef2fd)
Status: approved in conversation, section by section; this document is for review.

## Intent

The user, verbatim:

> "Is there burst attribution pipeline tested on the local Jetson?"

> "Another thinng, did we run a actual capture through the pipeline with the replay functionality?"

> "Let's also have a enable for burst attribution and burst isolation (frequency shift + decimation)"

Decisions taken in conversation:

| Question | Decision |
|---|---|
| Proceed how | Wire attribution into the streaming pipeline, then verify on nano-super |
| What isolation alone produces | Both: saved isolated-burst IQ files, and a feed to add-on modules |
| Where replay results go | Replay-only outputs; never the DB |
| IQ source for isolation | Grow the existing pre-trigger ring when isolation is on |

## Why this is needed (verified 2026-09-28)

- `AttributionWorker` exists only in `ContinuousProcessor` (the sweep pipeline).
  `build_processor` picks `StreamingProcessor` whenever `TRIGGER_ENABLED` is set or the
  config is not a sweep: the normal single-frequency setup on nano-super and the field
  sensor, and every replay. In those modes attribution never runs, even when
  `ATTRIBUTION_ENABLED=true`.
- Replay mode suppresses every DB write, and attribution writes by updating a
  `detections` row, so there would be nothing to update in replay.
- The feature's tests call the decoder and worker directly, so neither gap showed. The
  plan's acceptance step on nano-super ("replayed SSN capture or live B200mini") was
  never run.

**Success means:** with the switches on, a replayed wideband SSN capture on nano-super
yields SilverSpring-Mesh / protocol 383 on its strong bursts, isolated bursts are saved
and handed to modules, the live B200mini pipeline keeps its latency and drops nothing
because of the stage, and every burst the gate picks ends in exactly one reported state.

## Settings

| Setting | Default | Meaning |
|---|---|---|
| `ISOLATION_ENABLED` | `false` | Cut picked bursts out of the IQ (frequency shift and decimate); save them and offer them to modules |
| `ATTRIBUTION_ENABLED` | `false` | Decode isolated bursts with rtl_433. Turning it on forces isolation on |
| `ISOLATION_LOOKBACK_SEC` | `1.5` | Minimum IQ ring length while isolation is on |
| `ISOLATION_MAX_BURST_SEC` | `0.5` | Longer bursts are isolated only up to this length |
| `ISOLATION_SNR_DB` | `13.0` | Gate: dB over the noise floor a burst needs to be isolated |
| `ISOLATION_MAX_PER_SEC` | `20` | Gate: most bursts isolated per second, strongest first |
| `ISOLATION_QUEUE_MAX` | `64` | Bounded queues into the isolation worker and the rtl_433 worker |
| `BURST_ARCHIVE_MAX_GB` | `2.0` | Cap on saved burst files |
| `ATTRIBUTION_RTL433_PATH` | `""` | Unchanged: explicit rtl_433 path, or auto-discover |

`ATTRIBUTION_SNR_DB`, `ATTRIBUTION_MAX_PER_CHUNK` and `ATTRIBUTION_QUEUE_MAX` (branch-only,
never released) are replaced by the isolation gate and queue settings: one gate, not two.
All new settings appear on the config page with help text.

## Architecture

```
processing/isolate.py     Pure DSP. slice_and_channelize(ring_data, ring_start, burst,
                          sample_rate, center_freq) -> IsolatedBurst | Skip(reason).
                          Reuses channelize.select_rate_and_protocols and the shift +
                          resample_poly path of channelize_to_cs16.
pipeline/isolation.py     The stage: gate, bounded queue, one worker thread, fan-out.
pipeline/attribution.py   Existing rtl_433 discovery, decode, StrongestQueue and
                          AttributionWorker, reused; output routed by a sink.
```

- `BurstFingerprint` gains optional `start_sample` / `stop_sample` (absolute stream
  sample positions); `None` where unknown (sweep pipeline, older paths).
- `CircularBuffer` gains `read_range(start, end) -> ndarray | None`: a copy of stream
  samples `[start, end)` if all are still in the ring, else `None`; safe against the
  receiver writing (same lock as `read_with_position`).
- `UpstreamModule` gains an optional `feed_burst(iq: np.ndarray, sample_rate: int,
  meta: dict) -> None` with a no-op default; `ModuleManager.feed_bursts` calls it on
  every module. Existing modules are unaffected.
- The sweep pipeline keeps its whole-capture channelize but routes through the same
  stage (gate, fan-out, states), so both pipelines behave the same.

## Data flow (streaming)

1. The burst thread already feeds each PSD grid to the rolling detector; it also passes
   that grid's `chunk_start`.
2. The detector records the stream position of every PSD row (not derived from a row
   count, so dropped chunks cannot shift it). Completed bursts carry `start_sample` and
   `stop_sample`.
3. The burst thread hands completed bursts with the current center frequency and sample
   rate to the isolation queue with `put_nowait`; it never blocks.
4. The isolation worker thread applies the gate (SNR over the detector's noise floor,
   strongest first, `ISOLATION_MAX_PER_SEC`), reads each picked burst's range plus a
   2 ms guard band on each side from the ring with `read_range`, and channelizes it:
   shift the burst's peak frequency to DC, decimate to the tier rate
   (1.6 Msps at or above 200 kHz bandwidth, else 1.0 Msps).
5. Fan-out per isolated burst:
   - **Save** as SigMF (`ci16_le`, `core:frequency` = the burst's peak frequency,
     `core:sample_rate` = tier rate, burst_id, times, SNR, source capture in the global)
     under `STORAGE_PATH/bursts/<YYYYMMDD>/<burst_id>.sigmf-{meta,data}`. Linked to its
     detection by burst_id; no schema change.
   - **Modules**: `ModuleManager.feed_bursts`.
   - **Attribution** (when on): the existing `StrongestQueue` into `AttributionWorker`.
6. The rtl_433 worker (asyncio task, as today) decodes and writes `model`, `protocol_id`,
   `attribution` onto the `detections` row. If the row is not there yet, the update is
   retried once after 2 s.

All isolation DSP runs on the isolation worker thread: never on the receiver, dispatch
or burst threads, or the event loop.

## Replay

Isolation and attribution run the same way; only their outputs are routed differently,
and nothing is written to the DB.

- Burst files: `STORAGE_PATH/bursts/replay-<capture stem>/`.
- Attribution results: the live UI burst overlay shows a protocol label on the burst,
  and results are written to `<capture stem>.attribution.json` beside the replayed
  capture (a list of burst_id, times, peak frequency, SNR, model, protocol_id,
  attribution, state). When the replay is also being recorded, the results are merged
  into that recording's `.detections.json` sidecar.

## Storage

- `bursts/` is bounded by `BURST_ARCHIVE_MAX_GB`, oldest first, checked by the storage
  loop.
- The storage governor treats burst files like automatic captures: evicted first at
  step 1. At step 3 isolation stops saving files and keeps feeding modules and
  attribution.
- `/api/health` storage block gains `bursts_gb`.

## States and failure handling

Every burst the gate picks ends in exactly one state:

| State | Meaning |
|---|---|
| `isolated` | Cut out and fanned out |
| `iq_expired` | Its samples have already left the ring (or the ring was rebuilt) |
| `too_long` | Isolated, but only the first `ISOLATION_MAX_BURST_SEC` |
| `queue_full` | Dropped because a queue was full; the weakest burst goes first |
| `error` | An exception in isolation; logged with the burst_id |

- Counters for each state, plus rtl_433 outcomes (decoded, attempted-not-decoded,
  timeout), appear in `/api/health` under `isolation` and are logged once a minute when
  non-zero.
- rtl_433 not found: attribution is off with a WARNING; isolation still runs.
- A decode that times out is marked attempted-not-decoded, as today.
- Changing the sample rate or frequency rebuilds the ring; bursts from before the change
  become `iq_expired`.
- At startup, with isolation on, the ring size (`max(TRIGGER_PRE_SEC,
  ISOLATION_LOOKBACK_SEC)` at the configured rate: about 336 MB at 56 Msps, 156 MB at
  26 Msps) is checked against the existing recording RAM cap; if it does not fit,
  isolation is disabled with an ERROR log and a health flag rather than risking OOM.

## Testing

Unit (workstation):

- Isolation DSP: a synthetic FSK burst at a known offset inside a longer wideband
  signal comes out centered, at the tier rate, at the right length; an expired range
  yields `iq_expired`; an over-long burst is truncated and marked `too_long`.
- Detector: bursts carry correct `start_sample` / `stop_sample`, including across a
  dropped chunk.
- `CircularBuffer.read_range`: in range, partly overwritten, wraparound.
- Stage: gate order and rate limit; every picked burst lands in exactly one state; the
  queue drops the weakest when full; attribution forces isolation on; rtl_433 missing
  leaves isolation running.
- Outputs: saved SigMF loads with the sigmf library; replay writes the
  `.attribution.json` and makes no DB writes; `feed_burst` is called on modules.
- Storage: burst cap and step-1 eviction; saving stops at step 3.
- End to end: the three `ssn_bursts` fixtures embedded in a synthetic 26 Msps
  wideband stream at offsets, run through the real streaming pipeline in replay mode,
  decode as SilverSpring-Mesh / 383. Skips where rtl_433 or the fixtures are absent.

Acceptance on nano-super, MAXN, results in a dated `docs/debugging/` record:

1. Replay: download `feb4_19-39-48` (strongest decode, 919.399 MHz at 75 dB) and
   `feb5_05-29-58` (three decoding bursts) with `gdown` from the Drive IDs in
   `feature-reqs/rtl433-burst-attribution.md`; replay with isolation and attribution
   on. Pass: the known bursts decode as SilverSpring-Mesh / 383 at the recorded
   frequencies; weaker ones get attempted-not-decoded; `.attribution.json` and burst
   SigMF files written; no DB writes.
2. Live B200mini at 915 MHz, 26 Msps: pipeline latency and `excess_ms` stay near the
   baseline (about 270 ms) with both switches on; no dropped chunks or overflows caused
   by the stage; state counters consistent; any on-air SSN decodes (on-air SSN traffic
   is not guaranteed, so replay is the decode proof).
3. RAM: ring size and process RSS within budget at the 1.5 s lookback.

## Out of scope

- A UI panel for browsing burst files beyond the overlay label and download links.
- The offline `/captures/attribute` route.
- Decoders other than rtl_433.
- Merging this branch to main (a separate decision after acceptance).

## Open items

- On-air SSN at the lab is unknown; live decoding may show nothing.
- Whether 1.5 s of lookback is enough at 56 Msps under load (latency there was about
  1 s before the latency fix); measured in acceptance, and the setting can be raised.
