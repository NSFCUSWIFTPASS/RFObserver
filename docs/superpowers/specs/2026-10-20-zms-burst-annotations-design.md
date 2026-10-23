# Bursts and attribution as OpenZMS annotations

Status: draft, 2026-10-20. Supersedes the 2026-10-18 draft ("Bursts and attribution in ZMS
observations"), which embedded bursts in the SigMF metadata (`rfobs:bursts`) and left the
timing open. OpenZMS is adding a batched annotation API and sensor-set observation UUIDs;
this design uses both. Open questions for OpenZMS are in
`docs/openzms-annotation-questions.md`.

## Goal

Every burst RFObserver detects goes to OpenZMS once, as an annotation on the averaged
observation of the window it started in, with what is known about it: when and where it was,
how strong, and what rtl_433 made of it. Every burst carries an `attribution` object,
including when attribution is switched off, so a consumer can tell "not decoded" from "never
tried" from "attribution disabled". The link is the observation's UUID, not timestamps.

## Decisions

| Decision | Choice |
|---|---|
| Transport | Annotations, batched. Nothing burst-related in the SigMF metadata |
| When a burst is sent | Once, when final: detected, and attribution finished, skipped or timed out |
| Batch trigger | 20 final bursts or 0.5 s since the oldest one became sendable, whichever is first |
| Attribution timeout | 10 s after detection; then sent as `failed` with reason `timeout` |
| Observation link, now | The observation id returned by OpenZMS when it accepts the observation |
| Observation link, later | A UUID the sensor generates per window and sends with the observation |
| Observations | Unchanged: submitted when their window closes, no hold |

## How it fits together

```
window closes ──> observation submitted ──> accepted (id) ──┐
                                                            ├──> annotation queue ──> batch POST
burst detected ──> attribution (rtl_433, <= 10 s) ──> final ┘     (20 or 0.5 s)
```

1. Each averaged window gets an observation id. Phase 1: the `id` in OpenZMS's `201`
   response. Phase 2: a `uuid4` the sensor generates when the window opens, stored on the
   `avg_windows` row and sent with the observation.
2. Each detection is assigned to the window containing its `start_time` (a burst that crosses
   a window boundary belongs to the earlier window) and records that window's row id, and in
   phase 2 its observation UUID.
3. A burst becomes final when its attribution status is anything other than `pending`, or 10 s
   after detection.
4. A final burst is sendable once its observation has been accepted. Bursts of a rejected or
   failed observation are not sent (see Error handling).
5. The sender flushes sendable bursts as one batch when 20 are waiting or the oldest has waited
   0.5 s. A batch only ever names accepted observations, because OpenZMS rejects a whole batch
   when one observation is unknown.

Annotations cannot be updated after creation, which is why a burst waits until it is final
rather than being sent at detection and amended.

## Annotation

Mapping onto the OpenZMS annotation fields (`store.Annotation` in zms-dst):

| Field | Value |
|---|---|
| `observation_id` | The observation the burst's window was submitted as |
| `type` | `"rfobs-burst"` |
| `name` | The rtl_433 model when decoded (e.g. `"ssnmesh-like"`), else `"burst"` |
| `description` | `null` |
| `min_freq`, `max_freq` | Burst band edges in Hz, integers (`center -/+ bandwidth / 2`) |
| `sample_start`, `sample_count` | `null`: the observation carries an averaged PSD, not IQ samples |
| `value` | The burst object below |
| `creator_id` | Per OpenZMS's answer (the gRPC call takes it from the caller today) |

There is no time column on annotations, so the burst's times live in `value`.

### Burst object (`value`)

Every key is always present; values that do not apply are `null`.

```json
{
  "schema": "rfobs-burst/v1",
  "burst_id": "4b80876d-42dd-45e0-bf3e-e3ae038eb214",
  "start": "2026-10-15T16:06:56.787000+00:00",
  "stop": "2026-10-15T16:06:56.799800+00:00",
  "duration_ms": 12.8,
  "center_freq_hz": 904007813.0,
  "bandwidth_hz": 218750.0,
  "peak_freq_hz": 904010000.0,
  "peak_power_db": -80.2,
  "peak_snr_db": 21.4,
  "attribution": { ... }
}
```

| Key | Meaning | Source |
|---|---|---|
| `schema` | Version of this object | constant |
| `burst_id` | Detection id, the same id the sensor's database and burst files use | `detections.burst_id` |
| `start`, `stop` | UTC, ISO 8601 with offset | `detections.start_time`, `stop_time` |
| `duration_ms` | Burst duration | `detections.duration_ms` |
| `center_freq_hz`, `bandwidth_hz` | Burst band midpoint and width | `detections` |
| `peak_freq_hz` | Frequency of the peak bin | `detections.peak_freq_hz` |
| `peak_power_db` | Peak bin power (the sensor's PSD units) | `detections.peak_power_db` |
| `peak_snr_db` | Peak power minus the detector's per-bin noise floor at the peak bin | new, computed at detection |

`peak_snr_db` is computed when the burst is detected, from the rolling detector's per-bin
noise floor, so every burst has it whether or not isolation runs. (Today an SNR exists only
inside the isolation gate, for bursts that reach it.)

### Attribution object

```json
"attribution": {
  "enabled": true,
  "status": "decoded",
  "reason": null,
  "decoder": "rtl_433",
  "attempted_at": "2026-10-15T16:06:56.906498+00:00",
  "model": "ssnmesh-like",
  "protocol_id": 383,
  "frames": 1,
  "fields": {"id": 1234}
}
```

| Key | Meaning |
|---|---|
| `enabled` | Whether attribution was switched on when the burst was detected |
| `status` | One of the values below |
| `reason` | Why a burst was `not_attempted` or `failed`; `null` otherwise |
| `decoder` | `"rtl_433"` when attribution is enabled, else `null` |
| `attempted_at` | When rtl_433 ran (`null` unless `decoded`, `not_decoded` or `failed` after a run) |
| `model` | rtl_433 model of the first decoded frame (`decoded` only) |
| `protocol_id` | rtl_433 protocol number (`decoded` only) |
| `frames` | Number of decoded frames (`decoded` only) |
| `fields` | Decoded content of the first frame, without rtl_433's bookkeeping keys (`time`, `model`, `count`, `num_rows`, `rows`, `codes`, `mod`, `freq*`, `rssi`, `snr`, `noise`, `protocol`, `mic`); `decoded` only |

`status` (there is no `pending`: a burst is only sent when final):

| Value | Meaning |
|---|---|
| `decoded` | rtl_433 recognised the burst |
| `not_decoded` | rtl_433 ran and nothing matched |
| `not_attempted` | Attribution was on but this burst was not tried; see `reason` |
| `failed` | rtl_433 errored (`reason: "error"`) or the 10 s timeout passed (`reason: "timeout"`) |
| `disabled` | Attribution was off: `enabled` is `false` and every key other than `enabled` and `status` is `null` |

`reason` for `not_attempted`, from the isolation outcomes the pipeline already has:

| Value | Isolation outcome today |
|---|---|
| `below_snr` | Under `ISOLATION_SNR_DB` at the peak bin (counted as `gated_out`) |
| `rate_limited` | Over `ISOLATION_MAX_PER_SEC` (counted as `gated_out`) |
| `iq_expired` | Samples already overwritten in the IQ ring |
| `too_long` | Longer than `ISOLATION_MAX_BURST_SEC` |
| `queue_full` | Isolation queue full |
| `rtl433_unavailable` | Attribution on but rtl_433 not found |
| `error` | Isolation raised an error for this burst |

Raw bit rows (rtl_433 `rows`, the hex payload) are not sent: they can be kilobytes per burst,
and at HCRO volumes (sensor .35 logs about 568k detections a day) they would dominate. They
stay in the sensor's database and the fleet Detections download.

## Sensor changes

1. **Window id.** `avg_windows` gains `observation_id TEXT` (nullable): the OpenZMS id once
   accepted (phase 1), or the sensor UUID from creation (phase 2). The ZMS submit path records
   the outcome per window: accepted (with id), rejected, or failed.
2. **Detection to window.** `detections` gains `avg_window_id INTEGER` (and, in phase 2,
   `observation_id TEXT`), set when the detection is stored, from the window containing its
   start.
3. **Per-burst isolation outcome.** `pipeline/isolation.py` counts outcomes but does not record
   them per burst. Record each burst's outcome (including the split of `gated_out` into
   `below_snr` and `rate_limited`, which today share one counter) where the sender can read it
   by `burst_id`.
4. **Peak SNR at detection.** The rolling detector computes `peak_snr_db` from its per-bin noise
   floor when it emits a burst; it is stored with the detection.
5. **Annotation sender** (`zms/annotations.py`). In-memory queue of bursts waiting to be final
   or sendable, the 20 / 0.5 s flush, and the 10 s attribution timeout. Builds the burst object
   from the detection row plus its attribution and isolation outcome. Sent through the OpenZMS
   client's batch call.
6. **Observation UUID (phase 2).** Generate the UUID when the window opens, send it with the
   observation the way OpenZMS specifies, store it on the window and its detections. The
   sender then no longer needs the `201` id, and a resubmitted observation keeps its UUID.
7. **SigMF.** No change to `sigmf_formatter.py` (no `rfobs:bursts`).

## Error handling

- **Observation rejected or failed:** its bursts are dropped from the queue and counted
  (`annotations_dropped_no_observation`); observations are not retried today. In phase 2, if
  OpenZMS makes a resubmit with the same UUID idempotent, a retried observation would let its
  bursts go out.
- **Annotation batch rejected:** logged with the status and the first error; the batch is
  retried once after 5 s, then dropped and counted. A batch never mixes in an observation that
  was not accepted, so an all-or-nothing rejection points at a real problem, not at one
  missing observation.
- **Sensor restart:** the queue is in memory. Bursts still waiting at shutdown are lost, as
  unsent observations are today. Their data stays in the sensor's database.
- **Missing pieces:** a burst whose isolation outcome or attribution record is missing is sent
  with the status that can still be established; it is never held past the 10 s timeout.
- **Backlog:** the queue is capped (default 10,000 bursts). Beyond that the oldest are dropped
  and counted, so a ZMS outage cannot grow memory without bound.

## Settings

| Setting | Default | Meaning |
|---|---|---|
| `ZMS_ANNOTATIONS_ENABLED` | `false` | Send burst annotations (needs ZMS submission on) |
| `ZMS_ANNOTATION_BATCH` | `20` | Flush at this many sendable bursts |
| `ZMS_ANNOTATION_FLUSH_SEC` | `0.5` | Flush when the oldest sendable burst has waited this long |
| `ZMS_ATTRIBUTION_TIMEOUT_SEC` | `10` | Send a burst as `failed` / `timeout` after this long |
| `ZMS_ANNOTATION_QUEUE_MAX` | `10000` | Queue cap |

## Testing

- Burst object: every key present for each `status` and `reason`, including attribution off;
  `name` is the model when decoded; `min_freq`/`max_freq` integers from the band.
- Window assignment: a burst crossing a boundary goes to the window containing its start.
- Sender: flush at 20 and at 0.5 s; a burst waits for its attribution, then goes out after
  10 s as `failed` / `timeout`; nothing is sent before its observation is accepted; bursts of
  a rejected observation are dropped and counted; a batch never contains a non-accepted
  observation; the queue cap drops oldest first.
- Each burst is sent exactly once.
- Peak SNR: computed from the detector's noise floor at the peak bin.
- Client: the batch call against a fake OpenZMS (request shape, error handling).

## Not in scope

- The interference flag and per-bin violations (still always false; separate change, see the
  violation-flag note).
- Raw bit rows.
- Retrying failed observations (depends on OpenZMS making resubmits idempotent).

## Depends on OpenZMS

See `docs/openzms-annotation-questions.md`. Building the sender waits on the batch endpoint's
transport and auth; everything up to the burst object (window ids, detection to window,
per-burst outcome, peak SNR) can be built now.
