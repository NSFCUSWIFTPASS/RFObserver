# Bursts and attribution in ZMS observations

Status: draft, 2026-10-18. One decision open (Timing, below); everything else agreed.

## Goal

Every burst RFObserver detects goes to OpenZMS inside the averaged observation, with what is
known about it: when and where it was, how strong, and what rtl_433 made of it. Every burst
always carries an `attribution` object, including when attribution is switched off, so a
consumer can tell "not decoded" from "never tried" from "attribution disabled".

## Where it goes

Observations are SigMF archives built by `zms/sigmf_formatter.py` (PSD plus a `.sigmf-meta`
JSON document). The bursts go in that metadata under a new optional extension, so OpenZMS's
own `openzms-core:` keys are unchanged and a consumer that does not know the extension can
ignore it:

```json
"core:extensions": [
  {"name": "ntia-algorithm", "version": "v2.0.0", "optional": false},
  {"name": "ntia-sensor", "version": "v2.0.0", "optional": false},
  {"name": "openzms-core", "version": "v1.0.0", "optional": true},
  {"name": "rfobs", "version": "v1.0.0", "optional": true}
],
"rfobs:bursts": [ ... ]
```

The key is present on every observation; an observation with no bursts to report has
`"rfobs:bursts": []`.

## Burst object

Every key is always present; values that do not apply are `null`.

```json
{
  "burst_id": "4b80876d-42dd-45e0-bf3e-e3ae038eb214",
  "start": "2026-10-15T16:06:56.787000+00:00",
  "stop": "2026-10-15T16:06:56.799800+00:00",
  "duration_ms": 12.8,
  "window_start": "2026-10-15T16:06:56.500000+00:00",
  "center_freq_hz": 904007813.0,
  "bandwidth_hz": 218750.0,
  "freq_low_hz": 903898438.0,
  "freq_high_hz": 904117188.0,
  "peak_freq_hz": 904010000.0,
  "peak_power_db": -80.2,
  "peak_snr_db": 21.4,
  "attribution": { ... }
}
```

| Key | Meaning | Source |
|---|---|---|
| `burst_id` | Detection id, the same id the sensor's database and burst files use | `detections.burst_id` |
| `start`, `stop` | UTC, ISO 8601 with offset | `detections.start_time`, `stop_time` |
| `duration_ms` | Burst duration | `detections.duration_ms` |
| `window_start` | Start of the averaged window the burst belongs to; needed only by some timing options (see Timing) | the avg window the detection joins |
| `center_freq_hz`, `bandwidth_hz` | Burst band midpoint and width | `detections` |
| `freq_low_hz`, `freq_high_hz` | `center -/+ bandwidth / 2` | derived |
| `peak_freq_hz` | Frequency of the peak bin | `detections.peak_freq_hz` |
| `peak_power_db` | Peak bin power (the sensor's PSD units) | `detections.peak_power_db` |
| `peak_snr_db` | Peak power minus the detector's per-bin noise floor at the peak bin | new, computed at detection |

`peak_snr_db` is computed when the burst is detected, from the rolling detector's per-bin
noise floor, so every burst has it whether or not isolation runs. (Today an SNR exists only
inside the isolation gate, for bursts that reach it.)

## Attribution object

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
| `reason` | Why a burst was `not_attempted`; `null` otherwise |
| `decoder` | `"rtl_433"` when attribution is enabled, else `null` |
| `attempted_at` | When rtl_433 ran (`null` unless `decoded`, `not_decoded` or `failed`) |
| `model` | rtl_433 model of the first decoded frame (`decoded` only) |
| `protocol_id` | rtl_433 protocol number (`decoded` only) |
| `frames` | Number of decoded frames (`decoded` only) |
| `fields` | Decoded content of the first frame, without rtl_433's bookkeeping keys (`time`, `model`, `count`, `num_rows`, `rows`, `codes`, `mod`, `freq*`, `rssi`, `snr`, `noise`, `protocol`, `mic`); `decoded` only |

`status`:

| Value | Meaning |
|---|---|
| `decoded` | rtl_433 recognised the burst |
| `not_decoded` | rtl_433 ran and nothing matched |
| `not_attempted` | Attribution was on but this burst was not tried; see `reason` |
| `failed` | rtl_433 errored or timed out |
| `disabled` | Attribution was off: `enabled` is `false` and every key other than `enabled` and `status` is `null` |
| `pending` | Exists only if the chosen timing can send a burst before its attribution is final |

`reason` (for `not_attempted`), from the isolation outcomes the pipeline already has:

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
and at HCRO volumes (sensor .35 logs about 568k detections a day) they would dominate the
archive. They stay in the sensor's database and the fleet Detections download.

## Pipeline changes

1. **Per-burst isolation outcome.** `pipeline/isolation.py` counts outcomes but does not record
   them per burst. Record each burst's outcome (including the split of `gated_out` into
   `below_snr` and `rate_limited`, which today share one counter) where the submit path can
   read it by `burst_id`.
2. **Peak SNR at detection.** The rolling detector computes `peak_snr_db` from its per-bin noise
   floor when it emits a burst; it is stored with the detection.
3. **Attribution status per burst.** Built from the detection's attribution record (`decoded`,
   `not_decoded`), the attribution worker's `failed` outcome, the isolation outcome above, and
   the attribution switch.
4. **Formatter.** `create_sigmf_metadata` takes the burst list, adds the `rfobs` extension and
   `rfobs:bursts`. `submit_observation` passes it through.
5. **Which bursts go in which observation**: per the Timing decision.

## Timing (OPEN, waiting on the user)

A window is submitted the moment it closes. A burst is only reported by the detector
0.5-1.3 s after it starts (it waits for the burst to end), and rtl_433 runs after that, so
when a window closes its last bursts are not known yet and few are attributed. Options:

| Option | What happens | Cost |
|---|---|---|
| A. Hold each observation (~5 s, configurable) | Submitted once its window's bursts are final; bursts sit in their own window; unresolved ones `pending` | The whole observation, including the interference flag, arrives ~5 s later (the user's objection) |
| B. Report each burst once when final (recommended) | Observations go out at window close as now; each burst is reported once, in the next observation sent after its attribution is final, with `window_start` naming its own window | An observation's burst list is not its own window's; consumers join by `window_start` |
| C. Short hold (~2 s) | As A with a shorter delay | Interference delayed ~2 s; slow rtl_433 runs still end `pending` |
| D. Send twice | A burst is sent when detected (`pending`) and again when final | ZMS sees each burst twice and must merge by `burst_id` |

`window_start` is needed by B (and harmless in the others); `pending` exists only in A, C
and D.

## Error handling

- A burst whose isolation outcome or attribution is missing (for example the sensor restarted
  between detection and submit) is sent with the status that can still be established; it is
  never dropped.
- ZMS submission failures keep today's behaviour (logged, observation not retried).

## Testing

- Formatter: the extension entry and `rfobs:bursts` are present, empty list when no bursts, and
  every key of a burst and of its attribution is present for each status.
- Status mapping: one case per `status` and per `reason`, including attribution off.
- Peak SNR: computed from the detector's noise floor at the peak bin.
- Timing: per the chosen option (each burst exactly once for B; `pending` and hold delay for A/C).

## Not in scope

- The interference flag and per-bin violations (still always false; separate change, see the
  violation-flag note).
- Raw bit rows.
