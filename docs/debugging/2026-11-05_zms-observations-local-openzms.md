# RFObserver observations into OpenZMS: end-to-end check on a local deployment

## The question

2026-11-05. Do RFObserver's observations work with OpenZMS? Checked end to end: a mock-receiver
RFObserver at main 2481f36 (Python 3.10 venv) submitting to the local OpenZMS dev stack on this
workstation (`~/GitHub/zms/deploy`, built and started 2026-11-05; zms-dst 33bca10, zms-zmc
8036ff4, zms-identity d50ea4b). Attributions were out of scope.

Symptom on the first run: every observation rejected, 125 of 125 in 70 s:

```
POST http://localhost:3220/v1/observations "HTTP/1.1 400 Bad Request"
DST rejected SigMF: 400 {"error":"error processing sigmf metadata (ext openzms-core): invalid UUID length: 0"}
```

## The answer

RFObserver sends `"metric_id": ""` in every `openzms-core:values` entry when `RFOBS_ZMS_METRIC_ID`
is unset, and DST parses `metric_id` as a UUID, so the whole observation is rejected. With a real
metric id, plus a DST volume and volume policy for the monitor (server-side setup DST needs for
any client), all observations are stored with their values and DST's `psd-mean` annotation.

## Procedure

1. **Find the target.** `~/GitHub/DevOps/docs/notes/docker-stacks.md` documents the local stack;
   `ss -ltn` showed 3200-3253 listening; `GET /v1/version` on identity answered. Docker itself was
   not reachable (socket permission), so everything below goes through the HTTP APIs.
2. **Register a sensor in ZMC** with the bootstrap element token (`BOOTSTRAP_USER_ELEMENT_TOKEN` in
   `deploy/docker-compose.yml`, element "powder"): location, radio, radio port (rx, 902-928 MHz),
   monitor (`types inline,sweep`, `formats sigmf-archive`). The instance had no monitors and no
   observations before, so every result below is from this test.
3. **Run a mock sensor** from a detached worktree of main, storage and DB in a scratch dir,
   `RFOBS_ZMS_*` pointed at the stack, `timeout 70`. Controls for: the sensor code as shipped,
   with no `.env` from the repo.
4. **Read the error and trace it** to `src/rfobserver/zms/sigmf_formatter.py`
   (`"metric_id": metric_id or ""`) and DST's `store.Value.MetricId *uuid.UUID`
   (`pkg/store/model.go`).
5. **Test the hypothesis with one change:** create a DST metric (`POST /v1/metrics`) and rerun with
   `RFOBS_ZMS_METRIC_ID`. The 400 went away; a 403 appeared (next layer).
6. **Read the 403:** `applyObservationVolumePolicy` (`pkg/server/northbound/handlers.go`) needs a
   volume policy matching the observation's element, monitor and kind. Created a disk volume
   (`prefix /data/observations`, admin token) and a policy scoped to the test monitor.
7. **Rerun and verify what DST stored:** observation list, one observation with
   `X-Api-Elaborate: true`, its raw archive, and the series values.

## Evidence

Run 2 (metric set, no volume policy), 25 s:

```
     43 POST http://localhost:3220/v1/observations "HTTP/1.1 403 Forbidden"
DST rejected SigMF: 403 {"error":"no volume_policy found for this observation"}
```

Run 3 (metric and volume policy), 25 s:

```
     43 POST http://localhost:3220/v1/observations "HTTP/1.1 201 Created"
```

One stored observation:

```
total 43
  kind: psd   types: rfs.psd.v1   format: sigmf   labels: ota,single-freq
  min_freq: 2409000000   max_freq: 2464972656   freq_step: 27343
  starts_at: 2026-11-05T17:36:02.38532Z   ends_at: None   interference: False
  datatype rf32_le   volume 755cadd2-...   criticality 0
raw: 200 20480 bytes application/sigmf-archive
annotations 1   (with X-Api-Elaborate: true)
  type psd-mean name sigmf-psd-mean stat mean len 2048 min 2409000000 max 2464998464 step 27343
```

Values, read from the auto-created default series (`GET /v1/series/{id}/values/internal`):

```
values 43, distinct observations 43
{'fields': {'max': -28.51, 'mean': -36.95, 'median': -54.48, 'kurtosis': 4.88},
 'tags': {'name': 'Zone 0', 'max_freq': 2464972656, 'min_freq': 2409000000, 'interference': False}}
```

The sensor's `message_count` counts only accepted observations (`zms/monitor.py`), so its status
showed 0 sent during run 1.

Deployed sensors (read-only `GET /api/zms/status`): 10.1.42.31 `enabled false`, monitor id set;
10.1.42.32 `enabled false`, no monitor id. Not affected today.

## Measured and REJECTED (do not retry)

- **"Annotations and values are missing"**: `GET /observations/{id}?elaborate=true` returned 0
  annotations and 0 values. Wrong probe: DST reads elaboration from the `X-Api-Elaborate` header,
  not a query parameter, and values are not returned on the observation at all; they live in a
  series. Both exist (see Evidence).
- **`GET /v1/values?observation_id=...`**: not a JSON list endpoint on this build (non-JSON
  response). Read values through `/v1/series/{id}/values/internal`.

## Measurement traps hit

- The mock receiver covers 2409-2465 MHz, not 902-928 MHz; the test radio port says 902-928 MHz
  and DST did not check the observation against it.
- `rfobserver run` under `timeout` logs `ERROR Second SIGTERM during shutdown; exiting now`; this
  is the shutdown path, not a ZMS error.

## Server-side setup an OpenZMS deployment needs for RFObserver

1. A ZMC monitor (with location, radio, rx radio port) and its id in `RFOBS_ZMS_MONITOR_ID`.
2. A DST metric, its id in `RFOBS_ZMS_METRIC_ID` (today RFObserver cannot submit without it).
3. A DST volume and a volume policy matching the monitor (or a broader policy); without it DST
   answers 403 to every observation.

## Open, not yet answered

- `ends_at` is null: RFObserver sends no end time / duration for the averaged window.
- The `psd-mean` annotation's `max_freq` is `x_start + x_step * length` (2464998464) while the
  observation's is the last bin centre (2464972656), and `freq_step` is truncated to an integer
  (27343 for 27343.75 Hz), which drifts 1.5 kHz over 2048 bins.
- Behaviour against a non-local OpenZMS (auth through a real identity service, TLS).
- Annotations RFObserver itself sends (out of scope here).

## Fix (2026-11-05, branch fix/zms-metric-id)

User decision: the metric id is required. `settings.zms` is built only when `ZMS_METRIC_ID` is
set too; `settings.zms_missing` names the unset fields. Without it the monitor does not start and
the startup log, `POST /api/zms/enable` and the Config page (status next to the OpenZMS toggle,
from the 1 Hz heartbeat) say what is missing. The Config page gains a Metric ID field, and the
formatter refuses an empty metric id instead of sending `""`.

Verified against the same local stack:

```
no metric:   WARNING ZMS enabled but not started: missing RFOBS_ZMS_METRIC_ID   (0 POSTs)
with metric: 34 POST .../v1/observations "HTTP/1.1 201 Created"
Config page: {"after":{"checked":false,"status":"Missing: metric id"},"errors":[]}
```

A first version showed the enable endpoint's error text on the page, but the heartbeat replaced
it with "disabled" within a second; the reason now comes from the status payload (`missing`).
