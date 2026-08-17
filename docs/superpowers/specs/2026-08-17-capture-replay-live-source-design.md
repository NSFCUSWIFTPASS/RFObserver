# Capture replay as a live source (threshold-tuning mode)

## Goal

Let a recorded IQ capture be replayed through the *live* RFObserver detection
pipeline and watched in the dashboard exactly as if it were streaming from the
SDR, so detection thresholds can be tuned against a known signal (e.g. the
December SSM FHSS capture) without turning on the observatory transmitters.
The replay paces to real time (with a speed control), loops continuously, and
its detections are shown live but never persisted or emitted upstream. Threshold
edits apply to the running pipeline instantly so the effect is visible on the
next loop.

## Background (from code audit)

- **Swappable receiver behind a supervisor.** `pipeline/supervisor.py`
  `PipelineSupervisor(build_receiver, build_processor)` builds the receiver +
  processor lazily on `set_active(True)` and tears them down on
  `set_active(False)` (the "Sensor Active"/Standby toggle). `pipeline/app.py`
  constructs it with a `build_receiver()` closure (mock vs real `Receiver`) and a
  `build_processor(receiver)` closure (streaming vs sweep). The supervisor is on
  `app.state.supervisor`; `GET/POST /api/sensor` (`web/routes/api.py`) drive it.
- **File replay receiver already exists.** `capture/replay_receiver.py`
  `FileReplayReceiver(MockReceiver)` serves a `SigmfCapture` chunk-by-chunk from a
  memmap in the exact SC16-int32 format the SDR/mock produce, via
  `recv_chunk(out_buf)`. Today it runs with **no real-time pacing** and, when
  exhausted, drains trailing noise (does not loop). `pipeline/replay.py`
  `run_replay(...)` already assembles a one-shot batch replay and loads either a
  SigMF file (`sigmf_reader.load_sigmf`) or a headerless raw file
  (`sigmf_reader.load_raw(path, datatype, sample_rate_hz, center_freq_hz)`).
- **Receiver thread drives `recv_chunk`.** `pipeline/streaming.py` runs a
  dedicated receiver thread (~line 448) that calls `self._receiver.recv_chunk(buf)`
  in a tight loop (~line 521). A blocking sleep inside `recv_chunk` paces the
  stream without touching the event loop.
- **Live overlay is independent of the DB.** The dashboard waterfall + burst
  rectangles come from the in-memory `LiveBroadcast` WS (`_broadcast.publish(...)`
  high-res path, `streaming.py` ~line 1296), NOT from the detections table. DB
  insert, NATS, and ZMS are separate side effects:
  - `insert_detection` in `_drain_burst_results` (`streaming.py` ~line 1524),
  - NATS/ZMS publish in `_publish_processed` / stats paths,
  - recording writes in `_write_recording_chunk` / `_write_recording_metadata`.
  So detections can be shown live while skipping every persistence/egress path.
- **Live threshold reconfigure already works.** `web/routes/config.py` writes
  `TRIGGER_THRESHOLD_DB`, `BURST_THRESHOLD_HIGH_DB`, `BURST_THRESHOLD_LOW_RATIO`
  (and others), persists to `.env`, and calls `processor.reconfigure()` on the
  running pipeline — no restart. This is the tuning mechanism reused as-is.
- **Managed captures vs the test file.** Managed captures are `<base>.sc16` +
  `<base>.json` (with `sample_rate_hz`/`center_freq_hz`/`gain_db`) in the app's
  storage, listed by the Captures page. The December test file is a raw `.dat`
  outside that storage:
  `/home/orencollaco/Documents/iq_capture_hcro-rpi-002_2025-12-12T19-33-52.92Z_915MHz_26.0Msps_20.0s_35dB_ssm_fhss_OVF.dat`
  (2.0 GB, ci16_le, 26 MS/s, 915 MHz, 20 s, 35 dB). Its parameters are encoded in
  the HCRO filename convention `_<c>MHz_<sr>Msps_<dur>s_<gain>dB_`.

## Decisions (from brainstorming)

1. Replay **takes over the single pipeline** (SDR → Standby while replaying); no
   second pipeline instance.
2. **Real-time pacing with a speed control** (0.25x / 1x / 2x / 4x).
3. **Loop continuously** at end-of-capture.
4. **Conditional banner on the dashboard** (visible only during replay); the
   normal SDR dashboard is unchanged. Selection lives on the **Captures page**.
5. **Threshold controls in the banner**, wired to the existing live-reconfigure.
6. **Ephemeral detections**: shown live, never written to the DB / NATS / ZMS,
   and no recording during replay.
7. **Raw-file selection by path + auto-parsed params** (HCRO filename
   convention, editable); managed `.sc16` captures also get a "Replay" button.

## Design

### Part 1 - Paced, looping `FileReplayReceiver`

Extend `FileReplayReceiver` (`capture/replay_receiver.py`) with two modes,
selected by constructor args (defaults preserve today's batch behavior):

- `paced: bool = False` and a mutable `speed: float` (via a thread-safe holder,
  e.g. a small `_speed` attribute guarded by a `threading.Lock`, with a
  `set_speed(x)` method the API calls). When `paced`, `recv_chunk` sleeps
  `len(out_buf) / (sample_rate_hz * speed)` seconds (measured against a
  monotonic clock so it self-corrects for processing time) after filling the
  buffer. `sample_rate_hz` comes from the capture. Speed changes take effect on
  the next chunk.
- `loop: bool = False`. When `loop`, exhausting the capture seeks `self._pos = 0`
  and continues from the start instead of setting `_exhausted` / draining noise.

Batch `run_replay` keeps constructing the receiver with `paced=False, loop=False`
(unchanged).

### Part 2 - Ephemeral "replay mode" on the processor

Add a `replay_mode: bool = False` constructor arg to `StreamingProcessor`
(`pipeline/streaming.py`), stored as `self._replay_mode`. When true, gate the
side effects so only the live WS view runs:

- `_drain_burst_results`: skip the `insert_detection` call (still consume the
  queue and still let the live overlay broadcast happen).
- `_publish_processed` (and any per-detection NATS/ZMS publish): skip NATS +
  ZMS when `replay_mode`.
- Recording: refuse to start/continue recording when `replay_mode` (guard the
  recording-start trigger and `_write_recording_chunk`/`_write_recording_metadata`
  so a replay never writes `.sc16`/`.json`/`.psd`/`.detections.json`).

The high-res `_broadcast.publish(...)` PSD + burst overlay path is left intact —
that is what the dashboard renders. `reconfigure()` (threshold live-reload) is
left intact.

### Part 3 - Supervisor replay override + settings snapshot/restore

`PipelineSupervisor` gains a runtime override used for the next start:

- `async def start_replay(self, receiver: IReceiver) -> None`: stop the current
  pipeline if active, set an internal `_receiver_override = receiver` and
  `_replay = True`, then start. `_start` uses `_receiver_override` when set
  (skipping `build_receiver`) and passes `replay_mode=True` into
  `build_processor` when `_replay`.
- `async def stop_replay(self) -> None`: stop the pipeline, clear the override
  and `_replay`, and return to the pre-replay state (see below).

`build_processor(receiver, *, replay_mode=False)` gains the keyword and forwards
it to `StreamingProcessor(..., replay_mode=replay_mode)`.

**Tuning snapshot/restore.** The pipeline reads `settings.BANDWIDTH`,
`FREQUENCY_START`, `FREQUENCY_STEP`, `FREQUENCY_END`, `GAIN` at build time, so the
replay must run at the capture's tuning. The `/api/replay/start` handler (Part 4)
snapshots those five settings, sets `BANDWIDTH = sample_rate_hz`,
`FREQUENCY_START = center_freq_hz`, `FREQUENCY_STEP = 0`, `FREQUENCY_END =
FREQUENCY_START` (force single-freq streaming), `GAIN = gain_db`, then calls
`start_replay`. `/api/replay/stop` restores the snapshot and returns the sensor
to its pre-replay active state (Standby if it was in Standby, else re-activate
SDR). These programmatic changes are **not** persisted to `.env` (unlike the
user-initiated threshold edits, which continue to persist — that is the tuning
output worth keeping).

### Part 4 - Endpoints + filename parser

`web/routes/api.py` (or a small `web/routes/replay.py` router mounted alongside):

- `POST /api/replay/start` body
  `{path?: str, filename?: str, sample_rate_hz?: float, center_freq_hz?: float,
    gain_db?: float, datatype?: str, speed?: float}`:
  - Managed capture: `filename` set → resolve via the captures storage +
    `_validate_filename`, read params from its `.json`, load via
    `load_raw(path, datatype="ci16_le", sample_rate_hz, center_freq_hz)`.
  - Raw file: `path` set → the given params (parsed/edited client-side) are
    required; load via `load_raw`. Reject a `path` that does not exist (404) and
    one outside an allowed set with a clear 400 (see Security).
  - Build `FileReplayReceiver(cap, ReceiverConfig(gain, sample_rate, dur),
    paced=True, loop=True)` with the initial `speed`, snapshot+set tuning
    (Part 3), and `await supervisor.start_replay(receiver)`. Return the new
    replay status.
- `POST /api/replay/stop`: `await supervisor.stop_replay()`, restore tuning,
  return status.
- `POST /api/replay/speed` body `{speed: float}`: set the live speed on the
  running replay receiver (`receiver.set_speed`); 409 if no replay is active.
- **Replay status is published on the existing `/ws/live` heartbeat.** The
  dashboard already consumes a periodic `type: "heartbeat"` message
  (`dashboard.html` ~line 927; built in `pipeline/app.py::_heartbeat_loop`
  ~line 199, which has the supervisor in scope). Add a `"replay"` field to that
  payload: `null` when not replaying, else `{source: str, speed: float,
  looping: true}` (`source` = the file's basename), sourced from a
  `supervisor.replay_status()` helper. The banner keys off this — no new poll.
  `GET /api/sensor` also gains the same `replay` field for API completeness
  (used by the config page's existing sensor poll), but the dashboard banner is
  driven by the heartbeat.
- **Filename parser** `parse_capture_filename(name) -> dict` (in
  `capture/sigmf_reader.py` or a small helper): regex for the HCRO convention
  `_(\d+(?:\.\d+)?)MHz_`, `_(\d+(?:\.\d+)?)Msps_`, `_(\d+(?:\.\d+)?)s_`,
  `_(\d+(?:\.\d+)?)dB_`; returns `{center_freq_hz, sample_rate_hz, duration_sec,
  gain_db}` with missing keys omitted. Used by the client to prefill the raw-file
  form; also usable server-side as a fallback.

### Part 5 - UI: Captures-page launcher + dashboard banner

- **Captures page** (`web/templates/captures.html`):
  - Each managed capture (the detail/selection we already have) gets a
    **"Replay this capture"** button → `POST /api/replay/start {filename}` →
    navigate to the dashboard.
  - A small **"Replay a raw file"** panel: a path input; on blur/change it calls
    `parse_capture_filename` (client-side regex mirroring the server) to prefill
    editable `center_freq_hz` / `sample_rate_hz` / `gain_db` / `datatype`
    (default `ci16_le`) fields; a speed select; a **Start replay** button →
    `POST /api/replay/start {path, ...params, speed}` → navigate to the
    dashboard. Prefill the December SSM path is not hard-coded, but the panel
    handles that filename convention.
- **Dashboard** (`web/templates/dashboard.html`): a `#replay-banner` rendered
  **only** when the `/ws/live` heartbeat carries a `replay` object (the existing
  `data.type === "heartbeat"` handler, ~line 927, shows/hides it; hidden
  otherwise so the normal SDR dashboard is unchanged). The banner
  shows: source basename, `looping`, a speed selector (0.25x/1x/2x/4x →
  `POST /api/replay/speed`), a **Stop** button (`POST /api/replay/stop` →
  banner disappears, sensor returns to prior state), and the three threshold
  fields (`trigger_threshold_db`, `burst_threshold_high_db`,
  `burst_threshold_low_ratio`) wired to the **existing** config
  live-reconfigure endpoint so edits apply to the running replay immediately.

## Security / safety

- A raw `path` is arbitrary filesystem input. Restrict `/api/replay/start` to
  paths under an allowlist: the captures storage dir plus a configured
  replay-source dir (new setting `REPLAY_SOURCE_DIR`, default the user's
  `~/Documents` or empty = disabled), resolved with `Path.resolve()` and an
  `is_relative_to` check; reject traversal / disallowed roots with 400. Managed
  `filename` continues through `_validate_filename`.
- Replay must never emit upstream or persist: Part 2's gating is the guarantee;
  tests assert it explicitly.
- Replay takes over the one pipeline, so on a live sensor it pauses real
  monitoring until Stop. This mode is intended for a dev box or a deliberate
  maintenance window (documented in the banner copy / help text, not enforced).

## Testing

- **Part 1** (`tests/unit/test_replay_receiver.py`): `loop=True` seeks to 0 and
  keeps serving capture samples past exhaustion (no noise drain);
  `paced=True` sleeps ~`chunk/(rate*speed)` (assert against a fake/monotonic
  clock or a tolerance band) and `set_speed` changes the next chunk's delay.
  Existing batch behavior (`paced=False, loop=False`) unchanged.
- **Part 2** (`tests/unit/test_streaming_replay_mode.py`): with `replay_mode=True`,
  feed a burst and assert `insert_detection` is NOT called, NATS/ZMS publishers
  are NOT called, and no recording files are written, while `_broadcast.publish`
  IS called (live overlay preserved). With `replay_mode=False`, all fire as
  today.
- **Part 3/4** (`tests/unit/test_replay_routes.py`): `/api/replay/start` with a
  managed filename builds a paced+looping replay and flips `GET /api/sensor` to
  report the `replay` object with the capture's tuning; `/stop` clears it and
  restores the snapshotted settings + prior active state; `/speed` updates the
  running receiver (409 when idle); path allowlist rejects a disallowed/absent
  path (400/404). `parse_capture_filename` extracts the four fields from the HCRO
  name and omits missing ones.
- **Parts 5**: no unit tests (DOM); manual verification on the mock pipeline and
  a real replay of the December `.dat` (see Verification).
- Full CI per CLAUDE.md (ruff, ruff format, mypy, unit, integration@NATS:4222).

## Verification

1. Full CI.
2. Local mock pipeline: start a replay of a seeded managed capture, confirm the
   dashboard banner appears, the waterfall scrolls at 1x, speed changes take
   effect, threshold edits change the live overlay, Stop restores the dashboard.
3. Real replay of the December SSM `.dat` on a dev box (26 MS/s, 20 s loop):
   confirm SSM bursts appear in the live overlay and tuning the thresholds
   changes what is detected; confirm nothing is written to the DB / captures.
4. Local Jetson (nano-super) smoke per standing practice. Do NOT touch HCRO;
   provide redeploy/test steps for the user to run there.

## Files

- `src/rfobserver/capture/replay_receiver.py` - paced + looping modes,
  `set_speed`.
- `src/rfobserver/capture/sigmf_reader.py` - `parse_capture_filename` helper.
- `src/rfobserver/pipeline/streaming.py` - `replay_mode` gating of
  insert/NATS/ZMS/recording.
- `src/rfobserver/pipeline/supervisor.py` - `start_replay`/`stop_replay`,
  receiver override, `replay_mode` into `build_processor`, `replay_status()`.
- `src/rfobserver/pipeline/app.py` - `build_processor(..., replay_mode=...)`;
  add `"replay"` to the `_heartbeat_loop` payload from `supervisor.replay_status()`.
- `src/rfobserver/config.py` - `REPLAY_SOURCE_DIR` (allowlist root).
- `src/rfobserver/web/routes/api.py` (or new `web/routes/replay.py`) -
  `/api/replay/{start,stop,speed}`, `replay` field on `/api/sensor`.
- `src/rfobserver/web/templates/captures.html` - managed "Replay" button +
  raw-file launcher panel.
- `src/rfobserver/web/templates/dashboard.html` (dashboard template) -
  conditional `#replay-banner` (speed, stop, threshold fields).
- Tests: `test_replay_receiver.py`, `test_streaming_replay_mode.py`,
  `test_replay_routes.py`.

## Out of scope

- A second/parallel pipeline that keeps the SDR live during replay.
- Editing/deleting captures or importing the `.dat` as a managed capture.
- Side-by-side comparison of two threshold runs, or recording the replay's
  detections for later diffing.
- Any change to the SDR live path when no replay is active.
