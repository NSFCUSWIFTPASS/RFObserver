# Sensor Active toggle — design

## Problem

RFObserver continuously captures from the SDR and processes the stream. There is
no way to stop capture at runtime and free the SDR so another process can use it,
short of killing the whole service. We want a UI toggle that stops all capture
and processing, fully releases the SDR, and brings it back on demand — reflecting
the toggle state only after the backend confirms the transition actually happened.

## Decisions (from brainstorming)

- **Name:** "Sensor Active" in the UI; shows "Standby" when off.
- **Persistence:** persisted to `.env` (`RFOBS_SENSOR_ACTIVE`, default `true`), like
  the existing `ZMS_ENABLED` / `NATS_ENABLED` toggles. A disabled sensor stays
  disabled across restarts and does not silently re-claim the SDR.
- **Free depth:** fully release the device — close the UHD handle so another
  process can claim the SDR; re-enable re-initializes the hardware.
- **Confirmation:** the toggle settles to its new position only after the backend
  confirms the transition completed. Implemented as a synchronous request that
  performs the transition and returns the actual resulting state.

## Architecture

### PipelineSupervisor (`pipeline/app.py`)

A small supervisor owns the receiver + processor lifecycle via factory closures,
rather than pausing the receiver threads in place. Building a fresh processor on
each enable avoids reusing a stopped instance whose queues still hold `_STOP`
sentinels.

```
class PipelineSupervisor:
    def __init__(self, *, build_receiver, build_processor, on_processor_change=None)
    @property active: bool
    @property processor: <processor|None>
    async def set_active(active: bool) -> bool   # returns actual resulting state
```

- **Enable** (`_start`): `receiver = build_receiver()`; `receiver.initialize()` in an
  executor (claims + configures HW, blocking); `processor = build_processor(receiver)`;
  `task = create_task(processor.run())`; `active = True`; fire `on_processor_change`.
- **Disable** (`_stop`): `processor.stop()`; `await wait_for(task, timeout)` (cancel on
  timeout); `receiver.close()` in an executor (release device); clear references;
  `active = False`; fire `on_processor_change`.
- `set_active` is guarded by an `asyncio.Lock`; a redundant call (already in the
  requested state) is a no-op that returns the current state. The return value is
  the confirmation.
- `on_processor_change(processor|None)` lets the web layer keep
  `app.state.processor` and the heartbeat pointed at the live processor (or `None`
  in Standby).

Works for both the streaming and continuous (sweep) processors — the processor
type is chosen inside `build_processor`, exactly as `run()` chooses today.

### Startup wiring (`pipeline/app.py::run`)

- Construct `db`, `local_storage`, `broadcast`, optional `zms_monitor`,
  `nats_producer` as today (these are control-plane and stay up across Standby).
- Move receiver + processor construction into the two factory closures. The
  receiver is **not** initialized eagerly anymore — the supervisor initializes it
  on enable, so a persisted-disabled start never claims the SDR.
- Create the supervisor. If `settings.SENSOR_ACTIVE`: `await supervisor.set_active(True)`.
- Gather the long-lived tasks: web server, heartbeat, `zms_monitor.run()`. The
  supervisor owns the processor task internally (not in the top-level gather).
- Headless edge case (`WEB_PORT == 0`, no UI to toggle): honor `SENSOR_ACTIVE` at
  startup and await a never-set stop event so the process stays alive; there is no
  runtime toggle without the web server.

### Receiver (`capture/receiver.py`)

- Add `close()` to the `IReceiver` protocol.
- `Receiver.close()`: drop `rx_streamer` and `usrp` references (set to `None`) so
  UHD releases the USB device; guarded so a double-close is safe. `initialize()`
  already recreates them, so enable→disable→enable works.
- `MockReceiver.close()`: no-op (records closed state for tests).

### Web layer

- New setting `SENSOR_ACTIVE: bool = True` (`config.py`).
- `_run_web_server` sets `app.state.supervisor` and registers `on_processor_change`
  to update `app.state.processor`.
- Endpoints (`web/routes/api.py`):
  - `POST /api/sensor` body `{ "active": bool }` → `await supervisor.set_active(...)`,
    persist `SENSOR_ACTIVE` to settings + `.env`, return
    `{ "active": <confirmed>, "detail": <str> }`. If no supervisor (web-only mode):
    `409` with a clear message.
  - `GET /api/sensor` → `{ "active": bool }` for initial render.
- Heartbeat (`_heartbeat_loop`) reads the supervisor's current processor each tick
  so recording/ZMS/NATS status and the status bar reflect the live processor, and
  show "Standby" when inactive.

### UI (`config` page + status bar)

- Add a "Sensor Active" toggle to the config page. On change it POSTs to
  `/api/sensor` and settles to the response's confirmed `active`; on failure it
  reverts and surfaces the error.
- Status bar shows "Standby" when the sensor is inactive.

## Data flow

Toggle off → `POST /api/sensor {active:false}` → `supervisor.set_active(False)` →
`processor.stop()` + drain + `receiver.close()` → persist `.env` → respond
`{active:false}` → UI settles to Standby. Toggle on reverses it, re-initializing
the SDR before responding.

## Error handling

- HW `initialize()` failure on enable: `set_active` propagates; endpoint returns
  `500` with detail; `active` stays `false`; SDR left released.
- Task drain timeout on disable: cancel the task, still call `receiver.close()`.
- Web-only mode: `409`, no state change.
- `.env` persistence failure: logged, does not fail the toggle (matches existing
  `_persist_settings` behavior).

## Testing

- **Supervisor** (MockReceiver): enable/disable transitions flip `active` and
  `processor`; redundant calls are no-ops; `close()` invoked on disable;
  `set_active` returns the confirmed state; enable→disable→enable re-initializes.
- **Receiver**: `Receiver.close()` drops handles and is double-close safe (mock
  UHD); `MockReceiver.close()` no-op.
- **API**: `POST /api/sensor` toggles through a fake supervisor and returns the
  confirmed state; `409` in web-only mode; `GET /api/sensor` reflects state;
  persistence is invoked.
- **Settings**: `SENSOR_ACTIVE` default is `True`; `.env` round-trip persists a
  disabled state.

## Out of scope

- ZMS/NATS connections stay up during Standby (no data flows, so nothing
  publishes). They are control-plane, not per-capture.
- No auto-restart/backoff of a crashed processor beyond current behavior.
