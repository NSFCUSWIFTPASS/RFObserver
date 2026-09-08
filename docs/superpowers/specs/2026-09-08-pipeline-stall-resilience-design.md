# Pipeline stall resilience: isolate the UI, pin the hot path, and never die silently

- **Date:** 2026-09-08
- **Status:** Approved for planning
- **Scope:** Stop the capture pipeline from silently stopping under heavy Dashboard use, and make a stall self-recover. Three sequenced cuts, one spec. Cut 1 (safety net) is the actual outage fix and lands first; Cut 2 (core affinity) and Cut 3 (UI isolation) follow.
- **Baseline:** `main` (the code the field sensors run).

## Problem

A deployed HCRO field sensor's pipeline stopped advancing on its own, while the
operator was heavily using the Dashboard UI, and stayed stopped. The process
never exited, so `systemd`'s `Restart=on-failure` never fired.

Root cause is a chain, all in one process on one asyncio event loop
(`pipeline/app.py:130-138` runs uvicorn + heartbeat + cleanup as loop tasks;
`supervisor.py:106` runs `processor.run()` as another task on the same loop):

1. **A wide Dashboard query blocks the loop.** `/api/averaged/waterfall` calls
   `query_avg_waterfall` -> `_waterfall_aggregated` inline on the event loop
   (`api.py:956`); its per-row loop (`database.py:909-947`) decodes ~8 KB PSD
   blobs and runs numpy per-bin means across tens of thousands of windows with
   no `await` inside. A 24 h / ~74k-window aggregation blocks the single loop
   thread for seconds to tens of seconds. No DB query is offloaded to a thread.
2. **The blocked loop starves pipeline persistence.** While blocked,
   `_result_consumer_loop` (`streaming.py:1494`) cannot resume;
   `insert_detection` / `insert_avg_window` cannot be scheduled; results pile
   into `_result_queue` (maxsize 8, `streaming.py:257`) and silently drop.
3. **One shared aiosqlite connection** (`database.py:246`) serializes all reads
   and writes on a single worker thread; WAL does not help a single connection.
   Pipeline writes queue behind the heavy read.
4. **The 30 s write-timeout trips spuriously.** `asyncio.wait_for`'s deadline
   (`_guarded_write`, `database.py:216-244`) can only fire once the loop
   unblocks, so a long UI block manifests as "DB write stuck > 30 s" and forces
   a reconnect even with a healthy disk.
5. **An unguarded exception kills the pipeline task.** `_drain_burst_results`
   calls `await insert_detection(...)` at `streaming.py:1846` with **no
   try/except** (unlike `_persist_avg_window`, `streaming.py:1752`, which is
   guarded). A raised DB error (the unguarded reconnect retry at
   `database.py:242`, an `OperationalError`, disk-full) propagates up through
   `_result_consumer_loop` -> `run()`'s `try/finally` with no `except`
   (`streaming.py:412-441`), which cleans up the threads and re-raises.
6. **Nobody watches the task.** `supervisor._start` creates it with
   `asyncio.create_task` and attaches no done-callback (`supervisor.py:106`);
   it is only awaited inside a deliberate `_stop`. So the task dies with "Task
   exception was never retrieved", `_active` stays `True`, the heartbeat keeps
   echoing the dead processor's last state (`app.py:148-217`), uvicorn keeps
   serving, `/api/health` stays green. Process alive, pipeline dead, forever.

A separate latent bug compounds it: if disk I/O fails, `_delete_capture`
(`local.py:75-82`) raises an uncaught `OSError` that silently kills FIFO
eviction; the disk fills; the writer thread dies (`streaming.py:1155`); and the
blocking `self._recording_queue.put(None)` at `streaming.py:942` then wedges the
recctl thread forever with recording stuck in `"finalizing"`.

There is no liveness detection anywhere. A `Watchdog` class exists at
`utils/watchdog.py` but is **dead code** (never imported), and it is
asyncio-based, so it could not detect loop starvation or a dead task even if
wired: it must be a thread.

## Decisions (locked during brainstorming)

- **UI on its own thread + its own read-only DB connection** (not a separate
  process). Breaks the loop-starvation and the single-connection serialization.
  GIL still couples CPU-bound Python across threads; a separate process is the
  only full CPU isolation and is deferred unless GIL contention still shows up.
- **Core affinity is Python-only** (`os.sched_setaffinity`), config-driven, off
  by default, Linux-guarded. No kernel `isolcpus` / systemd `CPUAffinity=` in
  this effort.
- **Full safety net**, all of: guard `insert_detection`, supervise the pipeline
  task, a thread-based liveness watchdog, and the eviction/writer-deadlock fix.
- **Watchdog recovery = in-process restart, escalate to exit.** On a confirmed
  stall (and only when enabled), the watchdog first attempts an in-process
  pipeline restart via the supervisor; if the main loop does not complete the
  restart within a deadline (truly wedged), it escalates to `os._exit(non-zero)`
  so `systemd`'s existing `Restart=on-failure` (`RestartSec=5`) restarts fresh.
- **Phased delivery, safety net first.** One spec, three sequenced, independently
  shippable/testable cuts. The outage fix (Cut 1) reaches the sensor before the
  invasive UI rework (Cut 3).
- **Feature-gated and default-inert.** The watchdog and affinity are off by
  default; behaviour with them off is identical to today.

## Target architecture

```
process (cli.py: asyncio.run)
  MAIN THREAD / main loop
    - supervisor + processor.run()  (receiver/dispatch/burst/recctl threads, persistence)
    - ZMS monitor, retention cleanup
    - control-endpoint executors marshalled here from the web loop
    - WRITE db connection
  WEB THREAD / its own loop            (Cut 3)
    - uvicorn serve() + heartbeat
    - read-only endpoints served here against a READ-ONLY db connection
    - LiveBroadcast delivers to WS clients on this loop
  WATCHDOG THREAD (daemon)             (Cut 1)
    - reads last_progress monotonic ts; on stall -> restart via main loop, else os._exit
  RECEIVER + FFT worker threads pinned to configured CPUs   (Cut 2)
```

---

## Cut 1 - Safety net (ships first; fixes the outage)

Self-contained; no threading-topology change. Turns a silent permanent death
into a logged auto-recovery.

### 1a. Guard the detection insert

Wrap the `await self._db.insert_detection(...)` in `_drain_burst_results`
(`streaming.py:1846`) in `try/except Exception: logger.exception(...)`, mirroring
`_persist_avg_window` (`streaming.py:1752-1770`). A DB error on one burst must
never propagate into `_result_consumer_loop` / `run()`. (Also give the
`database.py:242` post-reconnect retry its own `asyncio.wait_for` timeout so the
retry cannot hang forever.)

### 1b. Supervise the pipeline task

In `supervisor._start`, after `self._task = asyncio.create_task(processor.run())`,
attach `self._task.add_done_callback(self._on_task_done)`. `_on_task_done`:
- If `task.cancelled()` -> return (deliberate stop).
- If `task.exception()` is not None and `self._active` is still True (i.e. not a
  deliberate `_stop`) -> log the traceback and schedule an immediate in-process
  restart (rebuild receiver + processor via the same path `_start` uses),
  guarded so a restart failure escalates to the watchdog's exit path. Never
  leave `_active=True` pointing at a dead task.

This catches task **death**. The watchdog (1c) catches task **stall** (alive but
not progressing) and total loop wedge.

### 1c. Thread-based liveness watchdog

Rewrite `utils/watchdog.py` as a daemon **thread** (not asyncio). Contract:
- The pipeline records forward progress by updating a monotonic
  `last_progress` timestamp on **every processed chunk** - set in
  `_result_consumer_loop` when a result is drained (`streaming.py:1494+`), which
  only advances when capture->process->persist is actually flowing.
- The watchdog thread wakes every few seconds and reads
  `now - last_progress`. It acts only when: the watchdog is enabled AND the
  supervisor reports active (never in Standby/replay-stopped).
- On `stale > RFOBS_WATCHDOG_TIMEOUT_SEC`: log the stall, then attempt recovery
  via `asyncio.run_coroutine_threadsafe(supervisor.restart(), main_loop)` and
  wait up to `RFOBS_WATCHDOG_RESTART_DEADLINE_SEC`. If the future does not
  complete (loop wedged) or the restart raises -> log and `os._exit(EXIT_STALL)`
  so systemd restarts the whole process. After a successful restart, reset
  `last_progress` and resume monitoring.
- `supervisor.restart()` is a new coroutine = `_stop()` then `_start()` under the
  existing `_lock`, reusing `_STOP_TIMEOUT_SEC`.

The watchdog thread and `os._exit` fallback are what make this immune to exactly
the failure that occurred: it does not live on the starved loop and does not die
with the pipeline task.

### 1d. Eviction / writer deadlock

- `_delete_capture` (`local.py:75-82`): wrap each `unlink()` in
  `try/except OSError: logger.warning(...)` so one unremovable file cannot abort
  eviction; `enforce_cap` (`local.py:92-106`) guarded so it never raises into
  `_finalize_recording`.
- `_recording_queue.put(None)` (`streaming.py:942`): replace the unbounded
  blocking put with a bounded `put(None, timeout=...)`; on `queue.Full` (writer
  dead/stalled) log and proceed rather than blocking recctl forever, so
  `_end_recording`'s `finally` always returns state to `"idle"` and continuous
  recording re-arms.
- If `_file_writer_loop` (`streaming.py:1120-1156`) crashes, surface it so
  recording state does not silently wedge in `"finalizing"` (either restart the
  writer or fail the in-flight recording back to idle).

### Cut 1 settings (`config.py`)

- `WATCHDOG_ENABLED: bool = False`
- `WATCHDOG_TIMEOUT_SEC: float = 30.0` (chunks flow sub-second; 30 s stale is
  unambiguously stalled)
- `WATCHDOG_RESTART_DEADLINE_SEC: float = 10.0`

### Cut 1 tests

- Watchdog thread: fires when `last_progress` goes stale, no-op when fresh, no-op
  when disabled or Standby; on a fake wedged loop it takes the exit path (inject
  a stub `os._exit`).
- Done-callback: a pipeline task that raises triggers a restart and does not
  leave `_active=True` on a dead task; a cancelled task (deliberate stop) does
  not.
- `insert_detection` raising inside `_drain_burst_results` does NOT kill the
  consumer loop (integration: inject a DB error, assert the pipeline keeps
  producing).
- `_delete_capture` swallows `OSError` and eviction continues past a bad file.
- `_recording_queue.put(None)` returns (does not block) when the writer is dead;
  recording state returns to `"idle"`.

---

## Cut 2 - Core affinity (small, self-contained)

Reduce UHD overflow / sample drops by pinning the receiver and FFT worker
threads to configured CPUs.

- New settings: `RECEIVER_CPUS: str = ""` and `WORKER_CPUS: str = ""`
  (comma-separated core ids; empty = no pinning). A single `PIPELINE_CPUS` may
  back both if simpler.
- From inside each thread's entry (the receiver loop thread, and each
  ThreadPoolExecutor worker via an initializer), call
  `os.sched_setaffinity(0, parsed_cpus)` guarded by
  `hasattr(os, "sched_setaffinity")` and `try/except OSError` (log and continue;
  never fatal). Linux-only; a no-op elsewhere.
- Optional `os.nice()` on non-critical threads; no SCHED_FIFO / kernel changes.

### Cut 2 tests

- Given a CPU list, the receiver/worker threads call `sched_setaffinity` with the
  parsed set (patch and assert); empty config = never called; unsupported
  platform = guarded no-op. No assertions about actual scheduling.

---

## Cut 3 - UI isolation (largest rework; lands last)

Move all web serving off the pipeline loop and give it its own read connection.

### 3a. Web thread + own loop

Start uvicorn + the heartbeat on a dedicated daemon thread running its own
event loop, instead of as tasks in `app.py`'s main-loop `gather`
(`app.py:130-138`, `_run_web_server` `app.py:243-274`, `_heartbeat_loop`
`app.py:148-217`). The main loop keeps the supervisor, ZMS, retention, and the
pipeline task.

### 3b. Read-only DB connection for the web layer

Open a second `SensorDatabase` in read-only mode (SQLite `mode=ro` /
`query_only=ON`, WAL) and set it as `app.state.database` on the web thread; the
pipeline keeps the write connection on the main loop. WAL + two connections give
true concurrent read/write, removing the single-worker serialization. Read-only
guard also means a web bug can never write.

### 3c. Endpoint split (the load-bearing decision)

- **Read-only endpoints** (waterfall, averaged stats, detections, iq-captures,
  status/history) run entirely on the web thread against the read connection.
  This moves the heavy `_waterfall_aggregated` work permanently off the pipeline
  loop - the fix for mechanism #1/#2.
- **Control endpoints** (sensor active toggle, recording start/stop, `/config`
  reconfigure, replay start/stop) mutate live pipeline state owned by the main
  loop; they must be marshalled with
  `asyncio.run_coroutine_threadsafe(coro, main_loop)` and awaited from the web
  handler. A small helper on `app.state` holds the main loop reference.
- Enumerate every route during implementation and classify each read vs control;
  the classification is part of the plan.

### 3d. Loop-aware LiveBroadcast

`LiveBroadcast` (`web/websocket.py`) currently assumes one loop. WS clients now
live on the web loop, but the pipeline publishes frames from the main loop.
Make publish schedule delivery onto the web loop via `call_soon_threadsafe` (or
route pipeline frames through a thread-safe queue the web loop drains). The
heartbeat, now on the web thread, reads the read DB connection and
GIL-safe processor attribute snapshots.

### Cut 3 tests

- Read endpoints serve correct data via the read-only connection while the write
  connection commits (integration: concurrent read+write, no error).
- A control endpoint's mutation actually reaches the main-loop supervisor
  (marshalling round-trips).
- A heavy waterfall request does not delay pipeline persistence (integration
  timing: pipeline chunk cadence holds while a large aggregation runs).
- Broadcast frames published from the main loop are delivered to a WS client on
  the web loop.

---

## Deployment notes

- `systemd` unit is unchanged: `Restart=on-failure` + `RestartSec=5` already
  exist and are exactly what the watchdog's `os._exit` escalation relies on. No
  `WatchdogSec`/`Type=notify` change in this effort.
- Enable order on a sensor: deploy Cut 1, turn `WATCHDOG_ENABLED=true`, verify a
  forced stall restarts (and that normal operation never trips it), then Cut 2
  affinity, then Cut 3.
- The local `.177` sensor is healthy and is the safe place to validate each cut
  before the HCRO field box.

## Open items / risks

- Cut 3 GIL caveat: a thread does not isolate CPU-bound Python. If, after Cut 3,
  a heavy query still measurably perturbs the pipeline (GIL contention), the
  next lever is a separate web **process** reading the DB read-only - a larger
  change, explicitly out of scope here.
- Watchdog timeout must sit comfortably above the worst legitimate persistence
  latency so it never false-fires; 30 s is deliberately generous given
  sub-second chunk cadence. Confirm on the field box's real load.
- `supervisor.restart()` interacts with replay mode and the `_lock`; the plan
  must ensure the watchdog never restarts a deliberately-stopped or replaying
  sensor.
