# Shutdown signals: SIGTERM skips cleanup, SIGINT hangs on Python 3.10

Date: 2026-09-14. Follows issues 4 and 8 in
`2026-09-14_stall-safety-net-hardware-validation.md`.

## The question

Why does a production stop (SIGTERM) of `rfobserver run` skip all of `app.run()`'s
cleanup? And why does a SIGINT stop on nano-super leave the process alive until
systemd SIGKILLs it?

Symptoms on nano-super: Jetson Orin Nano, L4T R36.5, Python 3.10.12,
uvicorn 0.51.0, aiosqlite 0.22.1, B200mini, systemd transient unit.

- SIGTERM: `Result=success` in under 0.2 s. There is no "Sensor deactivated (SDR
  released)" line.
- SIGINT: KeyboardInterrupt at 16:12:52.458. The event loop was closed by 16:12:52.95.
  systemd SIGKILLed the process at 16:13:22 (`Result=timeout`).

## The answer

- **SIGTERM (issue 4).** Neither Python nor the app handles SIGTERM. uvicorn
  catches it, finishes its own shutdown, restores `SIG_DFL` and re-raises the
  signal. The kernel then kills the process, so `run()`'s `finally` never runs.
  Headless (`WEB_PORT=0`), there is no uvicorn and SIGTERM kills the process
  outright. Either way:
  - the in-progress recording loses its `.json` and `.psd.json` sidecars;
  - the SDR and the DB are not closed.
- **SIGINT hang (issue 8).** This happens on Python 3.10 only.
  - uvicorn's re-raise, or the default handler when headless, raises
    KeyboardInterrupt out of the loop.
  - 3.10's `asyncio.run` then cancels every task, including the supervisor's
    processor task.
  - The `finally` in `run()` awaits `supervisor.set_active(False)`. Inside it,
    `_stop()` is waiting in `wait_for(task)` on that already-cancelled task, so it
    raises CancelledError.
  - That aborts the rest of the `finally`, so `db.close()` never runs.
  - aiosqlite's worker thread is non-daemon. `threading._shutdown` then waits on
    it forever.

  Python 3.11's `asyncio.Runner` cancels only the main task on SIGINT, so cleanup
  completes there. UHD is not involved: the hang reproduces with the mock
  receiver.

## Procedure

All local steps used the mock receiver (`RFOBS_MOCK_RECEIVER=true`,
`SENSOR_ACTIVE=true`), so no SDR was involved. The scripts are in
`2026-09-14_shutdown-signals/`.

1. **First SIGINT repro (`shutdown_repro.sh INT 8899`, Python 3.11).**
   - The process outlived SIGINT until SIGKILL at 40 s.
   - py-spy could not attach (ptrace is denied). A faulthandler dump (SIGUSR1)
     showed the main thread idle in `select` inside `asyncio.run`, with the
     streaming threads still alive.
   - The app coroutine was parked. uvicorn had finished, but `gather` still
     awaited the heartbeat and the keep-alive `Event().wait()`.
2. **Checked the signal disposition of the repro itself.**
   - `bash -c 'sleep 30 & grep SigIgn /proc/$!/status'` gave `0x6`, meaning
     SIGINT and SIGQUIT are ignored. A job started with `&` from a
     non-interactive shell inherits SIGINT as SIG_IGN.
   - uvicorn restored SIG_IGN and re-raised into nothing.
   - Step 1 was therefore a **repro artifact**, not the Jetson bug.
   - `probe_stop.py` now resets SIGINT to `default_int_handler`, as a terminal or
     systemd does.
3. **Signal matrix, run in parallel:** {SIGINT, SIGTERM} x {web on, headless} x
   {3.11 `.venv`, 3.10 venv}, 8 runs. This isolates the Python version, uvicorn
   and the signal.
4. **3.10 headless SIGINT dump.**
   - The only threads left were the aiosqlite worker and the main thread, which
     was in `threading._shutdown`.
   - The log shows cleanup starting ("MockReceiver stopped streaming") but no
     "Sensor deactivated".
5. **Probe (`probe_stop.py`).** It wraps `set_active` and `SensorDatabase.close`
   and logs only. This confirmed that `set_active(False)` raised CancelledError
   and that `close()` was never reached.
6. **Cost of skipped cleanup (`rec_term.sh`, 3.11, web on).** Started a manual
   disk recording, sent the signal 4 s later, then listed the capture files.
7. **Versions on nano-super**, to rule out a local-only difference: uvicorn
   0.51.0, aiosqlite 0.22.1, Python 3.10.12. Locally the versions are uvicorn
   0.41.0, aiosqlite 0.22.1 and Python 3.11. `capture_signals()` has the same
   re-raise in both uvicorn versions.

## Evidence

Signal matrix (step 3):

```
m-int-head-310   still alive at 40 s: SIGKILL   exit rc=137 after 41.10 s
m-int-web-310    still alive at 40 s: SIGKILL   exit rc=137 after 41.10 s   (Finished server process)
m-int-head-311   exit rc=130 after 0.50 s   Sensor deactivated (SDR released)
m-int-web-311    exit rc=130 after 0.50 s   Finished server process, Sensor deactivated (SDR released)
m-term-head-310  exit rc=143 after 0.51 s   (no Sensor deactivated)
m-term-web-310   exit rc=143 after 0.51 s   Finished server process (no Sensor deactivated)
m-term-head-311  exit rc=143 after 0.50 s   (no Sensor deactivated)
m-term-web-311   exit rc=143 after 0.50 s   Finished server process (no Sensor deactivated)
```

3.10 headless SIGINT, from the traceback and faulthandler dump (step 4):

```
MockReceiver stopped streaming
Receiver loop exiting (running=False)
Traceback (most recent call last):
  ...
  File "/usr/lib/python3.10/asyncio/runners.py", line 44, in run
    return loop.run_until_complete(main)
  ...
  File "/usr/lib/python3.10/selectors.py", line 469, in select
KeyboardInterrupt
Thread 0x0000728b041ff640 (most recent call first):
  File ".../aiosqlite/core.py", line 59 in _connection_worker_thread
Current thread 0x0000728b4515b000 (most recent call first):
  File "/usr/lib/python3.10/threading.py", line 1567 in _shutdown
```

Probe (step 5), 3.10 headless SIGINT:

```
probe ERROR PROBE set_active(False) raised CancelledError
(no "PROBE db.close() reached" line; exit rc=137 after 41.10 s)
```

The same probe on 3.11 with the web server on, from `rec_term.sh INT`:

```
Recording saved: MOCK0001-...T214112.sc16 (789248000 bytes, 4.3s, 0 dropped, ...)
Sensor deactivated (SDR released)
PROBE db.close() reached (read_only=True)
PROBE db.close() reached (read_only=False)
```

Recording files after the stop (step 6):

```
SIGTERM, rc=143:
manual/MOCK0001-Oren-Dell-Ubuntu-20260914T214103.psd 157286400
manual/MOCK0001-Oren-Dell-Ubuntu-20260914T214103.sc16 781056000
(no .json, no .psd.json, no "Recording saved")

SIGINT (3.11), rc=130:
manual/MOCK0001-Oren-Dell-Ubuntu-20260914T214112.json 492
manual/MOCK0001-Oren-Dell-Ubuntu-20260914T214112.psd 158924800
manual/MOCK0001-Oren-Dell-Ubuntu-20260914T214112.psd.json 41721
manual/MOCK0001-Oren-Dell-Ubuntu-20260914T214112.sc16 789248000
```

Code paths:

- **uvicorn `Server.capture_signals`** (0.41 and 0.51): installs handlers with
  `signal.signal`. On exit it restores the originals and then calls
  `signal.raise_signal(captured)`.
- **Python 3.10 `asyncio.run`:** `finally: _cancel_all_tasks(loop)` cancels
  every task.
- **Python 3.11 `Runner._on_sigint`:** the first SIGINT only runs
  `main_task.cancel()`.
- **`PipelineSupervisor._stop`:** `await asyncio.wait_for(task, ...)`. It
  catches TimeoutError and Exception, not CancelledError.
- **`aiosqlite.Connection`:** `Thread(target=_connection_worker_thread, ...)`,
  with no `daemon=True`.

## Measured and REJECTED (do not retry)

- **"UHD teardown with streaming still active causes the SIGINT linger"**, the
  suggestion in the validation doc's issue 8. Rejected: the hang reproduces
  exactly with the mock receiver (step 3), and the thread left alive is
  aiosqlite's.
- **"Streaming worker threads keep a headless run alive after SIGINT"**, as noted
  in the validation doc's Fix status. Rejected:
  - The streaming threads are all `daemon=True` (`streaming.py:446-451`, and
    `945` for the writer).
  - The 3.10 dump shows only the aiosqlite thread.
  - On 3.11 a headless SIGINT exits in 0.5 s with full cleanup.
  - The earlier observation came from the SIG_IGN repro artifact (trap 1)
    and/or 3.10.
- **"The event loop is still running, so an await is parked in `run()`"** (step
  1). True only in the SIG_IGN artifact. With a real SIGINT the loop exits.

## Measurement traps

1. **A background job from a non-interactive shell ignores SIGINT.** Anything
   launched with `&` inside a script has SigIgn `0x6`. Python keeps SIG_IGN, so
   `signal.getsignal(SIGINT)` is not `default_int_handler`, and uvicorn restores
   and re-raises into nothing. Reset the handler in the child (`probe_stop.py`),
   or test under systemd.
2. **py-spy needs ptrace.** Here it fails with "Permission Denied". Use
   `faulthandler.register(signal.SIGUSR1, all_threads=True)` and `kill -USR1`.
3. **The Python version decides the SIGINT outcome.** The dev venv is 3.11 and
   the Jetson is 3.10. A 3.11-only test shows a clean SIGINT exit and hides
   issue 8. Keep a 3.10 venv with the repo installed for shutdown tests.
4. **`rc=143` / `Result=success` looks clean.** systemd counts death by SIGTERM
   as success, so issue 4 is invisible in unit status. Check the log for
   "Sensor deactivated" and check the recording sidecars.

## Fix direction (branch C)

- The app owns SIGINT and SIGTERM with `loop.add_signal_handler`, which sets a
  stop event.
- uvicorn runs with a `Server` subclass whose `capture_signals()` is a no-op. On
  the stop event it gets `should_exit = True`.
- `run()` waits for the stop event or a task failure. It then:
  - shuts down the web server within a time limit;
  - cancels the background loops;
  - runs cleanup in order, with each step isolated so one failure cannot skip
    the DB close;
  - returns normally (exit 0).
- A second signal during cleanup forces an immediate exit.
- The handlers are removed on exit.

This removes the KeyboardInterrupt path, so 3.10's cancel-all teardown is no
longer reached on a signal.

## Open, not yet answered

- Whether the SDR comes back cleanly after a SIGTERM-killed process on UHD 4.1.
  It did in the validation runs, but that was not checked deliberately.
- ~~uvicorn startup failure (a port bind error) calls `sys.exit(1)` inside
  `serve()`. SystemExit then leaves the loop through the same 3.10 cancel-all
  path. Not tested.~~ Answered by branch C's final review:
  - Before the fix it hung forever on both 3.10 and 3.11. The unit stayed
    "active" with no web server and no pipeline.
  - Fixed in a2c7c4e: the DB closes sit in an outer `finally`.
  - Now it exits rc 1 (3.11, uvicorn 0.41) or rc 3 (3.10, uvicorn 0.53) in
    0.5 s, and logs "Shutdown complete".
- A SIGINT or SIGTERM during startup is not ordered. That window runs before
  `install_stop_signals`: the DB connect, the ZMS/NATS start and the SDR init
  (about 2.3 s or more). It is parked as a follow-up.
- The `rfobserver web` command (`uvicorn.run`) still uses uvicorn's own
  handling. Its lifespan shutdown runs before the re-raise. Not tested.

## Fix verification (branch C, 87458f6)

Local end-to-end, mock receiver, using the same scripts:

```
m-int-head-310   exit rc=0 after 1.00 s     (before: SIGKILL at 41 s)
m-int-web-310    exit rc=0 after 0.50 s     (before: SIGKILL at 41 s)
m-term-head-310  exit rc=0 after 1.01 s     (before: rc=143, no cleanup)
m-term-web-310   exit rc=0 after 1.00 s     (before: rc=143, no cleanup)
m-int-head-311   exit rc=0 after 0.51 s
m-int-web-311    exit rc=0 after 0.50 s
m-term-head-311  exit rc=0 after 0.50 s     (before: rc=143, no cleanup)
m-term-web-311   exit rc=0 after 0.51 s     (before: rc=143, no cleanup)
```

- Every run logs "Received SIG...", "Sensor deactivated (SDR released)", a
  close on each DB, and "Shutdown complete".
- `rec_term.sh` was run for TERM and INT on 3.11 and 3.10. Each capture has
  `.json`, `.psd` and `.psd.json`, and the log has "Recording saved".

nano-super, B200mini 322750B, systemd transient unit (`start.sh`,
`Restart=on-failure`), with a manual recording started 5 s before the stop:

```
SIGTERM (systemctl stop):      stopped after 0.98 s; inactive; not restarted 7 s later
  22:04:38,968 Received SIGTERM; shutting down
  22:04:39,429 Recording saved: 322750B-nano-super-20260914T220433.sc16 (1305344000 bytes, 5.7s, ...)
  22:04:39,558 Sensor deactivated (SDR released)
  22:04:39,567 Shutdown complete
  files: .json 478, .psd 183500800, .psd.json 41719, .sc16 1305344000
SIGINT (systemctl kill -s INT): stopped after 1.08 s; inactive; not restarted
  22:05:20,339 Received SIGINT ... 22:05:20,878 Shutdown complete; all four files
SIGTERM with a /ws/live client connected: stopped after 0.84 s
  client: "received 1012 (service restart)"
```

The SDR was re-claimed cleanly on each following start, which answers the
first open item for the SIGTERM path, now that it is an ordered stop.

## Measurement traps (added)

5. **The 3.10 test venv has uvicorn 0.53.0, but the Jetsons run 0.51.0.** The
   local 3.10 results cover 0.53. nano-super covers 0.51.
