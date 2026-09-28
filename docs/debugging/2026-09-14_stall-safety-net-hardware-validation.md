# Stall safety net (Cut 1) on real hardware: validation results and issues to fix

## The question

2026-09-14: "Have we thoroughly tested the pipeline-stall-cut1-safety-net branch
on the local Jetson with MAXN eanbled?"

Before this session the answer was no. The only trace was a 13-minute checkout
on nano-super on 2026-09-09, when the unit suite ran and nothing else did: no
live pipeline, no watchdog, no forced stall. `.177` still runs a35a079 from
2026-08-25.

Hardware and build used for this validation:

- **Box:** `nano-super` (192.168.97.153), Jetson Orin Nano devkit (DT
  `p3767-0005`), L4T R36.5.0, Ubuntu 22.04, Python 3.10.12.
- **SDR:** USRP B200mini, serial 322750B, over USB 3, UHD 4.1.0.5.
- **Power:** nvpmodel **15W**, CPU capped at **1.51 GHz**. MAXN is not available
  on this flash; see issue 9.
- **Code:** 81aa9e1, which is `origin/feat/pipeline-stall-cut1-safety-net` and
  local `main`. Local `main` is 9 commits ahead of `origin/main`, so this code
  is **not pushed to `origin/main`**.
- **Settings:** 915 MHz, 56 Msps (default `BANDWIDTH`), gain 40, 3 PSD workers
  (the default), continuous trigger at -52.5 dB, `ARCHIVE_MAX_GB=2`,
  `WATCHDOG_ENABLED=true`, timeout 30 s, restart deadline 10 s.
- **How it ran:** as a transient systemd unit that mirrors production
  (`Type=simple`, `Restart=on-failure`, `RestartSec=5`, default `KillSignal`),
  from a throwaway worktree, with scratch DB and storage.

## The answer

The safety net works on hardware. All of these passed:

- the 1 h live-SDR soak
- crash auto-restart, including backoff and give-up
- in-process watchdog restart
- wedge or hang escalating to `exit 90`, then a systemd restart
- the guarded DB write

But it is **not ready to enable in the field**. With the watchdog on, **one
Dashboard tab viewing 24 h over a field-sized DB (74k windows) takes the sensor
down**: the watchdog fires, the in-process restart misses its deadline, and the
process exits and gets restarted by systemd. That happened twice out of two
tries. The cause is DB-connection serialization, not a wedged event loop.

The run also confirmed a Python 3.10-only bug in `supervisor._stop()`, which CI
cannot catch because it tests only 3.11 and 3.12.

### Issues to fix

| # | Priority | Issue | Evidence | Suggested fix |
|---|---|---|---|---|
| 1 | P1 (blocks enabling the watchdog) | Dashboard use trips the watchdog, then the process restarts | Dashboard runs 1 and 2 | Cut 3b first (separate read-only connection for the web layer); batch burst inserts; do not enable `WATCHDOG_ENABLED` in the field until then |
| 2 | ~~P1~~ P3 (see CORRECTION) | Py3.10: `_stop()` catches the builtin `TimeoutError`; `asyncio.wait_for` raises `asyncio.TimeoutError`, so a stop timeout is mislabelled at DEBUG as an "already-raised task exception" and the "did not stop in time" warning never fires. ~~so a hung task is never cancelled, and the hung task leaks~~ (withdrawn: `wait_for` cancels it) | F6 | `except (TimeoutError, asyncio.TimeoutError):  # noqa: UP041` at `supervisor.py:137`, as `streaming.py:1527` already does; add Python 3.10 to the CI matrix |
| 3 | P2 | Restart deadline (10 s) is shorter than `_STOP_TIMEOUT_SEC` (15 s) plus SDR init (about 2.3 s), so any stall that does not honour `stop()` within about 7 s always escalates to exit; the 15 s cancel path in `_stop()` never runs under the watchdog | F3b vs F5, Dashboard runs | Decide deliberately: raise the deadline to about 20 s, or accept exit as the remedy and document it |
| 4 | P2 | Production SIGTERM stop skips all app cleanup: uvicorn 0.51 re-raises SIGTERM with `SIG_DFL` after serving, so `app.run()`'s `finally` (SDR release, DB close, recording finalize) never runs | SIGTERM stops | Own the signal (handle SIGTERM in the app, or stop uvicorn from re-raising), then run the supervisor, DB and recording teardown |
| 5 | P2 | A recording is labelled "0 dropped" while UHD overflows remove about 20% of its samples: 30.1 s wall time holds 24.1 s of samples (5,409,536,000 B / 4 / 56e6) | calibration run | Count UHD overflow gaps (or sample shortfall versus wall time) into the recording's drop metadata |
| 6 | P2 (capacity) | At 56 Msps on the 15 W / 1.51 GHz Orin Nano, about 20% of chunks are dropped and there are about 3,800 UHD overflows per hour; more PSD workers do not help (see REJECTED) | soak, worker A/B | Reflash for MAXN Super and re-measure; otherwise lower the sample rate. Do not add workers |
| 7 | P3 | RSS grows about 30 MB/s while the event loop is wedged (about 850 to 1,608 MB during the 60 s wedge). Mechanism not verified (hypothesis: results pile up in the blocked loop's ready queue via `call_soon_threadsafe`, bypassing drop-on-overflow) | F4 sampler | Investigate; with the watchdog on it is bounded to about 45 s, but with it off a long wedge could OOM an 8 GB box |
| 8 | P3 | SIGINT stop: the event loop closes within 0.5 s, then the process lingers until systemd SIGKILLs it at 30 s. Cause not determined | A/B stop | Only matters for Ctrl-C or `KillSignal=SIGINT` runs; look at UHD teardown with streaming still active |
| 9 | Infra | nano-super cannot run MAXN: it has the non-Super flash, the BPMP caps the CPU at 1.51 GHz, and `nvpower.sh` resets nvpmodel to 15W at every boot | nvpmodel checks | Reflash with `jetson-orin-nano-devkit-super.conf`, then rerun the soak and the worker A/B |
| 10 | P3 (design) | After crash give-up the process stays up with the sensor inactive; only an ERROR log and the UI show it, and systemd or health checks cannot see it | F7 | Decide whether give-up should surface in `/api/health` or exit |

### Fix status (updated 2026-09-14)

Branch A (`feat/pipeline-stall-cut1-safety-net`) was merged into local `main` as
901dadf (not pushed). Hardware re-verified on nano-super with the live
B200mini.

| # | Status | Commits | Hardware evidence |
|---|---|---|---|
| 2 | FIXED | 76ccb71 (tooling to py3.10, CI matrix), 6a7d6f4 (except clause) | Hang: "Processor did not stop in time; cancelling" logged at WARNING |
| 3 | FIXED | a2158a9 (`WATCHDOG_STOP_TIMEOUT_SEC=5`), 858bf52 (misconfiguration warning, accurate comment) | Hang that ignores stop: in-process restart in 7.5 to 7.6 s, `NRestarts` 0 (was exit 90) |
| 7 | FIXED | e615876 (`_LoopHandoff`), 302ff64 (bounded-queue guard, `handoff_dropped` in the TIMING log) | 60 s wedge: RSS peak 1,091 MB (was 1,608 MB); still exits 90 as designed |
| 10 | FIXED | 9d8b484 (health pipeline block, exit 91), fe94b1b (re-init failure also gives up; per-call hook flag; replay clears) | Crash loop: health `degraded` for about 5 s, then exit 91, systemd restart, fresh process healthy |
| - | FIXED (found in review) | 4483edb | `/api/sensor` persists a stop before awaiting it, so a watchdog exit mid-stop can no longer bring the sensor back active |

Parked from the final review, not fixed:

- An endless crash cycle at about 1/min with no systemd `StartLimit`. This is
  the behavior chosen for issue 10.
- Health stays HTTP 200 while degraded.
- Reactivating within the 5 s exit window still exits.
- Integration tests run only on 3.11 in CI.
- There is no unit test for the watchdog's `stop_timeout` wiring. It was
  verified on hardware instead.

**Issue 1: FIXED on branch B** (`fix/db-contention-read-connection`, merged
into local `main` as fd0faaf, not pushed). Commits:

- c6720d6 and b184070: a read-only SensorDatabase.
- 5720bf4: batched `insert_detections`.
- 2c84f26: the drain saves one batch per drain.
- 75694f5: the web layer and heartbeat read from the reader; ui-prefs writes on
  the writer.
- 5fa25d4: waterfall and stats aggregations capped at one each.
- bbcfe34: keyset-paged scans, so no reader statement spans an await.
- 77f876b: WAL capped at 64 MB, and WAL mode verified.
- eaf5ac1, 176f82b, 4e07281: ui-prefs merge on the writer, tests, and
  startup/shutdown ordering.

Hardware on nano-super (live B200mini, watchdog on, 74k-window 24 h DB):

| Load | Before branch B | After branch B |
|---|---|---|
| 1 tab | watchdog exit (beacon 31 to 33 s) | beacon max 1.2 s, no slow writes |
| 4 tabs, read connection only (75694f5) | - | watchdog exit (event loop pegged at 100%) |
| 4 tabs x 180 s, with the cap (5fa25d4) | - | 0 watchdog, beacon max 1.8 s |
| 4 tabs x 15 min, WAL/freshness (5fa25d4 vs 4e07281) | WAL 3 to 33 MB and growing; reader up to 295 detections stale | WAL levels off at 15 MB (rewinding); reader lag at most 45 (typically 1 to 10) |

Cost: with 4 tabs open, requests queue, so the waterfall takes about 94 s
median.

Parked from branch B's final review:

- Python 3.10 `asyncio.Semaphore` lets a newcomer barge past a waiter.
- A scan keeps running after its tab closes.
- A queued waterfall (about 94 s) would exceed a 60 s reverse-proxy timeout if
  the UI is ever put behind a proxy.
- Checkpoint cost moves into pipeline commits.
- Sweep mode (`ContinuousProcessor`) still inserts detections one at a time.

Found for branch C: a headless run (`WEB_PORT=0`) does not exit on SIGINT
(streaming worker threads). This predates the branch. (Withdrawn: see the
branch C CORRECTION at the end.)

**Issues 4 and 8: FIXED on branch C** (`fix/shutdown-signals`, merged into
local `main` as 297a9ac, not pushed). Root cause and evidence are in
`2026-09-14_shutdown-signals.md`. Commits:

- ac4ca17: `install_stop_signals` and a uvicorn server without its own signal
  capture.
- 87458f6: `run()` stops in order: web server within 5 s, the loops, the
  pipeline, ZMS and NATS, then the DBs, and exits 0.
- a2c7c4e: the DB closes run even when cleanup raises CancelledError; worker
  errors during shutdown are logged; uvicorn's graceful timeout is 3 s;
  compose gets `stop_grace_period: 30s`.
- 1bf3c39: typing nit.

Results:

| Stop | Before branch C | After branch C |
|---|---|---|
| SIGTERM, nano-super, recording in progress | dies in under 0.2 s; no cleanup; no `.json`/`.psd.json` | 0.98 s (1.16 s on merged main); recording saved; SDR released; DBs closed |
| SIGINT, nano-super | SIGKILLed by systemd at 30 s | 1.08 s, ordered, not restarted |
| SIGTERM, open `/ws/live` client | - | 0.84 s, client gets 1012 |
| Local mock, 3.10 SIGINT (web and headless) | hangs until SIGKILL (41 s) | 0.5 to 1.0 s, rc 0 |
| Web port already in use at startup | hangs forever, and the unit stays "active" | rc 1 on 3.11 or 3 on 3.10 in 0.5 s, so `Restart=on-failure` fires |

Parked from branch C's final review:

- A signal during startup, before the handlers are installed, is not ordered.
  The handlers go in after the DB connect, the ZMS/NATS start and the SDR init.
- A web task that returns normally (a uvicorn lifespan startup failure on 0.41)
  goes unnoticed. This predates the branch.
- The watchdog restart deadline and the supervisor give-up `os._exit` can fire
  during a long cleanup. Both are deliberate escape hatches.

**Issue 5: FIXED on branch D** (`fix/recording-overflow-accounting`, merged
into local `main` as bfbac90, not pushed). Root cause and evidence are in
`2026-09-14_recording-overflow-accounting.md`. Commits:

- 79452c9: the receiver measures each overflow gap from the packet
  `time_spec`, in integer ticks.
- a8e0229: the capture `.json` gains `overflow_events`, `lost_samples`,
  `gaps`, `gaps_truncated` and `time_span_sec`. This commit also fixes
  `CircularBuffer.read()` returning an empty pre-roll when exactly full.
- a8fc78e: TIMING `ovf=`/`lost=`, and `/api/health` `pipeline.overflow_*`.
- 9b13160, 4b27c41, 8bb5662:
  - a stream-position continuity check, which closes a manual-start race that
    skipped a chunk with no gap recorded;
  - same-index gaps merged;
  - the sidecar window, the `too_new` check and the viewer use `time_span_sec`;
  - `start_time` anchored at the pre-roll read.

| 30 s capture, nano-super 15 W, 56 MS/s | Before branch D | After branch D |
|---|---|---|
| Reported loss | "0 dropped" | 63 overflow gaps, 6.058 s lost (19.5%) |
| Accounted span | 24.1 s of samples in a 30.1 s span, unexplained | 24.991 s + 6.058 s = 31.049 s = 1.028 s pre-roll + 30.021 s (auto-stop 30.0 s) |
| Live visibility | none | `/api/health` overflow_events 13 to 80; TIMING `ovf=` |

Parked from branch D's reviews:

- The captures UI still shows only "Dropped Chunks". No "Lost samples" row
  was added (out of scope).
- Retunes in multi-frequency streaming may count dead time as a gap. Not
  verified.
- Pre-existing: `stop_recording` sets "idle" after a 15 s `_end_done` timeout
  even while finalize is still running.
- The pre-roll copy holds the ring lock for about one chunk. The loss this
  causes is now accounted for as a gap.

Still open: issues 6 and 9, which need the nano-super reflash. After the
reflash, rerun the soak and the worker A/B at real MAXN.

The spec's deployment note (`2026-09-08-pipeline-stall-resilience-design.md`,
"Enable order") says to turn `WATCHDOG_ENABLED=true` right after Cut 1. Issue 1
says that order would restart the sensor whenever someone opens a wide
Dashboard view. Enable the watchdog only after Cut 3b, or after issue 1 is fixed
some other way.

## The procedure that produced it

The scripts are in `2026-09-14_stall-safety-net-hardware-validation/`, next to
this file. `harness.py` wraps `rfobserver run` and monkeypatches in fault
injection and instrumentation, so the repo is not modified. Faults are armed
with `touch ctl/<fault>`. Every 10 s the harness logs a status line with:

- beacon age
- maximum event-loop lag
- supervisor state and processor id
- counts of the recv, dispatch, burst and recctl threads (a duplicate would
  mean a leaked pipeline)
- any DB call slower than 2 s (`db-slow`)

1. **Baseline checks.** Ran the full CI set locally (ruff check, ruff format,
   mypy, unit, integration with a throwaway NATS): all pass. Ran the unit suite
   on the box under Python 3.10.12: 409 pass. This controls for "is the code
   broken before hardware".
2. **MAXN check.** Rebooted with MAXN pinned as the nvpmodel default. The box
   came back at 15W with a 1.51 GHz cap. This isolated a flash or firmware limit
   from a configuration problem (issue 9).
3. **Worker A/B** (`ab_workers.sh`, 90 s per setting, trigger off). This
   isolates processing drops from UHD overflows. Done at the user's request to
   "add more workers if we're dropping samples".
4. **Field-scale seed** (`seed_windows.py`). Cloned a real avg_windows row into
   74,000 windows over the previous 24 h, giving a 657 MB DB, about the field
   box's 74k windows per day. This makes the Dashboard do the field box's amount
   of work.
5. **1 h soak** (16:20:37 to 17:20:40 UTC) with no UI load. This controls for
   "does normal operation ever trip the watchdog" and checks the eviction cap.
6. **Dashboard load** (`dash_load.py`, 1 tab). It fires the Averaged page's
   four `loadAll()` requests over a sliding 24 h "Now" range, so the waterfall
   cache never hits. Run 1 was before DB timing existed; run 2 had `db-slow`
   timing, to measure where the time goes.
7. **Fault injection, F1 to F7** (table below). F3b was added after F3 failed to
   exercise the in-process restart.
8. **Stop behaviour.** Stopped the unit with SIGINT (my first launcher) and with
   SIGTERM (production).

## The evidence

### Worker A/B (90 s each, live SDR, 56 Msps, trigger off)

| workers | chunks recv | dropped | drop % | UHD overflows / 90 s | median latency | CPU |
|---|---|---|---|---|---|---|
| 3 (default) | 2400 | 453 | 18.9 | 70 | 250 ms | 447% |
| 4 | 2350 | 396 | 16.9 | 121 | 274 ms | 473% |
| 5 | 2300 | 316 | 13.7 | 140 | 397 ms | 453% |

The ideal is 90 s / 36.6 ms, or 2459 chunks. The share actually processed is
79.2%, 79.5% and 80.7%, which is flat. Extra workers only move loss from
processing drops into UHD overflows, which are gaps in the raw IQ that
recordings are written from.

### 1 h soak (watchdog on, no UI load)

```
status_lines=357  watchdog_lines=0  died_lines=0  db_slow=0
max_beacon_age=0.4  max_loop_lag=0.07  procs=1  thread_sets: 357 x recv=1 dispatch=1 burst=1
recordings_saved=244  rotations=243  auto/ steady at ~2.0 GB (cap 2 GB)
chunks: recv#93500 dropped=19310 (20.7%)  uhd_overflows=3823
NRestarts=0  RSS 752..1090 MB (flat, no trend; sampler.csv)
```

### Dashboard run 2 (1 tab, 24 h, field-scale DB) with DB call timing

Request latencies: waterfall 23.8 s and 16.0 s; stats 16.8 s and 12.4 s;
iq-captures 6.9 s and 6.5 s; detections.json up to 0.2 s.

```
17:21:14 db-slow insert_detection took 3.8s
17:21:16 db-slow query_iq_captures took 6.7s
17:21:16 db-slow count_detections took 5.2s      <- O(1) MAX(id): pure queue wait
17:21:19 STALLTEST status beacon_age=10.5 max_loop_lag=1.91
17:21:26 db-slow _stats_aggregated took 16.6s
17:21:30 STALLTEST status beacon_age=21.0 max_loop_lag=1.33
17:21:33 db-slow _waterfall_aggregated took 23.6s
17:21:40.844 Watchdog: pipeline stalled (31.3s since last progress); restarting
17:21:41 db-slow insert_iq_capture took 3.4s / insert_detection took 3.4s
17:21:50.846 Watchdog: in-process restart failed
17:21:50.850 Watchdog: restart did not complete in 10.0s; exiting for systemd
17:21:50 systemd: Main process exited, code=exited, status=90
17:21:59.370 Sensor activated   (new PID; Dashboard requests got RemoteDisconnected)
```

Every `insert_detection` took 2.7 to 3.8 s. Burst arrival in steady state was
19,465 bursts over 50 min, about 6.5/s (mean 4.9 per drain, max 69). During the
33 s of load only 8 bursts were saved.

Mechanism:

- Every DB call goes through one aiosqlite worker: the pipeline writes, the
  heartbeat's per-second `count_detections`, and the web reads.
- `insert_detection` needs two trips through that queue (execute and commit).
- `_drain_burst_results` saves bursts one at a time *before*
  `beacon.mark()` (`streaming.py:1544-1550`). Saving falls about 20x below the
  arrival rate, so the consumer never reaches the mark.
- The restart cannot finish within 10 s because `_stop()` waits for that same
  consumer. In run 1, "SDR released" landed at 16:19:16.317, the second the
  Dashboard's second waterfall request finished.

Run 1 (16:18:36, no DB timing): watchdog at 16:19:08.53 (32.8 s); the stop took
7.8 s and USRP init 2.3 s; exit 90 at 16:19:18.67; active again at 16:19:27.16.

### Fault injection

| Test | Injected | Result | Timings |
|---|---|---|---|
| F1 | `insert_detection` raises 3 times | PASS: 3 "insert_detection failed for burst ...; skipping", beacon at most 0.1 s, same PID, no restart | 17:23:04 |
| F2 | consumer raises once | PASS: "died unexpectedly", backoff 1.0 s, SDR released and re-init, new processor, single thread set | crash 17:23:45.893 to active 17:23:49.260 (3.4 s) |
| F3 | `kill -STOP` 45 s, then CONT | Pipeline self-recovered: consumer marked the beacon before the watchdog's next tick; no restart. Restart path NOT exercised (race) | CONT 17:25:21.8, beacon 0.1 s at 17:25:22.2 |
| F3b | consumer stalls but honours `stop()` | PASS: in-process restart succeeded, same PID, NRestarts 0 | detect 17:27:40.632 (32.9 s), "pipeline restarted" 17:27:43.392 (2.76 s) |
| F4 | `time.sleep(60)` on the event loop | PASS as designed: restart cannot run, exit 90, systemd restart | detect 17:28:58.456 (32.0 s), exit 17:29:08.46, active 17:29:17.086 |
| F5 | consumer awaits forever, ignores `stop()`, deadline 10 s | PASS as designed: exit 90, systemd restart | detect 17:30:37.153 (34.4 s), exit 17:30:47.155, active 17:30:55.557 |
| F6 | same as F5, deadline raised to 30 s | Issue 2 confirmed: mislabelled log, no warning. The task WAS cancelled, and the restart completed in-process (see CORRECTION) | see below |
| F7 | consumer raises on every run | PASS: backoffs 1, 2, 4, 8, 16 s; "crashed 6 times within 120s; giving up"; watchdog ignores the inactive sensor (beacon 31.6 s, no action); `POST /api/sensor {"active":true}` reactivates cleanly | 17:31:28 to give-up 17:32:13.426 |

F6 capture, with the supervisor logger at DEBUG:

```
17:34:40.306 Watchdog: pipeline stalled (32.8s since last progress); restarting
17:34:55.427 supervisor DEBUG Stop observed an already-raised task exception (already reported)
             ... raise exceptions.TimeoutError() from exc
             asyncio.exceptions.TimeoutError
17:34:55.504 Receiver closed (SDR released)
17:34:57.780 Sensor activated
17:34:57.840 Watchdog: pipeline restarted
```

There was no "Processor did not stop in time; cancelling" warning.
~~So the hung task was never cancelled. The restart reports success while the
old coroutine and its processor stay alive.~~ (Withdrawn, see CORRECTION:
`wait_for` cancelled it.) Pipeline threads were not duplicated: the old
recctl thread exits on `_running=False`. RSS was 813 MB before and 912 MB after, which is within steady-state noise
(and, per the CORRECTION, there is no leak to measure).

On Python 3.10: `asyncio.TimeoutError is TimeoutError` evaluates to `False`
(checked on the box).

### Stop behaviour

- **SIGTERM (production):** `Result=success` in under 0.2 s. The log ends at
  uvicorn's "Finished server process" with no "Sensor deactivated (SDR
  released)", so `app.run()`'s `finally` did not run. uvicorn 0.51
  `capture_signals()` re-raises the captured signal after restoring
  `SIG_DFL` (`uvicorn/server.py:336-337`).
- **SIGINT:** KeyboardInterrupt at 16:12:52.458. The loop was closed by
  16:12:52.95 (the harness thread saw "Event loop is closed"). systemd SIGKILLed
  the process at 16:13:22 (`Result=timeout`).

## Measured and REJECTED (do not retry)

- **More PSD workers to reduce drops.** Rejected: see the worker A/B table.
  Processed throughput stays flat at 79 to 81%, UHD overflows double (70 to
  140 per 90 s), and latency rises from 250 to 397 ms. The box is CPU-bound.
- **Reboot or nvpmodel changes to get MAXN on the current flash.** Rejected:
  `PM_CONFIG DEFAULT=2` plus a reboot came back at 15W. `nvpower.sh` re-points
  `/etc/nvpmodel.conf` to the non-Super conf because the machine is `p3767-0005`,
  not `-super`. `cpuinfo_max_freq` is 1510400 in every mode. Only a reflash
  lifts it.
- **"The waterfall decode wedges the event loop for many seconds" as the
  Dashboard stall mechanism.** Rejected: the maximum loop lag measured during
  24 h queries was 1.1 to 1.9 s, so the loop kept running. The stall is
  DB-queue serialization, shown by the `db-slow` evidence.
- **"A single pipeline write sits blocked behind the whole Dashboard query."**
  Rejected by Dashboard run 2: writes complete, each taking 2.7 to 3.8 s. It is
  the serial per-burst saving at that latency that starves the beacon.

## Measurement traps hit

- **"MAXN_SUPER" was a label only.** `nvpmodel -q` showed MAXN_SUPER from Sep 7
  to 14, while the clock cap stayed at 1.51 GHz. Check `cpuinfo_max_freq`, not
  the mode name.
- **My loop-lag probe under-reports a wedge.** It caps at 0.90 s per sample
  while its callback is pending; F4 showed `max_loop_lag=0.90` during a 60 s
  wedge. Trust beacon age for stalls.
- **The recording drop counter.** It only counts queue drops, not UHD overflow
  gaps (issue 5).
- **Trigger threshold -111 dB (copied from `.177`'s `.env`).** It fired on every
  chunk: back-to-back 30 s captures of 5.4 GB each, about 650 GB/h to the NVMe.
  Measured mean power is about -55 dB here, so the tests used -52.5 dB.
- **Races.** SIGSTOP does not reliably exercise the watchdog, because the
  consumer and watchdog race on resume (F3). Use a stop-honouring stall (F3b)
  to test the in-process path.
- **`pkill -f` matching its own shell.** `pkill -f <pattern>` over ssh killed
  its own shell twice (exit 255), because the pattern was in the command line.
  Use `pgrep -f "[s]ampler\.sh"`.
- **systemd-run environment.** Passing the same `--setenv` key twice fails with
  "Invalid environment block".
- **"Merged to main" was local only.** `origin/main` is still d7ef1b1; the fixes
  are only on local `main` and the feature branch.

## CORRECTION 2026-09-14 (later the same day): issue 2 and F6 overstated

Three claims made above about F6 are **WITHDRAWN**, and were reported that way
earlier in the session:

- "the hung task was never cancelled"
- "the old coroutine and its processor stay alive / leak"
- "a second processor starts"

`asyncio.wait_for` cancels the awaited task and waits for it to finish before
raising on timeout. Python 3.10 `asyncio/tasks.py` `wait_for`:
`await _cancel_and_wait(fut, loop=loop)` then
`raise exceptions.TimeoutError() from exc`, which is the exact traceback line in
the F6 capture. So the hung task WAS cancelled by `wait_for` itself.

The capture fits that: "Receiver closed (SDR released)" came 77 ms after the
timeout. That is the cancelled task's `finally` running, and there were no
duplicate threads.

What remains true: on 3.10 the timeout skips the intended
`except TimeoutError` branch. The "Processor did not stop in time; cancelling"
warning is never logged, and the event is mislabelled at DEBUG as "Stop observed
an already-raised task exception (already reported)". This is an observability
bug, so issue 2 is downgraded from P1 to P3. The fix and the Python 3.10 tooling
alignment are unchanged.

## Open, not yet answered

- Does a Dashboard left open in "Now" mode (periodic polling) cause a restart
  loop, and at what cadence? Only single 30 to 45 s load bursts were tested.
- Does Cut 3b alone (a separate read connection, web still on the main loop)
  remove issue 1? The 1 to 2 s loop slices would remain.
- The mechanism behind issue 7 (RSS growth during a wedge). (Issue 8 was later
  root-caused: see the branch C CORRECTION below.)
- A rerun at real MAXN after the reflash: soak, worker A/B, Dashboard load.
- `.177` (the plan's first validation target) has not been updated or tested.
- The field box's journal (see `2026-09-08_pipeline-silent-stall.md`, "Open")
  still has not been checked.

## CORRECTION 2026-09-14 (branch C investigation): issue 8 cause, headless SIGINT

Root-caused in `2026-09-14_shutdown-signals.md`. Two claims above are
**WITHDRAWN**:

- **Issue 8's fix hint, "look at UHD teardown with streaming still active".**
  The hang reproduces with the mock receiver. The mechanism:
  - Python 3.10's `asyncio.run` cancels every task on KeyboardInterrupt.
  - `set_active(False)` then raises CancelledError, which aborts `run()`'s
    `finally` before `db.close()`.
  - The non-daemon aiosqlite worker thread blocks interpreter exit.
  - 3.11 is not affected.
- **Fix status, "a headless run (`WEB_PORT=0`) does not exit on SIGINT
  (streaming worker threads)".**
  - The streaming threads are daemon threads.
  - On 3.11 a headless SIGINT exits in 0.5 s.
  - The observation came from a repro launched with `&` from a script, which
    inherits SIGINT as SIG_IGN.
  - On 3.10 the headless hang is real, but it is the same aiosqlite mechanism as
    issue 8.

Issue 4 is confirmed as stated. In addition, a SIGTERM during a recording leaves
the capture without its `.json` and `.psd.json` sidecars.
