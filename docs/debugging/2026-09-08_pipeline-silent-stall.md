# RFObserver pipeline stopped silently during heavy Dashboard use

## The question

2026-09-08. A deployed HCRO field sensor (Jetson, `rfobserver run`, `main`
baseline, StreamingProcessor) stopped advancing on its own. The operator did
not stop it, and it stayed stopped; the process stayed up (web UI kept
responding). The operator reports being heavy on the Dashboard UI just before
it stopped, and suspected the FIFO capture buffer "does not evict correctly."
Symptom: capture/detection production halts while the process and HTTP API stay
alive; `systemd`'s `Restart=on-failure` never fires.

## The answer

The web UI and the capture pipeline run in one process on one asyncio event
loop, sharing one SQLite connection. A wide Dashboard query
(`/api/averaged/waterfall`, 24 h / ~74k windows) runs a CPU-heavy blob-decode
loop inline on that loop with no `await`, blocking it for many seconds. That
starves pipeline persistence and can spuriously trip the 30 s DB write-timeout.
The write path then raises where it is **not guarded** - the `insert_detection`
call in `_drain_burst_results` (`streaming.py:1846`, unlike the guarded
`_persist_avg_window`) - and the exception propagates out of `run()`, killing
the pipeline task. The supervisor attaches no done-callback to that task
(`supervisor.py:106`), so it dies orphaned: `_active` stays `True`, the
heartbeat keeps echoing the dead processor, uvicorn keeps serving. Process
alive, pipeline dead, permanently, invisible to `Restart=on-failure`.

This is a code-derived root cause with high confidence, NOT yet confirmed
against the field box's own journal (see "Open").

## The procedure that produced it

The local `.177` sensor was checked first and is healthy (13-day uptime, timing
logs flowing) - so the failure is on the HCRO field box, whose logs were not in
hand. The diagnosis therefore came from the code, via two parallel focused
reads, each isolating one axis:

1. **Pipeline threading/eviction map** (control: "could the pipeline stall
   without the web layer?"). Enumerated every thread and queue, the FIFO
   disk-cap eviction, unhandled-exception handling per loop, and existing
   liveness. Isolated: the hot-path queues are correctly non-blocking
   (drop-on-overflow), so the stall is not simple backpressure; the real
   pipeline-only hazards are (a) an unguarded DB exception killing `run()` and
   (b) a disk-I/O-triggered eviction/writer deadlock.

2. **Web-vs-pipeline contention map** (the operator's "heavy Dashboard use"
   clue). Traced the process topology and the read path. Isolated: one shared
   loop, one shared DB connection, and the waterfall aggregation running inline
   on the loop - the mechanism that ties "hammering the Dashboard" to a pipeline
   stall.

The two maps agree on the same terminal failure (unguarded `insert_detection`
-> orphaned dead task), reached from two independent directions, which is why
confidence is high despite no field log.

Each measurement controlled for something: the healthy `.177` box controls for
"is the pipeline itself broken" (no - a different sensor on the same code runs
fine); the threading map controls for "is the web layer even necessary to
explain it" (a pure-pipeline path exists but needs a disk fault to trigger,
whereas the Dashboard path needs only load).

## The evidence

Deployed unit (from `.177`, representative): `Type=simple`,
`Restart=on-failure`, `RestartSec=5`, `WatchdogUSec=0`, `NotifyAccess=none`,
`NRestarts=0`. So only a non-zero **process exit** restarts it; a silent stall
does not.

Causal chain, with anchors:
- Shared loop: `cli.py:115` `asyncio.run(run(...))`; `app.py:130-138` gathers
  uvicorn + heartbeat; `supervisor.py:106` `create_task(processor.run())` on the
  same loop.
- Inline heavy query: `api.py:956` `await db.query_avg_waterfall(...)`;
  `database.py:909-947` per-row blob decode + numpy, no `await`. No query uses
  `asyncio.to_thread` (only recording start/stop does).
- Shared single connection: `database.py:246`.
- Spurious write-timeout: `_guarded_write` `database.py:216-244`; unguarded
  retry at `database.py:242`.
- Unguarded insert: `streaming.py:1846` (vs guarded `_persist_avg_window`
  `streaming.py:1752`); `run()` `try/finally` no `except` `streaming.py:412-441`.
- Orphaned task: `supervisor.py:106` (no `add_done_callback`); heartbeat echoes
  stale state `app.py:148-217`.
- Latent eviction/writer deadlock: `local.py:75-82` (uncaught `OSError`),
  `streaming.py:1155` (writer crash), `streaming.py:942` (blocking `put(None)`).
- Dead watchdog: `utils/watchdog.py` exists, never imported, and is asyncio-based
  (useless against loop starvation).

## Measured / reasoned and REJECTED (do not retry)

- **"FIFO buffer does not evict" as the primary cause - REJECTED as primary.**
  The eviction logic is correct and well-tested in isolation
  (`tests/unit/test_local_storage.py`). It only breaks as a *secondary* cascade
  after a disk-I/O fault (`OSError` in `_delete_capture`). The operator's heavy-
  Dashboard timing points at the loop-starvation path, not a disk fault. Kept as
  a real latent bug to fix, not the trigger.
- **Memory / OOM - REJECTED as primary.** `_waterfall_aggregated` streams via
  `cursor.fetchmany(5000)` with bounded accumulators (`database.py:906,890-891`);
  the 24 h case is aggregated mode (CPU-bound), not raw mode (which would
  materialize rows). Not a memory spike. Possible only as a secondary under many
  simultaneous tabs.
- **Sample drops as the cause - REJECTED.** Drops are graceful (drop-on-overflow
  queues); the pipeline keeps running through them. Drops did not stop it. (This
  is why core affinity, while worth adding, is hardening, not the fix.)
- **`Restart=on-failure` should have caught it - REJECTED.** It only acts on a
  non-zero process exit; the process never exited. This is the whole reason a
  thread-based watchdog with an `os._exit` escalation is needed.

## Measurement traps hit

- **The healthy sensor is misleading.** `.177` runs the same code and is fine;
  concluding "the pipeline is OK" from it would be wrong - the failure is
  load- and box-specific.
- **"Sensor Active" / `/api/health` are misleading.** They report the
  supervisor's stale belief and uvicorn's own liveness, neither of which tracks
  whether the pipeline task is alive or advancing. A green health check here
  means nothing about the pipeline.
- **`%CPU`/counters in the heartbeat do not detect a stall** - the heartbeat
  re-reads whatever the (possibly dead) processor last held; it never checks
  that a counter advanced.

## Open, not yet answered

- **Confirm the vector on the field box's journal.** Grep:
  `journalctl -u rfobserver | grep -E "Task exception was never retrieved|stuck >|reconnect|File writer crashed|Receiver loop crashed|Recording-control job failed|Read-only file system|No space left"`.
  Signature for the diagnosed chain: a burst of `GET /api/averaged/waterfall`,
  then `DB write ... stuck >30s`, then `Task exception was never retrieved`,
  after which `Detected N bursts` / `TIMING recv#` lines stop while `/api/*`
  keeps returning 200. If instead `File writer crashed` / `Read-only file
  system` appears with state stuck at `finalizing`, it is the eviction-deadlock
  vector.
- Whether the field box also had a real disk fault (which would mean both
  vectors fired).

## Fix

Design spec:
`docs/superpowers/specs/2026-09-08-pipeline-stall-resilience-design.md`
(three cuts: safety net first, then core affinity, then UI-thread isolation).
