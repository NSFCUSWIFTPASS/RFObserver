"""On-hardware validation harness for the pipeline-stall Cut 1 safety net.

Runs the real `rfobserver run` pipeline (live SDR) with fault injection and
instrumentation layered on by monkeypatching; the repo is not modified.

Faults are armed by creating a file in $STALLTEST_CTL (polled every 0.5 s):
  crash_once    next consumer-loop iteration raises RuntimeError (task dies)
  crash_always  every consumer-loop iteration raises (crash loop -> give up)
  crash_off     disarm crash_always
  hang_once     consumer loop awaits forever (pipeline hung, event loop alive)
  wedge_<N>     block the event loop with time.sleep(N) (loop wedged)
  dbfail_<N>    next N insert_detection calls raise sqlite3.OperationalError

Every 10 s it logs a STALLTEST status line: beacon age, max event-loop lag,
supervisor state, and counts of pipeline threads (duplicates = leaked pipeline).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import threading
import time
from pathlib import Path

CTL = Path(os.environ.get("STALLTEST_CTL", "/tmp/stalltest-ctl"))
CTL.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("stalltest")
for _lg in filter(None, os.environ.get("STALLTEST_DEBUG_LOGGERS", "").split(",")):
    logging.getLogger(_lg).setLevel(logging.DEBUG)

from rfobserver.pipeline import beacon as beacon_mod  # noqa: E402
from rfobserver.pipeline import streaming  # noqa: E402
from rfobserver.pipeline import supervisor as supervisor_mod  # noqa: E402
from rfobserver.storage import database  # noqa: E402

state: dict[str, object] = {
    "crash_once": False,
    "crash_always": False,
    "hang_once": False,
    "dbfail": 0,
    "beacon": None,
    "supervisor": None,
    "loop": None,
}

# --- capture instances -------------------------------------------------------

_orig_beacon_init = beacon_mod.ProgressBeacon.__init__


def _beacon_init(self, *a, **k):  # type: ignore[no-untyped-def]
    _orig_beacon_init(self, *a, **k)
    state["beacon"] = self


beacon_mod.ProgressBeacon.__init__ = _beacon_init  # type: ignore[method-assign]

_orig_sup_init = supervisor_mod.PipelineSupervisor.__init__


def _sup_init(self, *a, **k):  # type: ignore[no-untyped-def]
    _orig_sup_init(self, *a, **k)
    state["supervisor"] = self
    try:
        state["loop"] = asyncio.get_running_loop()
    except RuntimeError:
        pass


supervisor_mod.PipelineSupervisor.__init__ = _sup_init  # type: ignore[method-assign]

# --- fault injection ---------------------------------------------------------

_orig_drain = streaming.StreamingProcessor._drain_burst_results


async def _drain(self):  # type: ignore[no-untyped-def]
    if state["crash_always"] or state["crash_once"]:
        state["crash_once"] = False
        log.error("STALLTEST: injecting RuntimeError into consumer loop")
        raise RuntimeError("stalltest injected crash")
    if state["hang_once"]:
        state["hang_once"] = False
        log.error("STALLTEST: hanging consumer loop forever (event loop stays alive)")
        await asyncio.Event().wait()
    if state.get("softhang"):
        # Stall THIS processor instance only, but honour stop() promptly, so a
        # watchdog in-process restart can complete (new instance is unaffected).
        state["softhang"] = False
        log.error("STALLTEST: soft-hanging consumer (exits on stop)")
        while self._running:
            await asyncio.sleep(0.2)
        log.error("STALLTEST: soft-hang released by stop()")
    return await _orig_drain(self)


streaming.StreamingProcessor._drain_burst_results = _drain  # type: ignore[method-assign]

# Optional PSD worker-count override (repo hard-codes cpu_count - 3).
_PROC_WORKERS = int(os.environ.get("STALLTEST_PROC_WORKERS", "0"))
if _PROC_WORKERS > 0:
    _orig_sp_init = streaming.StreamingProcessor.__init__

    def _sp_init(self, *a, **k):  # type: ignore[no-untyped-def]
        _orig_sp_init(self, *a, **k)
        self._num_proc_workers = _PROC_WORKERS
        log.warning("STALLTEST: PSD workers overridden to %d", _PROC_WORKERS)

    streaming.StreamingProcessor.__init__ = _sp_init  # type: ignore[method-assign]

_orig_insert_det = database.SensorDatabase.insert_detection


async def _insert_det(self, *a, **k):  # type: ignore[no-untyped-def]
    n = int(state["dbfail"])  # type: ignore[call-overload]
    if n > 0:
        state["dbfail"] = n - 1
        log.error("STALLTEST: injecting OperationalError into insert_detection (%d left)", n - 1)
        raise sqlite3.OperationalError("stalltest injected: database is locked")
    return await _orig_insert_det(self, *a, **k)


database.SensorDatabase.insert_detection = _insert_det  # type: ignore[method-assign]


# Time every async SensorDatabase method; log any call slower than 2 s so a
# pipeline write queued behind a long Dashboard read shows up directly.
def _timed(name, fn):  # type: ignore[no-untyped-def]
    async def wrapper(self, *a, **k):  # type: ignore[no-untyped-def]
        t0 = time.monotonic()
        try:
            return await fn(self, *a, **k)
        finally:
            dt = time.monotonic() - t0
            if dt > 2.0:
                log.warning("STALLTEST db-slow %s took %.1fs", name, dt)

    return wrapper


for _name, _fn in list(vars(database.SensorDatabase).items()):
    if asyncio.iscoroutinefunction(_fn) and not _name.startswith("__"):
        setattr(database.SensorDatabase, _name, _timed(_name, _fn))

# Capture the web layer's read-only instance (branch with the connection split).
_orig_connect = database.SensorDatabase.connect


async def _connect_capture(self, *a, **k):  # type: ignore[no-untyped-def]
    await _orig_connect(self, *a, **k)
    if getattr(self, "read_only", False):
        state["reader"] = self


database.SensorDatabase.connect = _connect_capture  # type: ignore[method-assign]

# --- control + instrumentation thread ----------------------------------------

_lag = {"max": 0.0}


def _probe_lag(loop: asyncio.AbstractEventLoop) -> None:
    t0 = time.monotonic()
    done = threading.Event()

    def _cb() -> None:
        _lag["max"] = max(_lag["max"], time.monotonic() - t0)
        done.set()

    try:
        loop.call_soon_threadsafe(_cb)
    except RuntimeError:  # loop closed during shutdown
        return
    if not done.wait(0.9):
        # Still pending: count the time waited so far; the callback will
        # record the full lag when the loop finally runs it.
        _lag["max"] = max(_lag["max"], time.monotonic() - t0)


def _freshness() -> str:
    """Compare the web reader's view of detections with a fresh connection's.

    A reader statement held open across awaits pins its snapshot for every read
    on that connection, so reader_lag grows; a pinned snapshot also stops the
    WAL rewinding, so wal_mb grows.
    """
    r = state.get("reader")
    loop = state["loop"]
    if r is None or loop is None:
        return "reader=none"
    try:
        fut = asyncio.run_coroutine_threadsafe(r.count_detections(), loop)  # type: ignore[attr-defined,arg-type]
        rmax = int(fut.result(timeout=5))
    except Exception:  # noqa: BLE001
        rmax = -1
    path = r._db_path  # type: ignore[attr-defined]
    try:
        c = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
        fmax = int(c.execute("SELECT COALESCE(MAX(id), 0) FROM detections").fetchone()[0])
        c.close()
    except Exception:  # noqa: BLE001
        fmax = -1
    try:
        wal = os.path.getsize(path + "-wal") // 1048576
    except OSError:
        wal = -1
    return f"reader_max_id={rmax} fresh_max_id={fmax} reader_lag={fmax - rmax} wal_mb={wal}"


def _handle(name: str) -> None:
    loop = state["loop"]
    if name == "crash_once":
        state["crash_once"] = True
    elif name == "crash_always":
        state["crash_always"] = True
    elif name == "crash_off":
        state["crash_always"] = False
    elif name == "hang_once":
        state["hang_once"] = True
    elif name == "softhang":
        state["softhang"] = True
    elif name.startswith("wedge_") and loop is not None:
        secs = float(name.split("_", 1)[1])
        log.error("STALLTEST: wedging event loop for %.0fs", secs)
        loop.call_soon_threadsafe(time.sleep, secs)  # type: ignore[union-attr]
    elif name.startswith("dbfail_"):
        state["dbfail"] = int(name.split("_", 1)[1])
    else:
        log.error("STALLTEST: unknown control %r", name)
        return
    log.error("STALLTEST: armed %s", name)


def _control_thread() -> None:
    last_status = time.monotonic()
    while True:
        time.sleep(0.5)
        for f in sorted(CTL.iterdir()):
            try:
                f.unlink()
            except FileNotFoundError:
                continue
            _handle(f.name)
        loop = state["loop"]
        if loop is not None:
            _probe_lag(loop)  # type: ignore[arg-type]
        if time.monotonic() - last_status >= 10.0:
            last_status = time.monotonic()
            b = state["beacon"]
            sup = state["supervisor"]
            names = [t.name for t in threading.enumerate()]
            log.warning(
                "STALLTEST status beacon_age=%.1f max_loop_lag=%.2f active=%s "
                "proc=%s recv=%d dispatch=%d burst=%d recctl=%d threads=%d",
                b.age() if b is not None else -1.0,  # type: ignore[attr-defined]
                _lag["max"],
                getattr(sup, "active", None),
                hex(id(sup.processor)) if sup is not None and sup.processor else None,  # type: ignore[attr-defined]
                names.count("recv"),
                names.count("dispatch"),
                names.count("burst"),
                names.count("recctl"),
                len(names),
            )
            log.warning("STALLTEST fresh %s", _freshness())
            _lag["max"] = 0.0


threading.Thread(target=_control_thread, name="stalltest", daemon=True).start()

if __name__ == "__main__":
    import sys

    from rfobserver.cli import main

    sys.argv = ["rfobserver", "run"]
    main()
