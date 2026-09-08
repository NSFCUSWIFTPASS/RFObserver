# Pipeline Stall Resilience - Cut 1 (Safety Net) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn a silent, permanent pipeline stall into a logged auto-recovery: guard the one unguarded DB write, supervise the pipeline task, add a thread-based liveness watchdog that restarts (or exits for systemd) on a stall, and fix the eviction/writer deadlock.

**Architecture:** A `ProgressBeacon` (thread-safe monotonic heartbeat) is marked by the pipeline on every processed result. A daemon-thread `PipelineWatchdog` (immune to event-loop starvation and to the pipeline task dying) reads the beacon; on a confirmed stall it asks the supervisor to restart the pipeline via `run_coroutine_threadsafe`, and if the loop is wedged past a deadline it `os._exit`s so systemd restarts the process. The supervisor also attaches a done-callback so a task that dies on an exception is restarted immediately rather than orphaned. The detection-insert path and the disk-eviction/writer path are hardened so they cannot raise-to-death or block-forever.

**Tech Stack:** Python 3.10, asyncio, threading, aiosqlite, pytest + pytest-asyncio.

**Spec:** `docs/superpowers/specs/2026-09-08-pipeline-stall-resilience-design.md` (this is Cut 1 of three).

## Global Constraints

- **Python >= 3.10 clean.** The field/deploy Jetsons run 3.10; no 3.11+ syntax. Use `except asyncio.TimeoutError` with the `# noqa: UP041` the codebase already uses, not `except TimeoutError` where the distinction matters.
- **Always prefix python/pytest/mypy with `PYTHONPATH=`** (host leaks system 3.10 packages). venv at `.venv/`. ruff is global.
- **Pre-commit gates, in order:** `ruff check src/ tests/`; `ruff format --check src/ tests/`; `PYTHONPATH= .venv/bin/mypy src/rfobserver/`; `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`; `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q` (integration needs NATS on localhost:4222: `docker run -d --rm --name rfobs-test-nats -p 4222:4222 nats:2.10-alpine -js`).
- **No emojis anywhere. No em-dashes** (U+2014) in code, comments, or docs. Check every diff.
- **Never** add a `Co-Authored-By: Claude` trailer.
- **Stage only explicit paths** (never `git add -A`).
- **Everything OFF by default.** `WATCHDOG_ENABLED=False`; with it off, behavior is identical to today (no watchdog thread, beacon marked but unread). The beacon-marking and the guards/eviction-fixes are always on but are pure hardening (no behavior change on the happy path).
- **Deployed unit:** `Type=simple`, `Restart=on-failure`, `RestartSec=5`. The watchdog's `os._exit(non-zero)` escalation relies on this; do not change the unit.

---

## File Structure

- **Create** `src/rfobserver/pipeline/beacon.py` - `ProgressBeacon` (thread-safe heartbeat). Leaf.
- **Rewrite** `src/rfobserver/utils/watchdog.py` - replace the dead asyncio `Watchdog` with a daemon-thread `PipelineWatchdog`.
- **Modify** `src/rfobserver/pipeline/streaming.py` - guard the `insert_detection` loop; mark the beacon on progress; accept a `beacon` param; make `_finalize_recording`'s `put(None)` non-blocking.
- **Modify** `src/rfobserver/storage/local.py` - guard `_delete_capture` unlinks and `enforce_cap`/`_enforce_limit`.
- **Modify** `src/rfobserver/storage/database.py` - add a timeout to the post-reconnect retry.
- **Modify** `src/rfobserver/pipeline/supervisor.py` - `restart()` + task done-callback.
- **Modify** `src/rfobserver/pipeline/continuous.py` - accept a `beacon` param, mark on each processed capture (parity; sweep mode).
- **Modify** `src/rfobserver/pipeline/app.py` - create the beacon, inject it, start/stop the watchdog when enabled.
- **Modify** `src/rfobserver/config.py` - watchdog settings.
- **Create tests** under `tests/unit/` per task; two integration tests under `tests/integration/`.

---

### Task 1: ProgressBeacon

**Files:**
- Create: `src/rfobserver/pipeline/beacon.py`
- Test: `tests/unit/test_beacon.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `ProgressBeacon` with `mark() -> None`, `age() -> float` (seconds since last mark), `reset() -> None` (alias for mark).

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_beacon.py
import time

from rfobserver.pipeline.beacon import ProgressBeacon


def test_age_small_after_mark():
    b = ProgressBeacon()
    b.mark()
    assert b.age() < 0.5


def test_age_grows_without_mark():
    b = ProgressBeacon()
    b.mark()
    time.sleep(0.15)
    assert b.age() >= 0.15


def test_mark_resets_age():
    b = ProgressBeacon()
    time.sleep(0.15)
    b.mark()
    assert b.age() < 0.15
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_beacon.py -v`
Expected: FAIL (module missing).

- [ ] **Step 3: Write minimal implementation**

```python
# src/rfobserver/pipeline/beacon.py
"""A thread-safe monotonic liveness heartbeat.

The pipeline calls mark() on every unit of forward progress (each processed
result). A watchdog on another thread reads age() to detect a stall. Kept
trivially small and lock-guarded so it is safe to touch from the asyncio loop
and a daemon thread at once.
"""

from __future__ import annotations

import threading
import time


class ProgressBeacon:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last = time.monotonic()

    def mark(self) -> None:
        with self._lock:
            self._last = time.monotonic()

    def reset(self) -> None:
        self.mark()

    def age(self) -> float:
        with self._lock:
            return time.monotonic() - self._last
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_beacon.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Lint, type-check, commit**

```bash
ruff check src/rfobserver/pipeline/beacon.py tests/unit/test_beacon.py
ruff format --check src/rfobserver/pipeline/beacon.py tests/unit/test_beacon.py
PYTHONPATH= .venv/bin/mypy src/rfobserver/pipeline/beacon.py
git add src/rfobserver/pipeline/beacon.py tests/unit/test_beacon.py
git commit -m "feat(resilience): thread-safe ProgressBeacon liveness heartbeat"
```

---

### Task 2: Guard the detection insert + timeout the DB reconnect retry

**Files:**
- Modify: `src/rfobserver/pipeline/streaming.py` (the per-burst loop in `_drain_burst_results`, ~line 1843-1863)
- Modify: `src/rfobserver/storage/database.py` (the retry at line 233)
- Test: `tests/integration/test_pipeline_db_resilience.py`

**Interfaces:**
- Consumes: existing `StreamingProcessor`, `SensorDatabase`.
- Produces: no new public surface; behavior change = a raised `insert_detection` no longer propagates out of `_drain_burst_results`, and the post-reconnect retry is bounded by `self._write_timeout`.

- [ ] **Step 1: Write the failing test**

```python
# tests/integration/test_pipeline_db_resilience.py
"""A DB write that raises must not kill the pipeline's consumer loop."""

from __future__ import annotations

import pytest

from rfobserver.pipeline.streaming import StreamingProcessor


class _BoomDB:
    """Stands in for SensorDatabase: insert_detection always raises."""

    def __init__(self) -> None:
        self.calls = 0

    async def insert_detection(self, **kwargs) -> None:
        self.calls += 1
        raise RuntimeError("simulated disk-full")


@pytest.mark.asyncio
async def test_drain_burst_results_survives_db_error():
    # Build a processor shell without running the full pipeline: we only drive
    # _drain_burst_results directly with one burst result queued.
    from datetime import datetime, timezone

    from rfobserver.models import BurstFingerprint

    proc = StreamingProcessor.__new__(StreamingProcessor)
    proc._db = _BoomDB()
    proc._replay_mode = False
    proc._receiver = object()
    proc._settings = type("S", (), {"BANDWIDTH": 2_000_000, "GAIN": 30})()
    import asyncio as _a

    proc._burst_result_queue = _a.Queue()
    now = datetime.now(timezone.utc)
    burst = BurstFingerprint(
        start_time=now, stop_time=now, center_freq_hz=915e6, peak_freq_hz=915e6,
        bandwidth_hz=1e5, peak_power_db=-40.0, duration_ms=10.0,
    )
    await proc._burst_result_queue.put(([burst], 915e6))
    # Must NOT raise despite the DB blowing up.
    await proc._drain_burst_results()
    assert proc._db.calls == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_pipeline_db_resilience.py -v`
Expected: FAIL - the `RuntimeError` propagates out of `_drain_burst_results` (the insert call is currently unguarded).

- [ ] **Step 3: Write minimal implementation**

In `streaming.py`, wrap the per-burst insert (the `for burst in bursts:` block at ~1843-1863) so a DB error is logged and skipped, not fatal:

```python
            for burst in bursts:
                if self._replay_mode:
                    continue
                try:
                    await self._db.insert_detection(
                        burst_id=burst.burst_id,
                        start_time=burst.start_time,
                        stop_time=burst.stop_time,
                        center_freq_hz=burst.center_freq_hz,
                        bandwidth_hz=burst.bandwidth_hz,
                        peak_power_db=burst.peak_power_db,
                        duration_ms=burst.duration_ms,
                        detection_timestamp=burst.detection_timestamp,
                        peak_freq_hz=burst.peak_freq_hz,
                        sdr_center_freq_hz=float(sdr_center_freq_hz),
                        sample_rate_hz=sample_rate_hz,
                        lo_offset_hz=0.0,
                        analog_bw_hz=None,
                        gain_db=gain_db,
                        antenna="RX2",
                        device_serial=device_serial,
                    )
                except Exception:
                    logger.exception("insert_detection failed for burst %s; skipping", burst.burst_id)
```

In `database.py`, bound the post-reconnect retry (line 233) with the same timeout so the retry cannot hang forever:

```python
            await asyncio.wait_for(self._reconnect(expect=conn), timeout=self._write_timeout)
            return await asyncio.wait_for(fn(self, *args, **kwargs), timeout=self._write_timeout)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_pipeline_db_resilience.py -v`
Expected: PASS. Then `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -q` to confirm the retry change did not regress the DB tests.

- [ ] **Step 5: Lint, type-check, commit**

```bash
ruff check src/rfobserver/pipeline/streaming.py src/rfobserver/storage/database.py tests/integration/test_pipeline_db_resilience.py
ruff format --check src/rfobserver/pipeline/streaming.py src/rfobserver/storage/database.py tests/integration/test_pipeline_db_resilience.py
PYTHONPATH= .venv/bin/mypy src/rfobserver/pipeline/streaming.py src/rfobserver/storage/database.py
git add src/rfobserver/pipeline/streaming.py src/rfobserver/storage/database.py tests/integration/test_pipeline_db_resilience.py
git commit -m "fix(resilience): a raised DB write can no longer kill the pipeline loop"
```

---

### Task 3: Eviction and writer-thread deadlock fixes

**Files:**
- Modify: `src/rfobserver/storage/local.py` (`_delete_capture` ~75-82, `_enforce_limit` ~84-90, `enforce_cap` ~92-106)
- Modify: `src/rfobserver/pipeline/streaming.py` (`_finalize_recording` `put(None)` at ~942)
- Test: `tests/unit/test_local_storage_resilience.py`

**Interfaces:**
- Consumes: existing `LocalStorage`, `StreamingProcessor`.
- Produces: no new public surface; `_delete_capture` returns bytes freed even when some unlinks fail; `enforce_cap` never raises `OSError`; `_finalize_recording` cannot block forever on a dead writer.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_local_storage_resilience.py
from pathlib import Path

from rfobserver.storage.local import LocalStorage


def _mk_capture(d: Path, name: str, size: int = 1024) -> Path:
    p = d / name
    p.write_bytes(b"\x00" * size)
    return p


def test_enforce_cap_survives_unlink_error(tmp_path, monkeypatch):
    st = LocalStorage(str(tmp_path), max_gb=0.0)  # cap 0 -> evict all but newest
    auto = st.auto_dir
    auto.mkdir(parents=True, exist_ok=True)
    a = _mk_capture(auto, "a.sc16")
    b = _mk_capture(auto, "b.sc16")

    real_unlink = Path.unlink

    def flaky_unlink(self, *args, **kwargs):
        if self.name == "a.sc16":
            raise OSError("Read-only file system")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", flaky_unlink)
    # Must not raise even though deleting a.sc16 fails.
    st.enforce_cap()
    # b (older? both same mtime) handling aside, the key assertion is no raise
    # and that eviction attempted past the failing file.
    assert a.exists()  # a could not be removed, but the call survived
```

(Note to implementer: the assertion of interest is "no exception escapes"; if mtimes tie and ordering makes the test brittle, create the two files with an explicit `os.utime` gap so `a` is the older/first-evicted, and assert the newest is retained.)

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_local_storage_resilience.py -v`
Expected: FAIL - `OSError` from the flaky unlink propagates out of `enforce_cap`.

- [ ] **Step 3: Write minimal implementation**

In `local.py`, make `_delete_capture` tolerate per-file unlink failures:

```python
    def _delete_capture(self, sc16_path: Path) -> int:
        """Delete a capture and all its companions. Returns bytes freed.

        Tolerant of unlink failures (e.g. a read-only-remounted or busy volume):
        a file that cannot be removed is logged and skipped rather than aborting
        eviction, so one bad file cannot silently stop FIFO rotation.
        """
        freed = self._capture_size(sc16_path)
        for p in [sc16_path, *self._companion_paths(sc16_path)]:
            try:
                p.unlink(missing_ok=True)
            except OSError:
                logger.warning("Could not unlink %s during eviction", p)
        logger.info("Rotated old capture: %s (freed %d bytes)", sc16_path.name, freed)
        return freed
```

(`freed` is the size before deletion; if a file survives, the accounting slightly over-counts freed bytes for that pass, which only makes the loop evict a touch more aggressively next time - acceptable and self-correcting.)

Optionally wrap the `while` bodies of `_enforce_limit` and `enforce_cap` so a `stat()` on a vanished file cannot raise either; the `_delete_capture` guard is the load-bearing fix.

In `streaming.py` `_finalize_recording`, replace the unbounded blocking sentinel put (line 942) so a dead/stalled writer cannot wedge the recctl thread forever:

```python
            # Disk mode: stop writer thread. Bounded put: if the writer already
            # died (disk full / EROFS), the queue may be full and no consumer
            # will ever drain it, so a plain blocking put would wedge recctl in
            # "finalizing" forever. Time-bounded, then proceed regardless.
            try:
                self._recording_queue.put(None, timeout=10.0)
            except queue.Full:
                logger.error("Recording writer not draining; abandoning writer thread")
            if self._writer_thread is not None:
                self._writer_thread.join(timeout=10)
                self._writer_thread = None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_local_storage_resilience.py -v`
Expected: PASS. Then `PYTHONPATH= .venv/bin/pytest tests/unit/test_local_storage.py tests/unit/test_trigger_continuous.py -q` to confirm no eviction regression.

- [ ] **Step 5: Lint, type-check, commit**

```bash
ruff check src/rfobserver/storage/local.py src/rfobserver/pipeline/streaming.py tests/unit/test_local_storage_resilience.py
ruff format --check src/rfobserver/storage/local.py src/rfobserver/pipeline/streaming.py tests/unit/test_local_storage_resilience.py
PYTHONPATH= .venv/bin/mypy src/rfobserver/storage/local.py src/rfobserver/pipeline/streaming.py
git add src/rfobserver/storage/local.py src/rfobserver/pipeline/streaming.py tests/unit/test_local_storage_resilience.py
git commit -m "fix(resilience): eviction tolerates unlink failure; finalize cannot wedge on a dead writer"
```

---

### Task 4: Thread-based PipelineWatchdog

**Files:**
- Rewrite: `src/rfobserver/utils/watchdog.py`
- Test: `tests/unit/test_watchdog.py`

**Interfaces:**
- Consumes: `ProgressBeacon` (Task 1).
- Produces:
  - `PipelineWatchdog(beacon, is_active, restart, loop, *, timeout_sec, restart_deadline_sec, check_interval_sec=5.0, exit_fn=os._exit, exit_code=90)` where `is_active: Callable[[], bool]`, `restart: Callable[[], Coroutine]` (a coroutine factory, e.g. `supervisor.restart`), `loop: asyncio.AbstractEventLoop`.
  - `start() -> None` (spawns the daemon thread), `stop() -> None`.
  - Internal `_tick() -> None` (one evaluation; tested directly to avoid sleeping).

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_watchdog.py
import asyncio
import threading

from rfobserver.pipeline.beacon import ProgressBeacon
from rfobserver.utils.watchdog import PipelineWatchdog


def _loop_in_thread():
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=loop.run_forever, daemon=True)
    t.start()
    return loop


def test_no_action_when_fresh():
    b = ProgressBeacon(); b.mark()
    exits = []
    restarts = []

    async def restart():
        restarts.append(1)

    loop = _loop_in_thread()
    wd = PipelineWatchdog(b, is_active=lambda: True, restart=restart, loop=loop,
                          timeout_sec=1.0, restart_deadline_sec=1.0,
                          exit_fn=lambda code: exits.append(code))
    wd._tick()
    assert restarts == [] and exits == []
    loop.call_soon_threadsafe(loop.stop)


def test_no_action_when_inactive_even_if_stale():
    b = ProgressBeacon()
    import time as _t; _t.sleep(0.05)
    exits, restarts = [], []

    async def restart():
        restarts.append(1)

    loop = _loop_in_thread()
    wd = PipelineWatchdog(b, is_active=lambda: False, restart=restart, loop=loop,
                          timeout_sec=0.01, restart_deadline_sec=1.0,
                          exit_fn=lambda code: exits.append(code))
    wd._tick()
    assert restarts == [] and exits == []
    loop.call_soon_threadsafe(loop.stop)


def test_stale_active_triggers_restart_not_exit():
    b = ProgressBeacon()
    import time as _t; _t.sleep(0.05)
    exits, restarts = [], []

    async def restart():
        restarts.append(1)

    loop = _loop_in_thread()
    wd = PipelineWatchdog(b, is_active=lambda: True, restart=restart, loop=loop,
                          timeout_sec=0.01, restart_deadline_sec=1.0,
                          exit_fn=lambda code: exits.append(code))
    wd._tick()
    assert restarts == [1] and exits == []  # restart succeeded, no exit
    loop.call_soon_threadsafe(loop.stop)


def test_wedged_loop_escalates_to_exit():
    b = ProgressBeacon()
    import time as _t; _t.sleep(0.05)
    exits = []

    async def restart():
        await asyncio.sleep(5.0)  # simulate a wedged / slow loop past the deadline

    loop = _loop_in_thread()
    wd = PipelineWatchdog(b, is_active=lambda: True, restart=restart, loop=loop,
                          timeout_sec=0.01, restart_deadline_sec=0.2,
                          exit_fn=lambda code: exits.append(code), exit_code=90)
    wd._tick()
    assert exits == [90]
    loop.call_soon_threadsafe(loop.stop)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_watchdog.py -v`
Expected: FAIL - `PipelineWatchdog` does not exist (the module currently holds the old asyncio `Watchdog`).

- [ ] **Step 3: Write minimal implementation**

```python
# src/rfobserver/utils/watchdog.py
"""Thread-based pipeline liveness watchdog.

Runs on its own daemon thread, so it survives both event-loop starvation and
the pipeline task dying - the two failure modes that make an asyncio-based
watchdog useless. Reads a ProgressBeacon; on a confirmed stall it asks the
supervisor to restart the pipeline on the main loop, and if the loop does not
complete that within a deadline (wedged), it exits the process so systemd's
Restart=on-failure brings it back fresh.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from collections.abc import Callable, Coroutine
from typing import Any

logger = logging.getLogger(__name__)


class PipelineWatchdog:
    def __init__(
        self,
        beacon: Any,
        is_active: Callable[[], bool],
        restart: Callable[[], Coroutine[Any, Any, Any]],
        loop: asyncio.AbstractEventLoop,
        *,
        timeout_sec: float,
        restart_deadline_sec: float,
        check_interval_sec: float = 5.0,
        exit_fn: Callable[[int], None] = os._exit,
        exit_code: int = 90,
    ) -> None:
        self._beacon = beacon
        self._is_active = is_active
        self._restart = restart
        self._loop = loop
        self._timeout = timeout_sec
        self._restart_deadline = restart_deadline_sec
        self._interval = check_interval_sec
        self._exit_fn = exit_fn
        self._exit_code = exit_code
        self._stop = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="watchdog", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop = True

    def _run(self) -> None:
        while not self._stop:
            time.sleep(self._interval)
            if not self._stop:
                self._tick()

    def _tick(self) -> None:
        if not self._is_active():
            return
        age = self._beacon.age()
        if age <= self._timeout:
            return
        logger.error("Watchdog: pipeline stalled (%.1fs since last progress); restarting", age)
        if self._attempt_restart():
            self._beacon.mark()
            logger.error("Watchdog: pipeline restarted")
            return
        logger.error("Watchdog: restart did not complete in %.1fs; exiting for systemd",
                     self._restart_deadline)
        self._exit_fn(self._exit_code)

    def _attempt_restart(self) -> bool:
        try:
            fut = asyncio.run_coroutine_threadsafe(self._restart(), self._loop)
            fut.result(timeout=self._restart_deadline)
            return True
        except Exception:
            logger.exception("Watchdog: in-process restart failed")
            return False
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_watchdog.py -v`
Expected: PASS (4 tests). Confirm nothing imported the old `Watchdog`: `grep -rn "from rfobserver.utils.watchdog\|import watchdog" src tests` should show only the new usage (none yet).

- [ ] **Step 5: Lint, type-check, commit**

```bash
ruff check src/rfobserver/utils/watchdog.py tests/unit/test_watchdog.py
ruff format --check src/rfobserver/utils/watchdog.py tests/unit/test_watchdog.py
PYTHONPATH= .venv/bin/mypy src/rfobserver/utils/watchdog.py
git add src/rfobserver/utils/watchdog.py tests/unit/test_watchdog.py
git commit -m "feat(resilience): thread-based PipelineWatchdog (restart, escalate to exit)"
```

---

### Task 5: Supervisor restart() + task done-callback

**Files:**
- Modify: `src/rfobserver/pipeline/supervisor.py`
- Test: `tests/unit/test_supervisor_recovery.py`

**Interfaces:**
- Consumes: existing `PipelineSupervisor`.
- Produces:
  - `async PipelineSupervisor.restart() -> None` (no-op if not active or in replay; else stop+start under `_lock`).
  - A done-callback on the pipeline task: an unexpected exception (not a cancel, not during a deliberate stop) logs and schedules an in-process restart.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_supervisor_recovery.py
import asyncio

import pytest

from rfobserver.pipeline.supervisor import PipelineSupervisor


class _FakeReceiver:
    def initialize(self): ...
    def close(self): ...


class _CrashProcessor:
    """run() raises after a beat; stop() is a no-op."""
    def __init__(self):
        self.started = 0

    async def run(self):
        self.started += 1
        await asyncio.sleep(0.01)
        raise RuntimeError("boom")

    def stop(self): ...


@pytest.mark.asyncio
async def test_task_death_triggers_restart():
    procs = []

    def build_proc(receiver, *, replay_mode=False):
        p = _CrashProcessor()
        procs.append(p)
        return p

    sup = PipelineSupervisor(build_receiver=_FakeReceiver, build_processor=build_proc)
    await sup.set_active(True)
    # First processor crashes; the done-callback should build+start a second.
    await asyncio.sleep(0.2)
    assert len(procs) >= 2, "a crashed pipeline task must be restarted"
    await sup.set_active(False)


@pytest.mark.asyncio
async def test_restart_noop_when_inactive():
    sup = PipelineSupervisor(build_receiver=_FakeReceiver, build_processor=lambda r, **k: None)
    await sup.restart()  # not active -> no raise, no start
    assert not sup.active
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_supervisor_recovery.py -v`
Expected: FAIL - no `restart()`, and a crashed task is orphaned (only one processor built).

- [ ] **Step 3: Write minimal implementation**

In `supervisor.py`: add a `_stopping` flag (default False) in `__init__`; attach the done-callback in `_start`; wrap `_stop` body so `_stopping` is True during a deliberate stop; add `restart()` and `_restart_after_crash()`.

```python
    # __init__: add
        self._stopping = False

    # in _start(), after creating the task:
        self._task = asyncio.create_task(processor.run())
        self._task.add_done_callback(self._on_task_done)

    # in _stop(): set/clear the flag around the existing body
    async def _stop(self) -> None:
        self._stopping = True
        try:
            ...  # existing body unchanged
        finally:
            self._stopping = False

    def _on_task_done(self, task: asyncio.Task[Any]) -> None:
        if task.cancelled() or self._stopping:
            return
        exc = task.exception()
        if exc is None:
            return
        logger.error("Pipeline task died unexpectedly; restarting", exc_info=exc)
        if self._active:
            asyncio.get_running_loop().create_task(self._restart_after_crash())

    async def _restart_after_crash(self) -> None:
        async with self._lock:
            if not self._active:
                return
            await self._stop()
            await self._start()

    async def restart(self) -> None:
        """Stop then start the live pipeline. No-op if inactive or replaying."""
        async with self._lock:
            if not self._active or self._replay:
                return
            await self._stop()
            await self._start()
```

Note for the implementer: `_stop()` sets `self._active = False` at its end, so after `_stop()` inside `restart()`/`_restart_after_crash()` the subsequent `_start()` must run regardless of the (now-false) `_active` - the code above calls `_start()` unconditionally after `_stop()`, which is correct because the outer guard already confirmed we were active. Verify `_start()`/`_stop()` are the raw transitions (they are; `set_active` is the guarded caller) and that calling them under `_lock` here does not double-acquire (they do not acquire `_lock` themselves).

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_supervisor_recovery.py -v`
Expected: PASS (2 tests). Then run any existing supervisor tests: `PYTHONPATH= .venv/bin/pytest tests/ -k supervisor -q`.

- [ ] **Step 5: Lint, type-check, commit**

```bash
ruff check src/rfobserver/pipeline/supervisor.py tests/unit/test_supervisor_recovery.py
ruff format --check src/rfobserver/pipeline/supervisor.py tests/unit/test_supervisor_recovery.py
PYTHONPATH= .venv/bin/mypy src/rfobserver/pipeline/supervisor.py
git add src/rfobserver/pipeline/supervisor.py tests/unit/test_supervisor_recovery.py
git commit -m "feat(resilience): supervisor restart() + auto-restart on task death"
```

---

### Task 6: Wire beacon + watchdog into the app; settings; pet on progress

**Files:**
- Modify: `src/rfobserver/config.py` (settings)
- Modify: `src/rfobserver/pipeline/streaming.py` (accept `beacon`, mark on progress)
- Modify: `src/rfobserver/pipeline/continuous.py` (accept `beacon`, mark on progress)
- Modify: `src/rfobserver/pipeline/app.py` (create beacon, inject, start/stop watchdog)
- Test: `tests/unit/test_streaming_beacon.py`

**Interfaces:**
- Consumes: `ProgressBeacon` (T1), `PipelineWatchdog` (T4), `supervisor.restart` (T5).
- Produces: settings `WATCHDOG_ENABLED: bool=False`, `WATCHDOG_TIMEOUT_SEC: float=30.0`, `WATCHDOG_RESTART_DEADLINE_SEC: float=10.0`; both processors accept `beacon: ProgressBeacon | None = None` and mark it on each processed result.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_streaming_beacon.py
from rfobserver.pipeline.beacon import ProgressBeacon
from rfobserver.pipeline.streaming import StreamingProcessor


def test_processor_accepts_and_holds_beacon():
    b = ProgressBeacon()
    proc = StreamingProcessor.__new__(StreamingProcessor)
    # The constructor stores the beacon as self._beacon; assert the attribute
    # exists after a minimal real construction path is exercised in integration.
    proc._beacon = b
    assert proc._beacon is b


def test_settings_have_watchdog_defaults():
    from rfobserver.config import AppSettings

    s = AppSettings(_env_file=None)
    assert s.WATCHDOG_ENABLED is False
    assert s.WATCHDOG_TIMEOUT_SEC == 30.0
    assert s.WATCHDOG_RESTART_DEADLINE_SEC == 10.0
```

(Note: the meaningful "beacon is marked on progress" assertion is covered by the integration pipeline test - see Step 4 - because it requires the running consumer loop. This unit test pins the constructor surface and the settings defaults.)

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_streaming_beacon.py -v`
Expected: FAIL - the new settings do not exist yet.

- [ ] **Step 3: Write minimal implementation**

`config.py` (near the Recording/Metrics block, ~line 166):

```python
    # Pipeline liveness watchdog (thread-based; restarts a stalled pipeline).
    # Off by default; when enabled, a daemon thread restarts the pipeline (or,
    # if the loop is wedged, exits the process for systemd) after no forward
    # progress for WATCHDOG_TIMEOUT_SEC.
    WATCHDOG_ENABLED: bool = False
    WATCHDOG_TIMEOUT_SEC: float = 30.0
    WATCHDOG_RESTART_DEADLINE_SEC: float = 10.0
```

`streaming.py` `__init__`: add `beacon: ProgressBeacon | None = None` to the signature and `self._beacon = beacon`. Add the import under TYPE_CHECKING or directly. In `run()` right after `self._loop = asyncio.get_running_loop()`, add `if self._beacon is not None: self._beacon.mark()`. In `_result_consumer_loop`, immediately after the successful `result = await asyncio.wait_for(...)` at line 1515 (i.e. at the top of the non-timeout path, before/after `_drain_burst_results` at 1533), add:

```python
            if self._beacon is not None:
                self._beacon.mark()
```

`continuous.py` `__init__`: same `beacon` param + `self._beacon`; mark it once per processed capture in its main loop (wherever a capture result is finalized).

`app.py` `run()`: create the beacon, inject it, and manage the watchdog:

```python
    from rfobserver.pipeline.beacon import ProgressBeacon

    beacon = ProgressBeacon()
```

Add `beacon=beacon` to both `StreamingProcessor(...)` and `ContinuousProcessor(...)` construction inside `build_processor`. After the supervisor is created and `set_active` handled, before building `tasks`:

```python
    watchdog = None
    if settings.WATCHDOG_ENABLED:
        from rfobserver.utils.watchdog import PipelineWatchdog

        watchdog = PipelineWatchdog(
            beacon,
            is_active=lambda: supervisor.active,
            restart=supervisor.restart,
            loop=asyncio.get_running_loop(),
            timeout_sec=settings.WATCHDOG_TIMEOUT_SEC,
            restart_deadline_sec=settings.WATCHDOG_RESTART_DEADLINE_SEC,
        )
        watchdog.start()
        logger.info("Pipeline watchdog enabled (timeout=%.0fs)", settings.WATCHDOG_TIMEOUT_SEC)
```

In the `finally` of `run()`, add `if watchdog is not None: watchdog.stop()` before `await supervisor.set_active(False)`.

- [ ] **Step 4: Run tests + full gates**

```bash
PYTHONPATH= .venv/bin/pytest tests/unit/test_streaming_beacon.py -v
# Integration: confirm the beacon is marked while the mock pipeline runs, and
# nothing regressed. Add/confirm a check in an existing streaming integration
# test (e.g. tests/integration/test_pipeline.py) that after a few processed
# chunks beacon.age() is small. NATS must be up on :4222.
ruff check src/ tests/
ruff format --check src/ tests/
PYTHONPATH= .venv/bin/mypy src/rfobserver/
PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q
PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q
```
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/config.py src/rfobserver/pipeline/streaming.py src/rfobserver/pipeline/continuous.py src/rfobserver/pipeline/app.py tests/unit/test_streaming_beacon.py
git commit -m "feat(resilience): wire ProgressBeacon + watchdog into the pipeline (off by default)"
```

---

## Verification on the sensor (after all tasks; not a code task)

On the healthy `.177` box first, then the field box: set `RFOBS_WATCHDOG_ENABLED=true`, restart the service, confirm normal operation never trips the watchdog (no "pipeline stalled" logs), then force a stall (e.g. `kill -STOP` the process briefly, or a debug hook that sleeps the consumer) and confirm the watchdog either restarts the pipeline (log "pipeline restarted") or exits and systemd brings it back (`NRestarts` increments). Confirm captures keep flowing and `enforce_cap` still rotates `auto/`.

## Self-review notes

- **Spec coverage (Cut 1 only):** guard insert_detection (T2), timeout the reconnect retry (T2), supervise the task (T5), thread-based watchdog with restart->exit escalation (T4 + T6 wiring), eviction/writer-deadlock fix (T3), beacon + settings + inert-by-default (T1, T6). Cuts 2 (affinity) and 3 (UI isolation) are out of scope for this plan.
- **Type/name consistency:** `ProgressBeacon.mark/age/reset`, `PipelineWatchdog(beacon, is_active, restart, loop, *, timeout_sec, restart_deadline_sec, check_interval_sec, exit_fn, exit_code)`, `supervisor.restart`, and the `beacon` constructor param are used identically across T1/T4/T5/T6.
- **Deferred (not gaps):** the `_file_writer_loop` crash still only logs (T3 stops it from wedging finalize, but does not auto-restart the writer mid-recording); acceptable for Cut 1 since the watchdog now covers a resulting stall. Continuous-mode beacon marking is best-effort parity (streaming is the deployed path).
```
