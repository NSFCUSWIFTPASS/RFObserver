# Stall Safety Net Hardening (Branch A) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix issues 2, 3, 7 and 10 from the Cut 1 hardware validation on the
`feat/pipeline-stall-cut1-safety-net` branch, then merge the branch into local
`main`.

**Architecture:**

- Align the tooling with the Python 3.10 the Jetsons run.
- Make the supervisor's stop-timeout path log correctly on 3.10.
- Give watchdog-driven restarts a short stop timeout, so a cancellable hang
  restarts in-process instead of exiting.
- Bound the thread-to-loop result handoff, so a wedged loop cannot grow memory.
- Make crash give-up visible in `/api/health` and exit with code 91, so systemd
  starts a fresh process.

**Tech Stack:** Python 3.10 to 3.12, asyncio, FastAPI, pytest and
pytest-asyncio, ruff, mypy, GitHub Actions and hatch.

**Spec:** `docs/debugging/2026-09-14_stall-safety-net-hardware-validation.md`
(the "Issues to fix" table, plus its CORRECTION section). Design choices were
confirmed by the user on 2026-09-14:

- issue 3: shorter stop inside restart
- issue 10: surface in health, and exit
- issue 2: align ruff, mypy and CI with 3.10

## Global Constraints

- Code must run on Python >= 3.10. The Jetsons run 3.10.12. No 3.11+ syntax or
  APIs.
- For asyncio timeouts use `except (TimeoutError, asyncio.TimeoutError):  # noqa: UP041`,
  the codebase's existing pattern.
- Always prefix commands with `PYTHONPATH=`, because the host leaks 3.10 system
  packages.
- The local 3.10 test venv is at
  `/tmp/claude-1000/-home-orencollaco-GitHub-RFObserver/50689e78-60fa-453e-89bd-c3c811638a7b/scratchpad/venv310`
  (editable install of the repo). Below it is called `$V310`.
- No em-dashes and no emojis anywhere, in code, logs, docs or commit messages.
- Never `git add -A` or `git add .`: stage explicit paths only. Do NOT stage
  anything under `docs/`, which the user wants left uncommitted.
- No Claude co-author line in commits.
- Before the merge, run every CI check from `CLAUDE.md`: ruff check, ruff
  format --check, mypy, the unit tests, and the integration tests (throwaway
  NATS: `docker run -d --rm --name rfobs-test-nats -p 4222:4222 nats:2.10-alpine -js`).

---

### Task 0: Branch setup

- [ ] **Step 1:** Create the local branch from the remote tip, which equals
  local `main` at 81aa9e1.

```bash
git fetch origin
git checkout -b feat/pipeline-stall-cut1-safety-net origin/feat/pipeline-stall-cut1-safety-net
git log -1 --format=%h   # expect 81aa9e1
```

The uncommitted docs (`docs/debugging/...`, `docs/superpowers/plans/...`)
carry over into the working tree. Leave them unstaged.

### Task 1: Align tooling with Python 3.10 (issue 2 root cause)

**Files:**
- Modify: `pyproject.toml` (`[tool.ruff] target-version`, `[tool.mypy] python_version`, `[[tool.hatch.envs.test.matrix]] python`)
- Modify: `.github/workflows/ci.yml` (test-unit matrix)

**Interfaces:** none.

- [ ] **Step 1: Edit `pyproject.toml`**

```toml
[[tool.hatch.envs.test.matrix]]
python = ["3.10", "3.11", "3.12"]

[tool.ruff]
target-version = "py310"
```

```toml
[tool.mypy]
python_version = "3.10"
```

Keep the existing `UP017` ignore. It becomes redundant but is harmless.

- [ ] **Step 2: Edit `.github/workflows/ci.yml` test-unit matrix**

```yaml
      matrix:
        python-version: ["3.10", "3.11", "3.12"]
```

- [ ] **Step 3: Verify tooling is clean under the new targets**

Run: `ruff check src/ tests/ && ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/`
Expected: "All checks passed!", "125 files already formatted", "Success: no issues found". Pre-checked: all three pass with the py310 targets.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml .github/workflows/ci.yml
git commit -m "build: target Python 3.10 in ruff, mypy and the CI test matrix

The Jetson deployments run 3.10.12, but ruff targeted py311 (so UP041
rewrites asyncio.TimeoutError to the builtin, which is a different class on
3.10) and CI only tested 3.11/3.12, so a 3.10-only bug in supervisor._stop()
could not be caught."
```

### Task 2: Supervisor stop-timeout path correct on 3.10 (issue 2)

**Files:**
- Modify: `src/rfobserver/pipeline/supervisor.py:137` (the `except TimeoutError:` in `_stop`)
- Test: `tests/unit/test_supervisor_recovery.py`

**Interfaces:**
- Produces: the `_HungProcessor` test helper, which Task 3 reuses.

- [ ] **Step 1: Write the failing test.** Append to `tests/unit/test_supervisor_recovery.py`, adding `import logging` at the top.

```python
class _HungProcessor:
    """run() ignores stop() and only ends when cancelled."""

    def __init__(self) -> None:
        self.cancelled = False

    async def run(self) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise

    def stop(self) -> None: ...


@pytest.mark.asyncio
async def test_stop_timeout_warns_and_cancels(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A processor that ignores stop() must be cancelled AND reported as a timeout.

    On Python 3.10 asyncio.wait_for raises asyncio.TimeoutError, which is not
    the builtin TimeoutError, so a bare `except TimeoutError` misses it and the
    timeout was mislabelled as an already-raised task exception at DEBUG.
    """
    monkeypatch.setattr(supervisor_mod, "_STOP_TIMEOUT_SEC", 0.1)
    procs: list[_HungProcessor] = []

    def build_proc(receiver: object, *, replay_mode: bool = False) -> _HungProcessor:
        p = _HungProcessor()
        procs.append(p)
        return p

    sup = PipelineSupervisor(build_receiver=_FakeReceiver, build_processor=build_proc)
    await sup.set_active(True)
    with caplog.at_level(logging.WARNING, logger="rfobserver.pipeline.supervisor"):
        await asyncio.wait_for(sup.set_active(False), timeout=2.0)

    assert procs[0].cancelled, "the hung task must be cancelled"
    assert "did not stop in time" in caplog.text, "the timeout must be reported as one"
    assert not sup.active
```

- [ ] **Step 2: Run the test on 3.10 and verify it fails**

Run: `PYTHONPATH= $V310/bin/pytest tests/unit/test_supervisor_recovery.py::test_stop_timeout_warns_and_cancels -v -p no:cacheprovider`
Expected: FAIL on the `"did not stop in time"` assertion. `cancelled` is already True because `wait_for` cancels.

- [ ] **Step 3: Minimal fix** in `supervisor.py` `_stop`:

```python
                except (TimeoutError, asyncio.TimeoutError):  # noqa: UP041 - not the builtin on 3.10
                    logger.warning("Processor did not stop in time; cancelling")
```

- [ ] **Step 4: Verify it passes on 3.10 and 3.11**

Run: `PYTHONPATH= $V310/bin/pytest tests/unit/test_supervisor_recovery.py -q -p no:cacheprovider && PYTHONPATH= .venv/bin/pytest tests/unit/test_supervisor_recovery.py -q -p no:cacheprovider`
Expected: all pass on both.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/pipeline/supervisor.py tests/unit/test_supervisor_recovery.py
git commit -m "fix(supervisor): catch asyncio.TimeoutError on the stop-timeout path

On Python 3.10 asyncio.wait_for raises asyncio.TimeoutError, which the bare
except TimeoutError missed; the broad except then logged a stop timeout at
DEBUG as an already-raised task exception and the warning never fired.
Verified on nano-super (3.10.12). wait_for already cancels the task, so the
behavior was right; the report was wrong."
```

### Task 3: Watchdog restart uses a short stop timeout (issue 3)

**Files:**
- Modify: `src/rfobserver/pipeline/supervisor.py` (`_stop`, `restart`)
- Modify: `src/rfobserver/config.py` (add `WATCHDOG_STOP_TIMEOUT_SEC`)
- Modify: `src/rfobserver/pipeline/app.py:134-141` (watchdog `restart=`)
- Test: `tests/unit/test_supervisor_recovery.py`

**Interfaces:**
- Consumes: `_HungProcessor` (Task 2).
- Produces:
  - `PipelineSupervisor.restart(self, stop_timeout: float | None = None) -> None`
  - `PipelineSupervisor._stop(self, timeout: float | None = None) -> None`
  - `AppSettings.WATCHDOG_STOP_TIMEOUT_SEC: float = 5.0`

- [ ] **Step 1: Write the failing test**

```python
@pytest.mark.asyncio
async def test_restart_with_short_stop_timeout_replaces_hung_processor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A watchdog restart must not wait the full manual-stop timeout on a hang.

    The watchdog gives restart() a 10 s deadline; the manual stop timeout is
    15 s, so without a shorter stop timeout a hung-but-cancellable task always
    escalated to process exit.
    """
    procs: list[_HungProcessor] = []

    def build_proc(receiver: object, *, replay_mode: bool = False) -> _HungProcessor:
        p = _HungProcessor()
        procs.append(p)
        return p

    sup = PipelineSupervisor(build_receiver=_FakeReceiver, build_processor=build_proc)
    await sup.set_active(True)
    # Runs with the real 15 s _STOP_TIMEOUT_SEC: finishing within 2 s proves
    # stop_timeout is honoured.
    await asyncio.wait_for(sup.restart(stop_timeout=0.1), timeout=2.0)

    assert procs[0].cancelled, "the hung processor must be cancelled"
    assert len(procs) == 2 and sup.active, "a fresh processor must be running"

    # Only for teardown speed: the second processor is hung too.
    monkeypatch.setattr(supervisor_mod, "_STOP_TIMEOUT_SEC", 0.1)
    await asyncio.wait_for(sup.set_active(False), timeout=2.0)
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_supervisor_recovery.py::test_restart_with_short_stop_timeout_replaces_hung_processor -v -p no:cacheprovider`
Expected: FAIL with `TypeError: restart() got an unexpected keyword argument 'stop_timeout'`.

- [ ] **Step 3: Implement**

`supervisor.py`:

```python
    async def _stop(self, timeout: float | None = None) -> None:
        stop_timeout = _STOP_TIMEOUT_SEC if timeout is None else timeout
        ...
                    await asyncio.wait_for(task, timeout=stop_timeout)
```

```python
    async def restart(self, stop_timeout: float | None = None) -> None:
        """Stop then start the live pipeline. No-op if inactive or replaying.

        ``stop_timeout`` bounds how long to wait for the old task before
        cancelling it; the watchdog passes a value well inside its restart
        deadline so a hung-but-cancellable pipeline restarts in-process.
        """
        async with self._lock:
            if not self._active or self._replay:
                return
            await self._stop(timeout=stop_timeout)
            await self._start()
```

`config.py`, directly under `WATCHDOG_RESTART_DEADLINE_SEC`:

```python
    # How long a watchdog-driven restart waits for the stalled task before
    # cancelling it. Must leave room inside WATCHDOG_RESTART_DEADLINE_SEC for
    # teardown + SDR re-init (~2.3 s on a B200mini) or the restart escalates to exit.
    WATCHDOG_STOP_TIMEOUT_SEC: float = 5.0
```

`app.py`, in the `PipelineWatchdog(...)` call:

```python
            restart=lambda: supervisor.restart(stop_timeout=settings.WATCHDOG_STOP_TIMEOUT_SEC),
```

- [ ] **Step 4: Run the supervisor, watchdog and config tests**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_supervisor_recovery.py tests/unit/test_supervisor.py tests/unit/test_watchdog.py -q -p no:cacheprovider`, then the same with `$V310`.
Expected: all pass. Then `PYTHONPATH= .venv/bin/mypy src/rfobserver/` must be clean.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/pipeline/supervisor.py src/rfobserver/pipeline/app.py src/rfobserver/config.py tests/unit/test_supervisor_recovery.py
git commit -m "fix(watchdog): restart with a short stop timeout so a hang restarts in-process

The watchdog's 10 s restart deadline was shorter than the supervisor's 15 s
stop timeout, so any stall that did not honour stop() always escalated to
exit 90. Watchdog restarts now stop with WATCHDOG_STOP_TIMEOUT_SEC (5 s),
leaving room for teardown and SDR re-init; manual stops keep 15 s."
```

### Task 4: Bounded thread-to-loop result handoff (issue 7)

**Files:**
- Modify: `src/rfobserver/pipeline/streaming.py` (new `_LoopHandoff` next to `_put_nowait_drop_full` at line 148; construct handoffs after the queues at lines 260 and 337; use them at lines 1384 and 1449)
- Test: create `tests/unit/test_loop_handoff.py`

**Interfaces:**
- Produces: `streaming._LoopHandoff(q: asyncio.Queue[Any])`, with `.submit(loop, item) -> bool` and `.dropped: int`.

- [ ] **Step 1: Write the failing test** (`tests/unit/test_loop_handoff.py`)

```python
"""Thread-to-loop handoff must stay bounded while the event loop is blocked."""

import asyncio

from rfobserver.pipeline.streaming import _LoopHandoff


def test_handoff_bounded_while_loop_not_running() -> None:
    loop = asyncio.new_event_loop()
    try:
        q: asyncio.Queue[int] = asyncio.Queue(maxsize=4)
        h = _LoopHandoff(q)
        # Loop is not running: nothing drains, like a wedged loop.
        accepted = sum(h.submit(loop, i) for i in range(100))
        assert accepted == 4, "only maxsize callbacks may be in flight"
        assert h.dropped == 96

        loop.run_until_complete(asyncio.sleep(0))  # deliver pending callbacks
        assert q.qsize() == 4

        while not q.empty():
            q.get_nowait()
        assert h.submit(loop, 999), "delivered callbacks must free their slots"
    finally:
        loop.close()


def test_handoff_after_loop_closed_is_dropped_not_raised() -> None:
    loop = asyncio.new_event_loop()
    loop.close()
    h = _LoopHandoff(asyncio.Queue(maxsize=2))
    assert h.submit(loop, 1) is False
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_loop_handoff.py -v -p no:cacheprovider`
Expected: FAIL with `ImportError: cannot import name '_LoopHandoff'`.

- [ ] **Step 3: Implement** in `streaming.py`, directly below `_put_nowait_drop_full`:

```python
class _LoopHandoff:
    """Thread-to-loop handoff into an asyncio.Queue that stays bounded.

    call_soon_threadsafe alone queues one loop callback per item; while the
    loop is blocked those callbacks, and the results they hold, pile up without
    limit because the queue's own bound only applies once a callback runs
    (measured: RSS +30 MB/s during a 60 s loop wedge on nano-super). This caps
    callbacks in flight at the queue's maxsize and drops at the producer
    beyond that, which is the same drop-on-overflow outcome as before.
    """

    def __init__(self, q: asyncio.Queue[Any]) -> None:
        self._q = q
        self._limit = max(1, q.maxsize)
        self._pending = 0
        self._lock = threading.Lock()
        self.dropped = 0

    def submit(self, loop: asyncio.AbstractEventLoop, item: Any) -> bool:
        with self._lock:
            if self._pending >= self._limit:
                self.dropped += 1
                return False
            self._pending += 1
        try:
            loop.call_soon_threadsafe(self._deliver, item)
        except RuntimeError:  # loop closed during shutdown
            with self._lock:
                self._pending -= 1
            return False
        return True

    def _deliver(self, item: Any) -> None:
        with self._lock:
            self._pending -= 1
        _put_nowait_drop_full(self._q, item)
```

In `__init__`, right after the `self._result_queue` assignment (line 260):

```python
        self._result_handoff = _LoopHandoff(self._result_queue)
```

Right after the `self._burst_result_queue` assignment (line 337 block):

```python
        self._burst_handoff = _LoopHandoff(self._burst_result_queue)
```

Line 1384:

```python
        if self._loop is not None:
            self._result_handoff.submit(self._loop, result)
```

Line 1449:

```python
                if completed_bursts and self._loop is not None:
                    self._burst_handoff.submit(self._loop, (completed_bursts, int(freq_hz)))
```

Leave the per-recording `call_soon_threadsafe` sites (lines 1010 and 1114)
unchanged: they fire once per recording, not per chunk.

- [ ] **Step 4: Run the handoff and streaming tests** (3.11 and 3.10)

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_loop_handoff.py tests/unit/test_streaming.py tests/unit/test_streaming_beacon.py tests/unit/test_streaming_replay_mode.py tests/unit/test_streaming_avg_window.py -q -p no:cacheprovider`, then the same with `$V310`.
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/pipeline/streaming.py tests/unit/test_loop_handoff.py
git commit -m "fix(streaming): bound the thread-to-loop result handoff

Per-chunk results and burst batches were handed to the loop with a bare
call_soon_threadsafe, so while the loop was wedged the callbacks (and the
results they held) queued without limit: RSS grew ~30 MB/s during a 60 s
wedge on nano-super. Cap callbacks in flight at the queue's maxsize and drop
at the producer beyond that."
```

### Task 5: Crash give-up is visible and exits for systemd (issue 10)

**Files:**
- Modify: `src/rfobserver/pipeline/supervisor.py` (`__init__`, `set_active`, `_restart_after_crash`, new properties)
- Modify: `src/rfobserver/config.py` (add `EXIT_ON_CRASH_GIVE_UP`)
- Modify: `src/rfobserver/pipeline/app.py` (`make_give_up_handler`, supervisor wiring, pass `beacon` to `_run_web_server`)
- Modify: `src/rfobserver/web/app.py:52-54` (`/api/health`)
- Test: `tests/unit/test_supervisor_recovery.py`, `tests/unit/test_web_routes.py`, create `tests/unit/test_give_up_handler.py`

**Interfaces:**
- Produces:
  - `PipelineSupervisor(..., on_give_up: Callable[[], None] | None = None)`
  - `PipelineSupervisor.gave_up -> bool` and `.consecutive_crashes -> int` (properties)
  - `rfobserver.pipeline.app.make_give_up_handler(enabled: bool, exit_fn: Callable[[int], object] = os._exit, delay_sec: float = 5.0) -> Callable[[], None]`
  - `/api/health` JSON gains `"pipeline": {"active", "gave_up", "consecutive_crashes", "beacon_age_sec"}` when a supervisor is attached; `"status"` becomes `"degraded"` when `gave_up`.
  - `AppSettings.EXIT_ON_CRASH_GIVE_UP: bool = True`

- [ ] **Step 1: Write the failing tests**

In `tests/unit/test_supervisor_recovery.py`:

```python
@pytest.mark.asyncio
async def test_give_up_sets_flag_and_calls_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(supervisor_mod, "_CRASH_BACKOFF_CAP_SEC", 0.0)
    calls: list[int] = []
    crash = {"on": True}

    def build_proc(
        receiver: object, *, replay_mode: bool = False
    ) -> _CrashProcessor | _LongRunningProcessor:
        return _CrashProcessor() if crash["on"] else _LongRunningProcessor()

    sup = PipelineSupervisor(
        build_receiver=_FakeReceiver,
        build_processor=build_proc,
        on_give_up=lambda: calls.append(1),
    )
    assert not sup.gave_up and sup.consecutive_crashes == 0
    await sup.set_active(True)
    await asyncio.sleep(1.0)

    assert sup.gave_up and not sup.active
    assert calls == [1], "the give-up hook must fire exactly once"

    # A deliberate re-activation (with a healthy processor) clears the state.
    crash["on"] = False
    await sup.set_active(True)
    assert not sup.gave_up and sup.consecutive_crashes == 0 and sup.active
    await sup.set_active(False)
```

Create `tests/unit/test_give_up_handler.py`:

```python
import asyncio

import pytest

from rfobserver.pipeline.app import make_give_up_handler


@pytest.mark.asyncio
async def test_give_up_handler_exits_91_after_delay() -> None:
    codes: list[int] = []
    handler = make_give_up_handler(True, exit_fn=codes.append, delay_sec=0.05)
    handler()
    assert codes == [], "exit must be delayed so health can report the give-up"
    await asyncio.sleep(0.15)
    assert codes == [91]


@pytest.mark.asyncio
async def test_give_up_handler_disabled_never_exits() -> None:
    codes: list[int] = []
    make_give_up_handler(False, exit_fn=codes.append, delay_sec=0.0)()
    await asyncio.sleep(0.05)
    assert codes == []
```

In `tests/unit/test_web_routes.py`:

```python
def test_health_without_supervisor_has_no_pipeline_block(client):
    data = client.get("/api/health").json()
    assert data["status"] == "ok"
    assert "pipeline" not in data


def test_health_reports_pipeline_and_degrades_on_give_up(settings):
    app = create_app(settings)
    sup = MagicMock(active=False, gave_up=True, consecutive_crashes=6)
    app.state.supervisor = sup
    app.state.beacon = None
    data = TestClient(app).get("/api/health").json()
    assert data["status"] == "degraded"
    assert data["pipeline"] == {
        "active": False,
        "gave_up": True,
        "consecutive_crashes": 6,
        "beacon_age_sec": None,
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_supervisor_recovery.py::test_give_up_sets_flag_and_calls_hook tests/unit/test_give_up_handler.py tests/unit/test_web_routes.py -q -p no:cacheprovider`
Expected: FAIL. The supervisor rejects the unexpected `on_give_up` keyword, `make_give_up_handler` cannot be imported, and health returns `"ok"` with no `pipeline` block.

- [ ] **Step 3: Implement**

`supervisor.py` `__init__` signature and fields:

```python
        on_processor_change: Callable[[Any | None], None] | None = None,
        on_give_up: Callable[[], None] | None = None,
    ) -> None:
        ...
        self._on_give_up = on_give_up
        self._gave_up = False
```

Properties, after `receiver`:

```python
    @property
    def gave_up(self) -> bool:
        """True once crash auto-restart gave up; cleared by a manual activation."""
        return self._gave_up

    @property
    def consecutive_crashes(self) -> int:
        return self._consecutive_crashes
```

In `set_active`, the activation branch:

```python
            if active and not self._active:
                # A deliberate manual activation clears any prior crash streak.
                self._consecutive_crashes = 0
                self._gave_up = False
                await self._start()
```

In `_restart_after_crash`, the give-up branch:

```python
            async with self._lock:
                if self._active and not self._replay:
                    await self._stop()
                    self._gave_up = True
            if self._gave_up and self._on_give_up is not None:
                self._on_give_up()
            return
```

`config.py`, below the watchdog settings:

```python
    # After crash auto-restart gives up, exit (code 91) a few seconds later so
    # systemd's Restart=on-failure starts a fresh process (fresh USB/SDR state)
    # instead of leaving a live process with the sensor silently inactive.
    EXIT_ON_CRASH_GIVE_UP: bool = True
```

`pipeline/app.py`, module level (add `import os` and
`from collections.abc import Callable` under `TYPE_CHECKING`):

```python
_GIVE_UP_EXIT_CODE = 91
_GIVE_UP_EXIT_DELAY_SEC = 5.0


def make_give_up_handler(
    enabled: bool,
    exit_fn: Callable[[int], object] = os._exit,
    delay_sec: float = _GIVE_UP_EXIT_DELAY_SEC,
) -> Callable[[], None]:
    """Build the supervisor's give-up hook: exit for systemd after a short delay.

    The delay lets /api/health report the give-up and the log flush first. The
    SDR is already released by the supervisor's stop before this runs.
    """

    def handler() -> None:
        if not enabled:
            return
        logger.error(
            "Pipeline auto-restart gave up; exiting (code %d) in %.0fs so systemd "
            "starts a fresh process",
            _GIVE_UP_EXIT_CODE,
            delay_sec,
        )
        asyncio.get_running_loop().call_later(delay_sec, exit_fn, _GIVE_UP_EXIT_CODE)

    return handler
```

Supervisor construction in `run()`:

```python
    supervisor = PipelineSupervisor(
        build_receiver=build_receiver,
        build_processor=build_processor,
        on_give_up=make_give_up_handler(settings.EXIT_ON_CRASH_GIVE_UP),
    )
```

Pass the beacon to the web server:
`tasks.append(_run_web_server(settings, supervisor, db, broadcast, beacon))`.
In `_run_web_server`, add the `beacon: ProgressBeacon` parameter (import under
`TYPE_CHECKING`) and set `app.state.beacon = beacon` next to
`app.state.supervisor = supervisor`.

`web/app.py` health (change the return type to `dict[str, Any]` and import
`Any`):

```python
    @app.get("/api/health")
    async def health() -> dict[str, Any]:
        body: dict[str, Any] = {"status": "ok", "version": __version__}
        sup = getattr(app.state, "supervisor", None)
        if sup is not None:
            beacon = getattr(app.state, "beacon", None)
            body["pipeline"] = {
                "active": sup.active,
                "gave_up": sup.gave_up,
                "consecutive_crashes": sup.consecutive_crashes,
                # Only meaningful while running; a stale age while active is a stall.
                "beacon_age_sec": round(beacon.age(), 1)
                if beacon is not None and sup.active
                else None,
            }
            if sup.gave_up:
                body["status"] = "degraded"
        return body
```

Confirm `create_app` does not set `app.state.supervisor`. If it does, the
no-supervisor test must set it to `None` explicitly; `getattr(..., None)` is
used for that reason.

- [ ] **Step 4: Run the tests on 3.11 and 3.10, then mypy**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/ -q -p no:cacheprovider && PYTHONPATH= $V310/bin/pytest tests/unit/ -q -p no:cacheprovider && PYTHONPATH= .venv/bin/mypy src/rfobserver/`
Expected: all pass, and mypy is clean.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/pipeline/supervisor.py src/rfobserver/pipeline/app.py src/rfobserver/web/app.py src/rfobserver/config.py tests/unit/test_supervisor_recovery.py tests/unit/test_give_up_handler.py tests/unit/test_web_routes.py
git commit -m "feat(resilience): surface crash give-up in /api/health and exit 91 for systemd

After six crashes in 120 s the supervisor gave up and the process stayed up
with the sensor silently inactive. Health now reports a pipeline block
(active, gave_up, consecutive_crashes, beacon_age_sec) and status degraded on
give-up, and the process exits with code 91 five seconds later so
Restart=on-failure starts a fresh process. EXIT_ON_CRASH_GIVE_UP=false keeps
the old behavior."
```

### Task 6: Full checks and hardware verification on nano-super

Not a code task. The harness is in
`docs/debugging/2026-09-14_stall-safety-net-hardware-validation/`.

- [ ] **Step 1:** Run the full CI set locally, including integration with a throwaway NATS. Also run the unit suite under `$V310`.
- [ ] **Step 2:** Deploy the branch tip to nano-super without pushing anything:

```bash
ssh ocollaco@192.168.97.153 'rm -rf ~/rfobs-stall && mkdir -p ~/rfobs-stall ~/rfobs-stalltest'
git archive HEAD | ssh ocollaco@192.168.97.153 'tar -x -C ~/rfobs-stall'
scp docs/debugging/2026-09-14_stall-safety-net-hardware-validation/*.{py,sh} ocollaco@192.168.97.153:rfobs-stalltest/
```

The harness runs the box's existing venv with `PYTHONPATH=~/rfobs-stall/src`
(`start.sh`). Re-seed the DB only if a Dashboard test is needed; branch A does
not need one.
- [ ] **Step 3:** Re-run the faults and check the expected new outcomes:
  - **F5** (`hang_once`, default deadline): now "Watchdog: pipeline restarted" in-process, with no exit 90 and `NRestarts` unchanged.
  - **F6-equivalent:** "Processor did not stop in time; cancelling" is logged at WARNING.
  - **F4** (`wedge_60`): still exits 90. RSS in `sampler.csv` stays near baseline (under 1.1 GB), not 1.6 GB.
  - **F7** (`crash_always`): give-up, then `/api/health` shows `"status":"degraded"`, then exit 91 about 5 s later and a systemd restart with `NRestarts` +1.
  - **F2** and **F1:** unchanged, still pass.
- [ ] **Step 4:** Clean up nano-super: stop the unit, remove the worktree and test data. Leave `~/GitHub/RFObserver` untouched.

### Task 7: Merge and record

- [ ] **Step 1:** Merge into local `main`; do not push.

```bash
git checkout main
git merge --no-ff feat/pipeline-stall-cut1-safety-net -m "Merge: stall safety net hardening (py3.10 tooling, short watchdog stop, bounded handoff, give-up exit)"
```

- [ ] **Step 2:** Update the Status of issues 2, 3, 7 and 10 in the validation doc with the commit SHAs and the hardware results. Leave the doc uncommitted, as the user asked.
