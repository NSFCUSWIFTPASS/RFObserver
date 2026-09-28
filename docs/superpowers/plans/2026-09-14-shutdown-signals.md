# Shutdown Signals Fix (Branch C) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make SIGTERM and SIGINT stop `rfobserver run` in order, on Python
3.10 and 3.11, with or without the web server. "In order" means the recording
is finalized, the SDR is released, the DBs are closed, and the exit is 0
within seconds.

**Architecture:**

- Today nothing in the app handles SIGTERM. uvicorn catches it, then restores
  `SIG_DFL` and re-raises it, so the process dies before `run()`'s `finally`
  (issue 4).
- On 3.10, SIGINT becomes a KeyboardInterrupt, and `asyncio.run` then cancels
  every task. `supervisor.set_active(False)` raises CancelledError, which skips
  `db.close()`. The non-daemon aiosqlite thread then hangs interpreter exit
  (issue 8).
- Fix:
  - `run()` owns both signals through `loop.add_signal_handler`. The first
    signal sets a stop event; a second forces `os._exit(128 + signum)`.
  - uvicorn runs as a `Server` subclass whose `capture_signals()` is a no-op.
    The web task sets `should_exit` when the stop event fires.
  - `run()` waits for the stop event, or for a worker task to fail. It then:
    - gives the web server up to 5 s to finish;
    - cancels the other loops;
    - stops the pipeline, ZMS and NATS, with each step's failure logged, not
      fatal;
    - closes both DBs;
    - removes the handlers;
    - returns normally.
- No KeyboardInterrupt is raised on a signal, so 3.10's cancel-all teardown is
  never reached.

**Tech Stack:** Python 3.10 to 3.12, asyncio (unix signal handlers), uvicorn
0.34 to 0.51, pytest and pytest-asyncio (`asyncio_mode = "auto"`).

**Spec:** `docs/debugging/2026-09-14_shutdown-signals.md`: the root cause, the
evidence, and the "Fix direction (branch C)" section. It is the binding
authority. Issues 4 and 8 in
`docs/debugging/2026-09-14_stall-safety-net-hardware-validation.md` give the
context.

## Global Constraints

- Code must run on Python >= 3.10 (the Jetsons run 3.10.12). For asyncio
  timeouts use `except (TimeoutError, asyncio.TimeoutError):  # noqa: UP041`.
- Always prefix commands with `PYTHONPATH=`. The 3.10 test venv is
  `$V310` =
  `/tmp/claude-1000/-home-orencollaco-GitHub-RFObserver/50689e78-60fa-453e-89bd-c3c811638a7b/scratchpad/venv310`.
  Run each task's tests on `.venv` (3.11) and on `$V310`.
- No em-dashes and no emojis anywhere.
- Stage explicit paths only. Never `git add -A` or `git add .`. Never stage
  `docs/` or `.superpowers/`.
- Commit messages carry no `Co-Authored-By:` or `Claude-Session:` trailers.
  Check with `git log -1 --format=%B` and amend them away if the environment
  appends them.
- Tests that send real signals to the test process must first assert that
  `run()` or `install_stop_signals` has installed its handlers. A SIGINT or
  SIGTERM with the default handler would kill or abort the pytest session.
- Before the merge, run the full CI set: ruff check, ruff format --check, mypy,
  the unit suite on 3.11 and 3.10, and the integration tests (throwaway NATS:
  `docker run -d --rm --name rfobs-test-nats -p 4222:4222 nats:2.10-alpine -js`).

## File Structure

- `src/rfobserver/pipeline/app.py` (modify) holds everything:
  - two new helpers, `install_stop_signals()` and `_build_web_server()`;
  - `_run_web_server()`, which gains a `stop` parameter;
  - the serve/teardown section of `run()`, rewritten;
  - a new `_stop_workers()`.
- `tests/unit/test_app_shutdown.py` (create): the helper tests (Task 1) and a
  real-uvicorn stop test (Task 2).
- `tests/unit/test_app_db_lifecycle.py` (modify): its fixture fakes learn the
  stop event and `set_active`; it gains run-level signal tests (Task 2).

---

### Task 1: Signal and web-server helpers

**Files:**
- Modify: `src/rfobserver/pipeline/app.py` (imports, and new functions after
  `make_give_up_handler`)
- Create: `tests/unit/test_app_shutdown.py`

**Interfaces:**
- Produces:
  - `install_stop_signals(loop: asyncio.AbstractEventLoop, stop: asyncio.Event, force_exit: Callable[[int], object] = os._exit) -> Callable[[], None]`
  - `_build_web_server(config: uvicorn.Config) -> uvicorn.Server`
  - module constant `_STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM)`

- [ ] **Step 1: Write the failing tests** in `tests/unit/test_app_shutdown.py`:

```python
"""Shutdown signal ownership: run() handles SIGINT/SIGTERM, not uvicorn.

See docs/debugging/2026-09-14_shutdown-signals.md for why: uvicorn re-raises
SIGTERM with SIG_DFL after serving (cleanup skipped), and on Python 3.10 a
SIGINT KeyboardInterrupt makes asyncio.run cancel every task mid-cleanup.
"""

from __future__ import annotations

import asyncio
import os
import signal
import threading

import uvicorn

from rfobserver.pipeline import app as app_mod


def _handlers() -> tuple[object, object]:
    return signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)


def _assert_installed() -> None:
    # Guard: never send a real signal while the default handler is in place.
    sigint, sigterm = _handlers()
    assert sigint is not signal.default_int_handler, "SIGINT handler not installed"
    assert sigterm not in (signal.SIG_DFL, signal.SIG_IGN, None), "SIGTERM handler not installed"


async def _wait_for(predicate, timeout: float = 2.0) -> None:  # type: ignore[no-untyped-def]
    for _ in range(int(timeout / 0.01)):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met in time")


async def test_first_signal_sets_stop_second_forces_exit() -> None:
    stop = asyncio.Event()
    exits: list[int] = []
    remove = app_mod.install_stop_signals(asyncio.get_running_loop(), stop, force_exit=exits.append)
    try:
        _assert_installed()
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(stop.wait(), timeout=2)
        assert exits == [], "the first signal only requests a stop"
        os.kill(os.getpid(), signal.SIGINT)
        await _wait_for(lambda: bool(exits))
        assert exits == [128 + signal.SIGINT]
    finally:
        remove()
    assert _handlers() == (signal.default_int_handler, signal.SIG_DFL)


async def test_sigint_sets_stop() -> None:
    stop = asyncio.Event()
    remove = app_mod.install_stop_signals(asyncio.get_running_loop(), stop, force_exit=lambda c: None)
    try:
        _assert_installed()
        os.kill(os.getpid(), signal.SIGINT)
        await asyncio.wait_for(stop.wait(), timeout=2)
    finally:
        remove()


def test_outside_main_thread_is_a_noop() -> None:
    before = _handlers()
    result: list[object] = []

    def worker() -> None:
        loop = asyncio.new_event_loop()
        try:
            remove = app_mod.install_stop_signals(loop, asyncio.Event())
            remove()
            result.append("ok")
        except BaseException as exc:  # pragma: no cover - reported below
            result.append(exc)
        finally:
            loop.close()

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=5)
    assert result == ["ok"]
    assert _handlers() == before


def test_web_server_leaves_signal_handlers_alone() -> None:
    server = app_mod._build_web_server(uvicorn.Config(app=lambda *a: None))
    assert isinstance(server, uvicorn.Server)
    before = _handlers()
    with server.capture_signals():
        assert _handlers() == before, "uvicorn must not install its own handlers"
    assert _handlers() == before
```

- [ ] **Step 2: Run to verify they fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_app_shutdown.py -q`
Expected: FAIL with `AttributeError: module 'rfobserver.pipeline.app' has no attribute 'install_stop_signals'`

- [ ] **Step 3: Implement** in `src/rfobserver/pipeline/app.py`.

1. Add `import contextlib` and `import signal` to the stdlib imports.
2. Under `if TYPE_CHECKING:`, add `from collections.abc import Generator`
   (next to `Callable`) and `import uvicorn`.
3. After `_GIVE_UP_EXIT_DELAY_SEC`, add:

```python
_STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM)
```

4. After `make_give_up_handler`, add:

```python
def install_stop_signals(
    loop: asyncio.AbstractEventLoop,
    stop: asyncio.Event,
    force_exit: Callable[[int], object] = os._exit,
) -> Callable[[], None]:
    """Route SIGINT and SIGTERM to ``stop`` so run() can tear down in order.

    Left to the defaults, uvicorn re-raises SIGTERM with SIG_DFL after serving
    (the process dies before run()'s cleanup), and on Python 3.10 a SIGINT's
    KeyboardInterrupt makes asyncio.run cancel every task, which aborts the
    cleanup before the DB close and hangs exit on aiosqlite's non-daemon
    thread. See docs/debugging/2026-09-14_shutdown-signals.md.

    The first signal sets ``stop``; a second one exits at once with
    128 + signal number. Returns a function that removes the handlers. Signal
    handlers can only be installed from the main thread: elsewhere this logs a
    warning and returns a no-op.
    """
    received: list[signal.Signals] = []

    def on_signal(sig: signal.Signals) -> None:
        if received:
            logger.error("Second %s during shutdown; exiting now", sig.name)
            force_exit(128 + sig.value)
            return
        received.append(sig)
        logger.info("Received %s; shutting down", sig.name)
        stop.set()

    def remove() -> None:
        for sig in _STOP_SIGNALS:
            loop.remove_signal_handler(sig)

    try:
        for sig in _STOP_SIGNALS:
            loop.add_signal_handler(sig, on_signal, sig)
    except (RuntimeError, ValueError):
        remove()
        logger.warning("Cannot install SIGINT/SIGTERM handlers outside the main thread")
        return lambda: None
    return remove


def _build_web_server(config: uvicorn.Config) -> uvicorn.Server:
    """A uvicorn server that leaves SIGINT and SIGTERM to run()."""
    import uvicorn

    class _AppSignalsServer(uvicorn.Server):
        @contextlib.contextmanager
        def capture_signals(self) -> Generator[None, None, None]:
            # The stock version re-raises the captured signal after serving;
            # for SIGTERM that is SIG_DFL, which kills the process before
            # run()'s cleanup. run() owns the signals (install_stop_signals).
            yield

    return _AppSignalsServer(config)
```

If mypy objects to the subclass or the override signature, keep it
`Generator[None, None, None]` to match uvicorn's own annotation. Do not add a
`# type: ignore` without trying that first.

- [ ] **Step 4: Run the tests on both interpreters**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_app_shutdown.py -q && PYTHONPATH= $V310/bin/pytest tests/unit/test_app_shutdown.py -q`
Expected: 4 passed on each.

Then run: `ruff check src/ tests/ && ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/ && PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`
Expected: all clean and green.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/pipeline/app.py tests/unit/test_app_shutdown.py
git commit -m "feat(app): helpers to own SIGINT/SIGTERM and run uvicorn without its signal capture"
```

---

### Task 2: run() stops in order on a signal

**Files:**
- Modify: `src/rfobserver/pipeline/app.py`: `run()` from `tasks: list[Any] = []`
  to the end of its `finally`; `_run_web_server()`; a new `_stop_workers()`;
  the constant `_WEB_SHUTDOWN_TIMEOUT_SEC`.
- Modify: `tests/unit/test_app_db_lifecycle.py`
- Modify: `tests/unit/test_app_shutdown.py` (add one real-uvicorn test)

**Interfaces:**
- Consumes: `install_stop_signals`, `_build_web_server` (Task 1).
- Produces:
  - `_run_web_server(settings, supervisor, database, write_database, broadcast, beacon, stop: asyncio.Event) -> None`,
    where the new parameter comes LAST;
  - `_WEB_SHUTDOWN_TIMEOUT_SEC = 5.0`;
  - log lines `Received SIGTERM; shutting down` (from Task 1) and
    `Shutdown complete`.

- [ ] **Step 1: Update the lifecycle test fakes and add failing tests** in
  `tests/unit/test_app_db_lifecycle.py`.

1. Change the module docstring to:
   `"""run() lifecycle: DB connections, and an ordered stop on SIGINT/SIGTERM."""`
2. Add `import os` and `import signal`.
3. Extend `_Registry.__init__` with:

```python
        self.web_ignores_stop = False
        self.web_stopped_by_event = False
        self.set_active_calls: list[bool] = []
        self.fail_set_active_false = False
```

4. In the `reg` fixture, replace `fake_web_server` with the version below and
   patch `set_active`:

```python
    async def fake_web_server(*args: Any) -> None:
        registry.web_args = args
        stop = args[-1]
        assert isinstance(stop, asyncio.Event)
        if registry.web_ignores_stop:
            await asyncio.Event().wait()
        await stop.wait()
        registry.web_stopped_by_event = True

    async def fake_set_active(self: Any, active: bool) -> None:
        registry.set_active_calls.append(active)
        if not active and registry.fail_set_active_false:
            raise RuntimeError("pipeline stop failed")

    monkeypatch.setattr(
        "rfobserver.pipeline.supervisor.PipelineSupervisor.set_active", fake_set_active
    )
```

5. Append these tests:

```python
def _assert_handlers_installed() -> None:
    # Guard: a real SIGINT/SIGTERM with the default handler would end pytest.
    assert signal.getsignal(signal.SIGINT) is not signal.default_int_handler
    assert signal.getsignal(signal.SIGTERM) not in (signal.SIG_DFL, signal.SIG_IGN, None)


def _assert_handlers_restored() -> None:
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


async def _start(settings: AppSettings) -> asyncio.Task[None]:
    task = asyncio.create_task(app_mod.run(settings))
    for _ in range(50):
        await asyncio.sleep(0)
    assert not task.done(), "run() should still be serving"
    _assert_handlers_installed()
    return task


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
@pytest.mark.parametrize("web_port", [0, 8888])
async def test_signal_stops_run_in_order(
    reg: _Registry, tmp_path: Any, sig: signal.Signals, web_port: int
) -> None:
    task = await _start(_settings(tmp_path, web_port=web_port))
    os.kill(os.getpid(), sig)
    await asyncio.wait_for(task, timeout=5)  # returns normally: exit code 0
    assert reg.set_active_calls == [False], "the pipeline is stopped once"
    assert all(db.closed for db in reg.instances)
    if web_port:
        assert reg.web_stopped_by_event, "the web server exits on the stop event"
    _assert_handlers_restored()


async def test_web_server_that_ignores_stop_is_cancelled(
    reg: _Registry, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    reg.web_ignores_stop = True
    monkeypatch.setattr(app_mod, "_WEB_SHUTDOWN_TIMEOUT_SEC", 0.1)
    task = await _start(_settings(tmp_path, web_port=8888))
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(task, timeout=5)
    assert reg.set_active_calls == [False]
    assert all(db.closed for db in reg.instances)


async def test_pipeline_stop_failure_still_closes_the_dbs(reg: _Registry, tmp_path: Any) -> None:
    reg.fail_set_active_false = True
    task = await _start(_settings(tmp_path, web_port=8888))
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(task, timeout=5)
    assert all(db.closed for db in reg.instances)
    _assert_handlers_restored()


async def test_worker_failure_propagates_after_cleanup(
    reg: _Registry, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def dying_heartbeat(*args: Any) -> None:
        raise RuntimeError("heartbeat died")

    monkeypatch.setattr(app_mod, "_heartbeat_loop", dying_heartbeat)
    with pytest.raises(RuntimeError, match="heartbeat died"):
        await asyncio.wait_for(app_mod.run(_settings(tmp_path, web_port=8888)), timeout=5)
    assert reg.set_active_calls == [False]
    assert all(db.closed for db in reg.instances)
    _assert_handlers_restored()
```

6. In `test_web_run_serves_reader_and_closes_both`, after the existing asserts,
   add `_assert_handlers_restored()`.

- [ ] **Step 2: Add the real-uvicorn stop test** to `tests/unit/test_app_shutdown.py`:

```python
async def test_run_web_server_exits_on_stop_without_touching_signals() -> None:
    from types import SimpleNamespace

    from rfobserver.config import AppSettings
    from rfobserver.pipeline.beacon import ProgressBeacon
    from rfobserver.web.websocket import LiveBroadcast

    settings = AppSettings(_env_file=None)
    settings.WEB_HOST = "127.0.0.1"
    settings.WEB_PORT = 0  # ephemeral port
    supervisor = SimpleNamespace(processor=None, _on_processor_change=None)
    stop = asyncio.Event()
    before = _handlers()
    task = asyncio.create_task(
        app_mod._run_web_server(
            settings, supervisor, None, None, LiveBroadcast(), ProgressBeacon(), stop
        )
    )
    await asyncio.sleep(0.3)  # let uvicorn start serving
    assert not task.done(), "the server should still be serving"
    assert _handlers() == before, "uvicorn must not install its own handlers"
    stop.set()
    await asyncio.wait_for(task, timeout=5)
    assert _handlers() == before
```

If `create_app` or its lifespan needs more than a `SimpleNamespace` supervisor,
add the smallest attributes it reads, and say so in the report.

- [ ] **Step 3: Run to verify the new tests fail**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_app_db_lifecycle.py tests/unit/test_app_shutdown.py -q`
Expected: the new tests FAIL. `_run_web_server` takes no `stop`, and `run()`
installs no handlers, so `_start`'s guard assertion fails before any signal
is sent. The existing lifecycle tests may also fail on the fake's `args[-1]`
assertion. That is expected until Step 4.

- [ ] **Step 4: Implement** in `src/rfobserver/pipeline/app.py`.

1. Next to `_STOP_SIGNALS`, add:

```python
_WEB_SHUTDOWN_TIMEOUT_SEC = 5.0
```

2. In `run()`, replace everything from `tasks: list[Any] = []` to the end of
   the function with:

```python
    stop = asyncio.Event()
    remove_stop_signals = install_stop_signals(asyncio.get_running_loop(), stop)

    web_task: asyncio.Task[None] | None = None
    workers: list[asyncio.Task[Any]] = []
    if zms_monitor is not None:
        workers.append(asyncio.create_task(zms_monitor.run()))
    if read_db is not None:
        web_task = asyncio.create_task(
            _run_web_server(settings, supervisor, read_db, db, broadcast, beacon, stop)
        )
        workers.append(web_task)
        workers.append(
            asyncio.create_task(
                _heartbeat_loop(settings, supervisor, read_db, local_storage, broadcast)
            )
        )
    if settings.DB_RETENTION_DAYS > 0:
        workers.append(asyncio.create_task(_cleanup_loop(settings, db)))
    # Serve until a stop signal. The supervisor owns the processor task
    # independently of these, so a Standby or headless run waits here too.
    stop_task = asyncio.create_task(stop.wait())

    try:
        pending: set[asyncio.Task[Any]] = {stop_task, *workers}
        while not stop.is_set():
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for finished in done:
                if finished is stop_task or finished.cancelled():
                    continue
                exc = finished.exception()
                if exc is not None:
                    raise exc
    finally:
        if watchdog is not None:
            watchdog.stop()
        await _stop_workers(stop, stop_task, web_task, workers)
        # Each step is isolated so one failure cannot skip the DB close.
        try:
            await supervisor.set_active(False)
        except Exception:
            logger.exception("Shutdown: stopping the pipeline failed; continuing")
        if zms_monitor is not None:
            try:
                await zms_monitor.stop()
            except Exception:
                logger.exception("Shutdown: stopping the ZMS monitor failed; continuing")
        if nats_producer is not None:
            try:
                await nats_producer.close()
            except Exception:
                logger.exception("Shutdown: closing NATS failed; continuing")
        try:
            try:
                if read_db is not None:
                    await read_db.close()
            finally:
                await db.close()
            logger.info("Shutdown complete")
        finally:
            remove_stop_signals()


async def _stop_workers(
    stop: asyncio.Event,
    stop_task: asyncio.Task[Any],
    web_task: asyncio.Task[None] | None,
    workers: list[asyncio.Task[Any]],
) -> None:
    """Let the web server finish (bounded), then cancel the other loops."""
    stop.set()  # _run_web_server turns this into uvicorn's should_exit
    if web_task is not None and not web_task.done():
        _, still_running = await asyncio.wait({web_task}, timeout=_WEB_SHUTDOWN_TIMEOUT_SEC)
        if still_running:
            logger.warning(
                "Web server did not stop within %.0fs; cancelling", _WEB_SHUTDOWN_TIMEOUT_SEC
            )
    for task in (stop_task, *workers):
        task.cancel()
    await asyncio.gather(stop_task, *workers, return_exceptions=True)
```

3. In `_run_web_server`:
   - add the `stop: asyncio.Event` parameter last;
   - replace `server = uvicorn.Server(config)` and `await server.serve()` with
     the code below;
   - keep `import uvicorn` for `uvicorn.Config`.

```python
    server = _build_web_server(config)

    async def _exit_on_stop() -> None:
        await stop.wait()
        server.should_exit = True

    watcher = asyncio.create_task(_exit_on_stop())
    try:
        await server.serve()
    finally:
        watcher.cancel()
```

- [ ] **Step 5: Run the tests on both interpreters**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_app_db_lifecycle.py tests/unit/test_app_shutdown.py -q && PYTHONPATH= $V310/bin/pytest tests/unit/test_app_db_lifecycle.py tests/unit/test_app_shutdown.py -q`
Expected: all pass on each (4 existing lifecycle tests, 4 parametrized signal
cases, 3 more lifecycle tests, and 5 shutdown tests).

Then run: `ruff check src/ tests/ && ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/ && PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q && PYTHONPATH= $V310/bin/pytest tests/unit/ -x -q`
Expected: all clean and green.

- [ ] **Step 6: Commit**

```bash
git add src/rfobserver/pipeline/app.py tests/unit/test_app_db_lifecycle.py tests/unit/test_app_shutdown.py
git commit -m "fix(app): stop in order on SIGINT/SIGTERM instead of dying or hanging"
```

---

### Task 3: End-to-end verification (controller, not a subagent)

- [ ] **Local repro matrix.** Run
  `docs/debugging/2026-09-14_shutdown-signals/shutdown_repro.sh` for {INT,
  TERM} x {port, 0} x {`.venv`, `$V310`}.
  - Expected: every run exits within a few seconds, rc 0, with "Sensor
    deactivated" and "Shutdown complete".
- [ ] **Recording check.** Run `rec_term.sh TERM` and `rec_term.sh INT` on
  both interpreters.
  - Expected: each capture has its `.json` and `.psd.json` sidecars, and the
    log has "Recording saved".
- [ ] **nano-super with the B200mini under systemd** (`start.sh` from the
  validation folder):
  - Start a manual recording, then `systemctl stop` (SIGTERM). Expected: stop
    in under 20 s, `Result=success`, sidecars written, and the log has
    "Received SIGTERM", "Recording saved", "Sensor deactivated" and "Shutdown
    complete".
  - Repeat with `systemctl kill -s INT`. Expected: the unit is inactive and not
    restarted, since the exit code is 0.
- [ ] **Record the results** in the validation doc's Fix status. Then run the
  full CI set and merge into local `main` with `--no-ff`.
