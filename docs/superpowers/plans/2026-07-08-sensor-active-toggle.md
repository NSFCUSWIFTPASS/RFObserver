# Sensor Active toggle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a "Sensor Active" toggle that stops all capture/processing and fully releases the SDR when off, brings it back when on, reflects state only after backend confirmation, and persists across restarts.

**Architecture:** A `PipelineSupervisor` owns the receiver+processor lifecycle via factory closures; enabling initializes hardware and starts a fresh processor task, disabling stops the processor and calls `receiver.close()` to release the device. A new `RFOBS_SENSOR_ACTIVE` setting persists intent to `.env`. New `/api/sensor` endpoints drive the supervisor synchronously and return the confirmed state; the config page toggle settles only on that response.

**Tech Stack:** Python 3.11+, FastAPI, pydantic-settings, asyncio, pytest / pytest-asyncio, UHD (pyuhd) for the real receiver.

## Global Constraints

- Always run Python via the venv with a cleared PYTHONPATH: `PYTHONPATH= .venv/bin/<tool>`. ruff is global (`ruff ...`, no prefix).
- No emojis anywhere, including UI. UI follows Apple style.
- Before any commit run all of: `ruff check src/ tests/`, `ruff format --check src/ tests/`, `PYTHONPATH= .venv/bin/mypy src/rfobserver/`, `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`, and `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q` (integration needs NATS on localhost:4222 — start with `docker run -d --rm --name rfobs-nats-test -p 4222:4222 nats:latest`).
- Do not add any "Co-Authored-By: Claude" trailer to commits.
- Never eagerly initialize the SDR when `SENSOR_ACTIVE` is false — a disabled sensor must not claim the device on startup.

---

### Task 1: `SENSOR_ACTIVE` setting

**Files:**
- Modify: `src/rfobserver/config.py` (add field near the other `bool` toggles, e.g. after `ZMS_ENABLED`)
- Test: `tests/unit/test_config.py` (create if absent)

**Interfaces:**
- Produces: `AppSettings.SENSOR_ACTIVE: bool` (default `True`), env var `RFOBS_SENSOR_ACTIVE`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_config.py
from rfobserver.config import AppSettings


def test_sensor_active_defaults_true() -> None:
    s = AppSettings(_env_file=None)
    assert s.SENSOR_ACTIVE is True


def test_sensor_active_env_override(monkeypatch) -> None:
    monkeypatch.setenv("RFOBS_SENSOR_ACTIVE", "false")
    s = AppSettings(_env_file=None)
    assert s.SENSOR_ACTIVE is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_config.py -q`
Expected: FAIL (`AttributeError`/no `SENSOR_ACTIVE`).

- [ ] **Step 3: Add the field**

In `src/rfobserver/config.py`, in `class AppSettings`, add near the other boolean toggles:

```python
    # SENSOR_ACTIVE is the user-intent flag for capture. False means the sensor
    # is in Standby: no streaming/processing and the SDR is released. Persisted
    # to .env so a disabled sensor stays disabled across restarts.
    SENSOR_ACTIVE: bool = True
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_config.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/config.py tests/unit/test_config.py
git commit -m "Add SENSOR_ACTIVE setting for the sensor-active toggle"
```

---

### Task 2: `receiver.close()` — release the SDR

**Files:**
- Modify: `src/rfobserver/capture/receiver.py` (add `close` to the `IReceiver` Protocol ~line 50-67, and to `class Receiver` after `stop_streaming` ~line 245-256)
- Modify: `src/rfobserver/capture/mock_receiver.py` (add `close` after `stop_streaming` ~line 119)
- Test: `tests/unit/test_receiver_close.py` (create)

**Interfaces:**
- Produces: `IReceiver.close() -> None`; `Receiver.close()` drops `usrp`/`rx_streamer` refs; `MockReceiver.close()` sets `self._closed = True` and is a no-op otherwise. Both are safe to call twice and callable before `initialize()`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_receiver_close.py
from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import Receiver, ReceiverConfig


def _cfg() -> ReceiverConfig:
    return ReceiverConfig(gain_db=30, bandwidth_hz=1_000_000, duration_sec=0.1)


def test_mock_close_is_noop_and_idempotent() -> None:
    r = MockReceiver(_cfg())
    r.initialize()
    r.close()
    r.close()  # double close must not raise
    assert r._closed is True


def test_receiver_close_drops_handles() -> None:
    r = Receiver(_cfg())
    # Simulate an initialized device without touching hardware.
    r.usrp = object()
    r.rx_streamer = object()
    r.close()
    assert r.usrp is None
    assert r.rx_streamer is None
    r.close()  # idempotent, no raise


def test_receiver_close_before_init_is_safe() -> None:
    Receiver(_cfg()).close()  # must not raise even if never initialized
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_receiver_close.py -q`
Expected: FAIL (`AttributeError: 'Receiver' object has no attribute 'close'` / `_closed`).

- [ ] **Step 3: Implement `close()`**

In `src/rfobserver/capture/receiver.py`, add to the `IReceiver` Protocol (with the other method signatures):

```python
    def close(self) -> None: ...
```

Add to `class Receiver` after `stop_streaming`:

```python
    def close(self) -> None:
        """Release the SDR so another process can claim it.

        Drops the UHD streamer and device handles; Python/UHD frees the USB
        device when the last reference goes away. ``initialize()`` recreates
        them, so a closed receiver can be brought back. Safe to call twice or
        before ``initialize()``.
        """
        with self._hardware_lock:
            self.rx_streamer = None
            self.usrp = None
            self._streaming = False
        logger.info("Receiver closed (SDR released)")
```

Note: confirm `Receiver.__init__` sets `self.usrp`/`self.rx_streamer` (default `None`) and has `self._hardware_lock`; if `usrp`/`rx_streamer` are not pre-declared, initialize them to `None` in `__init__` so `close()` before `initialize()` works.

In `src/rfobserver/capture/mock_receiver.py`, add `self._closed = False` in `__init__` and after `stop_streaming`:

```python
    def close(self) -> None:
        """Release the (mock) device. No hardware to free; records state."""
        self._streaming = False
        self._closed = True
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_receiver_close.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/capture/receiver.py src/rfobserver/capture/mock_receiver.py tests/unit/test_receiver_close.py
git commit -m "Add receiver.close() to release the SDR"
```

---

### Task 3: `PipelineSupervisor`

**Files:**
- Create: `src/rfobserver/pipeline/supervisor.py`
- Test: `tests/unit/test_supervisor.py`

**Interfaces:**
- Consumes: `IReceiver.initialize()`, `IReceiver.close()`; a processor object exposing `async run()` and `stop()`.
- Produces:
  - `PipelineSupervisor(build_receiver: Callable[[], IReceiver], build_processor: Callable[[IReceiver], Any], on_processor_change: Callable[[Any | None], None] | None = None)`
  - `async set_active(active: bool) -> bool` — performs the transition, returns the actual resulting `active` state.
  - properties `active: bool`, `processor: Any | None`, `receiver: IReceiver | None`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_supervisor.py
import asyncio

import pytest

from rfobserver.pipeline.supervisor import PipelineSupervisor


class FakeReceiver:
    def __init__(self) -> None:
        self.initialized = 0
        self.closed = 0

    def initialize(self) -> None:
        self.initialized += 1

    def close(self) -> None:
        self.closed += 1


class FakeProcessor:
    def __init__(self, receiver: FakeReceiver) -> None:
        self.receiver = receiver
        self.started = False
        self._stop = asyncio.Event()

    async def run(self) -> None:
        self.started = True
        await self._stop.wait()

    def stop(self) -> None:
        self._stop.set()


def _make() -> tuple[PipelineSupervisor, list]:
    receivers: list[FakeReceiver] = []
    changes: list[object] = []

    def build_receiver() -> FakeReceiver:
        r = FakeReceiver()
        receivers.append(r)
        return r

    sup = PipelineSupervisor(
        build_receiver=build_receiver,
        build_processor=lambda r: FakeProcessor(r),
        on_processor_change=changes.append,
    )
    return sup, (receivers, changes)


@pytest.mark.asyncio
async def test_enable_initializes_and_starts() -> None:
    sup, (receivers, changes) = _make()
    result = await sup.set_active(True)
    assert result is True
    assert sup.active is True
    assert receivers[0].initialized == 1
    assert sup.processor is not None and sup.processor.started is True
    assert changes[-1] is sup.processor


@pytest.mark.asyncio
async def test_disable_stops_and_closes() -> None:
    sup, (receivers, changes) = _make()
    await sup.set_active(True)
    result = await sup.set_active(False)
    assert result is False
    assert sup.active is False
    assert sup.processor is None
    assert receivers[0].closed == 1
    assert changes[-1] is None


@pytest.mark.asyncio
async def test_redundant_calls_are_noops() -> None:
    sup, (receivers, _) = _make()
    await sup.set_active(True)
    await sup.set_active(True)  # no second receiver built
    assert len(receivers) == 1


@pytest.mark.asyncio
async def test_reenable_uses_fresh_receiver() -> None:
    sup, (receivers, _) = _make()
    await sup.set_active(True)
    await sup.set_active(False)
    await sup.set_active(True)
    assert len(receivers) == 2
    assert receivers[1].initialized == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_supervisor.py -q`
Expected: FAIL (module `supervisor` not found).

- [ ] **Step 3: Implement the supervisor**

```python
# src/rfobserver/pipeline/supervisor.py
"""Runtime start/stop of the capture pipeline (the "Sensor Active" toggle).

Owns the receiver + processor lifecycle so the sensor can be put into Standby
(processor stopped, SDR released) and brought back on demand, with the caller
awaiting the actual transition for confirmation.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from rfobserver.capture.receiver import IReceiver

logger = logging.getLogger(__name__)

# Bound on how long to wait for a stopped processor's run() to drain before
# cancelling it, so a wedged pipeline can't hang the toggle forever.
_STOP_TIMEOUT_SEC = 15.0


class PipelineSupervisor:
    """Starts/stops the capture pipeline and releases the SDR when inactive."""

    def __init__(
        self,
        build_receiver: Callable[[], IReceiver],
        build_processor: Callable[[IReceiver], Any],
        on_processor_change: Callable[[Any | None], None] | None = None,
    ) -> None:
        self._build_receiver = build_receiver
        self._build_processor = build_processor
        self._on_processor_change = on_processor_change
        self._receiver: IReceiver | None = None
        self._processor: Any | None = None
        self._task: asyncio.Task[Any] | None = None
        self._active = False
        self._lock = asyncio.Lock()

    @property
    def active(self) -> bool:
        return self._active

    @property
    def processor(self) -> Any | None:
        return self._processor

    @property
    def receiver(self) -> IReceiver | None:
        return self._receiver

    async def set_active(self, active: bool) -> bool:
        """Transition to ``active`` and return the actual resulting state.

        Redundant calls (already in the requested state) are no-ops. The return
        value is the confirmation the API/UI settle on.
        """
        async with self._lock:
            if active and not self._active:
                await self._start()
            elif not active and self._active:
                await self._stop()
            return self._active

    async def _start(self) -> None:
        loop = asyncio.get_running_loop()
        receiver = self._build_receiver()
        # initialize() claims + configures hardware (blocking) — run off-loop.
        await loop.run_in_executor(None, receiver.initialize)
        processor = self._build_processor(receiver)
        self._receiver = receiver
        self._processor = processor
        self._task = asyncio.create_task(processor.run())
        self._active = True
        logger.info("Sensor activated")
        self._notify(processor)

    async def _stop(self) -> None:
        loop = asyncio.get_running_loop()
        processor, task, receiver = self._processor, self._task, self._receiver
        if processor is not None:
            processor.stop()
        if task is not None:
            try:
                await asyncio.wait_for(task, timeout=_STOP_TIMEOUT_SEC)
            except (TimeoutError, asyncio.TimeoutError):
                logger.warning("Processor did not stop in time; cancelling")
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        if receiver is not None:
            await loop.run_in_executor(None, receiver.close)
        self._processor = None
        self._receiver = None
        self._task = None
        self._active = False
        logger.info("Sensor deactivated (SDR released)")
        self._notify(None)

    def _notify(self, processor: Any | None) -> None:
        if self._on_processor_change is not None:
            self._on_processor_change(processor)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_supervisor.py -q`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/pipeline/supervisor.py tests/unit/test_supervisor.py
git commit -m "Add PipelineSupervisor for runtime sensor start/stop"
```

---

### Task 4: Wire the supervisor into the pipeline orchestrator

**Files:**
- Modify: `src/rfobserver/pipeline/app.py` (`run`, `_heartbeat_loop`, `_run_web_server`)
- Test: covered by Task 3 (supervisor) + Task 5 (API) + existing integration run; add a focused smoke test `tests/unit/test_pipeline_wiring.py`

**Interfaces:**
- Consumes: `PipelineSupervisor`, `AppSettings.SENSOR_ACTIVE`.
- Produces: `app.state.supervisor` set on the web app; `app.state.processor` kept in sync via `on_processor_change`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_pipeline_wiring.py
"""The receiver factory must not initialize hardware until activation."""
import asyncio

import pytest

from rfobserver.pipeline.supervisor import PipelineSupervisor


class SpyReceiver:
    def __init__(self) -> None:
        self.initialized = False

    def initialize(self) -> None:
        self.initialized = True

    def close(self) -> None:
        pass


class Proc:
    def __init__(self, r: SpyReceiver) -> None:
        self._e = asyncio.Event()

    async def run(self) -> None:
        await self._e.wait()

    def stop(self) -> None:
        self._e.set()


@pytest.mark.asyncio
async def test_receiver_not_initialized_until_active() -> None:
    spies: list[SpyReceiver] = []

    def build() -> SpyReceiver:
        s = SpyReceiver()
        spies.append(s)
        return s

    sup = PipelineSupervisor(build_receiver=build, build_processor=lambda r: Proc(r))
    # Constructing the supervisor and staying inactive must not build/init a receiver.
    assert spies == []
    await sup.set_active(True)
    assert spies[0].initialized is True
    await sup.set_active(False)
```

- [ ] **Step 2: Run test to verify it fails, then passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_pipeline_wiring.py -q`
Expected: PASS immediately (this asserts the supervisor contract from Task 3; it guards the wiring intent). If it fails, Task 3 is wrong — fix there.

- [ ] **Step 3: Refactor `run()` to use the supervisor**

In `src/rfobserver/pipeline/app.py`, replace eager receiver init + fixed `processor.run()` with factory closures and a supervisor. Key changes:

Remove the eager block:
```python
    receiver: IReceiver
    if settings.MOCK_RECEIVER:
        receiver = MockReceiver(receiver_config)
        ...
    receiver.initialize()
```

Replace with factories (place after `db`, `local_storage`, `broadcast`, `zms_monitor`, `nats_producer` are built):

```python
    def build_receiver() -> IReceiver:
        if settings.MOCK_RECEIVER:
            logger.info("Using mock receiver")
            return MockReceiver(receiver_config)
        from rfobserver.capture.receiver import Receiver

        return Receiver(receiver_config)

    def build_processor(receiver: IReceiver) -> Any:
        is_sweep = settings.FREQUENCY_STEP > 0 and settings.FREQUENCY_END > settings.FREQUENCY_START
        use_streaming = settings.TRIGGER_ENABLED or not is_sweep
        if use_streaming:
            from rfobserver.modules.manager import ModuleManager
            from rfobserver.pipeline.streaming import StreamingProcessor

            proc = StreamingProcessor(
                receiver=receiver,
                database=db,
                local_storage=local_storage,
                settings=settings,
                broadcast=broadcast,
                zms_monitor=zms_monitor,
                nats_producer=nats_producer,
            )
            proc._module_manager = ModuleManager()
            logger.info("Using streaming pipeline")
            return proc
        from rfobserver.pipeline.continuous import ContinuousProcessor

        logger.info("Using batch pipeline (sweep mode)")
        return ContinuousProcessor(
            receiver=receiver,
            database=db,
            local_storage=local_storage,
            settings=settings,
            broadcast=broadcast,
            zms_monitor=zms_monitor,
            nats_producer=nats_producer,
        )

    from rfobserver.pipeline.supervisor import PipelineSupervisor

    supervisor = PipelineSupervisor(
        build_receiver=build_receiver,
        build_processor=build_processor,
    )
    if settings.SENSOR_ACTIVE:
        await supervisor.set_active(True)
```

Replace the `tasks = [processor.run()]` block and gather with a supervisor-aware version:

```python
    tasks: list[Any] = []
    if zms_monitor is not None:
        tasks.append(zms_monitor.run())
    if settings.WEB_PORT > 0:
        tasks.append(_run_web_server(settings, supervisor, db, broadcast))
        tasks.append(_heartbeat_loop(settings, supervisor, db, local_storage, broadcast))
    # Keep the process alive even in Standby / headless (no web) mode; the
    # supervisor owns the processor task independently of this gather.
    tasks.append(asyncio.Event().wait())

    try:
        await asyncio.gather(*tasks)
    finally:
        await supervisor.set_active(False)
        if zms_monitor is not None:
            await zms_monitor.stop()
        if nats_producer is not None:
            await nats_producer.close()
        await db.close()
```

- [ ] **Step 4: Update `_run_web_server` signature + wiring**

```python
async def _run_web_server(
    settings: AppSettings,
    supervisor: Any,
    database: object,
    broadcast: LiveBroadcast,
) -> None:
    import uvicorn

    from rfobserver.web.app import create_app

    app = create_app(settings)
    app.state.supervisor = supervisor
    app.state.database = database
    app.state.broadcast = broadcast
    app.state.processor = supervisor.processor

    def _sync_processor(processor: object | None) -> None:
        app.state.processor = processor

    supervisor._on_processor_change = _sync_processor

    config = uvicorn.Config(
        app, host=settings.WEB_HOST, port=settings.WEB_PORT, log_level=settings.LOG_LEVEL.lower()
    )
    server = uvicorn.Server(config)
    await server.serve()
```

- [ ] **Step 5: Update `_heartbeat_loop` to read the live processor**

Change signature `processor: object` → `supervisor: Any`, and inside the loop read the current processor each tick:

```python
    while True:
        try:
            processor = supervisor.processor
            module_manager = getattr(processor, "_module_manager", None)
            if processor is not None and hasattr(processor, "recording_status"):
                rec_status = processor.recording_status()
            else:
                rec_status = {"state": "idle", "file": None, "bytes": 0, "duration_sec": 0}
            ...  # rest unchanged, but pass `processor` (may be None) to the
                 # build_*_status_payload helpers
```

Also move `storage_path` derivation to before the loop as today, and drop the top-of-function `module_manager = getattr(processor, ...)` line (now computed per tick).

- [ ] **Step 6: Run checks + integration smoke**

Run:
```bash
PYTHONPATH= .venv/bin/mypy src/rfobserver/
PYTHONPATH= .venv/bin/pytest tests/unit/ -q
PYTHONPATH= RFOBS_MOCK_RECEIVER=true RFOBS_WEB_PORT=8888 timeout 6 .venv/bin/rfobserver run || true
```
Expected: mypy clean; unit green; the pipeline starts, logs "Sensor activated" and "Using streaming pipeline", serves without error until the timeout.

- [ ] **Step 7: Commit**

```bash
git add src/rfobserver/pipeline/app.py tests/unit/test_pipeline_wiring.py
git commit -m "Wire PipelineSupervisor into the pipeline orchestrator"
```

---

### Task 5: `/api/sensor` endpoints

**Files:**
- Modify: `src/rfobserver/web/routes/api.py` (add `GET`/`POST /sensor`; router already `prefix="/api"`)
- Test: `tests/unit/test_web_routes.py` (add cases; mirror existing ASGITransport pattern)

**Interfaces:**
- Consumes: `app.state.supervisor` (a `PipelineSupervisor` or `None`), `_persist_settings`.
- Produces:
  - `GET /api/sensor` → `{"active": bool}` (from `supervisor.active`, or `settings.SENSOR_ACTIVE` if no supervisor).
  - `POST /api/sensor` body `{"active": bool}` → `{"active": <confirmed bool>, "detail": str}`; `409` if no supervisor.

- [ ] **Step 1: Write the failing test**

```python
# add to tests/unit/test_web_routes.py
import httpx
import pytest
from httpx import ASGITransport

from rfobserver.web.app import create_app


class _FakeSupervisor:
    def __init__(self, active: bool = True) -> None:
        self._active = active
        self.calls: list[bool] = []

    @property
    def active(self) -> bool:
        return self._active

    async def set_active(self, active: bool) -> bool:
        self.calls.append(active)
        self._active = active
        return self._active


def _sensor_app(supervisor):
    app = create_app()
    app.state.supervisor = supervisor
    return app


@pytest.mark.asyncio
async def test_get_sensor_reflects_supervisor() -> None:
    app = _sensor_app(_FakeSupervisor(active=False))
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/api/sensor")
    assert r.status_code == 200
    assert r.json() == {"active": False}


@pytest.mark.asyncio
async def test_post_sensor_toggles_and_confirms() -> None:
    sup = _FakeSupervisor(active=True)
    app = _sensor_app(sup)
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/api/sensor", json={"active": False})
    assert r.status_code == 200
    assert r.json()["active"] is False
    assert sup.calls == [False]


@pytest.mark.asyncio
async def test_post_sensor_without_supervisor_is_409() -> None:
    app = create_app()  # web-only: no supervisor
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/api/sensor", json={"active": False})
    assert r.status_code == 409
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_web_routes.py -q -k sensor`
Expected: FAIL (404 — routes not defined).

- [ ] **Step 3: Implement the endpoints**

Add to `src/rfobserver/web/routes/api.py`:

```python
@router.get("/sensor")
async def sensor_state(request: Request) -> dict[str, Any]:
    """Current sensor-active state for initial UI render."""
    supervisor = getattr(request.app.state, "supervisor", None)
    if supervisor is not None:
        return {"active": bool(supervisor.active)}
    return {"active": bool(request.app.state.settings.SENSOR_ACTIVE)}


@router.post("/sensor")
async def sensor_set(request: Request) -> dict[str, Any]:
    """Enable/disable capture + streaming; returns the confirmed state.

    Persists the intent to .env so a disabled sensor stays disabled across
    restarts. Returns 409 when no pipeline is running (web-only mode).
    """
    from rfobserver.web.routes.config import _persist_settings

    supervisor = getattr(request.app.state, "supervisor", None)
    if supervisor is None:
        raise HTTPException(status_code=409, detail="Pipeline not running")

    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON") from exc
    if not isinstance(body, dict) or "active" not in body:
        raise HTTPException(status_code=400, detail="Missing 'active'")
    want = bool(body["active"])

    try:
        confirmed = await supervisor.set_active(want)
    except Exception as exc:
        logger.exception("Sensor toggle failed")
        raise HTTPException(status_code=500, detail=f"toggle failed: {exc}") from exc

    settings = request.app.state.settings
    settings.SENSOR_ACTIVE = confirmed
    _persist_settings(settings)
    logger.info("Sensor set active=%s via API (persisted)", confirmed)
    return {"active": confirmed, "detail": "active" if confirmed else "standby"}
```

Confirm `HTTPException` and `Request` are already imported at the top of `api.py` (they are, used by other routes).

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_web_routes.py -q -k sensor`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/rfobserver/web/routes/api.py tests/unit/test_web_routes.py
git commit -m "Add GET/POST /api/sensor endpoints"
```

---

### Task 6: Config-page toggle + Standby in status bar

**Files:**
- Modify: `src/rfobserver/web/templates/config.html` (add a "Sensor Active" toggle near the NATS/ZMS toggles ~line 195-229, and a JS handler mirroring the NATS one ~line 513-525)
- Modify: `src/rfobserver/web/routes/api.py` (`build_status_bar_html` shows "Standby" when inactive)

**Interfaces:**
- Consumes: `GET/POST /api/sensor`; `app.state.supervisor` for status.

- [ ] **Step 1: Add the toggle markup**

In `config.html`, in the same settings-card area as the NATS/ZMS toggles, add:

```html
        <label class="toggle-label" title="Enable/disable capture and streaming; releases the SDR when off">
            <input type="checkbox" id="sensor-toggle">
            <span class="toggle-text">Sensor Active</span>
        </label>
```

- [ ] **Step 2: Add the JS handler**

In the config page `<script>`, near the NATS toggle handler, add. It (a) loads current state on page load, and (b) only settles the checkbox to the backend-confirmed value:

```javascript
    // --- Sensor Active toggle ---
    const sensorToggle = document.getElementById('sensor-toggle');
    if (sensorToggle) {
        fetch('/api/sensor').then(r => r.json()).then(d => {
            sensorToggle.checked = !!d.active;
        }).catch(() => {});

        sensorToggle.addEventListener('change', async function () {
            const want = this.checked;
            sensorToggle.disabled = true;
            try {
                const r = await fetch('/api/sensor', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({active: want}),
                });
                if (!r.ok) {
                    this.checked = !want;  // revert; transition not confirmed
                    const e = await r.json().catch(() => ({}));
                    alert(e.detail || 'Sensor toggle failed');
                } else {
                    const d = await r.json();
                    this.checked = !!d.active;  // settle on confirmed state
                }
            } catch (_) {
                this.checked = !want;
            } finally {
                sensorToggle.disabled = false;
            }
        });
    }
```

- [ ] **Step 3: Show Standby in the status bar**

In `build_status_bar_html` (api.py), accept the sensor state and prefix "Standby" when off. Simplest: read from an optional arg. Change the signature to `build_status_bar_html(settings: Any, active: bool = True)` and prepend when inactive:

```python
    prefix = "" if active else '<span class="status-standby">Standby</span> <span class="status-sep">&middot;</span> '
    return (
        prefix
        + f"{display_name} "
        ...
    )
```

Update the two callers: `status_bar` route → `build_status_bar_html(request.app.state.settings, active=getattr(getattr(request.app.state, "supervisor", None), "active", True))`; and the heartbeat in `app.py` → pass `active=supervisor.active`. Add a minimal `.status-standby { color: var(--warn, #ff9f0a); font-weight: 600; }` rule to `static/style.css`.

- [ ] **Step 4: Verify in the browser**

Run:
```bash
PYTHONPATH= RFOBS_MOCK_RECEIVER=true RFOBS_WEB_PORT=8888 .venv/bin/rfobserver run
```
Then via puppeteer/curl on `http://localhost:8888`: `GET /api/sensor` → `{"active": true}`; `POST /api/sensor {"active": false}` → `{"active": false, ...}`; server logs "Sensor deactivated (SDR released)"; re-enable returns `{"active": true}` and logs "Sensor activated". Confirm the config page toggle reflects and drives this, and the dashboard status bar shows "Standby" while off.

- [ ] **Step 5: Full checks + commit**

Run every check in Global Constraints (ruff, format, mypy, unit, integration with NATS).

```bash
git add src/rfobserver/web/templates/config.html src/rfobserver/web/routes/api.py src/rfobserver/web/static/style.css
git commit -m "Add Sensor Active toggle to config page and status bar"
```

---

## Notes for the implementer

- The `Receiver.__init__` may not currently declare `self.usrp`/`self.rx_streamer` before `initialize()`. Add `self.usrp = None` / `self.rx_streamer = None` in `__init__` if needed so `close()` is safe pre-init (Task 2, Step 3 note).
- Do not remove the drop-on-overflow default in `StreamingProcessor`; live capture keeps dropping. The supervisor is orthogonal to that.
- ZMS/NATS connections intentionally stay up during Standby (control-plane). Do not tear them down in `_stop`.
