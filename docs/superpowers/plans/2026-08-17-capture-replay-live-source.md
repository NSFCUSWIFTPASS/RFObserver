# Capture Replay as a Live Source (threshold-tuning mode) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replay a recorded IQ capture through the live detection pipeline and watch it in the dashboard like SDR streaming, to tune thresholds against a known signal without live transmitters.

**Architecture:** Reuse the single pipeline. A paced+looping `FileReplayReceiver` becomes the receiver via a supervisor override; a `replay_mode` flag on `StreamingProcessor` suppresses all persistence/egress (DB, NATS, ZMS, recording) while leaving the live WS overlay on. New `/api/replay/{start,stop,speed}` endpoints drive it; replay status rides the existing `/ws/live` heartbeat so a conditional dashboard banner (with live threshold fields) shows only during replay.

**Tech Stack:** Python 3.10+, FastAPI, numpy memmap, threading, vanilla JS. Design spec: `docs/superpowers/specs/2026-08-17-capture-replay-live-source-design.md`.

## Global Constraints
- Prefix every Python command with `PYTHONPATH=` and use `.venv/bin`. ruff is global (`ruff check` + `ruff format --check` both must pass).
- No emojis, no em-dashes, no "Co-Authored-By" lines anywhere (code, UI, commits).
- Replay must NEVER write to the DB, publish to NATS/ZMS, or write recording files. The live `_broadcast.publish(...)` overlay path stays on.
- Replay takes over the single pipeline (SDR -> Standby); no second pipeline instance.
- Raw file paths are restricted to an allowlist root (`REPLAY_SOURCE_DIR` + captures storage), resolved and checked with `Path.resolve()` / `is_relative_to`.
- All CLAUDE.md checks stay green (ruff, ruff format, mypy `src/rfobserver/`, unit, integration@NATS:4222).
- `app.state.processor` is kept synced to the live processor by `supervisor._on_processor_change` (`pipeline/app.py`); endpoints read the processor from there.

---

### Task 1: Paced + looping `FileReplayReceiver`

**Files:**
- Modify: `src/rfobserver/capture/replay_receiver.py`
- Test: `tests/unit/test_replay_receiver.py` (create)

**Interfaces:**
- Consumes: `SigmfCapture` (`.raw`, `.datatype`, `.sample_rate_hz`, `.num_samples`), `ReceiverConfig`.
- Produces: `FileReplayReceiver(capture, receiver_config, max_samples=None, paced=False, loop=False, speed=1.0, source_name="")` with `.set_speed(x: float)`, `.speed` (property), `.loop` (bool), `.source_name` (str).

- [ ] **Step 1: Write the failing tests.**

Create `tests/unit/test_replay_receiver.py`:
```python
"""Paced + looping behavior of FileReplayReceiver."""

from __future__ import annotations

import numpy as np

from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.capture.replay_receiver import FileReplayReceiver
from rfobserver.capture.sigmf_reader import SigmfCapture


def _capture(n_samples: int) -> SigmfCapture:
    # interleaved I/Q int16 -> 2*n int16 values; use a ramp so we can detect wrap
    raw = np.arange(n_samples * 2, dtype=np.int16)
    return SigmfCapture(
        datatype="ci16_le", sample_rate_hz=1_000_000.0, center_freq_hz=915e6, raw=raw
    )


def _cfg() -> ReceiverConfig:
    return ReceiverConfig(gain_db=30, bandwidth_hz=1_000_000, duration_sec=1.0)


def test_loop_seeks_to_zero_instead_of_draining():
    cap = _capture(100)
    rx = FileReplayReceiver(cap, _cfg(), loop=True)
    buf = np.empty(60, dtype=np.int32)
    rx.recv_chunk(buf)  # samples 0..59
    rx.recv_chunk(buf)  # 60..99 then wrap to 0..19
    assert not rx.exhausted  # looping never sets exhausted
    first = buf[0]
    rx.recv_chunk(buf)  # continues from 20
    # after enough reads it keeps returning real (wrapping) capture data, not a
    # frozen drain: the buffer content changes across reads
    assert buf[0] != first or rx._pos != 0


def test_paced_sleeps_scaled_by_speed(monkeypatch):
    cap = _capture(10_000)
    rx = FileReplayReceiver(cap, _cfg(), paced=True, loop=True, speed=1.0)
    slept: list[float] = []
    monkeypatch.setattr("rfobserver.capture.replay_receiver.time.sleep", lambda s: slept.append(s))
    buf = np.empty(1000, dtype=np.int32)  # 1000 samples @ 1 MS/s = 1 ms at 1x
    rx.recv_chunk(buf)
    assert slept and abs(slept[-1] - 0.001) < 0.0005
    slept.clear()
    rx.set_speed(2.0)
    rx.recv_chunk(buf)
    assert slept and abs(slept[-1] - 0.0005) < 0.00025  # 2x -> half the delay


def test_unpaced_default_does_not_sleep(monkeypatch):
    cap = _capture(1000)
    rx = FileReplayReceiver(cap, _cfg())  # paced=False, loop=False (batch default)
    slept: list[float] = []
    monkeypatch.setattr("rfobserver.capture.replay_receiver.time.sleep", lambda s: slept.append(s))
    buf = np.empty(500, dtype=np.int32)
    rx.recv_chunk(buf)
    assert slept == []
```

- [ ] **Step 2: Run to verify failure.**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_replay_receiver.py -x -q`
Expected: FAIL (constructor lacks `paced`/`loop`/`speed`/`set_speed`; no `time` import).

- [ ] **Step 3: Implement paced + looping.**

In `src/rfobserver/capture/replay_receiver.py`, add `import time` and `import threading` (already imports threading). Extend `__init__` and `recv_chunk`:
```python
    def __init__(
        self,
        capture: SigmfCapture,
        receiver_config: ReceiverConfig,
        max_samples: int | None = None,
        *,
        paced: bool = False,
        loop: bool = False,
        speed: float = 1.0,
        source_name: str = "",
    ) -> None:
        super().__init__(receiver_config=receiver_config)
        self._cap = capture
        self._serial = "REPLAY"
        self._pos = 0
        self._n = (
            capture.num_samples if max_samples is None else min(capture.num_samples, max_samples)
        )
        self._paced = paced
        self.loop = loop
        self.source_name = source_name
        self._speed_lock = threading.Lock()
        self._speed = max(0.01, float(speed))
        self._exhausted = threading.Event()
        # ... (keep the existing drain-noise floor estimation block unchanged) ...

    @property
    def speed(self) -> float:
        with self._speed_lock:
            return self._speed

    def set_speed(self, speed: float) -> None:
        with self._speed_lock:
            self._speed = max(0.01, float(speed))
```
Rewrite `recv_chunk` so looping seeks to 0 and pacing sleeps after the fill:
```python
    def recv_chunk(self, out_buf: np.ndarray[Any, np.dtype[Any]]) -> int:
        n = len(out_buf)
        filled = 0
        while filled < n:
            remaining = self._n - self._pos
            if remaining <= 0:
                if self.loop:
                    self._pos = 0
                    remaining = self._n
                    if remaining <= 0:
                        break
                else:
                    self._exhausted.set()
                    self._fill_drain(out_buf, start=filled)
                    filled = n
                    break
            take = min(n - filled, remaining)
            sl = self._cap.raw[self._pos * 2 : (self._pos + take) * 2]
            out_buf[filled : filled + take] = to_sc16_int32(sl, self._cap.datatype)
            self._pos += take
            filled += take
        if self._paced:
            # Pace to wall-clock: n samples at sample_rate * speed.
            delay = n / (self._cap.sample_rate_hz * self.speed)
            if delay > 0:
                time.sleep(delay)
        return n
```
(Keep `_fill_drain`, `exhausted`, and the floor estimate as they are. `to_sc16_int32` and `Any` are already imported.)

- [ ] **Step 4: Run to verify pass.**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_replay_receiver.py -x -q`
Expected: PASS (3 tests). Then run the existing replay integration test to confirm batch mode is unchanged: `PYTHONPATH= .venv/bin/pytest tests/integration/test_replay_reference.py -x -q`.

- [ ] **Step 5: Lint + commit.**

```bash
ruff check src/ tests/ && ruff format --check src/ tests/
PYTHONPATH= .venv/bin/mypy src/rfobserver/
git add src/rfobserver/capture/replay_receiver.py tests/unit/test_replay_receiver.py
git commit -m "replay_receiver: paced + looping modes with live speed control"
```

---

### Task 2: `parse_capture_filename` helper

**Files:**
- Modify: `src/rfobserver/capture/sigmf_reader.py`
- Test: `tests/unit/test_sigmf_reader.py` (append)

**Interfaces:**
- Produces: `parse_capture_filename(name: str) -> dict[str, float]` returning any of `{"center_freq_hz", "sample_rate_hz", "duration_sec", "gain_db"}` it can extract from the HCRO convention `_<c>MHz_<sr>Msps_<dur>s_<gain>dB_`; omits keys not present.

- [ ] **Step 1: Write the failing test.** Append to `tests/unit/test_sigmf_reader.py`:
```python
def test_parse_capture_filename_hcro_convention():
    from rfobserver.capture.sigmf_reader import parse_capture_filename

    name = "iq_capture_hcro-rpi-002_2025-12-12T19-33-52.92Z_915MHz_26.0Msps_20.0s_35dB_ssm_fhss_OVF.dat"
    p = parse_capture_filename(name)
    assert p["center_freq_hz"] == 915e6
    assert p["sample_rate_hz"] == 26.0e6
    assert p["duration_sec"] == 20.0
    assert p["gain_db"] == 35.0


def test_parse_capture_filename_missing_fields_omitted():
    from rfobserver.capture.sigmf_reader import parse_capture_filename

    assert parse_capture_filename("random_file.dat") == {}
    p = parse_capture_filename("x_915MHz_26.0Msps.dat")
    assert p == {"center_freq_hz": 915e6, "sample_rate_hz": 26.0e6}
```

- [ ] **Step 2: Run to verify failure.**
Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_sigmf_reader.py -x -q -k parse_capture_filename`
Expected: FAIL (function not defined).

- [ ] **Step 3: Implement.** Add to `sigmf_reader.py` (with `import re` at top if absent):
```python
def parse_capture_filename(name: str) -> dict[str, float]:
    """Extract tuning params from the HCRO capture-filename convention.

    Matches `_<c>MHz_`, `_<sr>Msps_`, `_<dur>s_`, `_<gain>dB_` (any subset),
    returning Hz / seconds / dB. Keys with no match are omitted so callers can
    prefill a form and leave the rest editable.
    """
    out: dict[str, float] = {}
    patterns = {
        "center_freq_hz": (r"_(\d+(?:\.\d+)?)MHz", 1e6),
        "sample_rate_hz": (r"_(\d+(?:\.\d+)?)Msps", 1e6),
        "duration_sec": (r"_(\d+(?:\.\d+)?)s(?:_|\.)", 1.0),
        "gain_db": (r"_(\d+(?:\.\d+)?)dB", 1.0),
    }
    for key, (pat, scale) in patterns.items():
        m = re.search(pat, name)
        if m is not None:
            out[key] = float(m.group(1)) * scale
    return out
```

- [ ] **Step 4: Run to verify pass.**
Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_sigmf_reader.py -x -q -k parse_capture_filename`
Expected: PASS.

- [ ] **Step 5: Lint + commit.**
```bash
ruff check src/ tests/ && ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/
git add src/rfobserver/capture/sigmf_reader.py tests/unit/test_sigmf_reader.py
git commit -m "sigmf_reader: parse_capture_filename for HCRO tuning params"
```

---

### Task 3: `replay_mode` gating in `StreamingProcessor`

**Files:**
- Modify: `src/rfobserver/pipeline/streaming.py`
- Test: `tests/unit/test_streaming_replay_mode.py` (create)

**Interfaces:**
- Consumes: existing `StreamingProcessor.__init__` (add trailing kwarg).
- Produces: `StreamingProcessor(..., replay_mode: bool = False)` -> `self._replay_mode`. When true: `_drain_burst_results` skips `insert_detection`; `_publish_processed` skips ZMS submit + NATS `publish_stats`; `_begin_recording` is a no-op. `_broadcast.publish` unaffected.

- [ ] **Step 1: Write the failing test.** Create `tests/unit/test_streaming_replay_mode.py`. Build a `StreamingProcessor` with mock db/broadcast/zms/nats and drive the specific methods directly (no full run):
```python
"""replay_mode suppresses persistence/egress while keeping the live view."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from rfobserver.config import AppSettings


def _proc(replay_mode: bool, tmp_path):
    from rfobserver.pipeline.streaming import StreamingProcessor

    settings = AppSettings(_env_file=None, STORAGE_PATH=str(tmp_path), DB_PATH=str(tmp_path / "d.db"))
    db = MagicMock()
    db.insert_detection = AsyncMock()
    storage = MagicMock()
    storage.storage_path = tmp_path
    zms = MagicMock()
    nats = MagicMock()
    nats.publish_stats = AsyncMock()
    proc = StreamingProcessor(
        receiver=MagicMock(),
        database=db,
        local_storage=storage,
        settings=settings,
        broadcast=None,
        zms_monitor=zms,
        nats_producer=nats,
        replay_mode=replay_mode,
    )
    return proc, db, zms, nats


@pytest.mark.asyncio
async def test_replay_mode_skips_insert(tmp_path):
    proc, db, _zms, _nats = _proc(True, tmp_path)
    # queue one burst result and drain
    proc._burst_result_queue.put_nowait(([_fake_burst()], 915e6))
    proc._burst_result_queue.put_nowait(None)
    await proc._drain_burst_results()
    db.insert_detection.assert_not_called()


@pytest.mark.asyncio
async def test_normal_mode_inserts(tmp_path):
    proc, db, _zms, _nats = _proc(False, tmp_path)
    proc._burst_result_queue.put_nowait(([_fake_burst()], 915e6))
    proc._burst_result_queue.put_nowait(None)
    await proc._drain_burst_results()
    db.insert_detection.assert_called()


def test_replay_mode_begin_recording_is_noop(tmp_path):
    proc, _db, _zms, _nats = _proc(True, tmp_path)
    proc.start_recording()
    assert proc._recording_state == "idle"


def _fake_burst():
    from datetime import datetime, timezone

    b = MagicMock()
    b.burst_id = "b1"
    b.start_time = datetime.now(timezone.utc)
    b.stop_time = datetime.now(timezone.utc)
    b.center_freq_hz = 915e6
    b.bandwidth_hz = 1e6
    b.peak_power_db = -30.0
    b.duration_ms = 5.0
    b.detection_timestamp = datetime.now(timezone.utc)
    b.peak_freq_hz = 915e6
    return b
```
(If `_drain_burst_results` reads burst-result tuples differently, adjust `_fake_burst`/the queued item to match the real shape seen in `streaming.py` around line 1510. Consult the file before finalizing the fixture.)

- [ ] **Step 2: Run to verify failure.**
Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_streaming_replay_mode.py -x -q`
Expected: FAIL (`replay_mode` kwarg unknown).

- [ ] **Step 3: Implement gating.**
- In `__init__` (after `drop_on_overflow` param), add `replay_mode: bool = False` and `self._replay_mode = replay_mode`.
- In `_drain_burst_results` (~line 1524), wrap the `await self._db.insert_detection(...)` call: `if not self._replay_mode: await self._db.insert_detection(...)`. Keep consuming the queue and any live-broadcast side effects.
- In `_publish_processed` (~line 1455), guard the ZMS submit (~1483) and NATS `publish_stats` (~1495): `if self._replay_mode: return` at the top of the publish body (after computing anything the live UI still needs), OR wrap each egress call in `if not self._replay_mode:`. Do not suppress the live `_broadcast` calls (those are in the broadcast path, not `_publish_processed`).
- In `_begin_recording` (~line 639), first line: `if self._replay_mode: return`.

- [ ] **Step 4: Run to verify pass.**
Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_streaming_replay_mode.py -x -q` -> PASS. Then full unit: `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`.

- [ ] **Step 5: Lint + commit.**
```bash
ruff check src/ tests/ && ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/
git add src/rfobserver/pipeline/streaming.py tests/unit/test_streaming_replay_mode.py
git commit -m "streaming: replay_mode gates DB/NATS/ZMS/recording, keeps live overlay"
```

---

### Task 4: Supervisor `start_replay`/`stop_replay` + `replay_status` + `build_processor` kwarg

**Files:**
- Modify: `src/rfobserver/pipeline/supervisor.py`, `src/rfobserver/pipeline/app.py`
- Test: `tests/unit/test_supervisor.py` (create or append)

**Interfaces:**
- Consumes: `build_processor` now `Callable[[IReceiver], Any]` -> called as `self._build_processor(receiver, replay_mode=self._replay)`.
- Produces on `PipelineSupervisor`: `async def start_replay(self, receiver: IReceiver) -> None`, `async def stop_replay(self) -> None`, `def replay_status(self) -> dict[str, Any] | None`.
- `pipeline/app.py`: `build_processor(receiver, *, replay_mode: bool = False)` forwards `replay_mode` to `StreamingProcessor`; `_heartbeat_loop` payload gains `"replay": supervisor.replay_status()`.

- [ ] **Step 1: Write the failing test.** Create `tests/unit/test_supervisor.py`:
```python
"""Supervisor replay override + status."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from rfobserver.pipeline.supervisor import PipelineSupervisor


class _FakeReceiver:
    def __init__(self, name):
        self.source_name = name
        self.speed = 1.0
        self.loop = True

    def initialize(self):  # off-loop in _start
        pass

    def close(self):
        pass


class _FakeProc:
    def __init__(self):
        self.stopped = False

    async def run(self):
        import asyncio

        while not self.stopped:
            await asyncio.sleep(0.01)

    def stop(self):
        self.stopped = True


@pytest.mark.asyncio
async def test_start_replay_uses_override_and_reports_status():
    seen = {}

    def build_receiver():
        return _FakeReceiver("SDR")

    def build_processor(receiver, *, replay_mode=False):
        seen["replay_mode"] = replay_mode
        seen["receiver"] = receiver
        return _FakeProc()

    sup = PipelineSupervisor(build_receiver, build_processor)
    assert sup.replay_status() is None

    rx = _FakeReceiver("ssm_fhss_OVF.dat")
    rx.speed = 2.0
    await sup.start_replay(rx)
    assert seen["replay_mode"] is True
    assert seen["receiver"] is rx
    st = sup.replay_status()
    assert st == {"source": "ssm_fhss_OVF.dat", "speed": 2.0, "looping": True}

    await sup.stop_replay()
    assert sup.replay_status() is None
    assert not sup.active
```

- [ ] **Step 2: Run to verify failure.**
Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_supervisor.py -x -q`
Expected: FAIL (no `start_replay`).

- [ ] **Step 3: Implement supervisor changes.** In `supervisor.py`:
- In `__init__`, add `self._receiver_override: IReceiver | None = None` and `self._replay = False`.
- In `_start`, replace `receiver = self._build_receiver()` with `receiver = self._receiver_override or self._build_receiver()`, and replace `processor = self._build_processor(receiver)` with `processor = self._build_processor(receiver, replay_mode=self._replay)`.
- Add:
```python
    async def start_replay(self, receiver: IReceiver) -> None:
        """Stop any live pipeline and start with `receiver` in replay mode."""
        if self._active:
            await self.set_active(False)
        async with self._lock:
            self._receiver_override = receiver
            self._replay = True
            await self._start()

    async def stop_replay(self) -> None:
        """Stop the replay pipeline and clear the override (leaves sensor stopped)."""
        async with self._lock:
            if self._active:
                await self._stop()
            self._receiver_override = None
            self._replay = False

    def replay_status(self) -> dict[str, Any] | None:
        if not self._replay or self._receiver is None:
            return None
        rx = self._receiver
        return {
            "source": getattr(rx, "source_name", "") or "replay",
            "speed": float(getattr(rx, "speed", 1.0)),
            "looping": bool(getattr(rx, "loop", False)),
        }
```
Note: `_start`/`_stop` already acquire nothing themselves; `set_active` holds `self._lock`. To avoid a re-entrant deadlock, `start_replay` calls `set_active(False)` (which takes the lock) BEFORE taking the lock itself. Verify `_start`/`_stop` do not take `self._lock` internally (they don't in the current code — `set_active` does). Keep it that way.

- [ ] **Step 4: Implement app.py changes.** In `pipeline/app.py`:
- Change `def build_processor(receiver: IReceiver) -> Any:` to `def build_processor(receiver: IReceiver, *, replay_mode: bool = False) -> Any:` and pass `replay_mode=replay_mode` to the `StreamingProcessor(...)` constructor (leave the `ContinuousProcessor` branch unchanged; replay always uses streaming).
- In `_heartbeat_loop`, add `"replay": supervisor.replay_status(),` to the published heartbeat dict (alongside `"recording": rec_status`).

- [ ] **Step 5: Run to verify pass.**
Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_supervisor.py -x -q` -> PASS. Full unit: `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`.

- [ ] **Step 6: Lint + commit.**
```bash
ruff check src/ tests/ && ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/
git add src/rfobserver/pipeline/supervisor.py src/rfobserver/pipeline/app.py tests/unit/test_supervisor.py
git commit -m "supervisor: replay override + status; heartbeat carries replay state"
```

---

### Task 5: Config `REPLAY_SOURCE_DIR` + `/api/replay/{start,stop,speed}` + `/api/sensor` replay field

**Files:**
- Modify: `src/rfobserver/config.py`, `src/rfobserver/web/routes/api.py`
- Test: `tests/unit/test_replay_routes.py` (create)

**Interfaces:**
- Consumes: `app.state.supervisor`, `app.state.settings`, `_validate_filename` + storage helpers from `captures.py` (import or replicate the storage-path resolution), `load_raw`, `FileReplayReceiver`, `ReceiverConfig`.
- Produces: `POST /api/replay/start`, `POST /api/replay/stop`, `POST /api/replay/speed`; `GET /api/sensor` response gains `"replay"` (mirror of `supervisor.replay_status()`).

- [ ] **Step 1: Write the failing test.** Create `tests/unit/test_replay_routes.py`. Use a fake supervisor on `app.state` to avoid real hardware. Seed a raw `.dat` under a temp `REPLAY_SOURCE_DIR`.
```python
"""Replay control endpoints."""

from __future__ import annotations

import numpy as np
import pytest

from rfobserver.config import AppSettings
from rfobserver.web.app import create_app


class _FakeSupervisor:
    def __init__(self):
        self.active = False
        self._replay = None
        self.started_with = None

    async def start_replay(self, receiver):
        self.started_with = receiver
        self._replay = receiver

    async def stop_replay(self):
        self._replay = None

    def replay_status(self):
        if self._replay is None:
            return None
        rx = self._replay
        return {"source": rx.source_name, "speed": rx.speed, "looping": rx.loop}

    async def set_active(self, v):
        self.active = v
        return v


@pytest.fixture
def app_ctx(tmp_path):
    src = tmp_path / "replays"
    src.mkdir()
    # a tiny valid ci16_le raw file (100 samples)
    (src / "iq_capture_x_915MHz_1.0Msps_0.1s_30dB_test.dat").write_bytes(
        np.arange(200, dtype=np.int16).tobytes()
    )
    settings = AppSettings(
        _env_file=None,
        STORAGE_PATH=str(tmp_path / "storage"),
        DB_PATH=str(tmp_path / "d.db"),
        REPLAY_SOURCE_DIR=str(src),
    )
    app = create_app(settings)
    app.state.supervisor = _FakeSupervisor()
    return app, src


def _client(app):
    from fastapi.testclient import TestClient

    return TestClient(app)


def test_start_raw_replay_then_status_and_stop(app_ctx):
    app, src = app_ctx
    c = _client(app)
    path = str(src / "iq_capture_x_915MHz_1.0Msps_0.1s_30dB_test.dat")
    r = c.post(
        "/api/replay/start",
        json={"path": path, "sample_rate_hz": 1e6, "center_freq_hz": 915e6,
              "gain_db": 30, "datatype": "ci16_le", "speed": 2.0},
    )
    assert r.status_code == 200
    s = c.get("/api/sensor").json()
    assert s["replay"]["speed"] == 2.0 and s["replay"]["looping"] is True
    assert c.post("/api/replay/stop").status_code == 200
    assert c.get("/api/sensor").json()["replay"] is None


def test_speed_requires_active_replay(app_ctx):
    app, _src = app_ctx
    c = _client(app)
    assert c.post("/api/replay/speed", json={"speed": 4.0}).status_code == 409


def test_path_outside_allowlist_rejected(app_ctx):
    app, _src = app_ctx
    c = _client(app)
    r = c.post("/api/replay/start", json={"path": "/etc/passwd", "sample_rate_hz": 1e6})
    assert r.status_code in (400, 404)
```

- [ ] **Step 2: Run to verify failure.**
Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_replay_routes.py -x -q`
Expected: FAIL (`REPLAY_SOURCE_DIR` unknown / routes absent).

- [ ] **Step 3: Add the setting.** In `config.py`, add near other paths: `REPLAY_SOURCE_DIR: str = ""  # allowlist root for raw replay files; empty disables raw replay`.

- [ ] **Step 4: Implement the endpoints.** In `web/routes/api.py`, add (import `Path`, `load_raw`, `FileReplayReceiver`, `ReceiverConfig`, `parse_capture_filename` lazily inside handlers to avoid import cycles):
```python
def _resolve_replay_path(request: Request, path_str: str) -> Path:
    from pathlib import Path

    settings = request.app.state.settings
    roots = []
    if settings.REPLAY_SOURCE_DIR:
        roots.append(Path(settings.REPLAY_SOURCE_DIR).resolve())
    roots.append(Path(settings.STORAGE_PATH).resolve())
    p = Path(path_str).resolve()
    if not any(p.is_relative_to(r) for r in roots):
        raise HTTPException(status_code=400, detail="Path not in an allowed replay directory")
    if not p.exists():
        raise HTTPException(status_code=404, detail="Replay file not found")
    return p


@router.post("/replay/start")
async def replay_start(request: Request) -> dict[str, Any]:
    from rfobserver.capture.receiver import ReceiverConfig
    from rfobserver.capture.replay_receiver import FileReplayReceiver
    from rfobserver.capture.sigmf_reader import load_raw

    supervisor = getattr(request.app.state, "supervisor", None)
    if supervisor is None:
        raise HTTPException(status_code=503, detail="Pipeline not available")
    settings = request.app.state.settings
    body = await request.json()

    # Managed capture (filename) -> read params from its .json; else raw path.
    filename = body.get("filename")
    if filename:
        from rfobserver.web.routes.captures import _get_storage, _validate_filename

        storage = _get_storage(request)
        base = filename.replace(".sc16", "").replace(".json", "")
        sc16 = _validate_filename(base + ".sc16", storage)
        import json as _json

        meta = _json.loads(sc16.with_suffix(".json").read_text())
        path = sc16
        sample_rate = float(meta["sample_rate_hz"])
        center = float(meta["center_freq_hz"])
        gain = float(meta.get("gain_db", settings.GAIN))
        datatype = "ci16_le"
    else:
        path = _resolve_replay_path(request, body["path"])
        sample_rate = float(body["sample_rate_hz"])
        center = float(body.get("center_freq_hz", 0.0))
        gain = float(body.get("gain_db", settings.GAIN))
        datatype = body.get("datatype", "ci16_le")

    speed = float(body.get("speed", 1.0))
    cap = load_raw(path, datatype=datatype, sample_rate_hz=sample_rate, center_freq_hz=center)
    rx_cfg = ReceiverConfig(gain_db=int(gain), bandwidth_hz=int(sample_rate), duration_sec=1.0)
    receiver = FileReplayReceiver(
        cap, rx_cfg, paced=True, loop=True, speed=speed, source_name=Path(path).name
    )

    # Snapshot + set tuning so the pipeline runs at the capture's config.
    snap = {k: getattr(settings, k) for k in
            ("BANDWIDTH", "FREQUENCY_START", "FREQUENCY_STEP", "FREQUENCY_END", "GAIN")}
    snap["_active"] = bool(supervisor.active)
    request.app.state._replay_snapshot = snap
    object.__setattr__(settings, "BANDWIDTH", int(sample_rate))
    object.__setattr__(settings, "FREQUENCY_START", int(center))
    object.__setattr__(settings, "FREQUENCY_STEP", 0)
    object.__setattr__(settings, "FREQUENCY_END", int(center))
    object.__setattr__(settings, "GAIN", int(gain))

    await supervisor.start_replay(receiver)
    return {"replay": supervisor.replay_status()}


@router.post("/replay/stop")
async def replay_stop(request: Request) -> dict[str, Any]:
    supervisor = getattr(request.app.state, "supervisor", None)
    if supervisor is None:
        raise HTTPException(status_code=503, detail="Pipeline not available")
    await supervisor.stop_replay()
    settings = request.app.state.settings
    snap = getattr(request.app.state, "_replay_snapshot", None)
    prior_active = False
    if snap:
        prior_active = bool(snap.pop("_active", False))
        for k, v in snap.items():
            object.__setattr__(settings, k, v)
        request.app.state._replay_snapshot = None
    if prior_active:
        await supervisor.set_active(True)
    return {"replay": None}


@router.post("/replay/speed")
async def replay_speed(request: Request) -> dict[str, Any]:
    supervisor = getattr(request.app.state, "supervisor", None)
    rx = getattr(supervisor, "receiver", None) if supervisor is not None else None
    if supervisor is None or supervisor.replay_status() is None or rx is None:
        raise HTTPException(status_code=409, detail="No active replay")
    body = await request.json()
    rx.set_speed(float(body["speed"]))
    return {"replay": supervisor.replay_status()}
```
Then extend the existing `GET /api/sensor` handler to include `"replay": supervisor.replay_status() if supervisor is not None else None` in its returned dict.
(Note: the test's `_FakeSupervisor` lacks a `receiver` attribute, so `test_speed_requires_active_replay` hits the 409 branch via `replay_status() is None`. The real supervisor exposes `.receiver`.)

- [ ] **Step 5: Run to verify pass.**
Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_replay_routes.py -x -q` -> PASS. Full unit: `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`.

- [ ] **Step 6: Lint + commit.**
```bash
ruff check src/ tests/ && ruff format --check src/ tests/ && PYTHONPATH= .venv/bin/mypy src/rfobserver/
git add src/rfobserver/config.py src/rfobserver/web/routes/api.py tests/unit/test_replay_routes.py
git commit -m "api: replay start/stop/speed endpoints + sensor replay field + REPLAY_SOURCE_DIR allowlist"
```

---

### Task 6: UI - Captures-page launcher + dashboard replay banner

**Files:**
- Modify: `src/rfobserver/web/templates/captures.html`, `src/rfobserver/web/templates/dashboard.html`, `src/rfobserver/web/static/style.css`
- Manual verify (no unit tests for DOM).

- [ ] **Step 1: Captures-page controls.** In `captures.html`:
  - On the selected-capture detail (next to the existing viewer actions), add a `Replay this capture` button that calls `POST /api/replay/start {filename}` then `location.href = "/"` (dashboard). Disable it while a request is in flight.
  - Add a `Replay a raw file` panel: a text input for `path`; on `change`, run a client-side regex mirroring `parse_capture_filename` (`_(\d+(?:\.\d+)?)MHz`, `Msps`, `s`, `dB`) to prefill editable number inputs `center_freq_hz` (MHz shown), `sample_rate_hz` (Msps shown), `gain_db`, and a `datatype` select (default `ci16_le`); a `speed` select (0.25/1/2/4); and a `Start replay` button -> `POST /api/replay/start {path, sample_rate_hz, center_freq_hz, gain_db, datatype, speed}` -> `location.href = "/"`. Show server error text on non-200.

- [ ] **Step 2: Dashboard banner.** In `dashboard.html`:
  - Add a hidden `<div id="replay-banner">` (styled bar) above the waterfall containing: a source-name span, a `looping` label, a speed `<select>` (0.25/1/2/4x) that POSTs `/api/replay/speed`, a `Stop` button that POSTs `/api/replay/stop`, and three number inputs (`trigger_threshold_db`, `burst_threshold_high_db`, `burst_threshold_low_ratio`) that POST to the existing `/config/apply` on change (same payload shape the config page uses).
  - In the existing `ws.onmessage` `data.type === "heartbeat"` handler (~line 927), read `data.replay`: if present, populate + show `#replay-banner` (set the speed select + threshold inputs from it if provided) and set a body class/flag; if `null`, hide the banner. Do not otherwise alter the dashboard.

- [ ] **Step 3: Styles.** In `style.css`, add `#replay-banner` (Apple-style bar: subtle accent tint `rgba(0,113,227,0.08)`, rounded, padding, flex row, hidden by default via a `.hidden` utility or inline `display:none` toggled in JS). Reuse existing input/button classes.

- [ ] **Step 4: Manual verification.**
```bash
pkill -f "[r]fobserver run" 2>/dev/null; rm -rf /tmp/rfobs_replaytest
PYTHONPATH= RFOBS_MOCK_RECEIVER=true RFOBS_SENSOR_ACTIVE=false RFOBS_WEB_PORT=8891 \
  RFOBS_STORAGE_PATH=/tmp/rfobs_replaytest RFOBS_DB_PATH=/tmp/rfobs_replaytest/d.db \
  RFOBS_REPLAY_SOURCE_DIR=/home/orencollaco/Documents \
  .venv/bin/rfobserver run &
```
Open `http://localhost:8891/captures`, use the raw-file panel with the December `.dat` path, Start; confirm the dashboard shows the banner, the waterfall scrolls (~1x), speed changes take effect, threshold edits change the live overlay, Stop hides the banner and restores tuning. (Use puppeteer headless as in prior UI verification. Static-verify JS with `node --check` on the templates' script blocks if no browser.) Confirm no `.sc16`/detections rows are written during replay.

- [ ] **Step 5: Commit.**
```bash
ruff check src/ tests/ && ruff format --check src/ tests/
git add src/rfobserver/web/templates/captures.html src/rfobserver/web/templates/dashboard.html src/rfobserver/web/static/style.css
git commit -m "ui: capture-replay launcher on captures page + conditional dashboard replay banner"
```

---

### Task 7: Full verification + finish
- [ ] `ruff check src/ tests/ && ruff format --check src/ tests/`; `PYTHONPATH= .venv/bin/mypy src/rfobserver/`; `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`; `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q`.
- [ ] Real replay smoke of the December SSM `.dat` on this dev box (26 MS/s, 20 s loop): confirm SSM bursts appear in the live overlay, threshold edits change detections, nothing written to DB/captures.
- [ ] Local Jetson (nano-super) smoke per standing practice. Do NOT touch HCRO; provide the user redeploy/test steps for HCRO.
- [ ] superpowers:finishing-a-development-branch.

## Self-Review
- Spec coverage: Part 0/pacing -> Task 1; filename parse -> Task 2; ephemeral gating -> Task 3; supervisor override + heartbeat -> Task 4; endpoints + allowlist + sensor field -> Task 5; UI banner + launcher -> Task 6; verification -> Task 7.
- Types consistent: `FileReplayReceiver(..., paced, loop, speed, source_name)` + `set_speed`/`speed`/`loop`/`source_name` used identically in Tasks 1/4/5; `build_processor(receiver, *, replay_mode=False)` defined in Task 4 and relied on by the supervisor in the same task; `replay_status()` shape `{source, speed, looping}` produced in Task 4 and consumed in Tasks 5/6.
- No placeholders; every code step shows the code. Replay never persists/egresses (Task 3 gating, asserted by tests).
- Deadlock note captured in Task 4 (start_replay takes the lock only after set_active releases it).
