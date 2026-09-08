# rtl_433 Burst Attribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Attribute a protocol (and decoded fields) to live-detected RF bursts by channelizing the strongest ones out of the in-memory IQ and decoding them with an in-process rtl_433 worker, writing the result onto the burst's `detections` row.

**Architecture:** In the continuous processing path, after `detect_bursts`, gate bursts by SNR and top-N power, channelize each survivor to a narrowband `.cs16` and enqueue it on a bounded drop-strongest queue. A single background worker drains the queue, runs rtl_433 per a bandwidth-keyed rate/protocol policy, and merges the decode (or an attempted-marker) into the `detections` row by `burst_id`.

**Tech Stack:** Python 3.10, numpy, scipy.signal, aiosqlite, asyncio, rtl_433 (master build, file-only), pytest + pytest-asyncio.

**Spec:** `docs/superpowers/specs/2026-09-07-rtl433-burst-attribution-design.md`

## Global Constraints

- **Python >= 3.10 clean.** The live/deploy Jetsons run 3.10; no 3.11+ syntax.
- **Always run commands with the `PYTHONPATH=` prefix** (host leaks system 3.10 packages into the venv).
- **Pre-commit checks, in this order:** `ruff check src/ tests/`; `ruff format --check src/ tests/`; `PYTHONPATH= .venv/bin/mypy src/rfobserver/`; `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`; `PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q` (integration needs NATS on localhost:4222).
- **No emojis anywhere. No em-dashes** in code, comments, or docs.
- **Never** add a `Co-Authored-By: Claude` trailer to commits.
- **Stage only explicit paths** (never `git add -A` / `git add .`).
- **Feature is OFF by default** (`ATTRIBUTION_ENABLED=False`); inert without an rtl_433 binary.
- **rtl_433 on the target** is at `~/rtl_433_build/build/src/rtl_433` (aarch64, master, protocol 383), built file-only. Fixtures at `~/ssn_bursts/*.cs16`.
- **Two operating rates** (26 and 56 Msps): the channelizer derives the resample ratio dynamically from the live `sample_rate_hz`.

---

## File Structure

- **Create** `src/rfobserver/processing/channelize.py` - pure DSP: resample-ratio, shift+resample+int16 channelization, and the bandwidth-keyed rate/protocol policy. No I/O, no subprocess.
- **Create** `src/rfobserver/pipeline/attribution.py` - rtl_433 discovery, synchronous decode of a `.cs16` blob, the bounded drop-strongest queue, and the async `AttributionWorker` that drains + merges.
- **Modify** `src/rfobserver/storage/database.py` - three attribution columns via the existing migration, plus `update_detection_attribution`.
- **Modify** `src/rfobserver/config.py` - attribution settings block.
- **Modify** `src/rfobserver/pipeline/continuous.py` - construct/own the worker; producer hook in `_store_and_broadcast` (and carry `noise_floor_db` on `_ProcessResult`).
- **Create tests** under `tests/unit/` per task; one integration test under `tests/integration/`.

---

### Task 1: Channelize primitive + rate/protocol policy

**Files:**
- Create: `src/rfobserver/processing/channelize.py`
- Test: `tests/unit/test_channelize.py`

**Interfaces:**
- Consumes: nothing (leaf module).
- Produces:
  - `resample_ratio(sample_rate_hz: int, target_rate_hz: int) -> tuple[int, int]` returns `(up, down)` gcd-reduced.
  - `channelize_to_cs16(iq: np.ndarray, sample_rate_hz: float, offset_hz: float, target_rate_hz: int) -> bytes` returns interleaved little-endian int16 I/Q at `target_rate_hz`.
  - `select_rate_and_protocols(bandwidth_hz: float) -> tuple[int, list[list[str]]]` returns `(target_rate_hz, passes)` where each pass is an rtl_433 arg list.
  - Constant `TIER_BANDWIDTH_HZ: float = 200_000.0`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_channelize.py
import numpy as np
from rfobserver.processing.channelize import (
    resample_ratio,
    channelize_to_cs16,
    select_rate_and_protocols,
    TIER_BANDWIDTH_HZ,
)


def test_resample_ratio_reduces():
    assert resample_ratio(26_000_000, 1_600_000) == (4, 65)
    assert resample_ratio(56_000_000, 1_600_000) == (1, 35)


def test_channelize_shifts_offset_to_dc():
    # A pure tone at +300 kHz in a 26 Msps band must land at DC after channelizing
    # with offset_hz=300k, so its energy concentrates in the lowest FFT bins.
    fs = 26_000_000
    n = 260_000  # 10 ms
    t = np.arange(n)
    tone = np.exp(2j * np.pi * (300_000 / fs) * t).astype(np.complex64)
    cs16 = channelize_to_cs16(tone, fs, 300_000.0, 1_600_000)
    iq = np.frombuffer(cs16, dtype="<i2").astype(np.float32)
    ch = iq[0::2] + 1j * iq[1::2]
    spec = np.abs(np.fft.fftshift(np.fft.fft(ch)))
    peak = int(np.argmax(spec))
    center = len(spec) // 2
    assert abs(peak - center) <= 2  # peak sits at DC (center bin)


def test_channelize_output_is_interleaved_int16_at_target_len():
    fs = 26_000_000
    iq = np.ones(26_000, dtype=np.complex64)  # 1 ms
    cs16 = channelize_to_cs16(iq, fs, 0.0, 1_600_000)
    arr = np.frombuffer(cs16, dtype="<i2")
    assert arr.dtype == np.dtype("<i2")
    assert len(arr) % 2 == 0
    # 1 ms at 1.6 Msps ~= 1600 complex samples => ~3200 int16 (allow FIR edge slack)
    assert 3000 <= len(arr) <= 3400


def test_policy_two_tier_by_bandwidth():
    rate_wide, passes_wide = select_rate_and_protocols(TIER_BANDWIDTH_HZ + 1)
    assert rate_wide == 1_600_000
    assert ["-R", "383"] in passes_wide
    rate_narrow, passes_narrow = select_rate_and_protocols(100_000)
    assert rate_narrow == 1_000_000
    assert passes_narrow == [[]]  # one pass, full default set
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_channelize.py -v`
Expected: FAIL (module `rfobserver.processing.channelize` does not exist).

- [ ] **Step 3: Write minimal implementation**

```python
# src/rfobserver/processing/channelize.py
"""Channelize a detected burst out of wideband IQ into a narrowband .cs16 blob
that rtl_433 can decode. Ported from the validated gr-modules ssn_scan.py:
frequency-shift the burst to DC, resample to the target rate (resample_poly's
polyphase FIR is the anti-alias / low-pass stage), and pack as interleaved
little-endian int16. Pure DSP - no file or subprocess I/O.
"""

from __future__ import annotations

from math import gcd

import numpy as np
from scipy import signal as sig

# Bursts at or above this bandwidth take the 1.6 Msps SSN-mesh tier; narrower
# bursts take the 1.0 Msps full-default-decoder tier. Bandwidth-keyed per the
# spec's fixed two-tier policy.
TIER_BANDWIDTH_HZ: float = 200_000.0

_SSN_FLEX = "n=ssnmesh,m=FSK_PCM,s=16,l=16,r=8000"


def resample_ratio(sample_rate_hz: int, target_rate_hz: int) -> tuple[int, int]:
    """Return the gcd-reduced (up, down) for resample_poly to take
    sample_rate_hz -> target_rate_hz."""
    g = gcd(int(sample_rate_hz), int(target_rate_hz))
    return int(target_rate_hz) // g, int(sample_rate_hz) // g


def channelize_to_cs16(
    iq: np.ndarray, sample_rate_hz: float, offset_hz: float, target_rate_hz: int
) -> bytes:
    """Shift offset_hz to DC, resample to target_rate_hz, pack as <i2 I/Q."""
    n = np.arange(len(iq))
    mixer = np.exp(-2j * np.pi * (offset_hz / sample_rate_hz) * n).astype(np.complex64)
    shifted = iq.astype(np.complex64) * mixer
    up, down = resample_ratio(int(sample_rate_hz), int(target_rate_hz))
    res = sig.resample_poly(shifted, up, down)
    peak = float(np.max(np.abs(res))) if len(res) else 1.0
    scale = 30000.0 / (peak or 1.0)
    scaled = res * scale
    out = np.empty(len(res) * 2, dtype="<i2")
    out[0::2] = scaled.real.astype("<i2")
    out[1::2] = scaled.imag.astype("<i2")
    return out.tobytes()


def select_rate_and_protocols(bandwidth_hz: float) -> tuple[int, list[list[str]]]:
    """Bandwidth-keyed two-tier policy. Returns (target_rate_hz, passes) where
    each pass is an rtl_433 argument list (empty list = full default -R set)."""
    if bandwidth_hz >= TIER_BANDWIDTH_HZ:
        return 1_600_000, [["-R", "383"], ["-X", _SSN_FLEX]]
    return 1_000_000, [[]]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_channelize.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Lint, type-check, commit**

```bash
ruff check src/rfobserver/processing/channelize.py tests/unit/test_channelize.py
ruff format --check src/rfobserver/processing/channelize.py tests/unit/test_channelize.py
PYTHONPATH= .venv/bin/mypy src/rfobserver/processing/channelize.py
git add src/rfobserver/processing/channelize.py tests/unit/test_channelize.py
git commit -m "feat(attribution): channelize primitive + rate/protocol policy"
```

---

### Task 2: detections attribution columns + update method

**Files:**
- Modify: `src/rfobserver/storage/database.py` (add `_DETECTION_ATTRIBUTION_COLUMNS` near `_DETECTION_SDR_COLUMNS` ~line 143; extend `_migrate_detection_columns` ~line 254; add `update_detection_attribution`)
- Test: `tests/unit/test_database.py` (append)

**Interfaces:**
- Consumes: existing `SensorDatabase.insert_detection`, `query_detections`.
- Produces: `async SensorDatabase.update_detection_attribution(*, burst_id: str, model: str | None, protocol_id: int | None, attribution: str) -> None`. Adds columns `model TEXT`, `protocol_id INTEGER`, `attribution TEXT` (all nullable).

- [ ] **Step 1: Write the failing test**

```python
# append to tests/unit/test_database.py
async def test_update_detection_attribution_three_state(db):
    """A detection starts unattributed (null), can be marked attempted-not-decoded,
    or attributed with a model/protocol. Merge keys on burst_id."""
    from datetime import datetime, timezone

    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    await db.insert_detection(
        burst_id="b-attr-1",
        start_time=now,
        stop_time=now,
        center_freq_hz=915e6,
        bandwidth_hz=250e3,
        peak_power_db=-40.0,
        duration_ms=20.0,
        detection_timestamp=now,
    )
    rows = await db.query_detections(since=now, until=now)
    row = next(r for r in rows if r["burst_id"] == "b-attr-1")
    assert row["model"] is None and row["protocol_id"] is None and row["attribution"] is None

    await db.update_detection_attribution(
        burst_id="b-attr-1",
        model="SilverSpring-Mesh",
        protocol_id=383,
        attribution='[{"channel": 57}]',
    )
    rows = await db.query_detections(since=now, until=now)
    row = next(r for r in rows if r["burst_id"] == "b-attr-1")
    assert row["model"] == "SilverSpring-Mesh"
    assert row["protocol_id"] == 383
    assert row["attribution"] == '[{"channel": 57}]'


async def test_update_detection_attribution_missing_burst_is_noop(db):
    # Updating a burst_id that is not present affects zero rows and does not raise.
    await db.update_detection_attribution(
        burst_id="does-not-exist", model=None, protocol_id=None, attribution="{}"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py::test_update_detection_attribution_three_state -v`
Expected: FAIL (`AttributeError: 'SensorDatabase' object has no attribute 'update_detection_attribution'`, and/or KeyError on `model`).

- [ ] **Step 3: Write minimal implementation**

Add the column dict next to `_DETECTION_SDR_COLUMNS`:

```python
# near _DETECTION_SDR_COLUMNS (~line 143 in database.py)
_DETECTION_ATTRIBUTION_COLUMNS: dict[str, str] = {
    "model": "TEXT",
    "protocol_id": "INTEGER",
    "attribution": "TEXT",
}
```

Extend `_migrate_detection_columns` to also add the attribution columns (same PRAGMA/ALTER pattern already used for the SDR columns):

```python
    async def _migrate_detection_columns(self) -> None:
        assert self._db is not None
        async with self._db.execute("PRAGMA table_info(detections)") as cursor:
            existing = {row[1] for row in await cursor.fetchall()}
        for column, col_type in {
            **_DETECTION_SDR_COLUMNS,
            **_DETECTION_ATTRIBUTION_COLUMNS,
        }.items():
            if column not in existing:
                await self._db.execute(
                    f"ALTER TABLE detections ADD COLUMN {column} {col_type}"
                )
                logger.info("Migrated detections: added column %s", column)
        await self._db.commit()
```

(If the existing method body differs, preserve its index-creation/commit lines; only fold the attribution dict into the column loop.)

Add the update method (place beside `insert_detection`, and apply the same `@_guarded_write` decorator that the other write methods use):

```python
    @_guarded_write
    async def update_detection_attribution(
        self,
        *,
        burst_id: str,
        model: str | None,
        protocol_id: int | None,
        attribution: str,
    ) -> None:
        """Merge an rtl_433 result onto an existing detection. No-op if the
        burst_id is absent (e.g. pruned)."""
        assert self._db is not None
        await self._db.execute(
            "UPDATE detections SET model = ?, protocol_id = ?, attribution = ? "
            "WHERE burst_id = ?",
            (model, protocol_id, attribution, burst_id),
        )
        await self._db.commit()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -k attribution -v`
Expected: PASS (2 tests). Then run the full file to confirm no regression: `PYTHONPATH= .venv/bin/pytest tests/unit/test_database.py -q`.

- [ ] **Step 5: Lint, type-check, commit**

```bash
ruff check src/rfobserver/storage/database.py tests/unit/test_database.py
ruff format --check src/rfobserver/storage/database.py tests/unit/test_database.py
PYTHONPATH= .venv/bin/mypy src/rfobserver/storage/database.py
git add src/rfobserver/storage/database.py tests/unit/test_database.py
git commit -m "feat(attribution): detections model/protocol_id/attribution columns + merge"
```

---

### Task 3: rtl_433 discovery + synchronous decode

**Files:**
- Create: `src/rfobserver/pipeline/attribution.py`
- Test: `tests/unit/test_attribution_decode.py`

**Interfaces:**
- Consumes: nothing from earlier tasks (rtl_433 is external).
- Produces:
  - `find_rtl433(override: str | None = None) -> str | None` returns a path or None.
  - `decode_cs16(rtl_path: str, cs16_bytes: bytes, target_rate_hz: int, passes: list[list[str]], timeout_sec: float = 30.0) -> list[dict]` runs each pass until one decodes; returns the parsed JSON frame dicts (empty list on no decode).

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_attribution_decode.py
import os
from pathlib import Path

import pytest

from rfobserver.pipeline.attribution import find_rtl433, decode_cs16

FIXTURE = Path.home() / "ssn_bursts" / "burst_feb4_919MHz_75dB.cs16"
RTL = find_rtl433()

pytestmark = pytest.mark.skipif(
    RTL is None or not FIXTURE.exists(),
    reason="rtl_433 or SSN fixtures not present on this host",
)


def test_decode_ssn_fixture():
    # The fixture is already channelized to 1.6 Msps; decode it directly.
    frames = decode_cs16(RTL, FIXTURE.read_bytes(), 1_600_000, [["-R", "383"]])
    assert frames, "expected at least one decoded frame"
    assert frames[0]["model"] == "SilverSpring-Mesh"


def test_decode_noise_returns_empty():
    frames = decode_cs16(RTL, b"\x00\x00" * 4000, 1_600_000, [["-R", "383"]])
    assert frames == []
```

- [ ] **Step 2: Run test to verify it fails**

Run (on the target box, or any host with rtl_433 + fixtures): `PYTHONPATH= .venv/bin/pytest tests/unit/test_attribution_decode.py -v`
Expected: FAIL (module missing). On a host without rtl_433, the tests SKIP - that is acceptable, but do the red/green on a host that has them (the target .153 or the workstation).

- [ ] **Step 3: Write minimal implementation**

```python
# src/rfobserver/pipeline/attribution.py
"""rtl_433 per-burst attribution: discovery, decode of a channelized .cs16 blob,
a bounded drop-strongest queue, and the async worker that drains + merges. The
decode step is synchronous (subprocess); the worker calls it off the event loop.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile

logger = logging.getLogger(__name__)

_KNOWN_BUILD = os.path.expanduser("~/rtl_433_build/build/src/rtl_433")


def find_rtl433(override: str | None = None) -> str | None:
    """Locate rtl_433: explicit override, $RTL433, the known build path, PATH."""
    for cand in (override, os.environ.get("RTL433"), _KNOWN_BUILD, shutil.which("rtl_433")):
        if cand and os.path.exists(cand):
            return cand
    return None


def decode_cs16(
    rtl_path: str,
    cs16_bytes: bytes,
    target_rate_hz: int,
    passes: list[list[str]],
    timeout_sec: float = 30.0,
) -> list[dict]:
    """Run rtl_433 over the .cs16 blob, one pass at a time, returning the first
    pass that decodes anything. Empty list if nothing decodes."""
    with tempfile.NamedTemporaryFile(suffix=".cs16", delete=True) as tf:
        tf.write(cs16_bytes)
        tf.flush()
        for extra in passes:
            cmd = [rtl_path, "-s", f"{target_rate_hz}", "-F", "json", *extra, "-r", tf.name]
            try:
                proc = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=timeout_sec
                )
            except subprocess.TimeoutExpired:
                logger.warning("rtl_433 timed out after %.0fs", timeout_sec)
                continue
            frames = [
                json.loads(line)
                for line in proc.stdout.splitlines()
                if line.strip().startswith("{")
            ]
            if frames:
                return frames
    return []
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_attribution_decode.py -v`
Expected: PASS (2 tests) on a host with rtl_433 + fixtures.

- [ ] **Step 5: Lint, type-check, commit**

```bash
ruff check src/rfobserver/pipeline/attribution.py tests/unit/test_attribution_decode.py
ruff format --check src/rfobserver/pipeline/attribution.py tests/unit/test_attribution_decode.py
PYTHONPATH= .venv/bin/mypy src/rfobserver/pipeline/attribution.py
git add src/rfobserver/pipeline/attribution.py tests/unit/test_attribution_decode.py
git commit -m "feat(attribution): rtl_433 discovery + cs16 decode"
```

---

### Task 4: Bounded drop-strongest queue

**Files:**
- Modify: `src/rfobserver/pipeline/attribution.py` (add `AttributionItem` + `StrongestQueue`)
- Test: `tests/unit/test_attribution_queue.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `AttributionItem` dataclass: `burst_id: str`, `cs16: bytes`, `target_rate_hz: int`, `passes: list[list[str]]`, `power_db: float`.
  - `StrongestQueue(maxsize: int)` with `put_nowait(item: AttributionItem) -> bool` (returns False if the item was dropped, True if accepted; on overflow evicts the lowest `power_db`), `async get() -> AttributionItem` (returns highest `power_db` first), `qsize() -> int`, `dropped: int` counter.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_attribution_queue.py
import asyncio

import pytest

from rfobserver.pipeline.attribution import AttributionItem, StrongestQueue


def _item(power: float, tag: str) -> AttributionItem:
    return AttributionItem(
        burst_id=tag, cs16=b"", target_rate_hz=1_600_000, passes=[[]], power_db=power
    )


def test_queue_keeps_strongest_on_overflow():
    q = StrongestQueue(maxsize=2)
    assert q.put_nowait(_item(-50.0, "a")) is True
    assert q.put_nowait(_item(-40.0, "b")) is True
    # Full. A stronger item evicts the weakest ("a", -50).
    assert q.put_nowait(_item(-30.0, "c")) is True
    assert q.qsize() == 2
    assert q.dropped == 1
    # A weaker-than-all item is itself dropped, queue unchanged.
    assert q.put_nowait(_item(-99.0, "d")) is False
    assert q.qsize() == 2
    assert q.dropped == 2


@pytest.mark.asyncio
async def test_get_returns_strongest_first():
    q = StrongestQueue(maxsize=4)
    for p, tag in [(-50.0, "a"), (-20.0, "b"), (-35.0, "c")]:
        q.put_nowait(_item(p, tag))
    first = await asyncio.wait_for(q.get(), timeout=1.0)
    assert first.burst_id == "b"  # -20 is strongest
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_attribution_queue.py -v`
Expected: FAIL (`ImportError: cannot import name 'StrongestQueue'`).

- [ ] **Step 3: Write minimal implementation**

Add near the top of `attribution.py` (after imports; add `import asyncio` and `from dataclasses import dataclass, field`):

```python
@dataclass
class AttributionItem:
    burst_id: str
    cs16: bytes
    target_rate_hz: int
    passes: list[list[str]]
    power_db: float


class StrongestQueue:
    """Bounded queue that keeps the strongest items. On overflow the weakest
    (lowest power_db) is evicted; get() returns the strongest first. The live
    producer never blocks - put_nowait always returns immediately."""

    def __init__(self, maxsize: int) -> None:
        self._maxsize = maxsize
        self._items: list[AttributionItem] = []
        self._cond = asyncio.Condition()
        self.dropped = 0

    def qsize(self) -> int:
        return len(self._items)

    def put_nowait(self, item: AttributionItem) -> bool:
        if len(self._items) < self._maxsize:
            self._items.append(item)
            self._wake()
            return True
        weakest_idx = min(range(len(self._items)), key=lambda i: self._items[i].power_db)
        if item.power_db <= self._items[weakest_idx].power_db:
            self.dropped += 1
            return False
        self._items.pop(weakest_idx)
        self._items.append(item)
        self.dropped += 1
        self._wake()
        return True

    def _wake(self) -> None:
        # Notify any waiting get() without needing the lock (single-thread loop).
        for waiter in list(getattr(self._cond, "_waiters", []) or []):
            if not waiter.done():
                waiter.set_result(None)
                break

    async def get(self) -> AttributionItem:
        async with self._cond:
            while not self._items:
                await self._cond.wait()
            idx = max(range(len(self._items)), key=lambda i: self._items[i].power_db)
            return self._items.pop(idx)
```

Note for the implementer: `_wake` pokes waiters directly because `put_nowait` is
synchronous (called from the processing path) and cannot `async with` the
condition. If the private-attribute poke is fragile on the target Python, an
acceptable alternative is a plain `asyncio.Event` set on put and cleared in get;
keep the same public surface and re-run the tests.

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_attribution_queue.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Lint, type-check, commit**

```bash
ruff check src/rfobserver/pipeline/attribution.py tests/unit/test_attribution_queue.py
ruff format --check src/rfobserver/pipeline/attribution.py tests/unit/test_attribution_queue.py
PYTHONPATH= .venv/bin/mypy src/rfobserver/pipeline/attribution.py
git add src/rfobserver/pipeline/attribution.py tests/unit/test_attribution_queue.py
git commit -m "feat(attribution): bounded drop-strongest queue"
```

---

### Task 5: AttributionWorker (drain + decode + three-state merge)

**Files:**
- Modify: `src/rfobserver/pipeline/attribution.py` (add `AttributionWorker`)
- Test: `tests/integration/test_attribution_worker.py`

**Interfaces:**
- Consumes: `StrongestQueue`, `AttributionItem`, `decode_cs16` (Tasks 3-4); `SensorDatabase.update_detection_attribution` (Task 2).
- Produces:
  - `AttributionWorker(database, rtl_path: str, queue: StrongestQueue | None = None)`.
  - `worker.queue: StrongestQueue`.
  - `async worker.run() -> None` (drains until cancelled).
  - `worker.stop() -> None` (requests shutdown).
  - Attribution JSON contract: on decode, `attribution` is `json.dumps({"decoded": True, "at": <iso>, "frames": [...]})` with `model` = first frame's `model`, `protocol_id` = numeric protocol (383 for SSN; else null). On no decode, `attribution` = `json.dumps({"attempted": True, "decoded": False, "at": <iso>})`, `model`/`protocol_id` null.

- [ ] **Step 1: Write the failing test**

```python
# tests/integration/test_attribution_worker.py
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from rfobserver.pipeline.attribution import (
    AttributionItem,
    AttributionWorker,
    find_rtl433,
)
from rfobserver.storage.database import SensorDatabase

FIXTURE = Path.home() / "ssn_bursts" / "burst_feb4_919MHz_75dB.cs16"
RTL = find_rtl433()

pytestmark = pytest.mark.skipif(
    RTL is None or not FIXTURE.exists(),
    reason="rtl_433 or SSN fixtures not present on this host",
)


@pytest.mark.asyncio
async def test_worker_writes_attribution(tmp_path):
    db = SensorDatabase(str(tmp_path / "t.db"))
    await db.connect()
    try:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        await db.insert_detection(
            burst_id="w1", start_time=now, stop_time=now, center_freq_hz=919.4e6,
            bandwidth_hz=250e3, peak_power_db=-30.0, duration_ms=20.0,
            detection_timestamp=now,
        )
        worker = AttributionWorker(db, RTL)
        worker.queue.put_nowait(AttributionItem(
            burst_id="w1", cs16=FIXTURE.read_bytes(), target_rate_hz=1_600_000,
            passes=[["-R", "383"]], power_db=-30.0,
        ))
        task = asyncio.create_task(worker.run())
        for _ in range(200):
            rows = await db.query_detections(since=now, until=now)
            if rows and rows[0]["model"]:
                break
            await asyncio.sleep(0.05)
        worker.stop()
        task.cancel()
        with pytest.raises((asyncio.CancelledError, Exception)):
            await task
        rows = await db.query_detections(since=now, until=now)
        row = next(r for r in rows if r["burst_id"] == "w1")
        assert row["model"] == "SilverSpring-Mesh"
        assert row["protocol_id"] == 383
        assert json.loads(row["attribution"])["decoded"] is True
    finally:
        await db.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_attribution_worker.py -v`
Expected: FAIL (`ImportError: cannot import name 'AttributionWorker'`). (Skips if rtl_433/fixtures absent - run on .153 or the workstation.)

- [ ] **Step 3: Write minimal implementation**

Add to `attribution.py` (needs `from datetime import datetime, timezone`):

```python
class AttributionWorker:
    """Drains the queue, decodes each burst off the event loop, and merges the
    result onto its detections row (three-state: decoded / attempted-not-decoded)."""

    def __init__(self, database, rtl_path: str, queue: "StrongestQueue | None" = None) -> None:
        self._db = database
        self._rtl = rtl_path
        self.queue = queue if queue is not None else StrongestQueue(maxsize=64)
        self._stop = False

    def stop(self) -> None:
        self._stop = True

    async def run(self) -> None:
        while not self._stop:
            item = await self.queue.get()
            try:
                frames = await asyncio.to_thread(
                    decode_cs16, self._rtl, item.cs16, item.target_rate_hz, item.passes
                )
            except Exception:
                logger.exception("rtl_433 decode failed for burst %s", item.burst_id)
                continue
            now_iso = datetime.now(timezone.utc).isoformat()
            if frames:
                model = frames[0].get("model")
                proto = _protocol_id_for(model)
                attribution = json.dumps({"decoded": True, "at": now_iso, "frames": frames})
            else:
                model, proto = None, None
                attribution = json.dumps({"attempted": True, "decoded": False, "at": now_iso})
            try:
                await self._db.update_detection_attribution(
                    burst_id=item.burst_id, model=model, protocol_id=proto, attribution=attribution
                )
            except Exception:
                logger.exception("attribution merge failed for burst %s", item.burst_id)


def _protocol_id_for(model: str | None) -> int | None:
    """Map a decoded model string to its rtl_433 protocol id where known."""
    if model == "SilverSpring-Mesh":
        return 383
    return None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH= .venv/bin/pytest tests/integration/test_attribution_worker.py -v`
Expected: PASS on .153/workstation.

- [ ] **Step 5: Lint, type-check, commit**

```bash
ruff check src/rfobserver/pipeline/attribution.py tests/integration/test_attribution_worker.py
ruff format --check src/rfobserver/pipeline/attribution.py tests/integration/test_attribution_worker.py
PYTHONPATH= .venv/bin/mypy src/rfobserver/pipeline/attribution.py
git add src/rfobserver/pipeline/attribution.py tests/integration/test_attribution_worker.py
git commit -m "feat(attribution): worker drains queue, decodes, merges three-state result"
```

---

### Task 6: Config flags + wire the producer into the continuous pipeline

**Files:**
- Modify: `src/rfobserver/config.py` (attribution settings)
- Modify: `src/rfobserver/pipeline/continuous.py` (carry `noise_floor_db` on `_ProcessResult`; construct worker; producer hook in `_store_and_broadcast`)
- Test: `tests/unit/test_attribution_producer.py`

**Interfaces:**
- Consumes: `channelize_to_cs16`, `select_rate_and_protocols` (Task 1); `find_rtl433`, `AttributionWorker`, `AttributionItem`, `convert_bytes_to_complex` (existing) (Tasks 3-5).
- Produces:
  - New settings: `ATTRIBUTION_ENABLED: bool = False`, `ATTRIBUTION_RTL433_PATH: str = ""`, `ATTRIBUTION_SNR_DB: float = 13.0`, `ATTRIBUTION_MAX_PER_CHUNK: int = 40`, `ATTRIBUTION_QUEUE_MAX: int = 64`.
  - A module-level helper (testable without a live pipeline): `select_bursts_for_attribution(bursts: list[BurstFingerprint], noise_floor_db: float, snr_db: float, max_n: int) -> list[BurstFingerprint]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_attribution_producer.py
from datetime import datetime, timezone

from rfobserver.models import BurstFingerprint
from rfobserver.pipeline.continuous import select_bursts_for_attribution


def _burst(power_db: float, bw: float = 250e3) -> BurstFingerprint:
    now = datetime.now(timezone.utc)
    return BurstFingerprint(
        start_time=now, stop_time=now, center_freq_hz=915e6, peak_freq_hz=915.1e6,
        bandwidth_hz=bw, peak_power_db=power_db, duration_ms=20.0,
    )


def test_snr_gate_and_top_n():
    noise = -80.0
    bursts = [
        _burst(-40.0),  # SNR 40 -> pass
        _burst(-70.0),  # SNR 10 -> below 13 dB gate -> drop
        _burst(-50.0),  # SNR 30 -> pass
        _burst(-45.0),  # SNR 35 -> pass
    ]
    picked = select_bursts_for_attribution(bursts, noise, snr_db=13.0, max_n=2)
    # Two strongest above the gate: -40 (40 dB) and -45 (35 dB).
    powers = sorted(b.peak_power_db for b in picked)
    assert powers == [-45.0, -40.0]


def test_gate_drops_all_when_weak():
    assert select_bursts_for_attribution([_burst(-79.0)], -80.0, 13.0, 40) == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_attribution_producer.py -v`
Expected: FAIL (`ImportError: cannot import name 'select_bursts_for_attribution'`).

- [ ] **Step 3: Write minimal implementation**

Add the settings to `AppSettings` in `config.py` (near the other `BURST_*` / bool flags):

```python
    # --- rtl_433 per-burst attribution (off by default) ---
    ATTRIBUTION_ENABLED: bool = False
    ATTRIBUTION_RTL433_PATH: str = ""  # "" -> auto-discover via find_rtl433
    ATTRIBUTION_SNR_DB: float = 13.0  # skip bursts below this many dB over noise
    ATTRIBUTION_MAX_PER_CHUNK: int = 40  # top-N strongest bursts per chunk
    ATTRIBUTION_QUEUE_MAX: int = 64
```

Add the selection helper at module scope in `continuous.py`:

```python
def select_bursts_for_attribution(
    bursts: "list[BurstFingerprint]",
    noise_floor_db: float,
    snr_db: float,
    max_n: int,
) -> "list[BurstFingerprint]":
    """SNR gate + top-N-by-power. Returns the strongest bursts that clear the
    gate, most-powerful first, capped at max_n."""
    gated = [b for b in bursts if (b.peak_power_db - noise_floor_db) >= snr_db]
    gated.sort(key=lambda b: b.peak_power_db, reverse=True)
    return gated[:max_n]
```

Carry the noise floor on `_ProcessResult` so the producer can gate. Add `"noise_floor_db"` to its `__slots__` and constructor, set it from `detection_result.noise_floor_db` where `_ProcessResult(...)` is built (~line 366), defaulting to `0.0`.

Construct the worker in `ContinuousProcessor.__init__` when enabled:

```python
        self._attrib_worker = None
        self._attrib_task = None
        if settings.ATTRIBUTION_ENABLED:
            from rfobserver.pipeline.attribution import AttributionWorker, StrongestQueue, find_rtl433

            rtl = find_rtl433(settings.ATTRIBUTION_RTL433_PATH or None)
            if rtl is None:
                logger.warning("ATTRIBUTION_ENABLED but rtl_433 not found; attribution disabled")
            else:
                q = StrongestQueue(maxsize=settings.ATTRIBUTION_QUEUE_MAX)
                self._attrib_worker = AttributionWorker(database, rtl, queue=q)
```

Start the worker task in the pipeline's async entry (`run()`), guarded:

```python
        if self._attrib_worker is not None:
            self._attrib_task = asyncio.create_task(self._attrib_worker.run())
```

and on shutdown cancel it (mirror how `broadcast_task` is awaited/cancelled): call `self._attrib_worker.stop()` and cancel `self._attrib_task`.

Add the producer hook at the end of `_store_and_broadcast`, AFTER the detection-insert loop (so the rows exist for the later UPDATE):

```python
        # rtl_433 attribution: gate + channelize + enqueue (never blocks).
        if self._attrib_worker is not None and pr.bursts:
            from rfobserver.pipeline.attribution import AttributionItem
            from rfobserver.processing.channelize import (
                channelize_to_cs16,
                select_rate_and_protocols,
            )
            from rfobserver.processing.iq_utils import convert_bytes_to_complex

            picked = select_bursts_for_attribution(
                pr.bursts,
                pr.noise_floor_db,
                self._settings.ATTRIBUTION_SNR_DB,
                self._settings.ATTRIBUTION_MAX_PER_CHUNK,
            )
            if picked:
                data = convert_bytes_to_complex(pr.iq_bytes)
                fs = float(self._settings.BANDWIDTH)
                for b in picked:
                    offset = b.peak_freq_hz - float(pr.center_freq_hz)
                    rate, passes = select_rate_and_protocols(b.bandwidth_hz)
                    cs16 = channelize_to_cs16(data, fs, offset, rate)
                    self._attrib_worker.queue.put_nowait(
                        AttributionItem(
                            burst_id=b.burst_id, cs16=cs16, target_rate_hz=rate,
                            passes=passes, power_db=b.peak_power_db,
                        )
                    )
```

(For the whole-chunk channelization this cut slices from the full chunk IQ with a
DC-shift; a per-burst time-slice optimization is a later refinement and not needed
for correctness - the mixer + resample still centers the burst.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH= .venv/bin/pytest tests/unit/test_attribution_producer.py -v`
Expected: PASS (2 tests). Then the full unit suite: `PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q`.

- [ ] **Step 5: Full check + commit**

```bash
ruff check src/ tests/
ruff format --check src/ tests/
PYTHONPATH= .venv/bin/mypy src/rfobserver/
PYTHONPATH= .venv/bin/pytest tests/unit/ -x -q
# integration needs NATS on :4222 (throwaway: docker run -d --rm --name rfobs-test-nats -p 4222:4222 nats:2.10-alpine -js)
PYTHONPATH= .venv/bin/pytest tests/integration/ -x -q
git add src/rfobserver/config.py src/rfobserver/pipeline/continuous.py tests/unit/test_attribution_producer.py
git commit -m "feat(attribution): SNR-gated top-N producer wired into continuous pipeline"
```

---

## Verification on the target (.153, after all tasks)

Not a code task, but the acceptance gate. On `nano-super` @192.168.97.153 (rtl_433
built, MAXN enabled), run the pipeline with attribution on against a replayed SSN
capture (or the live B200mini), confirm strong SSN bursts get `model` =
"SilverSpring-Mesh" / `protocol_id` = 383 on their `detections` rows, weak bursts
get the attempted-not-decoded marker, and the live loop's `excess_ms` stays near
zero (the worker is not starving the loop). Enable via `RFOBS_ATTRIBUTION_ENABLED=true`.

## Self-review notes

- **Spec coverage:** queue (Task 4), in-process worker (Task 5), SNR gate + top-N
  (Task 6), drop-strongest backpressure (Task 4), three DB columns + merge by
  burst_id (Task 2), fixed two-tier policy (Task 1), JSON-array multi-frame +
  three-state marker (Task 5), dynamic resample ratio for 26/56 Msps (Task 1),
  off-by-default flag (Task 6), rtl_433 discovery (Task 3). All covered.
- **Types consistent:** `AttributionItem`/`StrongestQueue`/`AttributionWorker`/
  `decode_cs16`/`find_rtl433`/`channelize_to_cs16`/`select_rate_and_protocols`/
  `select_bursts_for_attribution`/`update_detection_attribution` names and
  signatures match across the tasks that define and consume them.
- **Deferred (not gaps):** per-burst time-slice channelization (whole-chunk is
  correct, just does more DSP); a UI column render for `model`/`attribution`
  (surfaces via `query_detections` `SELECT *` already; explicit overlay styling is
  a separate UI cut); the offline `/captures/attribute` route (out of scope for
  this live cut per the spec).
```
