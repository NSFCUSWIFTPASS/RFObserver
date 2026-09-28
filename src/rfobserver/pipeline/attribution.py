"""rtl_433 per-burst attribution: discovery, decode of a channelized .cs16 blob,
a bounded drop-strongest queue, and the async worker that drains + merges. The
decode step is synchronous (subprocess); the worker calls it off the event loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import subprocess
import tempfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

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
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
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


@dataclass
class AttributionItem:
    burst_id: str
    cs16: bytes
    target_rate_hz: int
    passes: list[list[str]]
    power_db: float
    # Burst context carried through to a sink (e.g. freq_hz, start_time_ms,
    # stop_time_ms, freq_low_hz, freq_high_hz, snr_db). Not used for decoding.
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class AttributionResult:
    item: AttributionItem
    model: str | None
    protocol_id: int | None
    attribution: str  # JSON: decoded frames, or the attempted-not-decoded marker
    outcome: str  # "decoded" | "not_decoded" | "failed"


Sink = Callable[[AttributionResult], Awaitable[None]]


def db_sink(database: Any, retry_delay_sec: float = 2.0) -> Sink:
    """Merge a result onto its detections row. The row is normally inserted
    within the same drain as the burst was detected; if it is not there yet,
    retry once after ``retry_delay_sec``."""

    async def sink(r: AttributionResult) -> None:
        kw = {
            "burst_id": r.item.burst_id,
            "model": r.model,
            "protocol_id": r.protocol_id,
            "attribution": r.attribution,
        }
        if await database.update_detection_attribution(**kw) == 0:
            await asyncio.sleep(retry_delay_sec)
            if await database.update_detection_attribution(**kw) == 0:
                logger.debug("attribution: no detection row for burst %s", r.item.burst_id)

    return sink


class ReplayFileSink:
    """Replay results never touch the DB: one JSON line per result."""

    def __init__(self, path: Path) -> None:
        self.path = path

    async def __call__(self, r: AttributionResult) -> None:
        row = {
            "burst_id": r.item.burst_id,
            **r.item.meta,
            "model": r.model,
            "protocol_id": r.protocol_id,
            "outcome": r.outcome,
            "attribution": json.loads(r.attribution) if r.attribution else None,
        }

        def append() -> None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "a") as fh:
                fh.write(json.dumps(row) + "\n")

        await asyncio.to_thread(append)


class StrongestQueue:
    """Bounded queue that keeps the strongest items. On overflow the weakest
    (lowest power_db) is evicted; get() returns the strongest first. The live
    producer never blocks - put_nowait always returns immediately."""

    def __init__(self, maxsize: int) -> None:
        self._maxsize = maxsize
        self._items: list[AttributionItem] = []
        self._not_empty = asyncio.Event()
        self.dropped = 0

    def qsize(self) -> int:
        return len(self._items)

    def put_nowait(self, item: AttributionItem) -> bool:
        if len(self._items) < self._maxsize:
            self._items.append(item)
            self._not_empty.set()
            return True
        weakest_idx = min(range(len(self._items)), key=lambda i: self._items[i].power_db)
        if item.power_db <= self._items[weakest_idx].power_db:
            self.dropped += 1
            return False
        self._items.pop(weakest_idx)
        self._items.append(item)
        self.dropped += 1
        self._not_empty.set()
        return True

    async def get(self) -> AttributionItem:
        while not self._items:
            self._not_empty.clear()
            await self._not_empty.wait()
        idx = max(range(len(self._items)), key=lambda i: self._items[i].power_db)
        item = self._items.pop(idx)
        if not self._items:
            self._not_empty.clear()
        return item


class AttributionWorker:
    """Drains the queue, decodes each burst off the event loop, and merges the
    result onto its detections row (three-state: decoded / attempted-not-decoded)."""

    def __init__(
        self,
        database: Any,
        rtl_path: str,
        queue: StrongestQueue | None = None,
        sinks: list[Sink] | None = None,
        on_outcome: Callable[[str], None] | None = None,
    ) -> None:
        self._db = database
        self._rtl = rtl_path
        self.queue = queue if queue is not None else StrongestQueue(maxsize=64)
        self._sinks = sinks if sinks is not None else [db_sink(database)]
        self._on_outcome = on_outcome
        self._stop = False

    def stop(self) -> None:
        self._stop = True

    def _count(self, outcome: str) -> None:
        if self._on_outcome is not None:
            self._on_outcome(outcome)

    async def run(self) -> None:
        while not self._stop:
            item = await self.queue.get()
            now_iso = datetime.now(timezone.utc).isoformat()
            try:
                frames = await asyncio.to_thread(
                    decode_cs16, self._rtl, item.cs16, item.target_rate_hz, item.passes
                )
            except Exception:
                logger.exception("rtl_433 decode failed for burst %s", item.burst_id)
                self._count("failed")
                continue
            if frames:
                model = frames[0].get("model")
                result = AttributionResult(
                    item,
                    model,
                    _protocol_id_for(model),
                    json.dumps({"decoded": True, "at": now_iso, "frames": frames}),
                    "decoded",
                )
            else:
                result = AttributionResult(
                    item,
                    None,
                    None,
                    json.dumps({"attempted": True, "decoded": False, "at": now_iso}),
                    "not_decoded",
                )
            self._count(result.outcome)
            for sink in self._sinks:
                try:
                    await sink(result)
                except Exception:
                    logger.exception("attribution sink failed for burst %s", item.burst_id)


def _protocol_id_for(model: str | None) -> int | None:
    """Map a decoded model string to its rtl_433 protocol id where known."""
    if model == "SilverSpring-Mesh":
        return 383
    return None
