"""Low-level tone detector: finds CW tones too weak for the burst detector and
writes them as Tone detections. Runs apart from burst detection and isolation.

Each closed averaging window hands its averaged PSD and stream span to
``ToneDetector.submit``. Stage A (``BinBaseline``) flags bins that rose over
their recent level; for each, stage B reads the window's IQ back from the ring
(1 to 2 s old by then), cuts a narrow channel around the bin and confirms and
measures the tone (``confirm_tones``). A tone still on at a window's end is
carried into the next window. When a tone ends it is held ``hold_sec`` (the
burst detector writes a burst only once it stops growing, up to about 1.3 s
after it starts), then: if narrow burst rows already cover 80 % of it, they
are labelled Tone; otherwise it is written as its own row. See
docs/superpowers/specs/2026-11-18-low-level-tone-detector-design.md.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import numpy as np

from rfobserver.processing.channelize import channelize
from rfobserver.processing.isolate import iq_to_complex
from rfobserver.processing.tone_detect import BinBaseline, Candidate, ToneSegment, confirm_tones

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

CHANNEL_RATE_HZ = 50_000
# The ring is read and channelized this much at a time (bounds the working
# set at 26 MS/s to tens of MB), with a margin either side for the filter.
READ_BLOCK_SEC = 0.25
READ_MARGIN_SEC = 0.002
QUEUE_WINDOWS = 4
# Burst rows narrower than this count as covering a tone; a wideband burst
# that happens to span it does not.
MAX_COVER_BANDWIDTH_HZ = 250_000.0
COVER_FRACTION = 0.8
# A segment continues a tone from the previous window within this.
CONTINUE_HZ = 50.0


@dataclass
class ToneWindow:
    """One closed averaging window: its averaged PSD (dB per bin, absolute
    frequency axis), the tuning key, and the stream samples it covers with the
    capture time of the first."""

    key: Any
    powers: Any
    freq_start_hz: float
    freq_step_hz: float
    sdr_center_hz: float
    start_sample: int
    end_sample: int
    start_time: datetime


@dataclass
class _Tone:
    freq_hz: float
    start: datetime
    stop: datetime
    end_sample: int
    sdr_center_hz: float
    bin_hz: float
    parts: list[tuple[float, float, float]] = field(default_factory=list)  # level, snr, s

    def level_db(self) -> float:
        w = np.array([p[2] for p in self.parts])
        return float(np.average([p[0] for p in self.parts], weights=w))

    def snr_db(self) -> float:
        w = np.array([p[2] for p in self.parts])
        return float(np.average([p[1] for p in self.parts], weights=w))


class ToneDetector:
    def __init__(
        self,
        db: Any,
        ring: Any,
        *,
        sample_rate_hz: float,
        threshold_db: float,
        meta: Callable[[], dict[str, Any]],
        hold_sec: float = 3.0,
    ) -> None:
        self._db = db
        self._ring = ring
        self._rate = float(sample_rate_hz)
        self._meta = meta
        self._hold = float(hold_sec)
        self._baseline = BinBaseline(threshold_db)
        self._queue: asyncio.Queue[ToneWindow | None] = asyncio.Queue(maxsize=QUEUE_WINDOWS)
        self._pending: list[_Tone] = []
        self._finals: set[asyncio.Task[None]] = set()
        self._stopping = False
        self.stats: dict[str, int] = {
            "windows": 0,
            "candidates": 0,
            "confirmed": 0,
            "labelled": 0,
            "inserted": 0,
            "too_late": 0,
            "dropped": 0,
            "errors": 0,
        }

    # -- Intake --

    def submit(self, window: ToneWindow) -> bool:
        """Queue a closed window; False (and counted) when the queue is full."""
        try:
            self._queue.put_nowait(window)
        except asyncio.QueueFull:
            self.stats["dropped"] += 1
            return False
        return True

    async def run(self) -> None:
        while True:
            w = await self._queue.get()
            if w is None:
                break
            try:
                await self._process(w)
            except Exception:
                self.stats["errors"] += 1
                logger.exception("tone detector: window failed")

    async def stop(self) -> None:
        """Finish queued windows, write every tone still open, and return."""
        await self._queue.put(None)
        # run() exits on the None; the caller awaits its task. Wait for it here
        # by draining: once the queue is empty, close what is left.
        while not self._queue.empty():
            await asyncio.sleep(0.01)
        await asyncio.sleep(0)
        self._stopping = True
        for t in self._pending:
            self._finalize(t)
        self._pending = []
        if self._finals:
            await asyncio.gather(*self._finals, return_exceptions=True)

    # -- Per window --

    async def _process(self, w: ToneWindow) -> None:
        self.stats["windows"] += 1
        cands = await asyncio.to_thread(self._baseline.update, w.key, w.powers)
        found: list[tuple[float, ToneSegment]] = []
        if cands:
            self.stats["candidates"] += len(cands)
            measured = await asyncio.to_thread(self._measure, w, cands)
            if measured is None:
                self.stats["too_late"] += 1
            else:
                found = measured
                self.stats["confirmed"] += len(found)
        self._advance(w, found)

    def _measure(
        self, w: ToneWindow, cands: list[Candidate]
    ) -> list[tuple[float, ToneSegment]] | None:
        """(tone frequency, segment) for each tone in the window's candidates;
        None when the window's IQ is no longer in the ring."""
        offsets = [w.freq_start_hz + c.peak_bin * w.freq_step_hz - w.sdr_center_hz for c in cands]
        ratio = CHANNEL_RATE_HZ / self._rate
        block = int(READ_BLOCK_SEC * self._rate)
        margin = int(READ_MARGIN_SEC * self._rate)
        start, end = w.start_sample, w.end_sample
        chans: list[list[np.ndarray[Any, np.dtype[Any]]]] = [[] for _ in cands]
        for b0 in range(start, end, block):
            b1 = min(b0 + block, end)
            a, z = max(start, b0 - margin), min(end, b1 + margin)
            raw = self._ring.read_range(a, z)
            if raw is None:
                return None
            iq = iq_to_complex(raw)
            k0, k1 = round((b0 - start) * ratio), round((b1 - start) * ratio)
            i0 = round((b0 - a) * ratio)
            for i, off in enumerate(offsets):
                ch = channelize(iq, self._rate, off, CHANNEL_RATE_HZ)
                chans[i].append(ch[i0 : i0 + (k1 - k0)])
        found = []
        for c, off, parts in zip(cands, offsets, chans, strict=True):
            x = np.concatenate(parts)
            span = (c.hi_bin - c.lo_bin + 1) * w.freq_step_hz
            for seg in confirm_tones(x, CHANNEL_RATE_HZ, w.freq_step_hz, search_hz=span):
                found.append((w.sdr_center_hz + off + seg.offset_hz, seg))
        return found

    def _advance(self, w: ToneWindow, found: list[tuple[float, ToneSegment]]) -> None:
        """Extend open tones with this window's segments, close the rest."""
        still: list[_Tone] = []
        used: set[int] = set()
        for t in self._pending:
            match = None
            if t.end_sample == w.start_sample:
                for i, (f, seg) in enumerate(found):
                    if i not in used and seg.touches_start and abs(f - t.freq_hz) <= CONTINUE_HZ:
                        match = i
                        break
            if match is None:
                self._finalize(t)
                continue
            used.add(match)
            f, seg = found[match]
            t.stop = w.start_time + timedelta(seconds=seg.start_s + seg.duration_s)
            t.parts.append((seg.level_db, seg.snr_db, seg.duration_s))
            if seg.touches_end:
                t.end_sample = w.end_sample
                still.append(t)
            else:
                self._finalize(t)
        for i, (f, seg) in enumerate(found):
            if i in used:
                continue
            t = _Tone(
                freq_hz=f,
                start=w.start_time + timedelta(seconds=seg.start_s),
                stop=w.start_time + timedelta(seconds=seg.start_s + seg.duration_s),
                end_sample=w.end_sample,
                sdr_center_hz=w.sdr_center_hz,
                bin_hz=w.freq_step_hz,
                parts=[(seg.level_db, seg.snr_db, seg.duration_s)],
            )
            if seg.touches_end:
                still.append(t)
            else:
                self._finalize(t)
        self._pending = still

    # -- Writing --

    def _finalize(self, t: _Tone) -> None:
        task = asyncio.get_running_loop().create_task(self._write(t))
        self._finals.add(task)
        task.add_done_callback(self._finals.discard)

    async def _write(self, t: _Tone) -> None:
        if self._hold > 0 and not self._stopping:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(self._hold)
        try:
            await self._label_or_insert(t)
        except Exception:
            self.stats["errors"] += 1
            logger.exception("tone detector: writing the tone at %.1f Hz failed", t.freq_hz)

    async def _label_or_insert(self, t: _Tone) -> None:
        duration = (t.stop - t.start).total_seconds()
        frame = {
            "model": "Tone",
            "protocol": "CW",
            "tone_hz": round(t.freq_hz, 1),
            "duration_ms": round(duration * 1000.0, 1),
            "level_dbfs": round(t.level_db(), 2),
            "snr_db": round(t.snr_db(), 1),
        }
        attribution = json.dumps(
            {
                "decoded": True,
                "at": datetime.now(timezone.utc).isoformat(),
                "source": "tone_detector",
                "frames": [frame],
            }
        )
        rows = await self._db.detections_overlapping(t.start, t.stop, t.freq_hz - 1, t.freq_hz + 1)
        rows = [r for r in rows if r["bandwidth_hz"] <= MAX_COVER_BANDWIDTH_HZ]
        if duration > 0 and _covered(rows, t.start, t.stop) >= COVER_FRACTION * duration:
            for r in rows:
                await self._db.update_detection_attribution(
                    burst_id=r["burst_id"], model="Tone", protocol_id=None, attribution=attribution
                )
            self.stats["labelled"] += len(rows)
            return
        meta = self._meta()
        burst_id = str(uuid.uuid4())
        await self._db.insert_detections(
            [
                {
                    "burst_id": burst_id,
                    "start_time": t.start,
                    "stop_time": t.stop,
                    "center_freq_hz": t.freq_hz,
                    "bandwidth_hz": t.bin_hz,
                    "peak_power_db": t.level_db(),
                    "duration_ms": duration * 1000.0,
                    "detection_timestamp": datetime.now(timezone.utc),
                    "peak_freq_hz": t.freq_hz,
                    "sdr_center_freq_hz": t.sdr_center_hz,
                    "sample_rate_hz": meta.get("sample_rate_hz"),
                    "lo_offset_hz": 0.0,
                    "analog_bw_hz": None,
                    "gain_db": meta.get("gain_db"),
                    "antenna": "RX2",
                    "device_serial": meta.get("device_serial"),
                }
            ]
        )
        await self._db.update_detection_attribution(
            burst_id=burst_id, model="Tone", protocol_id=None, attribution=attribution
        )
        self.stats["inserted"] += 1


def _covered(rows: list[dict[str, Any]], start: datetime, stop: datetime) -> float:
    """Seconds of [start, stop) that the rows' time spans cover together."""
    spans = sorted(
        (max(r["start_time"], start), min(r["stop_time"], stop))
        for r in rows
        if r["stop_time"] > start and r["start_time"] < stop
    )
    total = 0.0
    cur_s: datetime | None = None
    cur_e: datetime | None = None
    for s, e in spans:
        if cur_e is None or s > cur_e:
            if cur_s is not None and cur_e is not None:
                total += (cur_e - cur_s).total_seconds()
            cur_s, cur_e = s, e
        elif e > cur_e:
            cur_e = e
    if cur_s is not None and cur_e is not None:
        total += (cur_e - cur_s).total_seconds()
    return total
