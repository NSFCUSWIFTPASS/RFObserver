"""Tone detector: finds CW tones at listed frequencies, too weak for the burst
detector, and writes them as Tone detections. Runs apart from burst detection
and isolation, and leaves burst rows alone (a strong tone also leaves burst
fragments: the burst detector's floor absorbs a steady tone, and its times run
on its own, later clock).

The consumer hands every chunk's result to ``ToneDetector.on_result``. For each
listed frequency in the tuned band, the PSD grid column holding it (and the
mean of its guard bins) goes to that frequency's ``ToneTracker``; each tone
that ends is written as its own row. See
docs/superpowers/specs/2026-11-18-low-level-tone-detector-design.md.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import numpy as np

from rfobserver.processing.tone_detect import GUARD_BINS, ToneMeasure, ToneTracker

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)


class ToneDetector:
    def __init__(
        self,
        db: Any,
        *,
        freqs: list[float],
        threshold_db: float,
        meta: Callable[[], dict[str, Any]],
        hs_run: Callable[[float], str | None] = lambda t: None,
    ) -> None:
        self._db = db
        # The high-sensitivity run a time fell in (a tone carries the run it
        # started in, even when it is written after the run ended).
        self._hs_run = hs_run
        self._freqs = list(dict.fromkeys(float(f) for f in freqs))
        self._threshold = float(threshold_db)
        self._meta = meta
        # Per listed frequency: (sdr centre, bin index, bin width, tracker),
        # or None while it is outside the tuned band.
        self._tracks: dict[float, tuple[float, int, float, ToneTracker] | None] = {}
        # The tuning each frequency was last placed for (a retune re-places it).
        self._tuned: dict[float, float] = {}
        self._finals: set[asyncio.Task[None]] = set()
        self.stats: dict[str, int] = {
            "chunks": 0,
            "skipped": 0,
            "out_of_band": 0,
            "tones": 0,
            "inserted": 0,
            "errors": 0,
        }

    @property
    def freqs(self) -> list[float]:
        return list(self._freqs)

    def status_freqs(self) -> dict[float, str]:
        """Each listed frequency: "tracking" or "out of band" (or "waiting"
        before the first chunk)."""
        out = {}
        for f in self._freqs:
            if f not in self._tracks:
                out[f] = "waiting"
            else:
                out[f] = "tracking" if self._tracks[f] is not None else "out of band"
        return out

    def on_result(self, result: Any, row_sec: float) -> None:
        """Take one chunk: its PSD grid (dB, rows of ``row_sec``), tuning and
        capture time. Call on the event loop, in stream order."""
        if result.capture_time is None:
            self.stats["skipped"] += 1
            return
        self.stats["chunks"] += 1
        grid = result.psd_grid.grid
        axis = result.psd_grid.freq_axis
        center = float(result.center_freq_hz)
        for f in self._freqs:
            track = self._track(f, center, axis)
            if track is None:
                continue
            _, k, bin_hz, tracker = track
            rows = 10.0 ** (np.asarray(grid[:, k], dtype=np.float64) / 10.0)
            sides = [j for j in (k - GUARD_BINS, k + GUARD_BINS) if 0 <= j < grid.shape[1]]
            guard = (10.0 ** (np.asarray(grid[:, sides], dtype=np.float64) / 10.0)).mean(axis=1)
            for m in tracker.feed(rows, guard, float(result.capture_time), row_sec):
                self._finalize(f, bin_hz, center, m)

    def _track(
        self, f: float, center: float, axis: Any
    ) -> tuple[float, int, float, ToneTracker] | None:
        """This frequency's tracker for the current tuning; a retune closes the
        old one and starts over (the floor is the new tuning's)."""
        if self._tuned.get(f) == center:
            return self._tracks[f]
        cur = self._tracks.get(f)
        if cur is not None:
            old_center, _, old_bin_hz, tracker = cur
            for m in tracker.flush():
                self._finalize(f, old_bin_hz, old_center, m)
        self._tuned[f] = center
        rel = f - center
        bin_hz = float(axis[1] - axis[0]) if len(axis) > 1 else 0.0
        if bin_hz <= 0 or rel < axis[0] - bin_hz / 2 or rel > axis[-1] + bin_hz / 2:
            self.stats["out_of_band"] += 1
            self._tracks[f] = None
            logger.warning("Tone detector: %.0f Hz is outside the tuned band; not watched", f)
            return None
        k = int(np.argmin(np.abs(np.asarray(axis) - rel)))
        self._tracks[f] = (center, k, bin_hz, ToneTracker(self._threshold))
        return self._tracks[f]

    async def stop(self) -> None:
        """Write every tone still open, and wait for the writes."""
        for f, track in list(self._tracks.items()):
            if track is not None:
                center, _, bin_hz, tracker = track
                for m in tracker.flush():
                    self._finalize(f, bin_hz, center, m)
        if self._finals:
            await asyncio.gather(*self._finals, return_exceptions=True)

    # -- Writing --

    def _finalize(self, f: float, bin_hz: float, center: float, m: ToneMeasure) -> None:
        self.stats["tones"] += 1
        task = asyncio.get_running_loop().create_task(
            self._write(f, bin_hz, center, m, self._hs_run(m.start))
        )
        self._finals.add(task)
        task.add_done_callback(self._finals.discard)

    async def _write(
        self, f: float, bin_hz: float, center: float, m: ToneMeasure, hs_run: str | None
    ) -> None:
        try:
            await self._insert(f, bin_hz, center, m, hs_run)
        except Exception:
            self.stats["errors"] += 1
            logger.exception("Tone detector: writing the tone at %.0f Hz failed", f)

    async def _insert(
        self, f: float, bin_hz: float, center: float, m: ToneMeasure, hs_run: str | None
    ) -> None:
        start = datetime.fromtimestamp(m.start, tz=timezone.utc)
        stop = datetime.fromtimestamp(m.stop, tz=timezone.utc)
        duration = m.stop - m.start
        frame = {
            "model": "Tone",
            "protocol": "CW",
            "tone_hz": f,
            "duration_ms": round(duration * 1000.0, 1),
            "level_db": round(m.level_db, 2),
            "snr_db": round(m.snr_db, 2),
        }
        attribution = json.dumps(
            {
                "decoded": True,
                "at": datetime.now(timezone.utc).isoformat(),
                "source": "tone_detector",
                "frames": [frame],
            }
        )
        meta = self._meta()
        burst_id = str(uuid.uuid4())
        await self._db.insert_detections(
            [
                {
                    "burst_id": burst_id,
                    "start_time": start,
                    "stop_time": stop,
                    "center_freq_hz": f,
                    "bandwidth_hz": bin_hz,
                    "peak_power_db": m.level_db,
                    "duration_ms": duration * 1000.0,
                    "detection_timestamp": datetime.now(timezone.utc),
                    "peak_freq_hz": f,
                    "sdr_center_freq_hz": center,
                    "sample_rate_hz": meta.get("sample_rate_hz"),
                    "lo_offset_hz": 0.0,
                    "analog_bw_hz": None,
                    "gain_db": meta.get("gain_db"),
                    "antenna": "RX2",
                    "device_serial": meta.get("device_serial"),
                    "hs_run": hs_run,
                }
            ]
        )
        await self._db.update_detection_attribution(
            burst_id=burst_id, model="Tone", protocol_id=None, attribution=attribution
        )
        self.stats["inserted"] += 1
