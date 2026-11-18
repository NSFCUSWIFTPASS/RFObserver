"""Tone tracker for the tone detector. Pure - no I/O.

The burst detector needs a cell 30 dB over its bin's floor; a test tone 5 dB
over it never gets there, but it is plain in its own bin over time. The tone
detector watches only the PSD bins holding the frequencies it is given (a
daily RF-chain linearity check sends known tones), so for each one a
``ToneTracker`` takes that bin's power from every row of the PSD grid the
pipeline already computes (0.2 ms rows at 26 MS/s), averages it into 1 ms
blocks and tracks where it stands over a rolling floor of the bin's off blocks.
Bins a few away either side guard against broadband energy passing through.

A tone's level is the median of its on blocks with the floor taken out: a
B200mini receiving at 26 MS/s drops 5 dB for 3.4 ms every 102.4 ms
(docs/debugging/2026-11-18_b200-rx-gain-step.md), which a median leaves alone,
and gaps that short are bridged, so the level tracks what was transmitted.
See docs/superpowers/specs/2026-11-18-low-level-tone-detector-design.md.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any

import numpy as np

BLOCK_SEC = 0.001
MIN_ON_SEC = 0.020
# Off gaps up to this long inside a tone are bridged (the receiver's 3.4 ms
# step can drop a weak tone under the threshold).
BRIDGE_SEC = 0.006
# The floor is the median of the bin's last FLOOR_SEC of off blocks, known
# once it has MIN_FLOOR_SEC of them, and recomputed every FLOOR_EVERY blocks.
FLOOR_SEC = 30.0
MIN_FLOOR_SEC = 1.0
FLOOR_EVERY = 1000
# A tone longer than this is reported in pieces (a stuck carrier at a listed
# frequency must not grow without bound).
MAX_TONE_SEC = 60.0
# Rows more than this out of step with the last ones (a stall, a retune)
# close any open tone and start the blocks over.
GAP_SEC = 0.005
# Guard bins (GUARD_BINS either side of the tone's bin) must rise less than
# this share of the tone's rise: a CW tone leaks under -30 dB that far out,
# while broadband energy sweeping through (seen 2026-11-18: 23 to 42 ms,
# in both listed bins at once) lifts them about as much as the tone's bin.
GUARD_BINS = 3
MAX_GUARD_SHARE = 0.25


def parse_tone_freqs(value: str) -> list[float]:
    """Frequencies (Hz) from a comma-separated list; blanks are skipped and
    anything else that is not a number raises ValueError."""
    return [float(part) for part in (p.strip() for p in value.split(",")) if part]


@dataclass(frozen=True)
class ToneMeasure:
    """A tone in one bin: start and stop (epoch s), level (dB, the bin's power
    units, floor taken out) and SNR over the bin's floor (dB)."""

    start: float
    stop: float
    level_db: float
    snr_db: float


class ToneTracker:
    """One bin's tones, fed its grid rows (linear power) in time order, with
    the mean of its guard bins' power for the same rows."""

    def __init__(self, threshold_db: float) -> None:
        self._ratio = 10 ** (float(threshold_db) / 10)
        self._off: deque[float] = deque(maxlen=round(FLOOR_SEC / BLOCK_SEC))
        self._guard_off: deque[float] = deque(maxlen=round(FLOOR_SEC / BLOCK_SEC))
        self._floor: float | None = None
        self._guard_floor = 0.0
        self._since_floor = 0
        self._rem = np.zeros((0, 2))
        self._rem_t: float | None = None  # time of the first remainder row
        self._next_t: float | None = None  # time the next row should have
        self._seg_start: float | None = None
        self._seg_end = 0.0
        self._seg_on: list[float] = []
        self._seg_guard: list[float] = []
        self._gap = 0.0

    def feed(self, rows: Any, guard: Any, t0: float, row_sec: float) -> list[ToneMeasure]:
        """Take the bin's power (and its guard bins') for consecutive rows
        starting at ``t0`` (epoch s), each ``row_sec`` long; return the tones
        that ended."""
        found: list[ToneMeasure] = []
        if self._next_t is not None and abs(t0 - self._next_t) > GAP_SEC:
            found += self.flush()
        if not len(self._rem):
            self._rem_t = t0
        both = np.column_stack(
            [np.asarray(rows, dtype=np.float64), np.asarray(guard, dtype=np.float64)]
        )
        self._rem = np.concatenate([self._rem, both])
        self._next_t = t0 + len(rows) * row_sec
        per = max(1, round(BLOCK_SEC / row_sec))
        block_sec = per * row_sec
        n = len(self._rem) // per
        assert self._rem_t is not None
        blocks = self._rem[: n * per].reshape(n, per, 2).mean(axis=1)
        self._rem = self._rem[n * per :]
        t_first = self._rem_t
        self._rem_t += n * block_sec
        # Nearly always nothing is on and no tone is open: learn the floor
        # from the whole chunk at once.
        main = blocks[:, 0]
        quiet = self._floor is not None and not np.any(main >= self._floor * self._ratio)
        if quiet and self._seg_start is None:
            self._learn_many(main, blocks[:, 1])
            return found
        for i, (p, g) in enumerate(blocks.tolist()):
            found += self._block(p, g, t_first + i * block_sec, block_sec)
        return found

    def flush(self) -> list[ToneMeasure]:
        """Close an open tone (end of data, or a gap) and drop partial blocks."""
        self._rem = np.zeros((0, 2))
        self._next_t = None
        return self._close()

    def _block(self, p: float, g: float, t: float, dt: float) -> list[ToneMeasure]:
        if self._floor is None:
            self._learn(p, g)
            return []
        if p >= self._floor * self._ratio:
            if self._seg_start is None:
                self._seg_start = t
            self._seg_on.append(p)
            self._seg_guard.append(g)
            self._seg_end = t + dt
            self._gap = 0.0
            if self._seg_end - self._seg_start >= MAX_TONE_SEC:
                return self._close()
            return []
        if self._seg_start is not None:
            self._gap += dt
            if self._gap > BRIDGE_SEC:
                return self._close()
            return []
        self._learn(p, g)
        return []

    def _learn_many(self, ps: Any, gs: Any) -> None:
        self._off.extend(ps.tolist())
        self._guard_off.extend(gs.tolist())
        self._since_floor += len(ps)
        if self._since_floor >= FLOOR_EVERY:
            self._update_floor()

    def _learn(self, p: float, g: float) -> None:
        self._off.append(p)
        self._guard_off.append(g)
        self._since_floor += 1
        enough = len(self._off) >= round(MIN_FLOOR_SEC / BLOCK_SEC)
        if enough and (self._floor is None or self._since_floor >= FLOOR_EVERY):
            self._update_floor()

    def _update_floor(self) -> None:
        self._floor = float(np.median(self._off))
        self._guard_floor = float(np.median(self._guard_off))
        self._since_floor = 0

    def _close(self) -> list[ToneMeasure]:
        start, on, guard = self._seg_start, self._seg_on, self._seg_guard
        self._seg_start, self._seg_on, self._seg_guard, self._gap = None, [], [], 0.0
        if start is None or self._floor is None or self._seg_end - start < MIN_ON_SEC:
            return []
        tone = float(np.median(on)) - self._floor
        if tone <= 0:
            return []
        if float(np.median(guard)) - self._guard_floor > MAX_GUARD_SHARE * tone:
            return []
        return [
            ToneMeasure(
                start=start,
                stop=self._seg_end,
                level_db=10.0 * float(np.log10(tone)),
                snr_db=10.0 * float(np.log10(tone / self._floor)),
            )
        ]
