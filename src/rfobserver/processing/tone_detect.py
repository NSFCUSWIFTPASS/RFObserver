"""Low-level tone detector DSP. Pure - no I/O.

The burst detector needs a cell 30 dB over its bin's floor; a CW tone 5 dB over
the floor of a 12.7 kHz PSD bin never gets there, but it is plain once
integrated. Stage A (``BinBaseline``) flags bins of each averaged-window PSD
that rise over their own recent level; on 2026-11-18 twenty 100 ms pulses 5 dB
over the bin floor raised their bin 0.55 dB against about 0.04 dB of
window-to-window noise. Stage B (``confirm_tones``) takes a narrow channel cut
from the IQ ring around a flagged bin and confirms a single line in it, then
measures where it is on and its level.

Levels are medians over the on time: a B200mini receiving at 26 MS/s drops
5 dB for 3.4 ms every 102.4 ms (docs/debugging/2026-11-18_b200-rx-gain-step.md),
which a median leaves alone, so a tone's level tracks what was transmitted.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, TypeAlias

import numpy as np

IQ: TypeAlias = "np.ndarray[Any, np.dtype[Any]]"

# -- Stage A ---------------------------------------------------------------

# Windows of history per bin, and how many before anything is flagged.
HISTORY = 60
MIN_HISTORY = 10
# A bin must also rise this many of its own robust spreads (a bin near an
# intermittent signal wanders, and its threshold grows with it).
SPREAD_K = 6.0
# More than this share of bins flagged at once is a broad change (gain,
# temperature, a wideband signal), not a tone: nothing is flagged and the
# window feeds the baseline.
BROAD_FRACTION = 0.05


@dataclass(frozen=True)
class Candidate:
    """Adjacent flagged bins: their range, the bin that rose most, and by how much."""

    lo_bin: int
    hi_bin: int
    peak_bin: int
    rise_db: float


class BinBaseline:
    """Per-bin recent level of the averaged PSD for one tuning, and the bins
    that rise over it."""

    def __init__(self, threshold_db: float, max_candidates: int = 4) -> None:
        self.threshold_db = float(threshold_db)
        self.max_candidates = int(max_candidates)
        self._key: Any = None
        self._history: deque[np.ndarray[Any, np.dtype[Any]]] = deque(maxlen=HISTORY)

    def update(self, key: Any, powers: Any) -> list[Candidate]:
        """Flag this window's rising bins, and learn from the rest of it.

        ``key`` names the tuning (centre, bin count, gain...); a new key starts
        the history over. ``powers`` is the window's averaged PSD in dB.
        """
        p = np.asarray(powers, dtype=np.float32)
        if key != self._key or (self._history and len(self._history[0]) != len(p)):
            self._key = key
            self._history.clear()
        if len(self._history) < MIN_HISTORY:
            self._history.append(p)
            return []

        hist = np.stack(self._history)
        base = np.median(hist, axis=0)
        spread = 1.4826 * np.median(np.abs(hist - base), axis=0)
        rise = p - base
        # A drift common to all bins (temperature) is not a tone.
        rise = rise - float(np.median(rise))
        need = np.maximum(self.threshold_db, SPREAD_K * spread)
        flag = rise >= need
        if float(np.mean(flag)) > BROAD_FRACTION:
            self._history.append(p)
            return []
        # Flagged bins do not teach the baseline: a tone that stays on stays found.
        self._history.append(np.where(flag, base, p).astype(np.float32))
        if not flag.any():
            return []

        edges = np.diff(np.r_[0, flag.astype(np.int8), 0])
        starts = np.flatnonzero(edges == 1)
        ends = np.flatnonzero(edges == -1)
        found = []
        for a, b in zip(starts, ends, strict=True):
            k = a + int(np.argmax(rise[a:b]))
            found.append(Candidate(int(a), int(b - 1), int(k), float(rise[k])))
        found.sort(key=lambda c: c.rise_db, reverse=True)
        return found[: self.max_candidates]


# -- Stage B ---------------------------------------------------------------

# Envelope blocks: coherent means over 1 ms once the line is at DC.
BLOCK_SEC = 0.001
MIN_ON_SEC = 0.020
# Gaps this short inside a tone are bridged (the receiver's 3.4 ms step can
# drop a weak tone's blocks under the on level).
BRIDGE_SEC = 0.006
# The line's peak over the channel's noise per FFT bin; noise alone peaks about
# 10 dB over its median in a channel this size.
MIN_LINE_DB = 15.0
# Of the signal power (noise taken out) within half a PSD bin of the line, the
# share within LINE_HZ of it. A 20 ms pulse keeps most of its power within
# 250 Hz; FSK or a reader's PIE modulation spreads it.
LINE_HZ = 250.0
MIN_LINE_FRACTION = 0.8
# A block is on above this many noise sigmas, and above half the tone's level.
ON_SIGMA = 3.0
ON_FRACTION = 0.5


@dataclass(frozen=True)
class ToneSegment:
    """One stretch of a tone in the channel: its frequency offset from the
    channel centre, start and length (s from the channel's first sample), its
    level (dB re full scale, median over the on time), its SNR in a PSD bin
    of ``bin_hz``, and whether it runs into either end of the channel."""

    offset_hz: float
    start_s: float
    duration_s: float
    level_db: float
    snr_db: float
    touches_start: bool
    touches_end: bool


def _segments(on: np.ndarray[Any, np.dtype[Any]], bridge: int) -> list[tuple[int, int]]:
    edges = np.diff(np.r_[0, on.astype(np.int8), 0])
    starts = list(np.flatnonzero(edges == 1))
    ends = list(np.flatnonzero(edges == -1))
    merged: list[tuple[int, int]] = []
    for a, b in zip(starts, ends, strict=True):
        if merged and a - merged[-1][1] <= bridge:
            merged[-1] = (merged[-1][0], int(b))
        else:
            merged.append((int(a), int(b)))
    return merged


def confirm_tones(
    x: IQ, rate_hz: float, bin_hz: float, search_hz: float | None = None
) -> list[ToneSegment]:
    """The tone segments in a narrow channel ``x`` (complex, ``rate_hz``),
    looking for the line within ``search_hz`` (default ``bin_hz``) of its
    centre; empty when there is no single unmodulated line."""
    rate = float(rate_hz)
    n = len(x)
    if n < MIN_ON_SEC * rate:
        return []
    search = float(search_hz if search_hz is not None else bin_hz)

    w = np.hanning(n).astype(np.float32)
    spec = np.abs(np.fft.fft(x * w)) ** 2
    f = np.fft.fftfreq(n, 1.0 / rate)
    near = np.flatnonzero(np.abs(f) <= search)
    k = int(near[np.argmax(spec[near])])
    band = np.abs(f - f[k]) <= bin_hz / 2
    line = np.abs(f - f[k]) <= LINE_HZ
    # Noise per FFT bin, measured beside the line (the median of an
    # exponential is ln 2 of its mean): the channelizer's filter rolls off
    # toward the channel's edges, where a whole-channel median would sit
    # about 10 % under the noise at the line.
    beside = band & (np.abs(f - f[k]) > 2 * LINE_HZ)
    noise = float(np.median(spec[beside])) / np.log(2.0) if beside.any() else 0.0
    if noise <= 0:
        return []
    if spec[k] < noise * 10 ** (MIN_LINE_DB / 10):
        return []
    excess_band = float(spec[band].sum()) - noise * int(band.sum())
    excess_line = float(spec[line].sum()) - noise * int(line.sum())
    if excess_band <= 0 or excess_line / excess_band < MIN_LINE_FRACTION:
        return []

    # Parabolic interpolation of the log spectrum around the peak.
    df = rate / n
    offset = float(f[k])
    if 0 < k < n - 1:
        a, b, c = np.log(spec[k - 1]), np.log(spec[k]), np.log(spec[(k + 1) % n])
        den = a - 2 * b + c
        if den != 0:
            offset += 0.5 * (a - c) / den * df

    block = max(1, round(BLOCK_SEC * rate))
    nb = n // block
    t = np.arange(nb * block) / rate
    y = x[: nb * block] * np.exp(-2j * np.pi * offset * t).astype(np.complex64)
    amp = np.abs(y.reshape(nb, block).mean(axis=1))
    sigma2 = noise / float(np.sum(w.astype(np.float64) ** 2))  # per sample
    sigma_b = np.sqrt(sigma2 / block)
    first = amp > ON_SIGMA * sigma_b
    if not first.any():
        return []
    thr = max(ON_SIGMA * sigma_b, ON_FRACTION * float(np.median(amp[first])))
    min_blocks = round(MIN_ON_SEC / BLOCK_SEC)
    bridge = round(BRIDGE_SEC / BLOCK_SEC)

    found = []
    for a0, b0 in _segments(amp > thr, bridge):
        if b0 - a0 < min_blocks:
            continue
        level = float(np.median(amp[a0:b0]))
        found.append(
            ToneSegment(
                offset_hz=offset,
                start_s=a0 * block / rate,
                duration_s=(b0 - a0) * block / rate,
                level_db=20.0 * float(np.log10(level)),
                snr_db=10.0 * float(np.log10(level**2 / (sigma2 * bin_hz / rate))),
                touches_start=a0 == 0,
                touches_end=b0 == nb,
            )
        )
    return found
