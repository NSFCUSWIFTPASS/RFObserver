"""Recognize an unmodulated carrier (a CW tone) in a channelized burst.
Pure DSP - no I/O.

A tone keyed on for a while (a test transmitter, a stuck carrier) is detected
as a narrowband burst like any other, but no decoder claims it. A burst is a
tone when one spectral line holds nearly all its in-band power and its
envelope stays level while it is on: no dips (which a UHF RFID reader's PIE
pulses and on-off keying have) and no swing (amplitude modulation).
Isolation hands over the burst with a couple of ms of noise either side, so
the checks run on the span where the envelope is up.

A B200mini receiving at 26 MS/s drops 5 dB for 3.4 ms every 102.4 ms and
shows ghost lines 3 MHz either side while it does (2026-11-18 tone test), so
the level is judged by its median deviation, which a brief step leaves alone,
and a tone must last longer than the ghosts.
"""

from __future__ import annotations

from typing import Any, TypeAlias

import numpy as np

IQ: TypeAlias = "np.ndarray[Any, np.dtype[Any]]"

MODEL = "Tone"
PROTOCOL = "CW"

MIN_SEC = 0.005
# The receiver's ghost lines last 3.4 ms, 8.5 ms when two merge.
MIN_ON_SEC = 0.015
# Isolation centres the burst on its peak cell, a cell or two from the tone.
SEARCH_HZ = 50_000
LINE_HZ = 2_000
BAND_HZ = 150_000
# Share of the in-band power in the line. A tone at the detection threshold
# (about 11 dB above the snippet's noise) holds 0.97, at 3 dB 0.87; an HCRO RFID
# reader hop 0.68 to 0.71, two equal tones 0.5, an SSM hop 0.07.
MIN_LINE_FRACTION = 0.85
# Envelope level that bounds the on span.
ON_LEVEL = 0.5
# A dip: the envelope at about 2 us under DIP_LEVEL of the level. RFID PIE
# pulses are 1.6 to 13 us wide and 80 to 100 % deep; noise at the detection
# threshold rarely reaches this deep at 2 us (it does reach 0.5).
DIP_LEVEL = 0.3
DIP_SMOOTH_SEC = 2e-6
MAX_DIP_FRACTION = 0.01
# The slow envelope (about 100 us) bounds the on span.
SLOW_SMOOTH_SEC = 100e-6
# Level per 1 ms block while on: the median deviation from the median level
# may be this much of it. The 2026-11-18 over-the-air tones measured 0.002 to
# 0.010 (the receiver's step moves under 5 % of blocks); AM at 300 Hz 0.07 at
# 10 % depth, 0.14 at 20 %.
LEVEL_BLOCK_SEC = 0.001
MAX_LEVEL_DEVIATION = 0.05


def _iq(cs16: bytes) -> IQ:
    v = np.frombuffer(cs16, dtype="<i2").astype(np.float32)
    return v[0::2] + 1j * v[1::2]


def _smooth(x: IQ, samples: int) -> IQ:
    samples = max(1, samples)
    return np.convolve(np.abs(x), np.ones(samples) / samples, mode="same")


def classify_tone(cs16: bytes, rate_hz: int, freq_hz: float | None = None) -> dict[str, Any] | None:
    """The tone's description when the burst is one, else None.

    ``freq_hz`` is the absolute frequency at DC of the burst (isolation's
    ``freq_hz``); with it the tone's frequency is reported.
    """
    x = _iq(cs16)
    if len(x) < MIN_SEC * rate_hz:
        return None

    # The on span: where the slow envelope is up, between the guard noise.
    slow = _smooth(x, round(SLOW_SMOOTH_SEC * rate_hz))
    level = float(np.median(slow[slow > 0.3 * np.percentile(slow, 90)]))
    up = np.flatnonzero(slow >= ON_LEVEL * level)
    if len(up) == 0:
        return None
    a, b = int(up[0]), int(up[-1]) + 1
    if b - a < MIN_ON_SEC * rate_hz:
        return None
    on = x[a:b]

    spectrum = np.abs(np.fft.fft(on)) ** 2
    f = np.fft.fftfreq(len(on), 1.0 / rate_hz)
    near = np.flatnonzero(np.abs(f) <= SEARCH_HZ)
    offset = float(f[near[np.argmax(spectrum[near])]])
    in_band = float(spectrum[np.abs(f - offset) <= BAND_HZ].sum())
    line = float(spectrum[np.abs(f - offset) <= LINE_HZ].sum())
    line_fraction = line / in_band if in_band > 0 else 0.0
    if line_fraction < MIN_LINE_FRACTION:
        return None

    fast = _smooth(on, round(DIP_SMOOTH_SEC * rate_hz))
    if float(np.mean(fast < DIP_LEVEL * level)) > MAX_DIP_FRACTION:
        return None
    block = max(1, round(LEVEL_BLOCK_SEC * rate_hz))
    blocks = np.abs(on[: len(on) // block * block]).reshape(-1, block).mean(axis=1)
    middle = float(np.median(blocks))
    if float(np.median(np.abs(blocks - middle))) > MAX_LEVEL_DEVIATION * middle:
        return None

    tone_hz = freq_hz + offset if freq_hz is not None else None
    return {
        "model": MODEL,
        "protocol": PROTOCOL,
        "tone_hz": round(tone_hz, 1) if tone_hz is not None else None,
        "offset_hz": round(offset, 1),
        "duration_ms": round((b - a) * 1000.0 / rate_hz, 1),
        "line_fraction": round(line_fraction, 3),
    }
