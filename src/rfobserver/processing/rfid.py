"""Recognize a UHF RFID reader (EPC Gen2, ISO 18000-63) in a channelized burst.
Pure DSP - no I/O.

A reader keeps a carrier on for its whole dwell (up to 400 ms per channel in
the US) to power the tags, and talks to them by dipping that carrier in short
pulses whose spacing carries the data (pulse-interval encoding, PIE: data-0
one Tari, data-1 1.5 to 2 Tari, Tari 6.25 to 25 us). A burst is a reader when
one spectral line holds a large share of the in-band power and the envelope
dips at PIE spacing. US readers hop over 50 channels at 902.75 + n * 0.5 MHz;
the channel is reported when the carrier is on that raster. See
docs/debugging/2026-11-06_rfid-reader-hcro.md for the reader seen at HCRO.
"""

from __future__ import annotations

from typing import Any, TypeAlias

import numpy as np

IQ: TypeAlias = "np.ndarray[Any, np.dtype[Any]]"

MODEL = "RFID-Reader"
PROTOCOL = "EPC Gen2"

MIN_SEC = 0.005
# The carrier is searched within this offset of DC (isolation centres the burst
# on its peak cell, a cell or two from the carrier).
CARRIER_SEARCH_HZ = 50_000
CARRIER_LINE_HZ = 2_000
BAND_HZ = 150_000
# Share of the in-band power in the carrier line. HCRO reader hops: 0.68-0.69;
# an SSM hop 0.07, noise 0.01.
MIN_CARRIER_FRACTION = 0.3
# A dip is the envelope under this fraction of the carrier level.
DIP_LEVEL = 0.5
# Gen2 pulse widths are 0.265 to 0.525 Tari: 1.6 to 13 us over the Tari range.
PULSE_US = (1.5, 15.0)
# Pulse-to-pulse spacing of PIE symbols (6.25 us to 2 x 25 us, with margin);
# spacings over GAP_US are pauses between commands, not symbols.
SYMBOL_US = (6.0, 60.0)
GAP_US = 100.0
MIN_SYMBOL_FRACTION = 0.6
MIN_PULSES_PER_SEC = 2_000

US_RASTER_START_MHZ = 902.75
US_RASTER_STEP_MHZ = 0.5
US_CHANNELS = 50
RASTER_TOLERANCE_HZ = 25_000


def _iq(cs16: bytes) -> IQ:
    v = np.frombuffer(cs16, dtype="<i2").astype(np.float32)
    return v[0::2] + 1j * v[1::2]


def _us_channel_mhz(carrier_hz: float) -> float | None:
    k = round((carrier_hz / 1e6 - US_RASTER_START_MHZ) / US_RASTER_STEP_MHZ)
    if not 0 <= k < US_CHANNELS:
        return None
    channel = US_RASTER_START_MHZ + k * US_RASTER_STEP_MHZ
    if abs(carrier_hz - channel * 1e6) > RASTER_TOLERANCE_HZ:
        return None
    return round(channel, 3)


def _dips(env: IQ, rate_hz: int) -> tuple[IQ, IQ]:
    """Start and width (us) of each dip under DIP_LEVEL of the carrier level,
    with dips split by under 1 us of carrier merged into one."""
    us = 1e6 / rate_hz
    level = float(np.median(env[env > 0.3 * np.percentile(env, 90)]))
    low = (env < DIP_LEVEL * level).astype(np.int8)
    edges = np.diff(np.r_[0, low, 0])
    starts = np.flatnonzero(edges == 1)
    ends = np.flatnonzero(edges == -1)
    if len(starts) == 0:
        return starts.astype(float), starts.astype(float)
    joined = (starts[1:] - ends[:-1]) * us < 1.0
    keep_start = np.r_[True, ~joined]
    keep_end = np.r_[~joined, True]
    starts, ends = starts[keep_start], ends[keep_end]
    return starts * us, (ends - starts) * us


def classify_rfid_reader(
    cs16: bytes, rate_hz: int, freq_hz: float | None = None
) -> dict[str, Any] | None:
    """A reader's description when the burst is one, else None.

    ``freq_hz`` is the absolute frequency at DC of the burst (isolation's
    ``freq_hz``); with it the carrier frequency and US channel are reported.
    """
    x = _iq(cs16)
    n = len(x)
    if n < MIN_SEC * rate_hz:
        return None

    spectrum = np.abs(np.fft.fft(x)) ** 2
    f = np.fft.fftfreq(n, 1.0 / rate_hz)
    near = np.flatnonzero(np.abs(f) <= CARRIER_SEARCH_HZ)
    offset = float(f[near[np.argmax(spectrum[near])]])
    in_band = float(spectrum[np.abs(f - offset) <= BAND_HZ].sum())
    line = float(spectrum[np.abs(f - offset) <= CARRIER_LINE_HZ].sum())
    carrier_fraction = line / in_band if in_band > 0 else 0.0
    if carrier_fraction < MIN_CARRIER_FRACTION:
        return None

    smooth = max(1, round(rate_hz / 1e6))  # about 1 us
    env = np.convolve(np.abs(x), np.ones(smooth) / smooth, mode="same")
    starts, widths = _dips(env, rate_hz)
    pulse = (widths >= PULSE_US[0]) & (widths <= PULSE_US[1])
    if pulse.sum() < MIN_PULSES_PER_SEC * n / rate_hz:
        return None
    spacing = np.diff(starts[pulse])
    spacing = spacing[spacing < GAP_US]
    symbols = spacing[(spacing >= SYMBOL_US[0]) & (spacing <= SYMBOL_US[1])]
    if len(spacing) == 0 or len(symbols) / len(spacing) < MIN_SYMBOL_FRACTION:
        return None

    carrier_hz = freq_hz + offset if freq_hz is not None else None
    return {
        "model": MODEL,
        "protocol": PROTOCOL,
        "carrier_hz": round(carrier_hz, 1) if carrier_hz is not None else None,
        "channel_mhz": _us_channel_mhz(carrier_hz) if carrier_hz is not None else None,
        "carrier_fraction": round(carrier_fraction, 2),
        "pulses": int(pulse.sum()),
    }
