"""Isolate one detected burst from wideband IQ: read its samples (plus a small
guard band), shift its peak frequency to DC and decimate to the rtl_433 tier
rate. Pure DSP: no threads, files or subprocesses. The caller runs it on the
isolation worker thread.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np

from rfobserver.processing.channelize import channelize_to_cs16, select_rate_and_protocols
from rfobserver.processing.iq_utils import convert_sc16_to_complex

if TYPE_CHECKING:
    from collections.abc import Callable

    from rfobserver.models import BurstFingerprint

# Samples kept on each side of the burst so the decoder sees its edges.
GUARD_SEC = 0.002


@dataclass
class IsolatedBurst:
    burst_id: str
    cs16: bytes  # interleaved little-endian int16 I/Q at rate_hz, peak-normalized
    rate_hz: int
    passes: list[list[str]]
    freq_hz: float  # absolute frequency now at DC
    start_sample: int | None  # stream sample of cs16's first source sample
    num_source_samples: int  # wideband samples that went in
    truncated: bool  # longer than max_burst_sec; only its head was kept


def iq_to_complex(arr: np.ndarray[Any, np.dtype[Any]]) -> np.ndarray[Any, np.dtype[Any]]:
    """int32-packed SC16 (the ring's dtype) or complex -> complex64 in [-1, 1)."""
    if np.iscomplexobj(arr):
        return arr.astype(np.complex64, copy=False)
    return convert_sc16_to_complex(np.ascontiguousarray(arr))


def isolate_burst(
    burst: BurstFingerprint,
    *,
    read_range: Callable[[int, int], np.ndarray[Any, np.dtype[Any]] | None] | None,
    read_all: Callable[[], np.ndarray[Any, np.dtype[Any]] | None] | None,
    sample_rate_hz: float,
    center_freq_hz: float,
    max_burst_sec: float,
) -> IsolatedBurst | str:
    """Return the isolated burst, or ``"iq_expired"`` if its samples are gone.

    With ``start_sample`` / ``stop_sample`` the burst's own range (plus guard)
    is read via ``read_range``; without them (sweep pipeline) the whole capture
    from ``read_all`` is channelized, as the sweep path always did.
    """
    guard = int(GUARD_SEC * sample_rate_hz)
    truncated = False
    start: int | None = None
    if burst.start_sample is not None and burst.stop_sample is not None:
        start = max(0, burst.start_sample - guard)
        stop = burst.stop_sample
        max_n = int(max_burst_sec * sample_rate_hz)
        if stop - burst.start_sample > max_n:
            stop = burst.start_sample + max_n
            truncated = True
        stop += guard
        data = read_range(start, stop) if read_range is not None else None
    else:
        data = read_all() if read_all is not None else None
    if data is None or len(data) == 0:
        return "iq_expired"
    iq = iq_to_complex(data)
    rate, passes = select_rate_and_protocols(burst.bandwidth_hz)
    offset = float(burst.peak_freq_hz) - float(center_freq_hz)
    cs16 = channelize_to_cs16(iq, float(sample_rate_hz), offset, rate)
    return IsolatedBurst(
        burst_id=burst.burst_id,
        cs16=cs16,
        rate_hz=rate,
        passes=passes,
        freq_hz=float(burst.peak_freq_hz),
        start_sample=start,
        num_source_samples=len(iq),
        truncated=truncated,
    )
