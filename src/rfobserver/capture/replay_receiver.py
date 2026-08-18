"""A receiver that replays a recorded SigMF capture through the live pipeline.

Serves the memory-mapped capture to ``StreamingProcessor`` chunk-by-chunk in the
same SC16-int32 format the real/mock receivers produce, so recorded captures run
through the exact detection path. Streams from the memmap (no full load), and
after the capture is exhausted serves a short tail of noise (matched to the
capture's own floor) so a burst at the very end still flushes out of the rolling
detector's trailing margin.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Any

import numpy as np

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.sigmf_reader import SigmfCapture, to_sc16_int32

if TYPE_CHECKING:
    from rfobserver.capture.receiver import ReceiverConfig

logger = logging.getLogger(__name__)


class FileReplayReceiver(MockReceiver):
    """Replays a ``SigmfCapture`` as an SC16 stream.

    Pacing and looping are both optional and controlled by the ``paced`` and
    ``loop`` constructor args: unpaced batch replay serves chunks as fast as
    the pipeline can consume them, while paced replay sleeps to match
    wall-clock playback at ``speed`` real-time; ``loop`` re-seeks to the start
    of the capture on exhaustion instead of draining to a noise tail.
    """

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
        self._pos = 0  # sample index into the capture
        self._n = (
            capture.num_samples if max_samples is None else min(capture.num_samples, max_samples)
        )
        self._paced = paced
        self.loop = loop
        self.source_name = source_name
        self._speed_lock = threading.Lock()
        self._speed = max(0.01, float(speed))
        self._exhausted = threading.Event()
        self._drain_rng = np.random.default_rng(0)
        # Estimate the capture's NOISE FLOOR (not RMS) so trailing drain noise
        # matches the quiet parts. Using RMS would track a strong continuous
        # signal and make the drain flood the detector; too-quiet drain would
        # instead pull the per-bin floor down and manufacture a full-span burst.
        # The 20th percentile of per-sample magnitude approximates the floor.
        head = np.asarray(self._cap.raw[:400_000], dtype=np.float64)
        if self._cap.datatype == "cf32_le":
            head = head * 32767.0
        if head.size >= 2:
            iq = head[: (head.size // 2) * 2].reshape(-1, 2)
            mag = np.hypot(iq[:, 0], iq[:, 1])
            # per-component stddev of a Gaussian with this magnitude percentile
            self._drain_sd = float(np.percentile(mag, 20)) / 1.253 or 100.0
        else:
            self._drain_sd = 100.0

    @property
    def exhausted(self) -> bool:
        return self._exhausted.is_set()

    @property
    def speed(self) -> float:
        with self._speed_lock:
            return self._speed

    def set_speed(self, speed: float) -> None:
        with self._speed_lock:
            self._speed = max(0.01, float(speed))

    def _fill_drain(self, out_buf: np.ndarray[Any, np.dtype[Any]], start: int = 0) -> None:
        n = len(out_buf) - start
        if n <= 0:
            return
        sd = self._drain_sd
        i = (self._drain_rng.standard_normal(n) * sd).astype(np.int16)
        q = (self._drain_rng.standard_normal(n) * sd).astype(np.int16)
        packed = np.empty(n * 2, dtype=np.int16)
        packed[0::2] = i
        packed[1::2] = q
        out_buf[start:] = packed.view(np.int32)

    def recv_chunk(self, out_buf: np.ndarray[Any, np.dtype[Any]]) -> int:
        n = len(out_buf)  # samples requested
        if self._n <= 0:
            # Empty capture: looping would otherwise spin resetting `remaining`
            # to 0 forever and returning a partially/never-filled buffer. Serve
            # drain noise for the whole chunk instead of hanging or leaving
            # uninitialized tail data.
            self._fill_drain(out_buf)
            return n
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
