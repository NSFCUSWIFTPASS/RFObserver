"""RTL-SDR receiver (RTL2832U dongles, e.g. NooElec NESDR) via librtlsdr.

Same interface as the USRP Receiver, so the pipeline runs unchanged. The
library is loaded with ctypes from the system's librtlsdr (``apt install
librtlsdr0``); the user needs access to the USB device (the package's udev
rule gives it to group ``plugdev``).

Streaming uses librtlsdr's async read on a background thread: the library
keeps several USB transfers in flight, and each completed block is handed to
a bounded queue that ``recv_chunk`` drains. When the pipeline falls behind
the queue fills and blocks are dropped; the next block that is queued carries
how many bytes were lost just before it, so the loss is reported as a gap
(``last_gaps``) exactly where it happened, as the USRP receiver does for
overflows.

Samples arrive as unsigned 8-bit I/Q (offset 127.5) and are converted to the
pipeline's SC16 (int16 I/Q packed in int32). The dongle has no clock: a
chunk's ``last_device_time`` is the host time its first block arrived, less
the block's duration, and capture/clock.py turns that into the capture time.

Supported sample rates: 225,001 to 300,000 and 900,001 to 3,200,000 S/s
(above about 2.56 MS/s many dongles drop samples).
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import logging
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Any

import numpy as np

from rfobserver.capture.receiver import CaptureResult, RawCapture, ReceiverConfig

logger = logging.getLogger(__name__)

# Bytes per async USB block (2 bytes per complex sample): 128 Ki samples,
# about 55 ms at 2.4 MS/s. Must be a multiple of 512.
BLOCK_BYTES = 256 * 1024
TRANSFERS = 12  # USB transfers in flight inside librtlsdr
QUEUE_BLOCKS = 64  # blocks buffered between the USB thread and the pipeline (~3.5 s)
RECV_TIMEOUT_SEC = 2.0

_TUNER_TYPES = {
    0: "unknown", 1: "E4000", 2: "FC0012", 3: "FC0013", 4: "FC2580", 5: "R820T", 6: "R828D",
}  # fmt: skip
_CALLBACK = ctypes.CFUNCTYPE(None, ctypes.POINTER(ctypes.c_ubyte), ctypes.c_uint32, ctypes.c_void_p)


def valid_rate(rate: float) -> bool:
    return 225_001 <= rate <= 300_000 or 900_001 <= rate <= 3_200_000


def u8_to_sc16(raw: np.ndarray[Any, np.dtype[np.uint8]], out_i16: np.ndarray[Any, Any]) -> None:
    """Interleaved u8 I/Q (zero at 127.5) into interleaved int16: (2u - 255) * 128,
    so 0..255 maps to -32640..32640, symmetric, with no wrap at 255."""
    np.multiply(raw.astype(np.int16) * 2 - 255, 128, out=out_i16, casting="unsafe")


def _load_lib() -> Any:
    lib = ctypes.CDLL("librtlsdr.so.0")
    p = ctypes.c_void_p
    lib.rtlsdr_open.argtypes = [ctypes.POINTER(p), ctypes.c_uint32]
    lib.rtlsdr_close.argtypes = [p]
    lib.rtlsdr_set_sample_rate.argtypes = [p, ctypes.c_uint32]
    lib.rtlsdr_get_sample_rate.argtypes = [p]
    lib.rtlsdr_get_sample_rate.restype = ctypes.c_uint32
    lib.rtlsdr_set_center_freq.argtypes = [p, ctypes.c_uint32]
    lib.rtlsdr_get_center_freq.argtypes = [p]
    lib.rtlsdr_get_center_freq.restype = ctypes.c_uint32
    lib.rtlsdr_set_freq_correction.argtypes = [p, ctypes.c_int]
    lib.rtlsdr_set_tuner_gain_mode.argtypes = [p, ctypes.c_int]
    lib.rtlsdr_set_tuner_gain.argtypes = [p, ctypes.c_int]
    lib.rtlsdr_get_tuner_gains.argtypes = [p, ctypes.POINTER(ctypes.c_int)]
    lib.rtlsdr_get_tuner_type.argtypes = [p]
    lib.rtlsdr_set_agc_mode.argtypes = [p, ctypes.c_int]
    lib.rtlsdr_reset_buffer.argtypes = [p]
    lib.rtlsdr_read_sync.argtypes = [p, ctypes.c_void_p, ctypes.c_int, ctypes.POINTER(ctypes.c_int)]
    u32 = ctypes.c_uint32
    lib.rtlsdr_read_async.argtypes = [p, _CALLBACK, ctypes.c_void_p, u32, u32]
    lib.rtlsdr_cancel_async.argtypes = [p]
    lib.rtlsdr_get_device_usb_strings.argtypes = [ctypes.c_uint32] + [ctypes.c_char_p] * 3
    return lib


class RtlSdrReceiver:
    """RTL2832U receiver with the USRP Receiver's interface."""

    def __init__(
        self,
        receiver_config: ReceiverConfig,
        device_index: int = 0,
        ppm: int = 0,
        lib: Any = None,
    ) -> None:
        self._config = receiver_config
        self._index = device_index
        self._ppm = ppm
        self._lib = lib  # loaded on initialize() unless given (tests)
        self._dev: Any = None
        self._serial = ""
        self._lock = threading.Lock()
        self._streaming = False
        self._thread: threading.Thread | None = None
        # (data, arrival time, bytes dropped just before it)
        self._blocks: queue.Queue[tuple[bytes, float, int]] = queue.Queue(maxsize=QUEUE_BLOCKS)
        self._pending = b""  # bytes of a block not yet handed out
        self._pending_t = 0.0  # arrival time of _pending's first byte
        self._drop_pending = 0  # dropped since the last queued block (USB thread)
        self._callback = _CALLBACK(self._on_block)  # kept alive while streaming
        self.gain_db: float | None = None  # the tuner gain actually set
        self.last_gaps: list[tuple[int, int]] = []
        self.last_device_time: float | None = None
        self.overflow_events = 0
        self.overflow_lost_samples = 0

    @property
    def serial(self) -> str:
        return self._serial

    @property
    def config(self) -> ReceiverConfig:
        return self._config

    # -- lifecycle --

    def initialize(self) -> None:
        rate = self._config.bandwidth_hz
        if not valid_rate(rate):
            msg = (
                f"RTL-SDR cannot sample at {rate / 1e6:.3f} MS/s: set BANDWIDTH to "
                "0.225-0.3 or 0.9-3.2 MS/s (2.4 MS/s is reliable)"
            )
            raise RuntimeError(msg)
        with self._lock:
            if self._lib is None:
                self._lib = _load_lib()
            lib = self._lib
            if self._dev is None:
                m, p, s = (ctypes.create_string_buffer(256) for _ in range(3))
                lib.rtlsdr_get_device_usb_strings(self._index, m, p, s)
                dev = ctypes.c_void_p()
                r = lib.rtlsdr_open(ctypes.byref(dev), self._index)
                if r != 0:
                    msg = (
                        f"RTL-SDR device {self._index} did not open (error {r}): is it plugged "
                        "in, used by another program, or not accessible (group plugdev)?"
                    )
                    raise RuntimeError(msg)
                self._dev = dev
                serial = s.value.decode(errors="replace").strip() or str(self._index)
                self._serial = f"RTL{serial}"
                tuner = _TUNER_TYPES.get(lib.rtlsdr_get_tuner_type(dev), "unknown")
                logger.info(
                    "RTL-SDR opened: %s %s, serial %s, tuner %s",
                    m.value.decode(errors="replace"),
                    p.value.decode(errors="replace"),
                    serial,
                    tuner,
                )
            lib.rtlsdr_set_sample_rate(self._dev, int(rate))
            actual = lib.rtlsdr_get_sample_rate(self._dev)
            if actual != int(rate):
                logger.warning("RTL-SDR sample rate %d coerced to %d", rate, actual)
            if self._ppm:
                lib.rtlsdr_set_freq_correction(self._dev, int(self._ppm))
            self._set_gain(self._config.gain_db)
            lib.rtlsdr_set_agc_mode(self._dev, 0)
            lib.rtlsdr_reset_buffer(self._dev)

    def _set_gain(self, gain_db: float) -> None:
        """Manual gain at the tuner's nearest supported step."""
        lib = self._lib
        n = lib.rtlsdr_get_tuner_gains(self._dev, None)
        if n <= 0:
            self.gain_db = None
            return
        steps = (ctypes.c_int * n)()
        lib.rtlsdr_get_tuner_gains(self._dev, steps)
        best = min(steps, key=lambda g: abs(g - gain_db * 10))
        lib.rtlsdr_set_tuner_gain_mode(self._dev, 1)
        lib.rtlsdr_set_tuner_gain(self._dev, best)
        self.gain_db = best / 10.0
        if abs(self.gain_db - gain_db) > 0.05:
            logger.info("RTL-SDR gain %.1f dB set as %.1f dB (nearest step)", gain_db, self.gain_db)

    async def reconfigure(self, new_config: ReceiverConfig) -> None:
        loop = asyncio.get_running_loop()
        self._config = new_config
        await loop.run_in_executor(None, self.initialize)

    def close(self) -> None:
        self.stop_streaming()
        with self._lock:
            if self._dev is not None:
                self._lib.rtlsdr_close(self._dev)
                self._dev = None
        logger.info("RTL-SDR closed")

    async def get_temperature(self) -> float | None:
        return None

    # -- streaming --

    def _on_block(self, buf: Any, length: int, _ctx: Any) -> None:
        """librtlsdr's USB thread: copy the block out and queue it."""
        now = time.time()
        data = ctypes.string_at(buf, length)
        try:
            self._blocks.put_nowait((data, now, self._drop_pending))
        except queue.Full:
            self._drop_pending += length
        else:
            self._drop_pending = 0

    def _run_async(self) -> None:
        r = self._lib.rtlsdr_read_async(self._dev, self._callback, None, TRANSFERS, BLOCK_BYTES)
        if r != 0 and self._streaming:
            logger.error("RTL-SDR async read ended with error %d", r)

    def start_streaming(self, center_freq_hz: int) -> None:
        with self._lock:
            self._lib.rtlsdr_set_center_freq(self._dev, int(center_freq_hz))
            if self._streaming:
                return  # retuned in place; the stream continues
            self._lib.rtlsdr_reset_buffer(self._dev)
            self._clear()
            self._streaming = True
            self._thread = threading.Thread(target=self._run_async, name="rtlsdr-usb", daemon=True)
            self._thread.start()
        logger.info("RTL-SDR streaming at %.3f MHz", center_freq_hz / 1e6)

    def stop_streaming(self) -> None:
        with self._lock:
            if not self._streaming:
                return
            self._streaming = False
            self._lib.rtlsdr_cancel_async(self._dev)
            thread, self._thread = self._thread, None
        if thread is not None:
            thread.join(timeout=5)
        self._clear()
        logger.info("RTL-SDR streaming stopped")

    def _clear(self) -> None:
        with contextlib.suppress(queue.Empty):
            while True:
                self._blocks.get_nowait()
        self._pending = b""
        self._drop_pending = 0

    def recv_chunk(self, out_buf: np.ndarray[Any, Any]) -> int:
        """Fill *out_buf* (int32, SC16) from the stream; returns samples written.

        Blocks dropped while the pipeline was behind are reported as one gap
        at the point in this chunk where they were lost (offset, samples).
        """
        target = len(out_buf)
        out_i16 = out_buf.view(np.int16)
        rate = float(self._config.bandwidth_hz)
        gaps: list[tuple[int, int]] = []
        first_t: float | None = None
        total = 0
        while total < target:
            if not self._pending:
                try:
                    self._pending, self._pending_t, dropped = self._blocks.get(
                        timeout=RECV_TIMEOUT_SEC
                    )
                except queue.Empty:
                    logger.error("RTL-SDR: no samples for %.0f s", RECV_TIMEOUT_SEC)
                    break
                # A block's arrival is when its last sample came in.
                self._pending_t -= len(self._pending) / 2 / rate
                if dropped:
                    lost = dropped // 2
                    gaps.append((total, lost))
                    self.overflow_events += 1
                    self.overflow_lost_samples += lost
            take = min(target - total, len(self._pending) // 2)
            if first_t is None:
                first_t = self._pending_t
            raw = np.frombuffer(self._pending, dtype=np.uint8, count=2 * take)
            u8_to_sc16(raw, out_i16[2 * total : 2 * (total + take)])
            self._pending = self._pending[2 * take :]
            self._pending_t += take / rate
            total += take
        self.last_gaps = gaps
        self.last_device_time = first_t
        return total

    # -- batch capture (sweeps) --

    async def receive_samples(self, center_freq_hz: int) -> CaptureResult:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._receive_blocking, center_freq_hz)

    def _receive_blocking(self, center_freq_hz: int) -> CaptureResult:
        n = self._config.num_samples
        nbytes = ((2 * n + 511) // 512) * 512
        raw = (ctypes.c_ubyte * nbytes)()
        got = ctypes.c_int(0)
        with self._lock:
            self._lib.rtlsdr_set_center_freq(self._dev, int(center_freq_hz))
            self._lib.rtlsdr_reset_buffer(self._dev)
            stamp = datetime.now(timezone.utc)
            r = self._lib.rtlsdr_read_sync(self._dev, raw, nbytes, ctypes.byref(got))
        if r != 0 or got.value < 2 * n:
            msg = f"RTL-SDR capture truncated: {got.value // 2}/{n} samples (error {r})"
            raise RuntimeError(msg)
        iq = np.empty(2 * n, dtype=np.int16)
        u8_to_sc16(np.frombuffer(memoryview(raw), dtype=np.uint8, count=2 * n), iq)
        return CaptureResult(
            raw_capture=RawCapture(
                iq_data_bytes=iq.tobytes(), center_freq_hz=center_freq_hz, capture_timestamp=stamp
            ),
            receiver_config=self._config,
        )
