"""USRP hardware receiver interface.

Ported from rf_survey.receiver. Wraps UHD Python bindings for USRP
acquisition with thread-safe reconfiguration.

Uses double-buffering: two pre-allocated numpy arrays alternate between
capture and processing so the SDR never stalls waiting for the CPU.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class ReceiverConfig:
    gain_db: int
    bandwidth_hz: int
    duration_sec: float

    @property
    def num_samples(self) -> int:
        return int(self.duration_sec * self.bandwidth_hz)


@dataclass
class RawCapture:
    iq_data_bytes: bytes
    center_freq_hz: int
    capture_timestamp: datetime


@dataclass
class CaptureResult:
    raw_capture: RawCapture
    receiver_config: ReceiverConfig


class IReceiver(Protocol):
    """Protocol for receiver implementations (real + mock)."""

    # Batch capture (existing)
    async def receive_samples(self, center_freq_hz: int) -> CaptureResult: ...
    async def reconfigure(self, new_config: ReceiverConfig) -> None: ...
    async def get_temperature(self) -> float | None: ...

    # Streaming capture
    def start_streaming(self, center_freq_hz: int) -> None: ...
    def recv_chunk(self, out_buf: np.ndarray) -> int: ...
    def stop_streaming(self) -> None: ...

    # UHD overflow loss: gaps from the last recv_chunk() as
    # (offset_in_out_buf, lost_samples), and cumulative counters.
    last_gaps: list[tuple[int, int]]
    overflow_events: int
    overflow_lost_samples: int

    # Lifecycle
    def initialize(self) -> None: ...
    def close(self) -> None: ...

    @property
    def serial(self) -> str: ...

    @property
    def config(self) -> ReceiverConfig: ...


class Receiver:
    """USRP hardware receiver wrapping UHD Python bindings.

    Uses two pre-allocated capture buffers (A/B) that alternate each call.
    While the caller processes buffer A, the next capture fills buffer B.
    """

    def __init__(self, receiver_config: ReceiverConfig) -> None:
        self._hardware_lock = threading.Lock()
        self._config = receiver_config
        self._buffers: tuple[np.ndarray, np.ndarray] | None = None
        self._active_buf: int = 0  # index into _buffers: 0 or 1
        self._serial = ""
        self._streaming = False
        # UHD handles — created in initialize(), dropped in close().
        self.usrp: Any = None
        self.rx_streamer: Any = None
        # Overflow gap tracking (see recv_chunk). Tick = sample at the stream rate.
        self.last_gaps: list[tuple[int, int]] = []
        self.overflow_events = 0
        self.overflow_lost_samples = 0
        self._stream_rate = float(receiver_config.bandwidth_hz)
        self._next_tick: int | None = None

    @property
    def serial(self) -> str:
        return self._serial

    @property
    def config(self) -> ReceiverConfig:
        return self._config

    def initialize(self) -> None:
        import uhd

        logger.info("Initializing USRP hardware...")
        self.usrp = uhd.usrp.MultiUSRP("num_recv_frames=1024")
        self.usrp.set_rx_rate(self._config.bandwidth_hz, 0)
        # Hardware may coerce the requested rate; gap ticks must use the real one.
        self._stream_rate = float(self.usrp.get_rx_rate(0))
        self.usrp.set_rx_gain(self._config.gain_db, 0)
        self.usrp.set_rx_antenna("RX2", 0)

        self._serial = self.usrp.get_usrp_rx_info(0)["mboard_serial"]

        if "{}".format(self.usrp.get_mboard_sensor("ref_locked", 0)) != "Ref: unlocked":
            logger.info("Setting clock from external source")
            self.usrp.set_clock_source("external")
            self.usrp.set_time_source("external")
        else:
            logger.info("Setting clock to host time")
            self.usrp.set_time_now(uhd.types.TimeSpec(time.time()))

        st_args = uhd.usrp.StreamArgs("sc16", "sc16")
        st_args.channels = [0]
        self.rx_metadata = uhd.types.RXMetadata()
        self.rx_streamer = self.usrp.get_rx_stream(st_args)

        # Allocate double buffers
        n = self._config.num_samples
        self._buffers = (
            np.zeros(n, dtype=np.int32),
            np.zeros(n, dtype=np.int32),
        )
        self._active_buf = 0

        logger.info(
            "USRP initialization complete (serial=%s, double-buffer=%d samples)",
            self._serial,
            n,
        )
        self._reset_gap_tracking()

    async def reconfigure(self, new_config: ReceiverConfig) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._reconfigure_blocking, new_config)

    def _reconfigure_blocking(self, new_config: ReceiverConfig) -> None:
        with self._hardware_lock:
            self._config = new_config
            self.rx_streamer = None
            self.initialize()

    async def receive_samples(self, center_freq_hz: int) -> CaptureResult:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._receive_samples_blocking, center_freq_hz)

    def _receive_samples_blocking(self, center_freq_hz: int) -> CaptureResult:
        import uhd

        assert self.rx_streamer is not None
        assert self._buffers is not None

        with self._hardware_lock:
            config_snapshot = deepcopy(self._config)
            buf = self._buffers[self._active_buf]
            self._active_buf ^= 1  # swap for next call

            self.usrp.set_rx_freq(uhd.libpyuhd.types.tune_request(center_freq_hz), 0)

            # Wait for LO lock
            max_wait = 1.0
            start = time.monotonic()
            while not self.usrp.get_rx_sensor("lo_locked", 0).to_bool():
                if time.monotonic() - start > max_wait:
                    logger.error("LO failed to lock within %.1fs", max_wait)
                    break

            stream_cmd = uhd.types.StreamCMD(uhd.types.StreamMode.num_done)
            stream_cmd.num_samps = config_snapshot.num_samples
            stream_cmd.stream_now = True
            self.rx_streamer.issue_stream_cmd(stream_cmd)

            timeout = config_snapshot.duration_sec + 2.0
            capture_timestamp = datetime.now(timezone.utc)
            rx_metadata = uhd.types.RXMetadata()

            samples_received = self.rx_streamer.recv(buf, rx_metadata, timeout=timeout)

            if rx_metadata.error_code != uhd.types.RXMetadataErrorCode.none:
                raise RuntimeError(f"UHD recv error: {rx_metadata.strerror()}")

            if samples_received < config_snapshot.num_samples:
                raise RuntimeError(
                    f"Capture truncated: {samples_received}/{config_snapshot.num_samples}"
                )

            return CaptureResult(
                raw_capture=RawCapture(
                    iq_data_bytes=buf.tobytes(),
                    center_freq_hz=center_freq_hz,
                    capture_timestamp=capture_timestamp,
                ),
                receiver_config=config_snapshot,
            )

    # -- Streaming methods (called from a dedicated receiver thread) --

    def _reset_gap_tracking(self) -> None:
        """Forget where the last samples ended (a new stream is not a gap)."""
        self._next_tick = None

    def start_streaming(self, center_freq_hz: int) -> None:
        """Tune to *center_freq_hz* and begin continuous streaming."""
        import uhd

        assert self.rx_streamer is not None

        with self._hardware_lock:
            self.usrp.set_rx_freq(uhd.libpyuhd.types.tune_request(center_freq_hz), 0)

            # Wait for LO lock
            max_wait = 1.0
            start = time.monotonic()
            while not self.usrp.get_rx_sensor("lo_locked", 0).to_bool():
                if time.monotonic() - start > max_wait:
                    logger.error("LO failed to lock within %.1fs", max_wait)
                    break

            self._reset_gap_tracking()
            stream_cmd = uhd.types.StreamCMD(uhd.types.StreamMode.start_cont)
            stream_cmd.stream_now = True
            self.rx_streamer.issue_stream_cmd(stream_cmd)
            self._streaming = True

        logger.info("Started continuous streaming at %d Hz", center_freq_hz)

    def recv_chunk(self, out_buf: np.ndarray) -> int:
        """Fill *out_buf* (int32, SC16) with samples from the running stream.

        Calls ``rx_streamer.recv()`` in a loop until the buffer is full and
        returns the number of samples received. A UHD overflow ("O") drops
        samples between packets; every packet carries a time_spec, so the loss
        before a packet is measured exactly in integer ticks and reported via
        ``last_gaps`` (offsets into *out_buf*) and the cumulative
        ``overflow_events`` / ``overflow_lost_samples``. Float seconds are not
        precise enough at epoch device time (off by up to 13 samples at
        56 MS/s). See docs/debugging/2026-09-14_recording-overflow-accounting.md.
        """
        import uhd

        assert self.rx_streamer is not None
        total = 0
        target = len(out_buf)
        rx_md = uhd.types.RXMetadata()
        gaps: list[tuple[int, int]] = []

        while total < target:
            n = self.rx_streamer.recv(out_buf[total:], rx_md, timeout=1.0)
            if rx_md.error_code == uhd.types.RXMetadataErrorCode.overflow:
                logger.warning("UHD overflow (O): lost samples")
            elif rx_md.error_code != uhd.types.RXMetadataErrorCode.none:
                logger.error("UHD recv error: %s", rx_md.strerror())
                break
            if n > 0:
                if rx_md.has_time_spec:
                    tick = int(rx_md.time_spec.to_ticks(self._stream_rate))
                    if self._next_tick is not None and tick > self._next_tick:
                        lost = tick - self._next_tick
                        gaps.append((total, lost))
                        self.overflow_events += 1
                        self.overflow_lost_samples += lost
                    self._next_tick = tick + n
                else:
                    self._next_tick = None
            total += n

        self.last_gaps = gaps
        return total

    def stop_streaming(self) -> None:
        """Stop the continuous stream."""
        import uhd

        assert self.rx_streamer is not None

        stream_cmd = uhd.types.StreamCMD(uhd.types.StreamMode.stop_cont)
        stream_cmd.stream_now = True
        self.rx_streamer.issue_stream_cmd(stream_cmd)
        self._streaming = False
        self._reset_gap_tracking()
        logger.info("Stopped continuous streaming")

    def close(self) -> None:
        """Release the SDR so another process can claim it.

        Drops the UHD streamer and device handles; Python/UHD frees the USB
        device when the last reference goes away. ``initialize()`` recreates
        them, so a closed receiver can be brought back. Safe to call twice or
        before ``initialize()``.
        """
        with self._hardware_lock:
            self.rx_streamer = None
            self.usrp = None
            self._streaming = False
        logger.info("Receiver closed (SDR released)")

    async def get_temperature(self) -> float | None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._get_temperature_blocking)

    def _get_temperature_blocking(self) -> float | None:
        with self._hardware_lock:
            try:
                sensor = self.usrp.get_rx_sensor("temp", 0)
                return float(sensor.value)
            except Exception as e:
                logger.warning("Could not read temperature: %s", e)
                return None
