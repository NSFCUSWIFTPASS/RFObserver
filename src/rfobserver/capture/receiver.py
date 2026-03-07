"""USRP hardware receiver interface.

Ported from rf_survey.receiver. Wraps UHD Python bindings for USRP
acquisition with thread-safe reconfiguration.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

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

    async def receive_samples(self, center_freq_hz: int) -> CaptureResult: ...
    async def reconfigure(self, new_config: ReceiverConfig) -> None: ...
    async def get_temperature(self) -> float | None: ...

    @property
    def serial(self) -> str: ...

    @property
    def config(self) -> ReceiverConfig: ...


class Receiver:
    """USRP hardware receiver wrapping UHD Python bindings."""

    def __init__(self, receiver_config: ReceiverConfig) -> None:
        self._hardware_lock = threading.Lock()
        self._config = receiver_config
        self._capture_buffer: np.ndarray | None = None
        self._serial = ""

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

        total_samples = self._config.num_samples
        if self._capture_buffer is None or self._capture_buffer.size != total_samples:
            self._capture_buffer = np.zeros(total_samples, dtype=np.int32)

        logger.info("USRP initialization complete (serial=%s)", self._serial)

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
        assert self._capture_buffer is not None

        with self._hardware_lock:
            config_snapshot = deepcopy(self._config)

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
            capture_timestamp = datetime.now(UTC)
            rx_metadata = uhd.types.RXMetadata()

            samples_received = self.rx_streamer.recv(
                self._capture_buffer, rx_metadata, timeout=timeout
            )

            if rx_metadata.error_code != uhd.types.RXMetadataErrorCode.none:
                raise RuntimeError(f"UHD recv error: {rx_metadata.strerror()}")

            if samples_received < config_snapshot.num_samples:
                raise RuntimeError(
                    f"Capture truncated: {samples_received}/{config_snapshot.num_samples}"
                )

            return CaptureResult(
                raw_capture=RawCapture(
                    iq_data_bytes=self._capture_buffer.tobytes(),
                    center_freq_hz=center_freq_hz,
                    capture_timestamp=capture_timestamp,
                ),
                receiver_config=config_snapshot,
            )

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
