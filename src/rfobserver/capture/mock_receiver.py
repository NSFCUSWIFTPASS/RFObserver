"""Mock receiver for testing and development without USRP hardware."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

import numpy as np

from rfobserver.capture.receiver import CaptureResult, RawCapture, ReceiverConfig

logger = logging.getLogger(__name__)


class MockReceiver:
    """Generates synthetic IQ data for testing."""

    def __init__(self, receiver_config: ReceiverConfig, seed: int = 42) -> None:
        self._config = receiver_config
        self._serial = "MOCK0001"
        self._rng = np.random.default_rng(seed)
        self._capture_count = 0
        self._streaming = False
        self._stream_center_freq: int = 0
        self._closed = False

    @property
    def serial(self) -> str:
        return self._serial

    @property
    def config(self) -> ReceiverConfig:
        return self._config

    def initialize(self) -> None:
        logger.info("MockReceiver initialized (serial=%s)", self._serial)

    async def reconfigure(self, new_config: ReceiverConfig) -> None:
        self._config = new_config
        logger.info(
            "MockReceiver reconfigured: gain=%d, bw=%d",
            new_config.gain_db,
            new_config.bandwidth_hz,
        )

    async def receive_samples(self, center_freq_hz: int) -> CaptureResult:
        num_samples = self._config.num_samples

        # Simulate real capture duration so double-buffer timing is realistic
        await asyncio.sleep(self._config.duration_sec)

        # Generate noise floor
        noise_i = self._rng.integers(-500, 500, size=num_samples, dtype=np.int16)
        noise_q = self._rng.integers(-500, 500, size=num_samples, dtype=np.int16)

        # Inject a strong tone every 5th capture to create a visible spike
        if self._capture_count % 5 == 0:
            t = np.arange(num_samples) / self._config.bandwidth_hz
            tone_freq = self._config.bandwidth_hz * 0.2  # offset from center
            amplitude = 8000
            noise_i = noise_i + (amplitude * np.cos(2 * np.pi * tone_freq * t)).astype(np.int16)
            noise_q = noise_q + (amplitude * np.sin(2 * np.pi * tone_freq * t)).astype(np.int16)

        # Interleave I/Q as SC16
        iq = np.empty(num_samples * 2, dtype=np.int16)
        iq[0::2] = noise_i
        iq[1::2] = noise_q

        self._capture_count += 1

        return CaptureResult(
            raw_capture=RawCapture(
                iq_data_bytes=iq.tobytes(),
                center_freq_hz=center_freq_hz,
                capture_timestamp=datetime.now(timezone.utc),
            ),
            receiver_config=self._config,
        )

    # -- Streaming methods (called from a dedicated receiver thread) --

    def start_streaming(self, center_freq_hz: int) -> None:
        self._streaming = True
        self._stream_center_freq = center_freq_hz
        logger.info("MockReceiver started streaming at %d Hz", center_freq_hz)

    def recv_chunk(self, out_buf: np.ndarray[Any, np.dtype[Any]]) -> int:
        """Fill *out_buf* (int32, SC16) with synthetic samples.

        Sleeps for the equivalent real-time duration to simulate hardware pacing.
        """
        num_samples = len(out_buf)
        chunk_duration = num_samples / self._config.bandwidth_hz
        time.sleep(chunk_duration)

        # Generate noise as interleaved int16 pairs packed into int32
        noise_i = self._rng.integers(-500, 500, size=num_samples, dtype=np.int16)
        noise_q = self._rng.integers(-500, 500, size=num_samples, dtype=np.int16)

        # Inject a tone every 5th chunk
        if self._capture_count % 5 == 0:
            t = np.arange(num_samples) / self._config.bandwidth_hz
            tone_freq = self._config.bandwidth_hz * 0.2
            amplitude = 8000
            noise_i = noise_i + (amplitude * np.cos(2 * np.pi * tone_freq * t)).astype(np.int16)
            noise_q = noise_q + (amplitude * np.sin(2 * np.pi * tone_freq * t)).astype(np.int16)

        self._capture_count += 1

        # Pack interleaved int16 pairs into int32 view
        packed = np.empty(num_samples * 2, dtype=np.int16)
        packed[0::2] = noise_i
        packed[1::2] = noise_q
        out_buf[:] = packed.view(np.int32)
        return num_samples

    def stop_streaming(self) -> None:
        self._streaming = False
        logger.info("MockReceiver stopped streaming")

    def close(self) -> None:
        """Release the (mock) device. No hardware to free; records state."""
        self._streaming = False
        self._closed = True
        logger.info("MockReceiver closed")

    async def get_temperature(self) -> float | None:
        return float(45.0 + self._rng.uniform(-2, 2))
