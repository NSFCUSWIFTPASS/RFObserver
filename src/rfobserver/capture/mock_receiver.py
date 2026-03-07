"""Mock receiver for testing and development without USRP hardware."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

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

        # Generate noise + optional tone
        noise_i = self._rng.integers(-500, 500, size=num_samples, dtype=np.int16)
        noise_q = self._rng.integers(-500, 500, size=num_samples, dtype=np.int16)

        # Interleave I/Q as SC16
        iq = np.empty(num_samples * 2, dtype=np.int16)
        iq[0::2] = noise_i
        iq[1::2] = noise_q

        self._capture_count += 1

        return CaptureResult(
            raw_capture=RawCapture(
                iq_data_bytes=iq.tobytes(),
                center_freq_hz=center_freq_hz,
                capture_timestamp=datetime.now(UTC),
            ),
            receiver_config=self._config,
        )

    async def get_temperature(self) -> float | None:
        return float(45.0 + self._rng.uniform(-2, 2))
