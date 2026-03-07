"""Power-threshold trigger with hysteresis.

Python port of the iq2ram trigger logic. Monitors incoming IQ data blocks
and fires when mean power exceeds a threshold for consecutive observations.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class TriggerConfig:
    threshold_db: float = -40.0
    hysteresis_count: int = 3
    detect_duration_sec: float = 0.5
    pre_trigger_sec: float = 1.0


class PowerTrigger:
    """Monitors mean power and fires after consecutive threshold crossings."""

    def __init__(self, config: TriggerConfig) -> None:
        self.config = config
        self._consecutive_count = 0
        self._triggered = False

    @property
    def triggered(self) -> bool:
        return self._triggered

    def reset(self) -> None:
        self._consecutive_count = 0
        self._triggered = False

    def check(self, iq_block: np.ndarray) -> bool:
        """Check a block of complex IQ samples against the trigger threshold.

        Args:
            iq_block: Complex numpy array of IQ samples (normalized to [-1, 1]).

        Returns:
            True if the trigger has fired (consecutive count >= hysteresis).
        """
        if self._triggered:
            return True

        mean_power_db = compute_mean_power_db(iq_block)

        if mean_power_db > self.config.threshold_db:
            self._consecutive_count += 1
            logger.debug(
                "Trigger: %.1f dB [%d/%d] ABOVE",
                mean_power_db,
                self._consecutive_count,
                self.config.hysteresis_count,
            )
            if self._consecutive_count >= self.config.hysteresis_count:
                self._triggered = True
                logger.info("Trigger FIRED at %.1f dB", mean_power_db)
                return True
        else:
            if self._consecutive_count > 0:
                self._consecutive_count -= 1
            logger.debug(
                "Trigger: %.1f dB [%d/%d] below",
                mean_power_db,
                self._consecutive_count,
                self.config.hysteresis_count,
            )

        return False


def compute_mean_power_db(iq_data: np.ndarray) -> float:
    """Compute mean power in dB assuming 50-ohm impedance.

    Matches the C++ compute_mean_power_db from iq2ram.cpp.
    Input should be complex samples normalized to [-1, 1].
    """
    mag_sq = np.abs(iq_data) ** 2
    mean_power = np.mean(mag_sq / 50.0)
    return float(10.0 * np.log10(mean_power + 1e-20))
