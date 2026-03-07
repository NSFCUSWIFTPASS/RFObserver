"""PSD power violation detection with consecutive hysteresis.

Ported from zms-monitor/psd_processing.py.
"""

from __future__ import annotations

from collections import deque

import numpy as np


class PSDDetector:
    """Detect power violations in PSD bins using rolling baseline + hysteresis."""

    def __init__(
        self,
        consecutive_threshold: int = 2,
        avg_points: int = 50,
        sigma_multiplier: float = 3.0,
    ) -> None:
        self.consecutive_threshold = consecutive_threshold
        self.avg_points = avg_points
        self.sigma_multiplier = sigma_multiplier

        self._power_histories: dict[int, deque[float]] = {}
        self._consecutive_counts: dict[int, int] = {}

        self._average_history: deque[float] = deque(maxlen=avg_points)
        self._average_consecutive: int = 0

    def check_power_violation(self, power: float, bin_number: int) -> tuple[bool, float]:
        """Check if a PSD bin power exceeds the rolling baseline.

        Returns (violation, kurtosis) where violation is True if the
        consecutive count has reached the threshold.
        """
        if bin_number not in self._power_histories:
            synthetic = np.random.normal(power, 5.0, self.avg_points)
            self._power_histories[bin_number] = deque(synthetic, maxlen=self.avg_points)
            self._consecutive_counts[bin_number] = 0
            return False, 0.0

        history = self._power_histories[bin_number]
        pwr_mean = float(np.mean(history))
        pwr_sigma = float(np.std(history))
        pwr_change = power - pwr_mean

        from scipy.stats import kurtosis as _kurtosis

        history_kurtosis = float(_kurtosis(list(history), fisher=True, nan_policy="propagate"))
        if np.isnan(history_kurtosis):
            history_kurtosis = 0.0

        if pwr_change > self.sigma_multiplier * pwr_sigma:
            if self._consecutive_counts[bin_number] < self.consecutive_threshold + 2:
                self._consecutive_counts[bin_number] += 1
        else:
            if self._consecutive_counts[bin_number] > 0:
                self._consecutive_counts[bin_number] -= 1

        violation = self._consecutive_counts[bin_number] >= self.consecutive_threshold
        return violation, history_kurtosis

    def check_average_violation(self, average_power: float) -> tuple[bool, bool]:
        """Check if the average power exceeds the rolling baseline.

        Returns (violation, exceeds_threshold).
        """
        if len(self._average_history) == 0:
            synthetic = np.random.normal(average_power, 5.0, self.avg_points)
            for v in synthetic:
                self._average_history.append(float(v))
            return False, False

        avg_mean = float(np.mean(self._average_history))
        avg_sigma = 5.0  # fixed sigma matching reference
        avg_change = average_power - avg_mean

        exceeds = avg_change > self.sigma_multiplier * avg_sigma

        if exceeds:
            if self._average_consecutive < self.consecutive_threshold + 2:
                self._average_consecutive += 1
        else:
            if self._average_consecutive > 0:
                self._average_consecutive -= 1

        violation = self._average_consecutive >= self.consecutive_threshold
        return violation, exceeds

    def add_power_to_history(self, power: float, bin_number: int) -> None:
        """Add a clean (non-violation) observation to the bin baseline."""
        if bin_number in self._power_histories:
            self._power_histories[bin_number].append(power)

    def add_average_to_history(self, average_power: float) -> None:
        """Add a clean average power observation to the baseline."""
        if len(self._average_history) > 0:
            self._average_history.append(average_power)

    def reset(self) -> None:
        """Clear all state."""
        self._power_histories.clear()
        self._consecutive_counts.clear()
        self._average_history.clear()
        self._average_consecutive = 0
