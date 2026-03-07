"""Tests for rfobserver.processing.detection."""

from rfobserver.processing.detection import PSDDetector


def test_detector_first_observation_no_violation():
    detector = PSDDetector(consecutive_threshold=2, avg_points=50)
    violation, kurtosis = detector.check_power_violation(-60.0, bin_number=0)
    assert violation is False
    assert kurtosis == 0.0


def test_detector_consecutive_threshold():
    detector = PSDDetector(consecutive_threshold=2, avg_points=50, sigma_multiplier=3.0)

    # First call initializes
    detector.check_power_violation(-60.0, bin_number=0)

    # Subsequent calls with normal power should not trigger
    for _ in range(10):
        violation, _ = detector.check_power_violation(-60.0, bin_number=0)
        detector.add_power_to_history(-60.0, bin_number=0)
    assert violation is False


def test_detector_spike_triggers_violation():
    detector = PSDDetector(consecutive_threshold=2, avg_points=50, sigma_multiplier=3.0)

    # Initialize with baseline
    detector.check_power_violation(-60.0, bin_number=0)
    for _ in range(50):
        detector.check_power_violation(-60.0, bin_number=0)
        detector.add_power_to_history(-60.0, bin_number=0)

    # Inject repeated large spikes (well above 3-sigma)
    violation = False
    for _ in range(10):
        violation, _ = detector.check_power_violation(0.0, bin_number=0)
        if violation:
            break
    assert violation is True


def test_detector_average_first_observation():
    detector = PSDDetector()
    violation, exceeds = detector.check_average_violation(-50.0)
    assert violation is False
    assert exceeds is False


def test_detector_reset():
    detector = PSDDetector()
    detector.check_power_violation(-60.0, bin_number=0)
    detector.check_average_violation(-50.0)
    detector.reset()
    assert len(detector._power_histories) == 0
    assert len(detector._average_history) == 0


def test_detector_multiple_bins():
    detector = PSDDetector()
    detector.check_power_violation(-60.0, bin_number=0)
    detector.check_power_violation(-55.0, bin_number=1)
    detector.check_power_violation(-70.0, bin_number=2)
    assert len(detector._power_histories) == 3
