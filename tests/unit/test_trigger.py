"""Tests for rfobserver.capture.trigger."""

import numpy as np

from rfobserver.capture.trigger import PowerTrigger, TriggerConfig, compute_mean_power_db


def test_compute_mean_power_db_known_value():
    # Unit amplitude -> |z|^2 = 1, P = 1/50, dB = 10*log10(0.02) ~ -16.99
    data = np.ones(100, dtype=np.complex64)
    db = compute_mean_power_db(data)
    np.testing.assert_allclose(db, 10 * np.log10(1.0 / 50.0), atol=0.01)


def test_compute_mean_power_db_noise():
    rng = np.random.default_rng(42)
    data = 0.01 * (rng.standard_normal(1000) + 1j * rng.standard_normal(1000)).astype(np.complex64)
    db = compute_mean_power_db(data)
    assert db < -30  # Very low power noise


def test_trigger_no_fire_below_threshold():
    config = TriggerConfig(threshold_db=-20.0, hysteresis_count=3)
    trigger = PowerTrigger(config)

    # Low power data
    data = 0.001 * np.ones(100, dtype=np.complex64)
    for _ in range(10):
        assert trigger.check(data) is False


def test_trigger_fires_above_threshold():
    config = TriggerConfig(threshold_db=-50.0, hysteresis_count=2)
    trigger = PowerTrigger(config)

    # High power data (well above -50 dB)
    data = np.ones(100, dtype=np.complex64)
    # First check increments count
    assert trigger.check(data) is False
    # Second check reaches hysteresis
    assert trigger.check(data) is True


def test_trigger_stays_fired():
    config = TriggerConfig(threshold_db=-50.0, hysteresis_count=1)
    trigger = PowerTrigger(config)
    data = np.ones(100, dtype=np.complex64)
    trigger.check(data)
    assert trigger.triggered is True
    # Stays triggered even with low data
    assert trigger.check(0.001 * data) is True


def test_trigger_reset():
    config = TriggerConfig(threshold_db=-50.0, hysteresis_count=1)
    trigger = PowerTrigger(config)
    data = np.ones(100, dtype=np.complex64)
    trigger.check(data)
    assert trigger.triggered is True
    trigger.reset()
    assert trigger.triggered is False


def test_trigger_hysteresis_decay():
    config = TriggerConfig(threshold_db=-50.0, hysteresis_count=3)
    trigger = PowerTrigger(config)

    high = np.ones(100, dtype=np.complex64)
    low = 0.0001 * np.ones(100, dtype=np.complex64)

    # One high, then low -> count should decay
    trigger.check(high)
    assert trigger._consecutive_count == 1
    trigger.check(low)
    assert trigger._consecutive_count == 0
