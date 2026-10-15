"""CaptureClock: chunk capture times on the host clock from device time."""

from __future__ import annotations

import pytest

from rfobserver.capture.clock import JUMP_HOLD_SEC, WINDOW_SEC, CaptureClock

RATE = 1000.0  # samples per second
N = 100  # samples per chunk: 0.1 s


def test_without_device_time_uses_arrival_minus_duration() -> None:
    assert CaptureClock().chunk_start(None, N, RATE, 50.0) == pytest.approx(49.9)


def test_offset_is_the_least_delayed_chunk() -> None:
    """Device time runs 1000 s behind the host; chunks arrive 5-40 ms late.
    The estimate is the least late one, and every start uses it."""
    clock = CaptureClock()
    delays = [0.040, 0.012, 0.030, 0.005, 0.025]
    starts = []
    for i, d in enumerate(delays):
        dev = 100.0 + i * 0.1
        host_end = dev + 1000.0 + 0.1 + d
        starts.append(clock.chunk_start(dev, N, RATE, host_end))
    assert clock.offset == pytest.approx(1000.005)
    # The last start is the true start (1100.4) plus the best delay seen.
    assert starts[-1] == pytest.approx(1100.4 + 0.005)
    # Spacing between chunks is the device's, not the arrivals'.
    assert starts[-1] - starts[-2] == pytest.approx(0.1, abs=0.02)


def test_offset_follows_drift_as_old_minima_expire() -> None:
    """A crystal running fast moves the offset; a minimum older than the
    window is forgotten, so the estimate follows."""
    clock = CaptureClock()
    for i in range(int(3 * WINDOW_SEC)):  # one chunk a second, 3 windows
        dev = float(i)
        offset = 1000.0 - i * 1e-4  # 100 ppm, exaggerated
        clock.chunk_start(dev, N, RATE, dev + offset + 0.1 + 0.002)
    assert clock.offset == pytest.approx(1000.0 - (3 * WINDOW_SEC - 1) * 1e-4 + 0.002, abs=0.01)


def test_backward_jump_restarts_only_after_it_lasts() -> None:
    clock = CaptureClock()
    host = 0.0
    for i in range(20):
        host = 10.0 + i * 0.1
        clock.chunk_start(host - 0.1 - 0.001, N, RATE, host)  # offset ~0
    assert clock.offset == pytest.approx(0.001)
    # A 2 s stall: one chunk arrives late. Not a jump.
    host += 2.0
    clock.chunk_start(host - 2.0 - 0.1, N, RATE, host)
    assert clock.offset == pytest.approx(0.001)
    # The device time is reset 3 s back: every chunk now looks 3 s late.
    for _ in range(int(JUMP_HOLD_SEC / 0.1) + 2):
        host += 0.1
        start = clock.chunk_start(host - 3.0 - 0.1 - 0.001, N, RATE, host)
    assert clock.offset == pytest.approx(3.001)
    assert start == pytest.approx(host - 0.1 - 0.001 + 0.001, abs=1e-6)
