"""Tests for rfobserver.capture.buffer.GridPreBuffer (pre-trigger PSD grids)."""

import numpy as np

from rfobserver.capture.buffer import GridPreBuffer


def _grid(rows: int, bins: int, fill: float) -> np.ndarray:
    return np.full((rows, bins), fill, dtype=np.float32)


def test_prebuffer_empty_drain_returns_none():
    buf = GridPreBuffer(1.0)
    assert buf.drain() is None
    assert buf.rows == 0


def test_prebuffer_single_write_drain():
    buf = GridPreBuffer(1.0)
    freq = np.arange(8, dtype=np.float64)
    buf.write(_grid(5, 8, -90.0), freq, 0.1)
    roll = buf.drain()
    assert roll is not None
    assert roll.rows == 5
    assert len(roll.grids) == 1
    assert roll.grids[0].shape == (5, 8)
    assert roll.time_res == 0.1
    np.testing.assert_array_equal(roll.freq_axis, freq)
    assert roll.grid_min == -90.0 and roll.grid_max == -90.0
    # drain consumes the buffer
    assert buf.drain() is None
    assert buf.rows == 0


def test_prebuffer_trims_to_max_seconds():
    # max 1.0 s, each grid = 5 rows * 0.1 s = 0.5 s span.
    buf = GridPreBuffer(1.0)
    freq = np.arange(4, dtype=np.float64)
    for i in range(4):
        buf.write(_grid(5, 4, float(i)), freq, 0.1)
    roll = buf.drain()
    assert roll is not None
    # Only the last two 0.5 s grids fit in the 1.0 s window.
    assert len(roll.grids) == 2
    assert roll.rows == 10
    assert roll.grids[0][0, 0] == 2.0  # oldest kept is grid #2
    assert roll.grids[1][0, 0] == 3.0


def test_prebuffer_keeps_at_least_last_grid_when_bigger_than_window():
    # A single grid longer than the window is still retained (can't drop below 1).
    buf = GridPreBuffer(0.2)
    freq = np.arange(4, dtype=np.float64)
    buf.write(_grid(100, 4, 1.0), freq, 0.1)  # 10 s span > 0.2 s window
    roll = buf.drain()
    assert roll is not None
    assert len(roll.grids) == 1
    assert roll.rows == 100


def test_prebuffer_min_max_across_grids():
    buf = GridPreBuffer(10.0)
    freq = np.arange(4, dtype=np.float64)
    buf.write(_grid(2, 4, -80.0), freq, 0.1)
    g = _grid(2, 4, -50.0)
    g[0, 0] = -120.0
    buf.write(g, freq, 0.1)
    roll = buf.drain()
    assert roll is not None
    assert roll.grid_min == -120.0
    assert roll.grid_max == -50.0


def test_prebuffer_ignores_empty_or_bad_writes():
    buf = GridPreBuffer(1.0)
    freq = np.arange(4, dtype=np.float64)
    buf.write(np.zeros((0, 4), dtype=np.float32), freq, 0.1)  # no rows
    buf.write(_grid(3, 4, 1.0), freq, 0.0)  # non-positive time_res
    assert buf.drain() is None
    assert buf.rows == 0


def test_prebuffer_clear():
    buf = GridPreBuffer(1.0)
    freq = np.arange(4, dtype=np.float64)
    buf.write(_grid(5, 4, 1.0), freq, 0.1)
    buf.clear()
    assert buf.rows == 0
    assert buf.drain() is None
