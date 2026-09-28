"""Tests for rfobserver.capture.buffer.GridPreBuffer (pre-trigger PSD grids)."""

import numpy as np

from rfobserver.capture.buffer import GridPreBuffer, trim_grid_rows

SLICE = 2048


def _grid(rows: int, bins: int, fill: float) -> np.ndarray:
    return np.full((rows, bins), fill, dtype=np.float32)


def test_prebuffer_empty_drain_returns_none():
    buf = GridPreBuffer(1.0)
    assert buf.drain() is None
    assert buf.rows == 0


def test_prebuffer_single_write_drain():
    buf = GridPreBuffer(1.0)
    freq = np.arange(8, dtype=np.float64)
    buf.write(_grid(5, 8, -90.0), freq, 0.1, 0, SLICE)
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
        buf.write(_grid(5, 4, float(i)), freq, 0.1, i * 5 * SLICE, SLICE)
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
    buf.write(_grid(100, 4, 1.0), freq, 0.1, 0, SLICE)  # 10 s span > 0.2 s window
    roll = buf.drain()
    assert roll is not None
    assert len(roll.grids) == 1
    assert roll.rows == 100


def test_prebuffer_min_max_across_grids():
    buf = GridPreBuffer(10.0)
    freq = np.arange(4, dtype=np.float64)
    buf.write(_grid(2, 4, -80.0), freq, 0.1, 0, SLICE)
    g = _grid(2, 4, -50.0)
    g[0, 0] = -120.0
    buf.write(g, freq, 0.1, 2 * SLICE, SLICE)
    roll = buf.drain()
    assert roll is not None
    assert roll.grid_min == -120.0
    assert roll.grid_max == -50.0


def test_prebuffer_ignores_empty_or_bad_writes():
    buf = GridPreBuffer(1.0)
    freq = np.arange(4, dtype=np.float64)
    buf.write(np.zeros((0, 4), dtype=np.float32), freq, 0.1, 0, SLICE)  # no rows
    buf.write(_grid(3, 4, 1.0), freq, 0.0, 0, SLICE)  # non-positive time_res
    buf.write(_grid(3, 4, 1.0), freq, 0.1, 0, 0)  # non-positive slice_samples
    assert buf.drain() is None
    assert buf.rows == 0


def test_prebuffer_clear():
    buf = GridPreBuffer(1.0)
    freq = np.arange(4, dtype=np.float64)
    buf.write(_grid(5, 4, 1.0), freq, 0.1, 0, SLICE)
    buf.clear()
    assert buf.rows == 0
    assert buf.drain() is None


# --- Position-aware drain and row trimming ---------------------------------
#
# Grids reach the pre-buffer only after the chunk queue and the worker pool, so
# they lag the IQ by the pipeline latency. Selecting them by arrival order put
# the .psd ~820 ms out of step with its .sc16; selecting by stream position is
# the fix. See docs/debugging/2026-09-22_trigger-psd-iq-misalignment.md.

ROWS = 4


def test_drain_from_sample_keeps_only_rows_at_or_after_position():
    buf = GridPreBuffer(10.0)
    freq = np.arange(8, dtype=np.float64)
    for i in range(3):
        buf.write(_grid(ROWS, 8, float(i)), freq, 0.001024, i * ROWS * SLICE, SLICE)

    roll = buf.drain(from_sample=ROWS * SLICE)
    assert roll is not None
    assert roll.rows == 2 * ROWS
    assert roll.start_sample == ROWS * SLICE
    assert [float(g[0, 0]) for g in roll.grids] == [1.0, 2.0]


def test_drain_from_sample_trims_within_a_chunk():
    buf = GridPreBuffer(10.0)
    freq = np.arange(8, dtype=np.float64)
    buf.write(_grid(ROWS, 8, 7.0), freq, 0.001024, 0, SLICE)
    # Start one sample into row 1: the straddling row is dropped, so the first
    # kept row is row 2 and the reported position is a true row boundary.
    roll = buf.drain(from_sample=SLICE + 1)
    assert roll is not None
    assert roll.rows == ROWS - 2
    assert roll.start_sample == 2 * SLICE


def test_drain_from_sample_past_everything_returns_none():
    # The production case whenever pipeline latency exceeds TRIGGER_PRE_SEC:
    # every buffered grid predates the recording, so the pre-roll contributes
    # nothing and the covering rows arrive later as live grids.
    buf = GridPreBuffer(10.0)
    freq = np.arange(8, dtype=np.float64)
    buf.write(_grid(ROWS, 8, 1.0), freq, 0.001024, 0, SLICE)
    assert buf.drain(from_sample=10 * ROWS * SLICE) is None


def test_drain_without_position_returns_everything():
    buf = GridPreBuffer(10.0)
    freq = np.arange(8, dtype=np.float64)
    buf.write(_grid(ROWS, 8, 1.0), freq, 0.001024, 3 * SLICE, SLICE)
    roll = buf.drain()
    assert roll is not None
    assert roll.rows == ROWS
    assert roll.start_sample == 3 * SLICE


def test_trim_keeps_rows_inside_the_range():
    grid = _grid(ROWS, 8, 1.0)
    kept, start = trim_grid_rows(grid, 0, SLICE, 0, ROWS * SLICE)
    assert kept.shape[0] == ROWS
    assert start == 0


def test_trim_drops_rows_before_the_start():
    grid = np.arange(ROWS * 8, dtype=np.float32).reshape(ROWS, 8)
    kept, start = trim_grid_rows(grid, 0, SLICE, 2 * SLICE, ROWS * SLICE)
    assert kept.shape[0] == 2
    assert start == 2 * SLICE
    np.testing.assert_array_equal(kept[0], grid[2])


def test_trim_drops_a_row_straddling_the_start():
    kept, start = trim_grid_rows(_grid(ROWS, 8, 0.0), 0, SLICE, SLICE + 1, ROWS * SLICE)
    assert kept.shape[0] == ROWS - 2
    assert start == 2 * SLICE


def test_trim_drops_rows_past_the_end():
    # End halfway through row 2: rows 0 and 1 are wholly inside, row 2 is not.
    kept, start = trim_grid_rows(_grid(ROWS, 8, 0.0), 0, SLICE, 0, 2 * SLICE + 5)
    assert kept.shape[0] == 2
    assert start == 0


def test_trim_returns_empty_when_the_chunk_is_wholly_outside():
    kept, start = trim_grid_rows(_grid(ROWS, 8, 0.0), 0, SLICE, 100 * SLICE, 200 * SLICE)
    assert kept.shape[0] == 0
    assert start == -1


def test_trim_handles_a_chunk_starting_mid_recording():
    kept, start = trim_grid_rows(_grid(ROWS, 8, 0.0), 10 * SLICE, SLICE, 0, 1000 * SLICE)
    assert kept.shape[0] == ROWS
    assert start == 10 * SLICE


def test_trim_rejects_degenerate_arguments():
    grid = _grid(ROWS, 8, 0.0)
    assert trim_grid_rows(grid, 0, 0, 0, ROWS * SLICE)[1] == -1
    assert trim_grid_rows(grid, 0, SLICE, 5, 5)[1] == -1
    assert trim_grid_rows(np.zeros((0, 8), dtype=np.float32), 0, SLICE, 0, 99)[1] == -1
