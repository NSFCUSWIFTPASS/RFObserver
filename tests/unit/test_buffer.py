"""Tests for rfobserver.capture.buffer."""

import numpy as np

from rfobserver.capture.buffer import CircularBuffer


def test_buffer_basic_write_read():
    buf = CircularBuffer(100)
    data = np.arange(50, dtype=np.complex64)
    buf.write(data)
    result = buf.read()
    assert len(result) == 50
    np.testing.assert_array_equal(result, data)


def test_buffer_wrap_around():
    buf = CircularBuffer(10)
    # Write 7 samples
    buf.write(np.arange(7, dtype=np.complex64))
    # Write 7 more -> wraps around
    buf.write(np.arange(7, 14, dtype=np.complex64))

    result = buf.read()
    assert len(result) == 10
    # Should contain the last 10 samples in order: 4,5,6,7,8,9,10,11,12,13
    np.testing.assert_array_equal(result, np.arange(4, 14, dtype=np.complex64))


def test_buffer_overflow_single_write():
    buf = CircularBuffer(5)
    data = np.arange(20, dtype=np.complex64)
    buf.write(data)
    result = buf.read()
    assert len(result) == 5
    # Last 5 samples
    np.testing.assert_array_equal(result, np.arange(15, 20, dtype=np.complex64))


def test_buffer_capacity():
    buf = CircularBuffer(100)
    assert buf.capacity == 100
    assert buf.filled == 0
    buf.write(np.zeros(50, dtype=np.complex64))
    assert buf.filled == 50
    buf.write(np.zeros(60, dtype=np.complex64))
    assert buf.filled == 100  # capped at capacity


def test_buffer_clear():
    buf = CircularBuffer(10)
    buf.write(np.ones(10, dtype=np.complex64))
    buf.clear()
    assert buf.filled == 0
    result = buf.read()
    assert len(result) == 0


def test_buffer_empty_read():
    buf = CircularBuffer(10)
    result = buf.read()
    assert len(result) == 0


def test_read_range_returns_exact_stream_samples():
    buf = CircularBuffer(10, dtype=np.int32)
    buf.write(np.arange(7, dtype=np.int32))
    assert list(buf.read_range(2, 5)) == [2, 3, 4]
    buf.write(np.arange(7, 15, dtype=np.int32))  # holds stream 5..14
    assert buf.oldest_position == 5
    assert list(buf.read_range(8, 13)) == [8, 9, 10, 11, 12]
    assert list(buf.read_range(5, 15)) == list(range(5, 15))


def test_read_range_outside_the_ring_is_none():
    buf = CircularBuffer(10, dtype=np.int32)
    buf.write(np.arange(15, dtype=np.int32))  # holds 5..14
    assert buf.read_range(4, 8) is None  # start already overwritten
    assert buf.read_range(10, 16) is None  # end not written yet
    assert buf.read_range(8, 8) is None  # empty


def test_read_range_after_an_oversized_write():
    buf = CircularBuffer(10, dtype=np.int32)
    buf.write(np.arange(3, dtype=np.int32))
    buf.write(np.arange(3, 26, dtype=np.int32))  # larger than capacity: holds 16..25
    assert buf.oldest_position == 16
    assert list(buf.read_range(16, 26)) == list(range(16, 26))
    buf.write(np.arange(26, 30, dtype=np.int32))  # holds 20..29
    assert list(buf.read_range(24, 30)) == list(range(24, 30))


def test_read_tail_with_position():
    buf = CircularBuffer(10, dtype=np.int32)
    buf.write(np.arange(14, dtype=np.int32))
    data, end = buf.read_tail_with_position(3)
    assert end == 14 and list(data) == [11, 12, 13]
    data, end = buf.read_tail_with_position(100)  # clamps to what is held
    assert list(data) == list(range(4, 14))


def test_bounds_is_oldest_and_total_written():
    buf = CircularBuffer(10, dtype=np.int32)
    assert buf.bounds() == (0, 0)
    buf.write(np.arange(4, dtype=np.int32))
    assert buf.bounds() == (0, 4)
    buf.write(np.arange(22, dtype=np.int32))
    assert buf.bounds() == (16, 26)
    assert buf.bounds()[0] == buf.oldest_position
