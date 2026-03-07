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
