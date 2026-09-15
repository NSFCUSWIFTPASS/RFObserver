"""Mapping receive gaps into recording file positions (issue 5)."""

from __future__ import annotations

import queue
from typing import TYPE_CHECKING

import numpy as np

from rfobserver.capture.buffer import CircularBuffer
from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import _MAX_RECORDED_GAPS, StreamingProcessor, _preroll_gaps
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.local import LocalStorage

if TYPE_CHECKING:
    from pathlib import Path


def test_read_with_position_pairs_data_with_total_written() -> None:
    buf = CircularBuffer(10, dtype=np.int32)
    buf.write(np.arange(4, dtype=np.int32))
    data, end = buf.read_with_position()
    assert end == 4 and list(data) == [0, 1, 2, 3]
    buf.write(np.arange(4, 12, dtype=np.int32))  # wraps: holds stream 2..11
    data, end = buf.read_with_position()
    assert end == 12 and list(data) == list(range(2, 12))
    assert buf.total_written == 12


def test_read_at_exactly_one_capacity_returns_the_whole_buffer() -> None:
    # _write_pos wraps to 0 when the buffer fills exactly; the data is all valid.
    buf = CircularBuffer(10, dtype=np.int32)
    buf.write(np.arange(4, dtype=np.int32))
    buf.write(np.arange(4, 10, dtype=np.int32))
    data, end = buf.read_with_position()
    assert end == 10 and list(data) == list(range(10))
    assert list(buf.read()) == list(range(10))


def test_preroll_gaps_map_stream_positions_to_file_indices() -> None:
    # Pre-roll holds stream samples [100, 200).
    stream_gaps = [(50, 7), (100, 9), (130, 5), (199, 3), (200, 4)]
    assert _preroll_gaps(stream_gaps, start=100, end=200, written=100) == [[30, 5], [99, 3]]


def test_preroll_gaps_respect_what_was_actually_written() -> None:
    # RAM mode may keep only the first `written` pre-roll samples.
    assert _preroll_gaps([(130, 5), (180, 2)], start=100, end=200, written=50) == [[30, 5]]
    assert _preroll_gaps([(130, 5)], start=100, end=200, written=0) == []


def _proc(tmp_path: Path, **overrides: object) -> StreamingProcessor:
    storage_path = tmp_path / "storage"
    storage_path.mkdir()
    base = dict(
        FREQUENCY_START=915_000_000,
        FREQUENCY_END=915_000_000,
        BANDWIDTH=1_000_000,
        DURATION_SEC=0.5,
        GAIN=35,
        NUM_FFT_BINS=64,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage_path),
        DB_PATH=str(tmp_path / "test.db"),
        ARCHIVE_MAX_GB=1.0,
        _env_file=None,
    )
    base.update(overrides)
    settings = AppSettings(**base)
    rx = MockReceiver(
        receiver_config=ReceiverConfig(
            gain_db=settings.GAIN,
            bandwidth_hz=settings.BANDWIDTH,
            duration_sec=settings.DURATION_SEC,
        )
    )
    rx.initialize()
    return StreamingProcessor(
        receiver=rx,
        database=SensorDatabase(settings.DB_PATH),
        local_storage=LocalStorage(settings.STORAGE_PATH, max_gb=settings.ARCHIVE_MAX_GB),
        settings=settings,
    )


def test_written_chunk_gaps_land_at_their_file_position(tmp_path: Path) -> None:
    proc = _proc(tmp_path)
    proc._recording_buf = np.zeros(10_000, dtype=np.int32)  # RAM mode, room to spare
    proc._recording_buf_pos = 1000
    proc._write_recording_chunk(np.ones(500, dtype=np.int32), [(0, 7), (200, 3)])
    assert proc._recording_gaps == [[1000, 7], [1200, 3]]
    assert proc._recording_lost == 10
    assert proc._recording_overflows == 2
    assert proc._recording_dropped == 0


def test_dropped_ram_chunk_is_one_gap_without_double_counting(tmp_path: Path) -> None:
    proc = _proc(tmp_path)
    proc._recording_buf = np.zeros(1200, dtype=np.int32)  # no room for 500 more
    proc._recording_buf_pos = 1000
    proc._write_recording_chunk(np.ones(500, dtype=np.int32), [(0, 7), (200, 3)])
    # The chunk and the overflow losses inside it are one gap of 500 + 10.
    assert proc._recording_gaps == [[1000, 510]]
    assert proc._recording_lost == 510
    assert proc._recording_overflows == 2
    assert proc._recording_dropped == 1


def test_dropped_disk_chunk_is_one_gap_at_the_file_position(tmp_path: Path) -> None:
    proc = _proc(tmp_path)
    proc._recording_buf = None  # disk mode
    proc._recording_bytes = 4000  # 1000 samples already queued
    while True:
        try:
            proc._recording_queue.put_nowait(("iq", b""))
        except queue.Full:
            break
    proc._write_recording_chunk(np.ones(500, dtype=np.int32), [(100, 4)])
    assert proc._recording_gaps == [[1000, 504]]
    assert proc._recording_lost == 504
    assert proc._recording_overflows == 1
    assert proc._recording_dropped == 1
    assert proc._recording_bytes == 4000  # the .sc16 did not grow


def test_gap_list_is_capped_but_totals_keep_counting(tmp_path: Path) -> None:
    proc = _proc(tmp_path)
    extra = 5
    for i in range(1, _MAX_RECORDED_GAPS + extra + 1):
        proc._add_recording_gap(i, 2, overflow=True)
    assert len(proc._recording_gaps) == _MAX_RECORDED_GAPS
    assert proc._recording_gaps_truncated is True
    assert proc._recording_lost == 2 * (_MAX_RECORDED_GAPS + extra)
    assert proc._recording_overflows == _MAX_RECORDED_GAPS + extra


def test_gap_before_the_file_starts_is_not_recorded(tmp_path: Path) -> None:
    proc = _proc(tmp_path)
    proc._add_recording_gap(0, 5, overflow=True)
    assert proc._recording_gaps == []
    assert proc._recording_lost == 0
    assert proc._recording_overflows == 0


def test_ram_recording_start_maps_logged_preroll_gaps(tmp_path: Path) -> None:
    proc = _proc(tmp_path, RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0, TRIGGER_PRE_SEC=0.001)
    ring = proc._pre_trigger_buf
    assert ring.capacity == 1000
    # Stream 1500 samples: the ring keeps stream [500, 1500).
    ring.write(np.arange(1500, dtype=np.int32))
    proc._stream_gaps.extend([(400, 9), (500, 8), (750, 6), (1500, 5)])
    proc.start_recording()
    try:
        assert proc._recording_state == "recording"
        # Only the gap strictly inside the pre-roll lands, at its file index.
        assert proc._recording_gaps == [[250, 6]]
        assert proc._recording_lost == 6
        assert proc._recording_overflows == 1
    finally:
        proc.stop_recording()


def test_reconfigure_resets_the_stream_gap_log(tmp_path: Path) -> None:
    proc = _proc(tmp_path)
    proc._stream_gaps.append((10, 1))
    proc._recompute_chunk_params()
    assert list(proc._stream_gaps) == []
    assert proc._pre_trigger_buf.total_written == 0
