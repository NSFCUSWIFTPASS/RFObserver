"""Mapping receive gaps into recording file positions (issue 5)."""

from __future__ import annotations

import json
import queue
import threading
import time
from datetime import datetime
from typing import TYPE_CHECKING

import numpy as np
import pytest

from rfobserver.capture.buffer import CircularBuffer
from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import _MAX_RECORDED_GAPS, StreamingProcessor, _preroll_gaps
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.local import LocalStorage

if TYPE_CHECKING:
    from collections.abc import Callable
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


def test_receive_loss_reads_cumulative_counters_from_the_receiver(tmp_path: Path) -> None:
    proc = _proc(tmp_path)
    proc._receiver.overflow_events = 3
    proc._receiver.overflow_lost_samples = 900
    assert proc.receive_loss() == {"overflow_events": 3, "overflow_lost_samples": 900}


# -- Stream-position continuity on recording writes (manual-start race) --


def _ram_rec(tmp_path: Path, **overrides: object) -> StreamingProcessor:
    """RAM-buffered processor whose ring holds 1000 samples (1 ms at 1 MS/s)."""
    kw: dict[str, object] = dict(
        RECORDING_RAM_BUFFER=True, RECORDING_MAX_SEC=1.0, TRIGGER_PRE_SEC=0.001
    )
    kw.update(overrides)
    return _proc(tmp_path, **kw)


def _feed(
    proc: StreamingProcessor, start: int, stop: int, gaps: list[tuple[int, int]] | None = None
) -> tuple[np.ndarray, list[tuple[int, int]], int]:
    """Receiver side for stream [start, stop): log gaps, then write the ring.

    Samples carry their own stream position, so file contents are checkable.
    Returns what the receiver passes on: the chunk, its gaps and chunk_start.
    """
    ring = proc._pre_trigger_buf
    assert ring.total_written == start
    chunk = np.arange(start, stop, dtype=np.int32)
    gaps = gaps or []
    with proc._stream_gaps_lock:
        for off, lost in gaps:
            proc._stream_gaps.append((start + off, lost))
    ring.write(chunk)
    return chunk, gaps, start


def _file(proc: StreamingProcessor) -> list[int]:
    assert proc._recording_buf is not None
    return proc._recording_buf[: proc._recording_buf_pos].tolist()


def _meta(proc: StreamingProcessor) -> dict[str, object]:
    [js] = list(proc._storage.manual_dir.glob("*.json"))
    meta: dict[str, object] = json.loads(js.read_text())
    return meta


@pytest.mark.parametrize("ram", [True, False], ids=["ram", "disk"])
def test_skipped_chunk_becomes_one_gap_at_the_file_end(tmp_path: Path, ram: bool) -> None:
    # Mirrors race_probe.py: the pre-roll is stream [0, 1000), stream
    # [1000, 1500) never reaches the file, and the next write is [1500, 2000).
    proc = _ram_rec(tmp_path, RECORDING_RAM_BUFFER=ram)
    _feed(proc, 0, 1000)
    proc.start_recording()
    assert proc._recording_state == "recording"
    # Two overflows inside the skipped stretch, one right at its start.
    _feed(proc, 1000, 1500, [(0, 11), (200, 77)])
    chunk, gaps, start = _feed(proc, 1500, 2000, [(10, 5)])
    proc._write_recording_chunk(chunk, gaps, start)
    proc.stop_recording()

    meta = _meta(proc)
    assert meta["gaps"] == [[1000, 500 + 11 + 77], [1010, 5]]
    assert meta["lost_samples"] == 500 + 11 + 77 + 5
    assert meta["overflow_events"] == 3
    assert meta["dropped_chunks"] == 0
    [sc16] = list(proc._storage.manual_dir.glob("*.sc16"))
    data = np.fromfile(sc16, dtype=np.int32).tolist()
    assert data == list(range(1000)) + list(range(1500, 2000))


def test_skipped_chunk_does_not_double_count_the_next_chunks_first_gap(tmp_path: Path) -> None:
    proc = _ram_rec(tmp_path)
    _feed(proc, 0, 1000)
    proc.start_recording()
    _feed(proc, 1000, 1500, [(200, 77)])
    # A gap at the next chunk's offset 0 (stream 1500) is that chunk's own.
    chunk, gaps, start = _feed(proc, 1500, 2000, [(0, 5)])
    proc._write_recording_chunk(chunk, gaps, start)
    try:
        # One file position: the missing stretch and the chunk's own gap merge.
        assert proc._recording_gaps == [[1000, 500 + 77 + 5]]
        assert proc._recording_lost == 582
        assert proc._recording_overflows == 2
    finally:
        proc.stop_recording()


def test_duplicate_chunk_already_in_the_preroll_writes_nothing(tmp_path: Path) -> None:
    proc = _ram_rec(tmp_path)
    _feed(proc, 0, 500)
    chunk, gaps, start = _feed(proc, 500, 1000, [(10, 3)])
    proc.start_recording()  # pre-roll [0, 1000) already includes the chunk
    try:
        assert proc._recording_gaps == [[510, 3]]
        proc._write_recording_chunk(chunk, gaps, start)
        assert _file(proc) == list(range(1000))
        assert proc._recording_gaps == [[510, 3]]
        assert proc._recording_lost == 3
        assert proc._recording_overflows == 1
        # The next chunk continues right after the pre-roll.
        chunk, gaps, start = _feed(proc, 1000, 1500)
        proc._write_recording_chunk(chunk, gaps, start)
        assert _file(proc) == list(range(1500))
        assert proc._recording_gaps == [[510, 3]]
    finally:
        proc.stop_recording()


def test_partially_overlapping_chunk_is_trimmed_and_the_seam_gap_counted_once(
    tmp_path: Path,
) -> None:
    proc = _ram_rec(tmp_path)
    _feed(proc, 0, 800)
    # The chunk [800, 1300) is written to the ring in two steps so that the
    # pre-roll read sees only its first 200 samples ([800, 1000)).
    ring = proc._pre_trigger_buf
    with proc._stream_gaps_lock:
        proc._stream_gaps.append((900, 4))
    ring.write(np.arange(800, 1000, dtype=np.int32))
    proc.start_recording()  # pre-roll [0, 1000)
    try:
        assert proc._recording_gaps == [[900, 4]]
        with proc._stream_gaps_lock:
            proc._stream_gaps.extend([(1000, 6), (1100, 2)])
        ring.write(np.arange(1000, 1300, dtype=np.int32))
        chunk = np.arange(800, 1300, dtype=np.int32)
        proc._write_recording_chunk(chunk, [(100, 4), (200, 6), (300, 2)], 800)
        assert _file(proc) == list(range(1300))
        # 900 was mapped by the pre-roll; 1000 (the seam) and 1100 once each.
        assert proc._recording_gaps == [[900, 4], [1000, 6], [1100, 2]]
        assert proc._recording_lost == 12
        assert proc._recording_overflows == 3
    finally:
        proc.stop_recording()


def test_reconfigured_ring_rebaselines_instead_of_reporting_a_gap(tmp_path: Path) -> None:
    proc = _ram_rec(tmp_path)
    _feed(proc, 0, 1000)
    proc.start_recording()
    try:
        # A reconfigure swaps the ring: stream positions restart at 0.
        proc._recompute_chunk_params()
        chunk, gaps, start = _feed(proc, 0, 500)
        proc._write_recording_chunk(chunk, gaps, start)
        assert proc._recording_buf_pos == 1500
        assert proc._recording_gaps == []
        chunk, gaps, start = _feed(proc, 500, 1000)
        proc._write_recording_chunk(chunk, gaps, start)
        assert proc._recording_buf_pos == 2000
        assert proc._recording_gaps == []
        assert proc._recording_lost == 0
    finally:
        proc.stop_recording()


class _StartWhileWaiting:
    """Stands in for ``_rec_lock``: a manual start wins the lock while the
    receiver waits for it, so the receiver re-reads "recording" under it.

    ``after_read`` runs once the manual start has read the ring (the
    receiver's ring write for its chunk can land after that read).
    """

    def __init__(
        self, proc: StreamingProcessor, after_read: Callable[[], object] | None = None
    ) -> None:
        self._proc = proc
        self._real = proc._rec_lock
        self._after_read = after_read

    def __enter__(self) -> _StartWhileWaiting:
        self._proc._rec_lock = self._real
        self._proc.start_recording()
        if self._after_read is not None:
            self._after_read()
        self._real.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        self._real.release()


def test_chunk_written_to_the_ring_after_a_manual_starts_read_is_recorded(
    tmp_path: Path,
) -> None:
    proc = _ram_rec(tmp_path)
    _feed(proc, 0, 1000)
    fed: list[tuple[np.ndarray, list[tuple[int, int]], int]] = []
    proc._rec_lock = _StartWhileWaiting(  # type: ignore[assignment]
        proc, after_read=lambda: fed.append(_feed(proc, 1000, 1500, [(10, 77)]))
    )
    # The receiver saw "idle" before the start; its chunk is [1000, 1500).
    proc._check_trigger_and_record(np.arange(1000, 1500, dtype=np.int32), [(10, 77)], 1000)
    try:
        assert proc._recording_state == "recording"
        assert _file(proc) == list(range(1500))
        assert proc._recording_gaps == [[1010, 77]]
        assert proc._recording_lost == 77
        assert proc._recording_overflows == 1
    finally:
        proc.stop_recording()


def test_chunk_in_the_manual_starts_preroll_is_not_written_twice(tmp_path: Path) -> None:
    proc = _ram_rec(tmp_path)
    _feed(proc, 0, 500)
    chunk, gaps, start = _feed(proc, 500, 1000, [(10, 3)])
    proc._rec_lock = _StartWhileWaiting(proc)  # type: ignore[assignment]
    proc._check_trigger_and_record(chunk, gaps, start)
    try:
        assert proc._recording_state == "recording"
        assert _file(proc) == list(range(1000))
        assert proc._recording_gaps == [[510, 3]]
        assert proc._recording_lost == 3
    finally:
        proc.stop_recording()


def test_race_probe_manual_start_on_another_thread_keeps_the_chunk(tmp_path: Path) -> None:
    # The reviewer's race_probe.py: the ring read happens, the receiver writes
    # its next chunk, then hits the recording check while the start still runs.
    proc = _ram_rec(tmp_path)
    _feed(proc, 0, 1000)
    read_done = threading.Event()
    orig_drain = proc._grid_prebuf.drain

    def slow_drain(from_sample=None):  # type: ignore[no-untyped-def]
        read_done.set()  # the ring read happened just before drain()
        time.sleep(0.05)  # the web thread is still inside _begin_recording
        return orig_drain(from_sample)

    proc._grid_prebuf.drain = slow_drain  # type: ignore[method-assign]
    t = threading.Thread(target=proc.start_recording)
    t.start()
    assert read_done.wait(5.0)
    chunk, gaps, start = _feed(proc, 1000, 1500, [(10, 77)])
    proc._check_trigger_and_record(chunk, gaps, start)
    t.join()
    chunk, gaps, start = _feed(proc, 1500, 2000)
    proc._check_trigger_and_record(chunk, gaps, start)
    try:
        assert _file(proc) == list(range(2000))
        assert proc._recording_gaps == [[1010, 77]]
        assert proc._recording_lost == 77
        assert proc._recording_overflows == 1
    finally:
        proc.stop_recording()


def test_consecutive_drops_at_one_file_position_merge_into_one_gap(tmp_path: Path) -> None:
    proc = _proc(tmp_path)
    proc._recording_buf = np.zeros(1200, dtype=np.int32)  # no room for 500 more
    proc._recording_buf_pos = 1000
    proc._write_recording_chunk(np.ones(500, dtype=np.int32), [(100, 4)])
    proc._write_recording_chunk(np.ones(500, dtype=np.int32), [])
    assert proc._recording_gaps == [[1000, 1004]]
    assert proc._recording_lost == 1004
    assert proc._recording_overflows == 1
    assert proc._recording_dropped == 2


def test_a_merge_never_marks_the_gap_list_truncated(tmp_path: Path) -> None:
    proc = _proc(tmp_path)
    for i in range(1, _MAX_RECORDED_GAPS + 1):
        proc._add_recording_gap(i, 2, overflow=True)
    proc._add_recording_gap(_MAX_RECORDED_GAPS, 3, overflow=True)
    assert len(proc._recording_gaps) == _MAX_RECORDED_GAPS
    assert proc._recording_gaps[-1] == [_MAX_RECORDED_GAPS, 5]
    assert proc._recording_gaps_truncated is False
    assert proc._recording_lost == 2 * _MAX_RECORDED_GAPS + 3
    assert proc._recording_overflows == _MAX_RECORDED_GAPS + 1


def test_start_time_is_anchored_at_the_preroll_read(tmp_path: Path) -> None:
    proc = _ram_rec(tmp_path)
    _feed(proc, 0, 1000)
    t0 = time.time()
    proc.start_recording()
    t1 = time.time()
    time.sleep(0.3)  # finalize latency must not move start_time
    proc.stop_recording()
    start = datetime.fromisoformat(str(_meta(proc)["start_time"])).timestamp()
    pre = 1000 / 1_000_000
    assert t0 - pre - 0.01 <= start <= t1 - pre + 0.01
