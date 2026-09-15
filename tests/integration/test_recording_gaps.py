"""Recordings describe where UHD overflows removed samples (issue 5).

The .sc16 stays contiguous; the companion .json lists each gap as
``[file_sample_index, lost_samples]`` and reports the true time span.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import StreamingProcessor
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.local import LocalStorage

if TYPE_CHECKING:
    import numpy as np

BANDWIDTH = 2_000_000


def _settings(tmp_path: Path, **kw: Any) -> AppSettings:
    storage = tmp_path / "st"
    storage.mkdir()
    base = dict(
        FREQUENCY_START=915_000_000,
        FREQUENCY_END=915_000_000,
        BANDWIDTH=BANDWIDTH,
        DURATION_SEC=0.5,
        GAIN=30,
        NUM_FFT_BINS=256,
        PSD_TIME_RESOLUTION_MS=0.5,
        STREAMING_CHUNK_SLICES=10,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage),
        DB_PATH=str(tmp_path / "t.db"),
        ARCHIVE_MAX_GB=0.01,
        RECORDING_MAX_SEC=30.0,
        _env_file=None,
    )
    base.update(kw)
    return AppSettings(**base)


class _GapReceiver(MockReceiver):
    """MockReceiver that reports one overflow gap on selected chunks."""

    def __init__(self, *a, gap_on: set[int], lost: int, offset: int, **k) -> None:
        super().__init__(*a, **k)
        self._gap_on = gap_on
        self._lost = lost
        self._offset = offset
        self._calls = 0

    def recv_chunk(self, out_buf):  # type: ignore[no-untyped-def]
        n = super().recv_chunk(out_buf)
        self.last_gaps = [(self._offset, self._lost)] if self._calls in self._gap_on else []
        if self.last_gaps:
            self.overflow_events += 1
            self.overflow_lost_samples += self._lost
        self._calls += 1
        return n


def _receiver_config(settings: AppSettings) -> ReceiverConfig:
    return ReceiverConfig(
        gain_db=settings.GAIN,
        bandwidth_hz=settings.BANDWIDTH,
        duration_sec=settings.DURATION_SEC,
    )


async def _record(
    settings: AppSettings,
    db: SensorDatabase,
    receiver: MockReceiver,
    *,
    start_when: int,
    stop_when: int,
) -> tuple[dict[str, Any], Path, StreamingProcessor, list[tuple[int, int]]]:
    """Manual recording from ``_capture_count > start_when`` to ``>= stop_when``.

    Returns the capture .json, its .sc16, the processor, and every
    ``(len(pre_data), total_written)`` the pre-roll read returned.
    """
    receiver.initialize()
    storage = LocalStorage(storage_path=settings.STORAGE_PATH, max_gb=settings.ARCHIVE_MAX_GB)
    proc = StreamingProcessor(
        receiver=receiver, database=db, local_storage=storage, settings=settings
    )
    ring = proc._pre_trigger_buf
    reads: list[tuple[int, int]] = []
    orig_read = ring.read_with_position

    def spy_read() -> tuple[np.ndarray, int]:
        data, end = orig_read()
        reads.append((len(data), end))
        return data, end

    ring.read_with_position = spy_read  # type: ignore[method-assign]

    async def driver() -> None:
        for _ in range(2000):  # wait for streaming to start
            if proc._capture_count > start_when:
                break
            await asyncio.sleep(0.005)
        proc.start_recording()
        while proc._capture_count < stop_when:
            await asyncio.sleep(0.01)
        proc.stop_recording()
        await asyncio.sleep(0.1)
        proc.stop()

    await asyncio.wait_for(asyncio.gather(proc.run(), driver()), timeout=30.0)
    # Manual start_recording() writes into the manual/ subdir.
    manual = Path(settings.STORAGE_PATH) / "manual"
    sc16s = list(manual.glob("*.sc16"))
    assert len(sc16s) == 1, [p.name for p in sc16s]
    meta = json.loads(sc16s[0].with_suffix(".json").read_text())
    return meta, sc16s[0], proc, reads


@pytest.mark.asyncio
@pytest.mark.parametrize("ram", [False, True], ids=["disk", "ram"])
async def test_gaps_inside_recording_are_in_metadata(tmp_path: Path, ram: bool) -> None:
    settings = _settings(tmp_path, RECORDING_RAM_BUFFER=ram)
    receiver = _GapReceiver(_receiver_config(settings), gap_on={6, 9}, lost=1234, offset=17)
    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        meta, sc16, _proc, _reads = await _record(
            settings, db, receiver, start_when=2, stop_when=12
        )
    finally:
        await db.close()

    total = meta["total_samples"]
    assert meta["overflow_events"] == 2
    assert meta["lost_samples"] == 2468
    gaps = meta["gaps"]
    assert len(gaps) == 2
    for idx, lost in gaps:
        assert 0 < idx < total
        assert lost == 1234
    indices = [idx for idx, _ in gaps]
    assert indices == sorted(set(indices)), "gap positions must be distinct and increasing"
    assert meta["time_span_sec"] == round((total + 2468) / BANDWIDTH, 3)
    assert meta["gaps_truncated"] is False
    assert meta["dropped_chunks"] == 0
    # Metadata only: the .sc16 is exactly the samples received, no fill.
    assert sc16.stat().st_size == total * 4


@pytest.mark.asyncio
@pytest.mark.parametrize("ram", [False, True], ids=["disk", "ram"])
async def test_gap_before_recording_starts_is_in_preroll(tmp_path: Path, ram: bool) -> None:
    # 1 s of pre-roll covers hundreds of chunks, so nothing streamed before the
    # start has left the ring and call 1 sits well inside the pre-roll.
    settings = _settings(tmp_path, RECORDING_RAM_BUFFER=ram, TRIGGER_PRE_SEC=1.0)
    offset, lost = 17, 1234
    receiver = _GapReceiver(_receiver_config(settings), gap_on={1}, lost=lost, offset=offset)
    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        meta, _sc16, proc, reads = await _record(settings, db, receiver, start_when=3, stop_when=8)
    finally:
        await db.close()

    ring = proc._pre_trigger_buf
    chunk = proc._chunk_samples
    assert ring.capacity == int(settings.TRIGGER_PRE_SEC * settings.BANDWIDTH)
    assert ring.capacity >= 3 * chunk
    # One pre-roll read, at the recording start.
    assert len(reads) == 1
    pre_roll_samples, pre_end = reads[0]
    # The pre-roll is TRIGGER_PRE_SEC * BANDWIDTH capped by what had streamed.
    assert pre_roll_samples == min(ring.capacity, pre_end)
    assert pre_end >= 4 * chunk  # _capture_count >= 4 means 4+ chunks streamed

    assert meta["overflow_events"] == 1
    assert meta["lost_samples"] == lost
    [[idx, got_lost]] = meta["gaps"]
    assert got_lost == lost
    assert 0 < idx < pre_roll_samples
    # Exact position: stream sample chunk*1 + offset, shifted by the pre-roll start.
    assert idx == 1 * chunk + offset - (pre_end - pre_roll_samples)
    assert meta["time_span_sec"] == round((meta["total_samples"] + lost) / BANDWIDTH, 3)


@pytest.mark.asyncio
async def test_no_gaps_keeps_old_metadata_and_zero_loss(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    receiver = MockReceiver(_receiver_config(settings))
    db = SensorDatabase(settings.DB_PATH)
    await db.connect()
    try:
        meta, sc16, _proc, _reads = await _record(settings, db, receiver, start_when=2, stop_when=8)
    finally:
        await db.close()

    assert meta["overflow_events"] == 0
    assert meta["lost_samples"] == 0
    assert meta["gaps"] == []
    assert meta["gaps_truncated"] is False
    assert meta["time_span_sec"] == meta["duration_sec"]
    assert meta["dropped_chunks"] == 0
    assert sc16.stat().st_size == meta["total_samples"] * 4
