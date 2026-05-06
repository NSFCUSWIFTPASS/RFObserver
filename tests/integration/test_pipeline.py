"""Integration test: MockReceiver -> processing -> detection -> SQLite.

Tests both ContinuousProcessor (batch mode) and StreamingProcessor
(streaming mode) with a mock receiver to verify captures flow through
processing, burst detection, and local storage.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import numpy as np
import pytest

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.continuous import ContinuousProcessor
from rfobserver.pipeline.streaming import StreamingProcessor
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.local import LocalStorage

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def settings(tmp_path: Path) -> AppSettings:
    storage_path = tmp_path / "storage"
    storage_path.mkdir()
    return AppSettings(
        FREQUENCY_START=915_000_000,
        FREQUENCY_END=915_000_000,
        BANDWIDTH=1_000_000,  # low BW for fast test chunks
        DURATION_SEC=0.5,
        GAIN=35,
        NUM_FFT_BINS=64,
        PSD_TIME_RESOLUTION_MS=0.5,
        STREAMING_CHUNK_SLICES=10,  # small chunks for fast tests
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage_path),
        DB_PATH=str(tmp_path / "test.db"),
        ARCHIVE_MAX_GB=0.01,
        _env_file=None,
    )


@pytest.fixture
def receiver(settings: AppSettings) -> MockReceiver:
    rx_config = ReceiverConfig(
        gain_db=settings.GAIN,
        bandwidth_hz=settings.BANDWIDTH,
        duration_sec=settings.DURATION_SEC,
    )
    rx = MockReceiver(receiver_config=rx_config)
    rx.initialize()
    return rx


@pytest.fixture
async def db(settings: AppSettings) -> SensorDatabase:
    database = SensorDatabase(settings.DB_PATH)
    await database.connect()
    yield database  # type: ignore[misc]
    await database.close()


@pytest.fixture
def local_storage(settings: AppSettings) -> LocalStorage:
    return LocalStorage(
        storage_path=settings.STORAGE_PATH,
        max_gb=settings.ARCHIVE_MAX_GB,
    )


# ---------------------------------------------------------------------------
# ContinuousProcessor (batch mode) tests
# ---------------------------------------------------------------------------


@pytest.fixture
def batch_settings(tmp_path: Path) -> AppSettings:
    """Settings tuned for fast batch pipeline tests (very short capture)."""
    storage_path = tmp_path / "batch_storage"
    storage_path.mkdir()
    return AppSettings(
        FREQUENCY_START=915_000_000,
        FREQUENCY_END=915_000_000,
        BANDWIDTH=1_000_000,
        DURATION_SEC=0.001,  # very short for batch capture
        GAIN=35,
        NUM_FFT_BINS=64,
        PSD_TIME_RESOLUTION_MS=0.5,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage_path),
        DB_PATH=str(tmp_path / "batch_test.db"),
        ARCHIVE_MAX_GB=0.01,
        _env_file=None,
    )


@pytest.fixture
def batch_receiver(batch_settings: AppSettings) -> MockReceiver:
    rx_config = ReceiverConfig(
        gain_db=batch_settings.GAIN,
        bandwidth_hz=batch_settings.BANDWIDTH,
        duration_sec=batch_settings.DURATION_SEC,
    )
    rx = MockReceiver(receiver_config=rx_config)
    rx.initialize()
    return rx


@pytest.fixture
async def batch_db(batch_settings: AppSettings) -> SensorDatabase:
    database = SensorDatabase(batch_settings.DB_PATH)
    await database.connect()
    yield database  # type: ignore[misc]
    await database.close()


@pytest.fixture
def batch_local_storage(batch_settings: AppSettings) -> LocalStorage:
    return LocalStorage(
        storage_path=batch_settings.STORAGE_PATH,
        max_gb=batch_settings.ARCHIVE_MAX_GB,
    )


@pytest.fixture
def batch_processor(
    batch_receiver: MockReceiver,
    batch_db: SensorDatabase,
    batch_local_storage: LocalStorage,
    batch_settings: AppSettings,
) -> ContinuousProcessor:
    return ContinuousProcessor(
        receiver=batch_receiver,
        database=batch_db,
        local_storage=batch_local_storage,
        settings=batch_settings,
    )


@pytest.mark.asyncio
async def test_batch_pipeline_runs_and_stops(batch_processor: ContinuousProcessor) -> None:
    """Verify the batch pipeline loop starts, captures, and stops gracefully."""

    async def stop_after_captures() -> None:
        while batch_processor._capture_count < 2:
            await asyncio.sleep(0.01)
        batch_processor.stop()

    await asyncio.wait_for(
        asyncio.gather(batch_processor.run(), stop_after_captures()),
        timeout=10.0,
    )

    assert batch_processor._capture_count >= 2


@pytest.mark.asyncio
async def test_batch_pipeline_saves_files(
    batch_processor: ContinuousProcessor, batch_settings: AppSettings
) -> None:
    """Run the batch pipeline and verify .sc16 files are written."""

    async def stop_after_captures() -> None:
        while batch_processor._capture_count < 1:
            await asyncio.sleep(0.01)
        batch_processor.stop()

    await asyncio.wait_for(
        asyncio.gather(batch_processor.run(), stop_after_captures()),
        timeout=10.0,
    )

    sc16_files = list(Path(batch_settings.STORAGE_PATH).glob("*.sc16"))
    assert len(sc16_files) >= 1


@pytest.mark.asyncio
async def test_batch_pipeline_db_query_works(
    batch_processor: ContinuousProcessor, batch_db: SensorDatabase
) -> None:
    """Run the pipeline; detection query should not error."""

    async def stop_after_captures() -> None:
        while batch_processor._capture_count < 2:
            await asyncio.sleep(0.01)
        batch_processor.stop()

    await asyncio.wait_for(
        asyncio.gather(batch_processor.run(), stop_after_captures()),
        timeout=10.0,
    )

    detections = await batch_db.query_detections(limit=100)
    assert isinstance(detections, list)


# ---------------------------------------------------------------------------
# StreamingProcessor tests
# ---------------------------------------------------------------------------


@pytest.fixture
def streaming_processor(
    receiver: MockReceiver,
    db: SensorDatabase,
    local_storage: LocalStorage,
    settings: AppSettings,
) -> StreamingProcessor:
    return StreamingProcessor(
        receiver=receiver,
        database=db,
        local_storage=local_storage,
        settings=settings,
    )


@pytest.mark.asyncio
async def test_streaming_pipeline_runs_and_stops(
    streaming_processor: StreamingProcessor,
) -> None:
    """Verify the streaming pipeline starts, processes chunks, and stops."""

    async def stop_after_chunks() -> None:
        while streaming_processor._capture_count < 5:
            await asyncio.sleep(0.02)
        streaming_processor.stop()

    await asyncio.wait_for(
        asyncio.gather(streaming_processor.run(), stop_after_chunks()),
        timeout=10.0,
    )

    assert streaming_processor._capture_count >= 5


@pytest.mark.asyncio
async def test_streaming_pipeline_db_query_works(
    streaming_processor: StreamingProcessor, db: SensorDatabase
) -> None:
    """Run the streaming pipeline; detection query should not error."""

    async def stop_after_chunks() -> None:
        while streaming_processor._capture_count < 5:
            await asyncio.sleep(0.02)
        streaming_processor.stop()

    await asyncio.wait_for(
        asyncio.gather(streaming_processor.run(), stop_after_chunks()),
        timeout=10.0,
    )

    detections = await db.query_detections(limit=100)
    assert isinstance(detections, list)


@pytest.mark.asyncio
async def test_streaming_reconfigure_does_not_crash(
    streaming_processor: StreamingProcessor, settings: AppSettings
) -> None:
    """Verify calling reconfigure() doesn't crash a running pipeline."""

    async def reconfigure_and_stop() -> None:
        while streaming_processor._capture_count < 3:
            await asyncio.sleep(0.02)

        # Trigger reconfigure (settings already match — just tests the signal path)
        streaming_processor.reconfigure()
        await asyncio.sleep(0.1)

        streaming_processor.stop()

    await asyncio.wait_for(
        asyncio.gather(streaming_processor.run(), reconfigure_and_stop()),
        timeout=10.0,
    )

    assert streaming_processor._capture_count >= 3


@pytest.mark.asyncio
async def test_streaming_rapid_reconfigure_does_not_wedge(
    streaming_processor: StreamingProcessor, settings: AppSettings
) -> None:
    """Burst many reconfigures (incl. NUM_FFT_BINS change) without wedging.

    Regression test: in-flight results from the prior config interleaved with
    new-config results in _result_queue. The consumer's accum_powers list
    held mismatched-shape rows, np.mean() raised ValueError, and the
    consumer task exited — taking _running=False and all threads with it.
    """

    # Push the burst threshold high so MockReceiver Gaussian noise doesn't
    # generate thousands of false-positive detections — those bog the DB
    # write path and make this test flaky under load. The detector's
    # reconfigure path runs the same regardless of how many bursts get
    # emitted.
    object.__setattr__(settings, "BURST_THRESHOLD_HIGH_DB", 40.0)

    async def burst_reconfigures_and_stop() -> None:
        while streaming_processor._capture_count < 3:
            await asyncio.sleep(0.02)

        # Rapid pipeline-affecting changes including NUM_FFT_BINS (which
        # changes PSD row shape). Fired in a tight loop so the consumer
        # sees in-flight old-shape results interleaved with new-shape ones.
        for attr, val in (
            ("FREQUENCY_START", 916_000_000),
            ("BANDWIDTH", 2_000_000),
            ("GAIN", 30),
            ("NUM_FFT_BINS", 512),
            ("DURATION_SEC", 0.6),
        ):
            object.__setattr__(settings, attr, val)
            streaming_processor.reconfigure()

        # Let the pre-fix wedge pathway play out: when the consumer task
        # crashes on np.mean shape mismatch, _running flips False and all
        # threads exit. Sample count pre/post a settle window — if the
        # pipeline wedged, t1 == t0 and the assertion fires.
        await asyncio.sleep(1.0)
        t0 = streaming_processor._capture_count
        await asyncio.sleep(1.5)
        t1 = streaming_processor._capture_count
        streaming_processor.stop()
        assert t1 > t0, f"pipeline wedged after rapid reconfigure (t0={t0} t1={t1})"

    await asyncio.wait_for(
        asyncio.gather(streaming_processor.run(), burst_reconfigures_and_stop()),
        timeout=30.0,
    )


@pytest.mark.asyncio
async def test_streaming_manual_recording(
    streaming_processor: StreamingProcessor, settings: AppSettings
) -> None:
    """Start recording, capture chunks, stop, verify .sc16 + .json files."""

    async def record_and_stop() -> None:
        while streaming_processor._capture_count < 3:
            await asyncio.sleep(0.02)

        streaming_processor.start_recording()
        status = streaming_processor.recording_status()
        assert status["state"] == "recording"
        assert status["file"] is not None
        assert status["file"].endswith(".sc16")

        # Let chunks be recorded
        target = streaming_processor._capture_count + 5
        while streaming_processor._capture_count < target:
            await asyncio.sleep(0.02)

        # Bytes should be growing
        assert streaming_processor.recording_status()["bytes"] > 0

        streaming_processor.stop_recording()
        assert streaming_processor.recording_status()["state"] == "idle"
        streaming_processor.stop()

    await asyncio.wait_for(
        asyncio.gather(streaming_processor.run(), record_and_stop()),
        timeout=10.0,
    )

    # Verify .sc16 file created with data
    sc16_files = list(Path(settings.STORAGE_PATH).glob("*.sc16"))
    assert len(sc16_files) >= 1
    assert sc16_files[0].stat().st_size > 0

    # Verify companion .json metadata file
    json_files = list(Path(settings.STORAGE_PATH).glob("*.json"))
    assert len(json_files) >= 1
    import json

    meta = json.loads(json_files[0].read_text())
    assert meta["format"] == "sc16"
    assert meta["sample_rate_hz"] > 0
    assert meta["total_bytes"] > 0
    assert "dropped_chunks" in meta
    assert "center_freq_hz" in meta

    # Verify companion .npz PSD data
    npz_files = list(Path(settings.STORAGE_PATH).glob("*.npz"))
    assert len(npz_files) >= 1
    data = np.load(npz_files[0])
    assert "grid" in data
    assert "freq_axis" in data
    assert "time_resolution_s" in data
    assert data["grid"].shape[0] > 0  # has rows
    assert data["grid"].shape[1] > 0  # has bins


@pytest.mark.asyncio
async def test_streaming_arm_trigger(
    streaming_processor: StreamingProcessor,
) -> None:
    """Arm trigger, verify state, disarm, verify idle."""

    async def arm_and_stop() -> None:
        while streaming_processor._capture_count < 2:
            await asyncio.sleep(0.02)

        streaming_processor.arm_trigger()
        status = streaming_processor.recording_status()
        assert status["state"] == "armed"
        assert status["file"] is None  # not recording yet
        assert status["bytes"] == 0

        streaming_processor.stop_recording()
        assert streaming_processor.recording_status()["state"] == "idle"

        streaming_processor.stop()

    await asyncio.wait_for(
        asyncio.gather(streaming_processor.run(), arm_and_stop()),
        timeout=10.0,
    )


@pytest.mark.asyncio
async def test_streaming_recording_status_fields(
    streaming_processor: StreamingProcessor,
) -> None:
    """Verify recording_status() returns all required fields."""

    async def check_and_stop() -> None:
        while streaming_processor._capture_count < 2:
            await asyncio.sleep(0.02)

        status = streaming_processor.recording_status()
        assert "state" in status
        assert "file" in status
        assert "bytes" in status
        assert "duration_sec" in status
        assert "dropped_chunks" in status

        streaming_processor.stop()

    await asyncio.wait_for(
        asyncio.gather(streaming_processor.run(), check_and_stop()),
        timeout=10.0,
    )


@pytest.mark.asyncio
async def test_streaming_start_recording_while_armed(
    streaming_processor: StreamingProcessor, settings: AppSettings
) -> None:
    """Manual record while armed should start recording immediately."""

    async def arm_then_record() -> None:
        while streaming_processor._capture_count < 2:
            await asyncio.sleep(0.02)

        # Arm first
        streaming_processor.arm_trigger()
        assert streaming_processor.recording_status()["state"] == "armed"

        # Manual record overrides armed
        streaming_processor.start_recording()
        assert streaming_processor.recording_status()["state"] == "recording"

        target = streaming_processor._capture_count + 3
        while streaming_processor._capture_count < target:
            await asyncio.sleep(0.02)

        streaming_processor.stop_recording()
        assert streaming_processor.recording_status()["state"] == "idle"
        streaming_processor.stop()

    await asyncio.wait_for(
        asyncio.gather(streaming_processor.run(), arm_then_record()),
        timeout=10.0,
    )

    sc16_files = list(Path(settings.STORAGE_PATH).glob("*.sc16"))
    assert len(sc16_files) >= 1


@pytest.mark.asyncio
async def test_streaming_double_start_is_safe(
    streaming_processor: StreamingProcessor,
) -> None:
    """Calling start_recording twice should not crash or create two files."""

    async def double_start() -> None:
        while streaming_processor._capture_count < 2:
            await asyncio.sleep(0.02)

        streaming_processor.start_recording()
        file1 = streaming_processor.recording_status()["file"]

        # Second start while already recording — should be ignored
        streaming_processor.start_recording()
        file2 = streaming_processor.recording_status()["file"]
        assert file1 == file2  # same recording

        streaming_processor.stop_recording()
        streaming_processor.stop()

    await asyncio.wait_for(
        asyncio.gather(streaming_processor.run(), double_start()),
        timeout=10.0,
    )


@pytest.mark.asyncio
async def test_streaming_stop_while_idle_is_safe(
    streaming_processor: StreamingProcessor,
) -> None:
    """Calling stop_recording while idle should not crash."""

    async def stop_idle() -> None:
        while streaming_processor._capture_count < 2:
            await asyncio.sleep(0.02)

        # Stop while idle — should be a no-op
        streaming_processor.stop_recording()
        assert streaming_processor.recording_status()["state"] == "idle"

        streaming_processor.stop()

    await asyncio.wait_for(
        asyncio.gather(streaming_processor.run(), stop_idle()),
        timeout=10.0,
    )


# ---------------------------------------------------------------------------
# RAM-buffered recording tests
# ---------------------------------------------------------------------------


@pytest.fixture
def ram_settings(tmp_path: Path) -> AppSettings:
    storage_path = tmp_path / "ram_storage"
    storage_path.mkdir()
    return AppSettings(
        FREQUENCY_START=915_000_000,
        FREQUENCY_END=915_000_000,
        BANDWIDTH=1_000_000,
        DURATION_SEC=0.5,
        GAIN=35,
        NUM_FFT_BINS=64,
        PSD_TIME_RESOLUTION_MS=0.5,
        STREAMING_CHUNK_SLICES=10,
        RECORDING_RAM_BUFFER=True,
        RECORDING_MAX_SEC=5.0,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage_path),
        DB_PATH=str(tmp_path / "ram_test.db"),
        ARCHIVE_MAX_GB=0.01,
        _env_file=None,
    )


@pytest.fixture
def ram_receiver(ram_settings: AppSettings) -> MockReceiver:
    rx_config = ReceiverConfig(
        gain_db=ram_settings.GAIN,
        bandwidth_hz=ram_settings.BANDWIDTH,
        duration_sec=ram_settings.DURATION_SEC,
    )
    rx = MockReceiver(receiver_config=rx_config)
    rx.initialize()
    return rx


@pytest.fixture
async def ram_db(ram_settings: AppSettings) -> SensorDatabase:
    database = SensorDatabase(ram_settings.DB_PATH)
    await database.connect()
    yield database  # type: ignore[misc]
    await database.close()


@pytest.fixture
def ram_local_storage(ram_settings: AppSettings) -> LocalStorage:
    return LocalStorage(
        storage_path=ram_settings.STORAGE_PATH,
        max_gb=ram_settings.ARCHIVE_MAX_GB,
    )


@pytest.fixture
def ram_processor(
    ram_receiver: MockReceiver,
    ram_db: SensorDatabase,
    ram_local_storage: LocalStorage,
    ram_settings: AppSettings,
) -> StreamingProcessor:
    return StreamingProcessor(
        receiver=ram_receiver,
        database=ram_db,
        local_storage=ram_local_storage,
        settings=ram_settings,
    )


@pytest.mark.asyncio
async def test_ram_recording_creates_file(
    ram_processor: StreamingProcessor, ram_settings: AppSettings
) -> None:
    """RAM-buffered recording should flush to disk on stop."""

    async def record_and_stop() -> None:
        while ram_processor._capture_count < 3:
            await asyncio.sleep(0.02)

        ram_processor.start_recording()
        assert ram_processor.recording_status()["state"] == "recording"
        assert ram_processor._recording_buf is not None  # RAM buffer allocated

        target = ram_processor._capture_count + 5
        while ram_processor._capture_count < target:
            await asyncio.sleep(0.02)

        # Bytes should be tracked in RAM
        assert ram_processor.recording_status()["bytes"] > 0

        ram_processor.stop_recording()
        assert ram_processor._recording_buf is None  # buffer freed
        ram_processor.stop()

    await asyncio.wait_for(
        asyncio.gather(ram_processor.run(), record_and_stop()),
        timeout=10.0,
    )

    # Verify file flushed to disk
    sc16_files = list(Path(ram_settings.STORAGE_PATH).glob("*.sc16"))
    assert len(sc16_files) >= 1
    assert sc16_files[0].stat().st_size > 0

    # Verify companion JSON
    json_files = list(Path(ram_settings.STORAGE_PATH).glob("*.json"))
    assert len(json_files) >= 1
    import json

    meta = json.loads(json_files[0].read_text())
    assert meta["ram_buffered"] is True
    assert meta["total_bytes"] > 0
    assert meta["dropped_chunks"] == 0


@pytest.mark.asyncio
async def test_ram_recording_no_file_during_capture(
    ram_processor: StreamingProcessor, ram_settings: AppSettings
) -> None:
    """In RAM mode, no .sc16 file should exist while recording is active."""

    async def check_during_recording() -> None:
        while ram_processor._capture_count < 3:
            await asyncio.sleep(0.02)

        ram_processor.start_recording()

        target = ram_processor._capture_count + 3
        while ram_processor._capture_count < target:
            await asyncio.sleep(0.02)

        # File should NOT exist yet (still in RAM)
        sc16_files = list(Path(ram_settings.STORAGE_PATH).glob("*.sc16"))
        assert len(sc16_files) == 0

        ram_processor.stop_recording()

        # NOW file should exist
        sc16_files = list(Path(ram_settings.STORAGE_PATH).glob("*.sc16"))
        assert len(sc16_files) >= 1

        ram_processor.stop()

    await asyncio.wait_for(
        asyncio.gather(ram_processor.run(), check_during_recording()),
        timeout=10.0,
    )


@pytest.mark.asyncio
async def test_ram_recording_arm_and_fire(
    ram_processor: StreamingProcessor, ram_settings: AppSettings
) -> None:
    """Arm trigger in RAM mode, manually fire it, verify file."""
    # Set threshold very low so mock receiver's noise triggers it
    object.__setattr__(ram_settings, "TRIGGER_THRESHOLD_DB", -200.0)

    async def arm_and_wait() -> None:
        while ram_processor._capture_count < 2:
            await asyncio.sleep(0.02)

        ram_processor.arm_trigger()
        assert ram_processor.recording_status()["state"] == "armed"

        # With -200 dB threshold, trigger should fire on next chunk
        while ram_processor.recording_status()["state"] != "recording":
            await asyncio.sleep(0.02)

        # Let it record a bit
        target = ram_processor._capture_count + 3
        while ram_processor._capture_count < target:
            await asyncio.sleep(0.02)

        ram_processor.stop_recording()
        ram_processor.stop()

    await asyncio.wait_for(
        asyncio.gather(ram_processor.run(), arm_and_wait()),
        timeout=10.0,
    )

    sc16_files = list(Path(ram_settings.STORAGE_PATH).glob("*.sc16"))
    assert len(sc16_files) >= 1

    import json

    json_files = list(Path(ram_settings.STORAGE_PATH).glob("*.json"))
    meta = json.loads(json_files[0].read_text())
    assert meta["trigger_initiated"] is True
    assert meta["ram_buffered"] is True
