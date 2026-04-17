"""Integration test: MockReceiver -> processing -> detection -> SQLite.

Tests both ContinuousProcessor (batch mode) and StreamingProcessor
(streaming mode) with a mock receiver to verify captures flow through
processing, burst detection, and local storage.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

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
