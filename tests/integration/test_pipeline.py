"""Integration test: MockReceiver -> processing -> detection -> SQLite.

Runs the full pipeline (minus NATS) with a mock receiver to verify
captures flow through processing, burst detection, and local storage.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.continuous import ContinuousProcessor
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.local import LocalStorage


@pytest.fixture
async def pipeline_components(tmp_path):
    storage_path = tmp_path / "storage"
    storage_path.mkdir()
    db_path = tmp_path / "test.db"

    settings = AppSettings(
        FREQUENCY_START=915_000_000,
        FREQUENCY_END=915_000_000,
        BANDWIDTH=26_000_000,
        DURATION_SEC=0.001,  # very short capture for speed
        GAIN=35,
        NUM_FFT_BINS=64,
        PSD_TIME_RESOLUTION_MS=0.5,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage_path),
        DB_PATH=str(db_path),
        ARCHIVE_MAX_GB=0.01,
    )

    rx_config = ReceiverConfig(
        gain_db=settings.GAIN,
        bandwidth_hz=settings.BANDWIDTH,
        duration_sec=settings.DURATION_SEC,
    )
    receiver = MockReceiver(receiver_config=rx_config)

    db = SensorDatabase(str(db_path))
    await db.connect()

    local_storage = LocalStorage(
        storage_path=str(storage_path),
        max_gb=settings.ARCHIVE_MAX_GB,
    )

    processor = ContinuousProcessor(
        receiver=receiver,
        database=db,
        local_storage=local_storage,
        settings=settings,
    )

    yield processor, db, local_storage, settings

    await db.close()


@pytest.mark.asyncio
async def test_pipeline_runs_one_capture(pipeline_components):
    """Run the pipeline for one iteration and verify a file was saved."""
    processor, db, storage, settings = pipeline_components

    # Run one capture manually
    await processor._process_one_capture(settings.FREQUENCY_START)

    assert processor._capture_count == 1

    # Verify a .sc16 file was written
    sc16_files = list(Path(settings.STORAGE_PATH).glob("*.sc16"))
    assert len(sc16_files) == 1


@pytest.mark.asyncio
async def test_pipeline_loop_stops(pipeline_components):
    """Verify the loop can be started and stopped gracefully."""
    processor, db, storage, settings = pipeline_components

    async def stop_after_captures():
        while processor._capture_count < 2:
            await asyncio.sleep(0.01)
        processor.stop()

    await asyncio.gather(
        processor.run(),
        stop_after_captures(),
    )

    assert processor._capture_count >= 2


@pytest.mark.asyncio
async def test_pipeline_stores_detections_if_any(pipeline_components):
    """Run multiple captures; if bursts are detected they appear in the DB."""
    processor, db, storage, settings = pipeline_components

    for _ in range(3):
        await processor._process_one_capture(settings.FREQUENCY_START)

    # We can't guarantee bursts in noise, but the query should not error
    detections = await db.query_detections(limit=100)
    assert isinstance(detections, list)
