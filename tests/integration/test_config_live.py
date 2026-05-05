"""Integration test: change every Config-page option while RFObserver is running.

Spins up a StreamingProcessor against a MockReceiver concurrently with a FastAPI
ASGI client, then exercises every endpoint the Config page hits. The pipeline
must keep advancing its capture counter through each change.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import ReceiverConfig
from rfobserver.config import AppSettings
from rfobserver.pipeline.streaming import StreamingProcessor
from rfobserver.storage.database import SensorDatabase
from rfobserver.storage.local import LocalStorage
from rfobserver.web.app import create_app

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path


@pytest.fixture
def settings(tmp_path: Path) -> AppSettings:
    storage = tmp_path / "storage"
    storage.mkdir()
    return AppSettings(
        FREQUENCY_START=915_000_000,
        FREQUENCY_END=915_000_000,
        BANDWIDTH=1_000_000,
        DURATION_SEC=0.5,
        GAIN=35,
        NUM_FFT_BINS=256,
        PSD_TIME_RESOLUTION_MS=0.5,
        STREAMING_CHUNK_SLICES=10,
        MOCK_RECEIVER=True,
        STORAGE_PATH=str(storage),
        DB_PATH=str(tmp_path / "test.db"),
        ARCHIVE_MAX_GB=0.01,
        _env_file=None,
    )


@pytest.fixture
async def live_app(
    settings: AppSettings, tmp_path: Path
) -> AsyncIterator[tuple[AsyncClient, StreamingProcessor, AppSettings]]:
    """Run a streaming pipeline + ASGI client together; yield both."""
    db = SensorDatabase(settings.DB_PATH)
    await db.connect()

    rx_config = ReceiverConfig(
        gain_db=settings.GAIN,
        bandwidth_hz=settings.BANDWIDTH,
        duration_sec=settings.DURATION_SEC,
    )
    receiver = MockReceiver(receiver_config=rx_config)
    receiver.initialize()

    storage = LocalStorage(
        storage_path=settings.STORAGE_PATH,
        max_gb=settings.ARCHIVE_MAX_GB,
    )

    processor = StreamingProcessor(
        receiver=receiver,
        database=db,
        local_storage=storage,
        settings=settings,
    )

    app = create_app(settings)
    app.state.processor = processor
    app.state.database = db

    # cd to tmp so _persist_settings writing to "./.env" doesn't clobber the repo file
    import os

    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        run_task = asyncio.create_task(processor.run())
        try:
            # Wait until the pipeline has produced something so we know it's live.
            await _wait_for_chunks(processor, 3, timeout=5.0)
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                yield client, processor, settings
        finally:
            processor.stop()
            try:
                await asyncio.wait_for(run_task, timeout=5.0)
            except TimeoutError:
                run_task.cancel()
            await db.close()
    finally:
        os.chdir(cwd)


async def _wait_for_chunks(processor: StreamingProcessor, n: int, timeout: float) -> None:
    target = processor._capture_count + n
    deadline = asyncio.get_event_loop().time() + timeout
    while processor._capture_count < target:
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(
                f"Pipeline did not advance {n} chunks in {timeout}s "
                f"(stuck at {processor._capture_count}, wanted {target})"
            )
        await asyncio.sleep(0.02)


# Each entry: (form_key, JSON value to send, expected settings attribute name).
# Values are chosen to differ from the fixture defaults so reconfigure() actually fires.
_CONFIG_FIELDS: list[tuple[str, Any, str]] = [
    ("frequency_start", 920_000_000, "FREQUENCY_START"),
    ("frequency_end", 920_000_000, "FREQUENCY_END"),
    ("frequency_step", 1_000_000, "FREQUENCY_STEP"),
    ("bandwidth", 2_000_000, "BANDWIDTH"),
    ("gain", 30, "GAIN"),
    ("duration_sec", 0.6, "DURATION_SEC"),
    ("trigger_threshold_db", -100.0, "TRIGGER_THRESHOLD_DB"),
    ("burst_threshold_high_db", -50.0, "BURST_THRESHOLD_HIGH_DB"),
    ("burst_threshold_low_ratio", 0.45, "BURST_THRESHOLD_LOW_RATIO"),
    ("psd_time_resolution_ms", 1.0, "PSD_TIME_RESOLUTION_MS"),
    ("num_fft_bins", 512, "NUM_FFT_BINS"),
    ("archive_max_gb", 0.05, "ARCHIVE_MAX_GB"),
    ("history_days", 14, "HISTORY_DAYS"),
    ("nats_host", "127.0.0.1", "NATS_HOST"),
    ("nats_port", 4223, "NATS_PORT"),
    ("zms_zmc_http", "http://localhost:9210/v1", "ZMS_ZMC_HTTP"),
    ("zms_dst_http", "http://localhost:9220/v1", "ZMS_DST_HTTP"),
    ("zms_identity_http", "http://localhost:9200/v1", "ZMS_IDENTITY_HTTP"),
    ("zms_monitor_id", "00000000-0000-0000-0000-000000000001", "ZMS_MONITOR_ID"),
    ("zms_monitor_name", "test-monitor", "ZMS_MONITOR_NAME"),
]


@pytest.mark.asyncio
async def test_config_apply_every_field_keeps_pipeline_running(
    live_app: tuple[AsyncClient, StreamingProcessor, AppSettings],
) -> None:
    """POST every Config-page field while the pipeline is running.

    Asserts every POST returns 200, every value lands in the settings object,
    and the pipeline is still producing chunks at the end. Per-step throughput
    is not asserted — pipeline-affecting reconfigures briefly pause the
    receiver and can pile up under fast sequential changes.
    """
    client, processor, settings = live_app

    for form_key, value, attr in _CONFIG_FIELDS:
        r = await client.post("/config/apply", json={form_key: value})
        assert r.status_code == 200, f"{form_key}={value!r} got {r.status_code}: {r.text}"
        assert getattr(settings, attr) == value, f"{attr} not applied"

    for form_key, attr in (("zms_token", "ZMS_TOKEN"), ("nats_token", "NATS_TOKEN")):
        r = await client.post("/config/apply", json={form_key: "new-secret"})
        assert r.status_code == 200, f"{form_key} secret update failed: {r.text}"
        actual = getattr(settings, attr)
        assert isinstance(actual, SecretStr)
        assert actual.get_secret_value() == "new-secret"

    # Pipeline must recover and keep producing after the change burst.
    await _wait_for_chunks(processor, 1, timeout=30.0)


@pytest.mark.asyncio
async def test_config_invalid_fft_bins_rejected_pipeline_survives(
    live_app: tuple[AsyncClient, StreamingProcessor, AppSettings],
) -> None:
    client, processor, settings = live_app
    original = settings.NUM_FFT_BINS

    r = await client.post("/config/apply", json={"num_fft_bins": 999})
    assert r.status_code == 400
    assert original == settings.NUM_FFT_BINS  # unchanged
    await _wait_for_chunks(processor, 2, timeout=5.0)


@pytest.mark.asyncio
async def test_config_invalid_int_returns_400(
    live_app: tuple[AsyncClient, StreamingProcessor, AppSettings],
) -> None:
    client, processor, settings = live_app
    original = settings.GAIN

    r = await client.post("/config/apply", json={"gain": "not-a-number"})
    assert r.status_code == 400
    assert original == settings.GAIN
    await _wait_for_chunks(processor, 2, timeout=5.0)


@pytest.mark.asyncio
async def test_secret_empty_string_does_not_overwrite(
    live_app: tuple[AsyncClient, StreamingProcessor, AppSettings],
) -> None:
    client, processor, settings = live_app
    object.__setattr__(settings, "ZMS_TOKEN", SecretStr("preserved"))

    # Empty string should NOT overwrite
    r = await client.post("/config/apply", json={"zms_token": ""})
    assert r.status_code == 200
    assert settings.ZMS_TOKEN.get_secret_value() == "preserved"
    await _wait_for_chunks(processor, 1, timeout=3.0)

    # Non-empty value DOES overwrite
    r = await client.post("/config/apply", json={"zms_token": "fresh"})
    assert r.status_code == 200
    assert settings.ZMS_TOKEN.get_secret_value() == "fresh"
    await _wait_for_chunks(processor, 1, timeout=3.0)


@pytest.mark.asyncio
async def test_storage_set_path_changes_directory(
    live_app: tuple[AsyncClient, StreamingProcessor, AppSettings],
    tmp_path: Path,
) -> None:
    client, processor, settings = live_app
    new_path = tmp_path / "new_storage"

    r = await client.post("/api/storage/set-path", json={"path": str(new_path)})
    assert r.status_code == 200, r.text
    assert str(new_path) == settings.STORAGE_PATH
    assert new_path.exists()
    await _wait_for_chunks(processor, 2, timeout=5.0)


@pytest.mark.asyncio
async def test_zms_nats_enable_disable_status_round_trip(
    live_app: tuple[AsyncClient, StreamingProcessor, AppSettings],
) -> None:
    client, processor, settings = live_app

    # ZMS settings must be complete for /api/zms/enable to proceed.
    object.__setattr__(settings, "ZMS_ZMC_HTTP", "http://test/v1")
    object.__setattr__(settings, "ZMS_IDENTITY_HTTP", "http://test/v1")
    object.__setattr__(settings, "ZMS_TOKEN", SecretStr("t"))
    object.__setattr__(settings, "ZMS_MONITOR_ID", "00000000-0000-0000-0000-000000000001")

    async def noop_async(*_a: Any, **_kw: Any) -> None:
        return None

    with (
        patch("rfobserver.zms.monitor.ZmsMonitor.start", noop_async),
        patch("rfobserver.zms.monitor.ZmsMonitor.stop", noop_async),
        patch("rfobserver.transport.nats_producer.NatsProducer.connect", noop_async),
        patch("rfobserver.transport.nats_producer.NatsProducer.close", noop_async),
    ):
        # ZMS round-trip
        r = await client.post("/api/zms/enable")
        assert r.status_code == 200
        assert r.json()["status"] in ("enabled", "already_enabled")
        await _wait_for_chunks(processor, 1, timeout=10.0)

        r = await client.get("/api/zms/status")
        assert r.status_code == 200
        assert r.json()["enabled"] is True

        r = await client.post("/api/zms/disable")
        assert r.status_code == 200
        assert r.json()["status"] == "disabled"
        await _wait_for_chunks(processor, 1, timeout=10.0)

        # NATS round-trip
        r = await client.post("/api/nats/enable")
        assert r.status_code == 200
        assert r.json()["status"] in ("enabled", "already_enabled")
        await _wait_for_chunks(processor, 1, timeout=10.0)

        r = await client.get("/api/nats/status")
        assert r.status_code == 200
        assert "connected" in r.json()

        r = await client.post("/api/nats/disable")
        assert r.status_code == 200
        assert r.json()["status"] == "disabled"
        await _wait_for_chunks(processor, 1, timeout=10.0)
