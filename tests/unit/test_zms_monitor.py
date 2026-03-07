"""Tests for rfobserver.zms.monitor."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from rfobserver.config import ZmsSettingsGroup
from rfobserver.models import (
    IQStatistics,
    MetadataRecord,
    PSDData,
    ProcessedDataEnvelope,
)
from rfobserver.zms.monitor import ZmsMonitor


@pytest.fixture
def zms_settings():
    return ZmsSettingsGroup(
        zmc_http="http://zmc.test",
        identity_http="http://identity.test",
        token="tok-123",
        monitor_id="mon-1",
        monitor_name="Test Monitor",
        dst_http="http://dst.test",
    )


@pytest.fixture
def monitor(zms_settings):
    return ZmsMonitor(settings=zms_settings, heartbeat_interval=999)


@pytest.fixture
def envelope():
    meta = MetadataRecord(
        hostname="jetson",
        organization="test",
        frequency=915_000_000,
        timestamp=datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
        source_path=Path("/tmp/test.sc16"),
        gain=35,
        sampling_rate=26_000_000,
    )
    stats = IQStatistics(average=-110.0, max=-100.0, median=-112.0, std=3.5, kurtosis=0.1)
    psd = PSDData(
        powers=[-110.0] * 8,
        frequencies=[900e6 + i * 1e6 for i in range(8)],
        center_freq=915e6,
        sample_rate=26_000_000,
        num_bins=8,
    )
    return ProcessedDataEnvelope(metadata=meta, statistics=stats, psd_data=psd)


class TestZmsMonitor:
    @pytest.mark.asyncio
    async def test_submit_observation_success(self, monitor, envelope):
        monitor._client.send_sigmf_archive = AsyncMock(return_value=True)
        ok = await monitor.submit_observation(envelope)
        assert ok is True
        assert monitor.message_count == 1
        monitor._client.send_sigmf_archive.assert_called_once()

    @pytest.mark.asyncio
    async def test_submit_observation_failure(self, monitor, envelope):
        monitor._client.send_sigmf_archive = AsyncMock(return_value=False)
        ok = await monitor.submit_observation(envelope)
        assert ok is False
        assert monitor.message_count == 0

    @pytest.mark.asyncio
    async def test_submit_with_custom_kurtosis(self, monitor, envelope):
        monitor._client.send_sigmf_archive = AsyncMock(return_value=True)
        kurtosis = [1.0] * 8
        ok = await monitor.submit_observation(envelope, kurtosis_per_bin=kurtosis)
        assert ok is True

    @pytest.mark.asyncio
    async def test_start_sets_running(self, monitor):
        await monitor.start()
        assert monitor._running is True

    @pytest.mark.asyncio
    async def test_stop_clears_running(self, monitor):
        monitor._client.close = AsyncMock()
        await monitor.start()
        await monitor.stop()
        assert monitor._running is False

    def test_enqueue_reconfiguration(self, monitor):
        monitor.enqueue_reconfiguration({
            "status": "paused",
            "parameters": {"gain_db": 40},
            "pending_id": "p-1",
        })
        assert monitor._command_queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_process_reconfiguration_calls_callback(self, monitor):
        callback = AsyncMock()
        monitor._reconfiguration_callback = callback
        monitor._client.test_connection = AsyncMock(return_value=(True, True))
        monitor._client.get_monitor_info = AsyncMock(return_value={})

        await monitor._process_reconfiguration({
            "status": "paused",
            "parameters": {"gain_db": 40},
            "pending_id": "p-1",
        })

        callback.assert_called_once_with("paused", {"gain_db": 40})
        assert monitor.op_status == "paused"

    @pytest.mark.asyncio
    async def test_process_reconfiguration_active(self, monitor):
        callback = AsyncMock()
        monitor._reconfiguration_callback = callback
        monitor._client.test_connection = AsyncMock(return_value=(True, True))
        monitor._client.get_monitor_info = AsyncMock(return_value={})

        await monitor._process_reconfiguration({
            "status": "active",
            "parameters": {"gain_db": 35},
        })

        assert monitor.op_status == "active"

    def test_handle_ws_event_matching(self, monitor):
        event = {
            "header": {
                "source_type": 2,
                "code": 2010,
            },
            "object_": {
                "monitor_id": "mon-1",
                "id": "pending-42",
                "status": "active",
                "parameters": {"gain_db": 50},
            },
        }
        monitor._handle_ws_event(event, "mon-1")
        assert monitor._command_queue.qsize() == 1

    def test_handle_ws_event_wrong_monitor(self, monitor):
        event = {
            "header": {"source_type": 2, "code": 2010},
            "object_": {"monitor_id": "other-mon", "id": "p-1", "status": "active"},
        }
        monitor._handle_ws_event(event, "mon-1")
        assert monitor._command_queue.qsize() == 0

    def test_handle_ws_event_wrong_code(self, monitor):
        event = {
            "header": {"source_type": 2, "code": 9999},
            "object_": {"monitor_id": "mon-1"},
        }
        monitor._handle_ws_event(event, "mon-1")
        assert monitor._command_queue.qsize() == 0

    def test_zms_settings_property(self, zms_settings):
        assert zms_settings.dst_or_zmc == "http://dst.test"

    def test_zms_settings_fallback(self):
        s = ZmsSettingsGroup(
            zmc_http="http://zmc.test",
            identity_http="http://id.test",
            token="tok",
            monitor_id="m1",
        )
        assert s.dst_or_zmc == "http://zmc.test"
