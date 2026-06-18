"""Tests for rfobserver.web routes."""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from rfobserver.config import AppSettings
from rfobserver.web.app import create_app


@pytest.fixture
def settings():
    return AppSettings(_env_file=None)


@pytest.fixture
def client(settings):
    app = create_app(settings)
    return TestClient(app)


@pytest.fixture
def client_with_processor(settings):
    """Client with a mock processor attached to app state."""
    app = create_app(settings)
    mock_processor = MagicMock()
    app.state.processor = mock_processor
    return TestClient(app), settings, mock_processor


def test_health_endpoint(client):
    response = client.get("/api/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "version" in data


def test_api_status(client):
    response = client.get("/api/status")
    assert response.status_code == 200
    data = response.json()
    assert "version" in data
    assert "pipeline_running" in data


def test_dashboard_page(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "Dashboard" in response.text


def test_captures_page(client):
    response = client.get("/captures")
    assert response.status_code == 200
    assert "Captures" in response.text


def test_captures_list_returns_json(client):
    response = client.get("/captures/list")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


class TestCapturesPSD:
    def test_psd_missing_file(self, client):
        resp = client.get("/captures/psd/nonexistent.sc16")
        assert resp.status_code == 404

    def test_psd_path_traversal(self, client):
        resp = client.get("/captures/psd/../../../etc/passwd")
        # FastAPI normalizes ../  so this is either 400 (validation) or 404 (route)
        assert resp.status_code in (400, 404)

    def test_psd_returns_data(self, client, settings, tmp_path):
        # Create a mock .npz file in the storage path
        from pathlib import Path

        import numpy as np

        storage = Path(settings.STORAGE_PATH)
        storage.mkdir(parents=True, exist_ok=True)

        grid = np.random.uniform(-100, -60, (100, 64)).astype(np.float32)
        freq_axis = np.linspace(-500000, 500000, 64)
        npz_path = storage / "test-capture.npz"
        np.savez_compressed(
            npz_path,
            grid=grid,
            freq_axis=freq_axis,
            time_resolution_s=np.float64(0.001),
            center_freq_hz=np.int64(915000000),
            bandwidth_hz=np.int64(1000000),
        )
        # Also create the .sc16 so the capture shows up
        (storage / "test-capture.sc16").write_bytes(b"\x00" * 100)

        resp = client.get("/captures/psd/test-capture.sc16?start=0&count=50")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_rows"] == 100
        assert data["count"] == 50
        assert data["start"] == 0
        assert len(data["grid"]) == 50
        assert len(data["freq_axis"]) > 0
        assert data["center_freq_hz"] == 915000000
        # Global colour range spans the whole grid (not just the returned page),
        # so the lazy-loading client keeps stable colours across pages.
        assert data["grid_min"] == pytest.approx(float(grid.min()), abs=1e-3)
        assert data["grid_max"] == pytest.approx(float(grid.max()), abs=1e-3)
        # No calibration baked into this capture → null (viewer falls back to dBFS)
        assert data["cal_offset_db"] is None

        # Cleanup
        npz_path.unlink(missing_ok=True)
        (storage / "test-capture.sc16").unlink(missing_ok=True)

    def test_psd_returns_baked_cal_offset(self, client, settings):
        # A capture recorded under calibration bakes cal_offset_db into the
        # .npz; the endpoint surfaces it so the viewer can render dBm/Hz.
        from pathlib import Path

        import numpy as np

        storage = Path(settings.STORAGE_PATH)
        storage.mkdir(parents=True, exist_ok=True)

        grid = np.random.uniform(-100, -60, (20, 32)).astype(np.float32)
        npz_path = storage / "cal-capture.npz"
        np.savez_compressed(
            npz_path,
            grid=grid,
            freq_axis=np.linspace(-500000, 500000, 32),
            time_resolution_s=np.float64(0.001),
            center_freq_hz=np.int64(915000000),
            bandwidth_hz=np.int64(1000000),
            cal_offset_db=np.float64(-107.5),
        )
        (storage / "cal-capture.sc16").write_bytes(b"\x00" * 100)

        resp = client.get("/captures/psd/cal-capture.sc16?start=0&count=20")
        assert resp.status_code == 200
        assert resp.json()["cal_offset_db"] == pytest.approx(-107.5)

        npz_path.unlink(missing_ok=True)
        (storage / "cal-capture.sc16").unlink(missing_ok=True)

    def test_psd_pagination(self, client, settings):
        from pathlib import Path

        import numpy as np

        storage = Path(settings.STORAGE_PATH)
        storage.mkdir(parents=True, exist_ok=True)

        grid = np.random.uniform(-100, -60, (200, 32)).astype(np.float32)
        freq_axis = np.linspace(-500000, 500000, 32)
        npz_path = storage / "page-test.npz"
        np.savez_compressed(
            npz_path,
            grid=grid,
            freq_axis=freq_axis,
            time_resolution_s=np.float64(0.001),
            center_freq_hz=np.int64(915000000),
            bandwidth_hz=np.int64(1000000),
        )

        # Page 2
        resp = client.get("/captures/psd/page-test.npz?start=100&count=50")
        assert resp.status_code == 200
        data = resp.json()
        assert data["start"] == 100
        assert data["count"] == 50
        assert data["total_rows"] == 200

        npz_path.unlink(missing_ok=True)

    def test_captures_list_has_psd_field(self, client, settings):
        from pathlib import Path

        import numpy as np

        storage = Path(settings.STORAGE_PATH)
        storage.mkdir(parents=True, exist_ok=True)

        # Create .sc16 without .npz
        (storage / "no-psd.sc16").write_bytes(b"\x00" * 10)
        # Create .sc16 with .npz
        (storage / "with-psd.sc16").write_bytes(b"\x00" * 10)
        np.savez_compressed(
            storage / "with-psd.npz",
            grid=np.zeros((10, 8)),
            freq_axis=np.zeros(8),
            time_resolution_s=np.float64(0.001),
            center_freq_hz=np.int64(915000000),
            bandwidth_hz=np.int64(1000000),
        )

        resp = client.get("/captures/list")
        captures = resp.json()
        by_name = {c["filename"]: c for c in captures}

        if "no-psd.sc16" in by_name:
            assert by_name["no-psd.sc16"]["has_psd"] is False
        if "with-psd.sc16" in by_name:
            assert by_name["with-psd.sc16"]["has_psd"] is True

        # Cleanup
        (storage / "no-psd.sc16").unlink(missing_ok=True)
        (storage / "with-psd.sc16").unlink(missing_ok=True)
        (storage / "with-psd.npz").unlink(missing_ok=True)


def test_config_page(client):
    response = client.get("/config")
    assert response.status_code == 200
    assert "Configuration" in response.text


def test_history_page(client):
    response = client.get("/history")
    assert response.status_code == 200
    assert "Detection History" in response.text


# -- Config apply tests --


class TestConfigApply:
    def test_apply_gain(self, client_with_processor):
        client, settings, processor = client_with_processor
        resp = client.post("/config/apply", json={"gain": "50"})
        assert resp.status_code == 200
        assert "GAIN" in resp.json()["changed"]
        assert settings.GAIN == 50
        processor.reconfigure.assert_called_once()

    def test_apply_no_changes(self, client_with_processor):
        client, settings, _ = client_with_processor
        resp = client.post("/config/apply", json={"gain": str(settings.GAIN)})
        assert resp.status_code == 200
        assert resp.json()["changed"] == []

    def test_apply_num_fft_bins_valid(self, client_with_processor):
        client, settings, processor = client_with_processor
        for bins in [256, 512, 1024, 2048, 4096, 8192]:
            resp = client.post("/config/apply", json={"num_fft_bins": str(bins)})
            assert resp.status_code == 200
            assert bins == settings.NUM_FFT_BINS

    def test_apply_num_fft_bins_invalid(self, client_with_processor):
        client, settings, _ = client_with_processor
        original = settings.NUM_FFT_BINS
        resp = client.post("/config/apply", json={"num_fft_bins": "300"})
        assert resp.status_code == 400
        assert original == settings.NUM_FFT_BINS  # unchanged

    def test_apply_bandwidth_triggers_reconfigure(self, client_with_processor):
        client, settings, processor = client_with_processor
        resp = client.post("/config/apply", json={"bandwidth": "28000000"})
        assert resp.status_code == 200
        assert settings.BANDWIDTH == 28_000_000
        processor.reconfigure.assert_called_once()

    def test_apply_fft_bins_triggers_reconfigure(self, client_with_processor):
        client, settings, processor = client_with_processor
        resp = client.post("/config/apply", json={"num_fft_bins": "2048"})
        assert resp.status_code == 200
        processor.reconfigure.assert_called_once()

    def test_apply_frequency_triggers_reconfigure(self, client_with_processor):
        client, settings, processor = client_with_processor
        resp = client.post("/config/apply", json={"frequency_start": "900000000"})
        assert resp.status_code == 200
        assert settings.FREQUENCY_START == 900_000_000
        processor.reconfigure.assert_called_once()

    def test_apply_burst_threshold_triggers_reconfigure(self, client_with_processor):
        client, settings, processor = client_with_processor
        resp = client.post("/config/apply", json={"burst_threshold_high_db": "15.0"})
        assert resp.status_code == 200
        assert settings.BURST_THRESHOLD_HIGH_DB == 15.0
        processor.reconfigure.assert_called_once()

    def test_apply_storage_no_reconfigure(self, client_with_processor):
        client, settings, processor = client_with_processor
        resp = client.post("/config/apply", json={"archive_max_gb": "100"})
        assert resp.status_code == 200
        assert settings.ARCHIVE_MAX_GB == 100.0
        processor.reconfigure.assert_not_called()

    def test_apply_invalid_json(self, client):
        resp = client.post(
            "/config/apply",
            content=b"not json",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 400

    def test_apply_invalid_value(self, client_with_processor):
        client, _, _ = client_with_processor
        resp = client.post("/config/apply", json={"gain": "not_a_number"})
        assert resp.status_code == 400

    def test_config_page_shows_fft_dropdown(self, client):
        resp = client.get("/config")
        assert resp.status_code == 200
        assert "<select" in resp.text
        assert "num_fft_bins" in resp.text
        for n in [256, 512, 1024, 2048, 4096, 8192]:
            assert f'value="{n}"' in resp.text

    def test_apply_display_settings(self, client_with_processor):
        client, settings, processor = client_with_processor
        resp = client.post(
            "/config/apply",
            json={
                "cal_offset_db": "-107.5",
                "psd_scale_min_db": "-160",
                "psd_scale_max_db": "-80",
            },
        )
        assert resp.status_code == 200
        assert settings.CAL_OFFSET_DB == -107.5
        assert settings.PSD_SCALE_MIN_DB == -160.0
        assert settings.PSD_SCALE_MAX_DB == -80.0
        # Display-only settings must not bounce the pipeline
        processor.reconfigure.assert_not_called()

    def test_apply_display_settings_clear(self, client_with_processor):
        client, settings, _ = client_with_processor
        object.__setattr__(settings, "CAL_OFFSET_DB", -100.0)
        object.__setattr__(settings, "PSD_SCALE_MIN_DB", -150.0)
        resp = client.post(
            "/config/apply",
            json={"cal_offset_db": "", "psd_scale_min_db": ""},
        )
        assert resp.status_code == 200
        assert settings.CAL_OFFSET_DB is None
        assert settings.PSD_SCALE_MIN_DB is None

    def test_apply_cal_offset_zero_is_kept(self, client_with_processor):
        # 0.0 is a meaningful calibration value (display switches to dBm/Hz),
        # distinct from None (uncalibrated dBFS).
        client, settings, _ = client_with_processor
        resp = client.post("/config/apply", json={"cal_offset_db": "0"})
        assert resp.status_code == 200
        assert settings.CAL_OFFSET_DB == 0.0

    def test_config_page_shows_display_card(self, client):
        resp = client.get("/config")
        assert resp.status_code == 200
        assert "cal_offset_db" in resp.text
        assert "psd_scale_min_db" in resp.text
        assert "psd_scale_max_db" in resp.text


# -- Recording API tests --


class TestRecordingAPI:
    def test_recording_status_idle(self, client_with_processor):
        client, _, processor = client_with_processor
        processor.recording_status.return_value = {
            "state": "idle",
            "file": None,
            "bytes": 0,
            "duration_sec": 0,
            "dropped_chunks": 0,
        }
        resp = client.get("/api/recording/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["state"] == "idle"
        assert data["bytes"] == 0
        assert data["dropped_chunks"] == 0

    def test_recording_start(self, client_with_processor):
        client, _, processor = client_with_processor
        processor.recording_status.return_value = {
            "state": "recording",
            "file": "test.sc16",
            "bytes": 0,
            "duration_sec": 0,
            "dropped_chunks": 0,
        }
        resp = client.post("/api/recording/start")
        assert resp.status_code == 200
        processor.start_recording.assert_called_once()
        assert resp.json()["state"] == "recording"

    def test_recording_start_returns_file(self, client_with_processor):
        client, _, processor = client_with_processor
        processor.recording_status.return_value = {
            "state": "recording",
            "file": "322750B-ubuntu-20260418T120000.sc16",
            "bytes": 0,
            "duration_sec": 0,
            "dropped_chunks": 0,
        }
        resp = client.post("/api/recording/start")
        data = resp.json()
        assert data["file"] is not None
        assert data["file"].endswith(".sc16")

    def test_recording_arm(self, client_with_processor):
        client, _, processor = client_with_processor
        processor.recording_status.return_value = {
            "state": "armed",
            "file": None,
            "bytes": 0,
            "duration_sec": 0,
            "dropped_chunks": 0,
        }
        resp = client.post("/api/recording/arm")
        assert resp.status_code == 200
        processor.arm_trigger.assert_called_once()
        assert resp.json()["state"] == "armed"

    def test_recording_stop(self, client_with_processor):
        client, _, processor = client_with_processor
        processor.recording_status.return_value = {
            "state": "idle",
            "file": None,
            "bytes": 0,
            "duration_sec": 0,
            "dropped_chunks": 0,
        }
        resp = client.post("/api/recording/stop")
        assert resp.status_code == 200
        processor.stop_recording.assert_called_once()
        assert resp.json()["state"] == "idle"

    def test_recording_stop_from_armed(self, client_with_processor):
        """Stop while armed should return to idle."""
        client, _, processor = client_with_processor
        processor.recording_status.return_value = {
            "state": "idle",
            "file": None,
            "bytes": 0,
            "duration_sec": 0,
            "dropped_chunks": 0,
        }
        resp = client.post("/api/recording/stop")
        assert resp.status_code == 200
        processor.stop_recording.assert_called_once()

    def test_recording_status_all_fields(self, client_with_processor):
        client, _, processor = client_with_processor
        processor.recording_status.return_value = {
            "state": "recording",
            "file": "test-capture.sc16",
            "bytes": 1048576,
            "duration_sec": 2.5,
            "dropped_chunks": 3,
        }
        resp = client.get("/api/recording/status")
        data = resp.json()
        assert data["state"] == "recording"
        assert data["file"] == "test-capture.sc16"
        assert data["bytes"] == 1048576
        assert data["duration_sec"] == 2.5
        assert data["dropped_chunks"] == 3

    def test_recording_status_no_processor(self, client):
        """Status without processor returns idle defaults."""
        resp = client.get("/api/recording/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["state"] == "idle"

    def test_recording_start_no_processor(self, client):
        """Start without processor returns idle."""
        resp = client.post("/api/recording/start")
        assert resp.status_code == 200
        assert resp.json()["state"] == "idle"

    def test_recording_dashboard_has_controls(self, client):
        """Dashboard page should have record/arm/stop buttons."""
        resp = client.get("/")
        assert resp.status_code == 200
        assert "rec-btn" in resp.text
        assert "arm-btn" in resp.text
        assert "stop-btn" in resp.text


# -- Storage path tests --


class TestStoragePath:
    def test_set_path_valid(self, client_with_processor, tmp_path):
        client, settings, _ = client_with_processor
        new_path = str(tmp_path / "captures")
        resp = client.post("/api/storage/set-path", json={"path": new_path})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["path"] == new_path
        assert new_path == settings.STORAGE_PATH
        # Directory should have been created
        from pathlib import Path

        assert Path(new_path).is_dir()

    def test_set_path_creates_nested_dirs(self, client_with_processor, tmp_path):
        client, settings, _ = client_with_processor
        new_path = str(tmp_path / "a" / "b" / "c" / "captures")
        resp = client.post("/api/storage/set-path", json={"path": new_path})
        assert resp.status_code == 200
        from pathlib import Path

        assert Path(new_path).is_dir()

    def test_set_path_empty_rejected(self, client_with_processor):
        client, _, _ = client_with_processor
        resp = client.post("/api/storage/set-path", json={"path": ""})
        assert resp.status_code == 400

    def test_set_path_no_write_access(self, client_with_processor):
        client, _, _ = client_with_processor
        resp = client.post("/api/storage/set-path", json={"path": "/root/nope"})
        assert resp.status_code == 400

    def test_set_path_updates_processor_storage(self, client_with_processor, tmp_path):
        client, _, processor = client_with_processor
        from pathlib import Path
        from unittest.mock import MagicMock

        mock_storage = MagicMock()
        processor._storage = mock_storage
        new_path = str(tmp_path / "new_storage")
        resp = client.post("/api/storage/set-path", json={"path": new_path})
        assert resp.status_code == 200
        assert mock_storage.storage_path == Path(new_path)

    def test_config_page_shows_storage_path(self, client):
        resp = client.get("/config")
        assert resp.status_code == 200
        assert "storage-path" in resp.text
        assert "set-path-btn" in resp.text


# -- ZMS/NATS API tests --


class TestZmsNatsAPI:
    def test_zms_status_no_processor(self, client):
        resp = client.get("/api/zms/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["enabled"] is False
        assert "connected" in data

    def test_zms_status_with_processor(self, client_with_processor):
        client, _, processor = client_with_processor
        processor._zms_monitor = None
        resp = client.get("/api/zms/status")
        assert resp.status_code == 200
        assert resp.json()["enabled"] is False

    def test_zms_enable_incomplete_settings(self, client_with_processor):
        client, settings, _ = client_with_processor
        resp = client.post("/api/zms/enable")
        assert resp.status_code == 200
        # Settings incomplete (no ZMS fields set), should error
        assert resp.json()["status"] == "error"

    def test_zms_disable(self, client_with_processor):
        client, _, processor = client_with_processor
        processor._zms_monitor = None
        resp = client.post("/api/zms/disable")
        assert resp.status_code == 200
        assert resp.json()["status"] == "disabled"

    def test_nats_status(self, client):
        resp = client.get("/api/nats/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "connected" in data
        assert "host" in data
        assert "port" in data
        assert "enabled" in data
        # No processor attached -> not connected, no counts.
        assert data["connected"] is False
        assert data["stats_count"] == 0

    def test_nats_status_reflects_attached_producer(self, client_with_processor):
        client, _, processor = client_with_processor
        producer = MagicMock()
        producer.connected = True
        producer.stats_count = 7
        producer.dropped = 1
        processor._nats_producer = producer
        resp = client.get("/api/nats/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["connected"] is True
        assert data["stats_count"] == 7
        assert data["dropped"] == 1

    def test_nats_disable_with_no_producer(self, client_with_processor):
        client, _, processor = client_with_processor
        processor._nats_producer = None
        resp = client.post("/api/nats/disable")
        assert resp.status_code == 200
        assert resp.json()["status"] == "disabled"

    def test_config_page_shows_zms_section(self, client):
        resp = client.get("/config")
        assert resp.status_code == 200
        assert "OpenZMS" in resp.text
        assert "zms-toggle" in resp.text
        assert "zms-zmc" in resp.text

    def test_config_page_shows_nats_section(self, client):
        resp = client.get("/config")
        assert resp.status_code == 200
        assert "NATS" in resp.text
        assert "nats-host" in resp.text
        assert "nats-port" in resp.text

    def test_apply_nats_host(self, client_with_processor):
        client, settings, _ = client_with_processor
        resp = client.post("/config/apply", json={"nats_host": "nats.example.com"})
        assert resp.status_code == 200
        assert "NATS_HOST" in resp.json()["changed"]

    def test_apply_nats_token_only_on_nonempty(self, client_with_processor):
        """Empty token submission must not overwrite an existing token."""
        from pydantic import SecretStr

        client, settings, _ = client_with_processor
        object.__setattr__(settings, "NATS_TOKEN", SecretStr("existing-token"))

        resp = client.post("/config/apply", json={"nats_token": ""})
        assert resp.status_code == 200
        assert "NATS_TOKEN" not in resp.json()["changed"]
        assert settings.NATS_TOKEN.get_secret_value() == "existing-token"

        resp = client.post("/config/apply", json={"nats_token": "new-token"})
        assert resp.status_code == 200
        assert "NATS_TOKEN" in resp.json()["changed"]
        assert settings.NATS_TOKEN.get_secret_value() == "new-token"

    def test_config_page_shows_nats_toggle(self, client):
        resp = client.get("/config")
        assert resp.status_code == 200
        assert "nats-toggle" in resp.text

    def test_apply_zms_fields(self, client_with_processor):
        client, settings, _ = client_with_processor
        resp = client.post(
            "/config/apply",
            json={
                "zms_zmc_http": "http://zmc.test",
                "zms_monitor_id": "mon-1",
            },
        )
        assert resp.status_code == 200
        changed = resp.json()["changed"]
        assert "ZMS_ZMC_HTTP" in changed
        assert "ZMS_MONITOR_ID" in changed


# -- Detections fragment (SDR capture-context filtering) --


async def _detections_app_with_rows(settings, db_path):
    """Build an app with a connected DB holding two distinct-config detections."""
    from datetime import datetime

    from rfobserver.storage.database import SensorDatabase

    app = create_app(settings)
    database = SensorDatabase(db_path)
    await database.connect()
    app.state.database = database

    common = dict(
        start_time=datetime(2026, 1, 1),
        stop_time=datetime(2026, 1, 1, 0, 0, 1),
        center_freq_hz=915e6,
        bandwidth_hz=1e6,
        peak_power_db=-30.0,
        duration_ms=10.0,
        detection_timestamp=datetime(2026, 1, 1),
        sample_rate_hz=56e6,
        gain_db=40.0,
    )
    await database.insert_detection(burst_id="at915", sdr_center_freq_hz=915e6, **common)
    await database.insert_detection(burst_id="at2437", sdr_center_freq_hz=2437e6, **common)
    return app, database


async def test_detections_fragment_has_capture_column(settings, tmp_path):
    import httpx

    app, database = await _detections_app_with_rows(settings, str(tmp_path / "d.db"))
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/detections")
        assert resp.status_code == 200
        # Compact capture-context label rendered for each detection.
        assert "915.0 MHz / 56 MHz / 40 dB" in resp.text
        assert "2437.0 MHz / 56 MHz / 40 dB" in resp.text
    finally:
        await database.close()


async def test_detections_fragment_filters_by_sdr_center(settings, tmp_path):
    import httpx

    app, database = await _detections_app_with_rows(settings, str(tmp_path / "d.db"))
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/detections?sdr_center=915000000")
        assert resp.status_code == 200
        assert "915.0 MHz" in resp.text
        # The 2437 MHz capture is filtered out.
        assert "2437.0 MHz" not in resp.text
    finally:
        await database.close()


async def test_detections_fragment_empty_filter_is_unfiltered(settings, tmp_path):
    # The 'All' option submits an empty string; it must not 422 or filter.
    import httpx

    app, database = await _detections_app_with_rows(settings, str(tmp_path / "d.db"))
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/detections?sdr_center=&sample_rate=&gain=")
        assert resp.status_code == 200
        assert "915.0 MHz" in resp.text
        assert "2437.0 MHz" in resp.text
    finally:
        await database.close()
