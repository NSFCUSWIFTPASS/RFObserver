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
