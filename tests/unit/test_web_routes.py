"""Tests for rfobserver.web routes."""

import pytest
from fastapi.testclient import TestClient

from rfobserver.config import AppSettings
from rfobserver.web.app import create_app


@pytest.fixture
def client():
    settings = AppSettings(_env_file=None)
    app = create_app(settings)
    return TestClient(app)


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
