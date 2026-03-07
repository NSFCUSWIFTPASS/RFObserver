"""Tests for rfobserver.zms.client."""

from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from rfobserver.zms.client import ZmsClient


@pytest.fixture
def client():
    return ZmsClient(
        dst_http="http://dst.test",
        api_token="tok-123",
        monitor_id="mon-1",
        zmc_http="http://zmc.test",
    )


class TestZmsClient:
    @pytest.mark.asyncio
    async def test_initial_connection_state(self, client):
        assert client.connection_state == "unset"

    @pytest.mark.asyncio
    async def test_test_connection_both_ok(self, client):
        async def mock_get(url, **kw):
            resp = httpx.Response(200, request=httpx.Request("GET", url))
            return resp

        client._client = AsyncMock()
        client._client.get = mock_get
        zmc_ok, dst_ok = await client.test_connection()
        assert zmc_ok is True
        assert dst_ok is True
        assert client.connection_state == "up"

    @pytest.mark.asyncio
    async def test_test_connection_dst_down(self, client):
        call_count = 0

        async def mock_get(url, **kw):
            nonlocal call_count
            call_count += 1
            if "version" in url:
                raise httpx.ConnectError("refused")
            return httpx.Response(200, request=httpx.Request("GET", url))

        client._client = AsyncMock()
        client._client.get = mock_get
        zmc_ok, dst_ok = await client.test_connection()
        assert zmc_ok is True
        assert dst_ok is False
        assert client.connection_state == "down"

    @pytest.mark.asyncio
    async def test_send_sigmf_archive_success(self, client):
        async def mock_post(url, **kw):
            return httpx.Response(201, request=httpx.Request("POST", url))

        client._client = AsyncMock()
        client._client.post = mock_post
        ok = await client.send_sigmf_archive(b"\x00" * 100)
        assert ok is True
        assert client.connection_state == "up"

    @pytest.mark.asyncio
    async def test_send_sigmf_archive_failure(self, client):
        async def mock_post(url, **kw):
            return httpx.Response(400, text="bad request", request=httpx.Request("POST", url))

        client._client = AsyncMock()
        client._client.post = mock_post
        ok = await client.send_sigmf_archive(b"\x00")
        assert ok is False

    @pytest.mark.asyncio
    async def test_close(self, client):
        client._client = AsyncMock()
        await client.close()
        client._client.aclose.assert_called_once()
