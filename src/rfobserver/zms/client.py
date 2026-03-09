"""HTTP client for OpenZMS DST/ZMC APIs."""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class ZmsClient:
    """Client for OpenZMS HTTP APIs (DST observations + ZMC monitor info)."""

    def __init__(
        self,
        dst_http: str,
        api_token: str,
        monitor_id: str,
        zmc_http: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._dst_http = dst_http.rstrip("/")
        self._zmc_http = (zmc_http or dst_http).rstrip("/")
        self._api_token = api_token
        self._monitor_id = monitor_id
        self._client = httpx.AsyncClient(timeout=timeout)
        self._connection_state: str = "unset"  # unset | up | down

    @property
    def connection_state(self) -> str:
        return self._connection_state

    def _headers(self) -> dict[str, str]:
        return {
            "X-Api-Token": self._api_token,
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Connection test
    # ------------------------------------------------------------------

    async def test_connection(self) -> tuple[bool, bool]:
        """Test connectivity to ZMC and DST endpoints.

        Returns:
            (zmc_ok, dst_ok) booleans.
        """
        zmc_ok = False
        dst_ok = False

        try:
            r = await self._client.get(
                f"{self._zmc_http}/monitors/{self._monitor_id}",
                headers=self._headers(),
                timeout=15.0,
            )
            zmc_ok = r.status_code == 200
        except httpx.HTTPError:
            pass

        try:
            await self._client.get(f"{self._dst_http}/version", timeout=15.0)
            dst_ok = True  # any HTTP response means service is up
        except httpx.HTTPError:
            pass

        both_ok = zmc_ok and dst_ok
        prev = self._connection_state
        self._connection_state = "up" if both_ok else "down"

        if prev != self._connection_state:
            logger.info(
                "ZMS connection %s -> %s (zmc=%s, dst=%s)",
                prev,
                self._connection_state,
                zmc_ok,
                dst_ok,
            )

        return zmc_ok, dst_ok

    # ------------------------------------------------------------------
    # Observation submission (legacy CSV-inline format)
    # ------------------------------------------------------------------

    async def send_observation_csv(
        self,
        encoded_data: str,
        starts_at: str,
        ends_at: str,
        min_freq_hz: int,
        max_freq_hz: int,
        interference: bool = False,
    ) -> dict[str, Any] | None:
        """Submit a CSV-inline observation to DST."""
        payload = {
            "monitor_id": self._monitor_id,
            "types": "inline,sweep",
            "format": "rfs-csv-inline",
            "kind": "psd",
            "starts_at": starts_at,
            "ends_at": ends_at,
            "description": "rfs observation",
            "min_freq": min_freq_hz,
            "max_freq": max_freq_hz,
            "data": encoded_data,
            "interference": interference,
        }
        try:
            r = await self._client.post(
                f"{self._dst_http}/observations",
                json=payload,
                headers=self._headers(),
            )
            result: dict[str, Any] = r.json()
            return result
        except httpx.HTTPError as exc:
            logger.error("DST observation POST failed: %s", exc)
            return None

    # ------------------------------------------------------------------
    # SigMF archive submission
    # ------------------------------------------------------------------

    async def send_sigmf_archive(self, archive_bytes: bytes, *, gzip: bool = False) -> bool:
        """Upload a SigMF tar(.gz) archive to DST.

        Returns True on success (HTTP 200/201).
        """
        headers: dict[str, str] = {
            "Content-Type": "application/sigmf-archive",
            "X-Api-Token": self._api_token,
            "X-Api-Monitor-Id": self._monitor_id,
        }
        if gzip:
            headers["Content-Encoding"] = "gzip"
        try:
            r = await self._client.post(
                f"{self._dst_http}/observations",
                content=archive_bytes,
                headers=headers,
            )
            ok = r.status_code in (200, 201)
            if ok:
                if self._connection_state != "up":
                    self._connection_state = "up"
                    logger.info("ZMS DST connection restored")
            else:
                logger.warning("DST rejected SigMF: %d %s", r.status_code, r.text[:200])
            return ok
        except httpx.HTTPError as exc:
            logger.error("DST SigMF POST failed: %s", exc)
            if self._connection_state != "down":
                self._connection_state = "down"
                logger.warning("ZMS DST connection lost")
            return False

    # ------------------------------------------------------------------
    # Monitor info
    # ------------------------------------------------------------------

    async def get_monitor_info(self) -> dict[str, Any] | None:
        """Fetch monitor details from ZMC."""
        try:
            r = await self._client.get(
                f"{self._zmc_http}/monitors/{self._monitor_id}",
                headers=self._headers(),
            )
            if r.status_code == 200:
                result: dict[str, Any] = r.json()
                return result
            return None
        except httpx.HTTPError as exc:
            logger.error("ZMC monitor GET failed: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        await self._client.aclose()
