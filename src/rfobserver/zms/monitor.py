"""OpenZMS monitor integration.

Manages the full ZMS lifecycle:
- Heartbeat loop with status_ack_by deadline tracking
- WebSocket event subscription for reconfiguration commands
- Observation submission (SigMF archives to DST)
- Reconfiguration callback to pause/reconfigure/resume pipeline

Refactored from reference_software/rf-survey/src/rf_survey/monitor.py
and reference_software/zms-monitor/.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from rfobserver.zms.client import ZmsClient
from rfobserver.zms.sigmf_formatter import create_sigmf_archive

if TYPE_CHECKING:
    from rfobserver.config import ZmsSettingsGroup
    from rfobserver.models import ProcessedDataEnvelope

logger = logging.getLogger(__name__)

# ZMC event codes
EVENT_SOURCETYPE_ZMC = 2
EVENT_CODE_MONITOR_PENDING = 2010

ReconfigurationCallback = Callable[[str, dict[str, Any] | None], Awaitable[None]]
"""Async callback: (target_status, parameters_dict) -> None.

target_status is one of "active", "paused", "degraded", "down".
parameters_dict is None if only the status changed.
"""


class ZmsMonitor:
    """Full OpenZMS monitor with heartbeat, event subscription, and observation submission."""

    def __init__(
        self,
        settings: ZmsSettingsGroup,
        reconfiguration_callback: ReconfigurationCallback | None = None,
        heartbeat_interval: float = 60.0,
    ) -> None:
        self._settings = settings
        self._reconfiguration_callback = reconfiguration_callback
        self._heartbeat_interval = heartbeat_interval
        self._client = ZmsClient(
            dst_http=settings.dst_or_zmc,
            api_token=settings.token,
            monitor_id=settings.monitor_id,
            zmc_http=settings.zmc_http,
        )
        self._running = False
        self._message_count = 0
        self._status_ack_by: datetime | None = None
        self._op_status: str = "active"
        self._current_parameters: dict[str, Any] | None = None
        self._command_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    @property
    def message_count(self) -> int:
        return self._message_count

    @property
    def connection_state(self) -> str:
        return self._client.connection_state

    @property
    def op_status(self) -> str:
        return self._op_status

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the monitor (heartbeat + event listener)."""
        self._running = True
        logger.info("ZMS monitor started (id=%s)", self._settings.monitor_id)

    async def run(self) -> None:
        """Run the full monitor loop (heartbeat + command processing).

        Call this as an asyncio task. It runs until stop() is called.
        """
        self._running = True
        logger.info("ZMS monitor running (id=%s)", self._settings.monitor_id)

        try:
            await asyncio.gather(
                self._heartbeat_loop(),
                self._command_processor_loop(),
            )
        except asyncio.CancelledError:
            logger.info("ZMS monitor tasks cancelled")
        finally:
            await self._client.close()
            logger.info("ZMS monitor stopped")

    async def stop(self) -> None:
        """Signal the monitor to stop."""
        self._running = False
        await self._client.close()
        logger.info("ZMS monitor stopped")

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats, respecting status_ack_by deadlines."""
        while self._running:
            try:
                wait_time = self._heartbeat_interval
                if self._status_ack_by:
                    now = datetime.now(timezone.utc)
                    remaining = (self._status_ack_by - now).total_seconds()
                    wait_time = max(0.0, remaining)

                await asyncio.sleep(wait_time)

                if not self._running:
                    break

                await self._send_heartbeat()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("ZMS heartbeat error")
                await asyncio.sleep(10)

    async def _send_heartbeat(self) -> None:
        """Send a heartbeat PUT to ZMC and update status_ack_by."""
        zmc_ok, dst_ok = await self._client.test_connection()
        if zmc_ok:
            info = await self._client.get_monitor_info()
            if info and "status_ack_by" in info:
                try:
                    self._status_ack_by = datetime.fromisoformat(info["status_ack_by"])
                except (ValueError, TypeError):
                    self._status_ack_by = datetime.now(timezone.utc) + timedelta(
                        seconds=self._heartbeat_interval
                    )
            logger.debug(
                "Heartbeat sent (op_status=%s, next_ack=%s)",
                self._op_status,
                self._status_ack_by,
            )

    # ------------------------------------------------------------------
    # Reconfiguration command processing
    # ------------------------------------------------------------------

    async def _command_processor_loop(self) -> None:
        """Process reconfiguration commands from the event subscription."""
        while self._running:
            try:
                command = await asyncio.wait_for(
                    self._command_queue.get(),
                    timeout=5.0,
                )
                await self._process_reconfiguration(command)
            except (TimeoutError, asyncio.TimeoutError):  # noqa: UP041
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error processing ZMS command")

    async def _process_reconfiguration(self, command: dict[str, Any]) -> None:
        """Apply a reconfiguration command received from ZMS."""
        target_status = command.get("status", "active")
        parameters = command.get("parameters")
        pending_id = command.get("pending_id")

        logger.info(
            "Processing ZMS reconfiguration (pending_id=%s, status=%s)",
            pending_id,
            target_status,
        )

        if target_status == "paused":
            self._op_status = "paused"
        else:
            self._op_status = "active"

        if self._reconfiguration_callback:
            try:
                await self._reconfiguration_callback(target_status, parameters)
                if parameters:
                    self._current_parameters = parameters
                logger.info("Reconfiguration applied successfully")
            except Exception:
                logger.exception("Reconfiguration callback failed")

        await self._send_heartbeat()

    def enqueue_reconfiguration(self, command: dict[str, Any]) -> None:
        """Enqueue a reconfiguration command (e.g. from WebSocket event parsing).

        Expected keys: status (str), parameters (dict|None), pending_id (str|None).
        """
        self._command_queue.put_nowait(command)

    # ------------------------------------------------------------------
    # WebSocket event subscription
    # ------------------------------------------------------------------

    async def subscribe_events(self, ws_url: str) -> None:
        """Connect to ZMC WebSocket and listen for monitor pending events.

        This is a long-running coroutine. Add it to the TaskGroup if
        WebSocket reconfiguration is desired. Requires the `websockets`
        package.

        Args:
            ws_url: WebSocket URL for ZMC event stream.
        """
        try:
            import websockets
        except ImportError:
            logger.warning("websockets package not installed; ZMS event subscription disabled")
            return

        headers = {"X-Api-Token": self._settings.token}
        monitor_id = self._settings.monitor_id

        while self._running:
            try:
                async with websockets.connect(ws_url, additional_headers=headers) as ws:
                    logger.info("ZMS WebSocket connected for events")
                    async for message in ws:
                        try:
                            data = json.loads(message)
                            self._handle_ws_event(data, monitor_id)
                        except json.JSONDecodeError:
                            logger.warning("Invalid JSON from ZMS WebSocket")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("ZMS WebSocket error, reconnecting in 10s")
                await asyncio.sleep(10)

    def _handle_ws_event(self, data: dict[str, Any], monitor_id: str) -> None:
        """Parse a ZMC event and enqueue reconfiguration if applicable."""
        header = data.get("header", {})
        if header.get("source_type") != EVENT_SOURCETYPE_ZMC:
            return
        if header.get("code") != EVENT_CODE_MONITOR_PENDING:
            return

        obj = data.get("object_", data.get("object", {}))
        event_monitor_id = obj.get("monitor_id")
        if event_monitor_id != monitor_id:
            return

        logger.info("Received MonitorPending event (id=%s)", obj.get("id"))
        self.enqueue_reconfiguration(
            {
                "status": obj.get("status", "active"),
                "parameters": obj.get("parameters"),
                "pending_id": obj.get("id"),
            }
        )

    # ------------------------------------------------------------------
    # Observation submission
    # ------------------------------------------------------------------

    async def submit_observation(
        self,
        envelope: ProcessedDataEnvelope,
        *,
        kurtosis_per_bin: list[float] | None = None,
        violations: list[bool] | None = None,
        interference: bool = False,
        zones: list[dict[str, Any]] | None = None,
    ) -> bool:
        """Format and submit a processed capture as a SigMF observation.

        Args:
            envelope: Processed data from the pipeline.
            kurtosis_per_bin: Per-frequency-bin kurtosis (falls back to
                replicating the time-domain kurtosis across all bins).
            violations: Per-bin violation flags for zone reporting.
            interference: Overall interference flag.
            zones: Optional frequency zone definitions.

        Returns:
            True if the DST accepted the observation.
        """
        meta = envelope.metadata
        stats = envelope.statistics
        psd = envelope.psd_data

        if kurtosis_per_bin is None:
            kurtosis_per_bin = [stats.kurtosis] * psd.num_bins

        logger.debug("ZMS submit start (ts=%s)", meta.timestamp.isoformat())
        t0 = time.perf_counter()
        archive = await asyncio.to_thread(
            create_sigmf_archive,
            psd_powers=psd.powers,
            psd_frequencies=psd.frequencies,
            kurtosis_f=kurtosis_per_bin,
            center_freq=float(meta.frequency),
            sample_rate=meta.sampling_rate,
            gain=meta.gain,
            timestamp=meta.timestamp,
            serial=meta.serial,
            hostname=meta.hostname,
            monitor_id=self._settings.monitor_id,
            monitor_name=self._settings.monitor_name,
            metric_id=self._settings.metric_id,
            time_kurtosis=stats.kurtosis,
            pwr_avg=stats.average,
            pwr_max=stats.max,
            pwr_median=stats.median,
            interference=interference,
            violations=violations,
            zones=zones,
            gzip=False,
        )
        logger.debug(
            "ZMS archive built %d bytes in %.1fms",
            len(archive),
            (time.perf_counter() - t0) * 1000,
        )

        t1 = time.perf_counter()
        ok = await self._client.send_sigmf_archive(archive, gzip=False)
        logger.debug(
            "ZMS POST returned ok=%s in %.1fms",
            ok,
            (time.perf_counter() - t1) * 1000,
        )
        if ok:
            self._message_count += 1
        return ok
