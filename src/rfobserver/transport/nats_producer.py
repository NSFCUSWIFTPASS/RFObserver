"""NATS JetStream publisher for outbound sensor data.

Three streams, one per category. Only `rfobs.stats` is wired today.

- ``rfobs.stats.<hostname>``  -- ProcessedDataEnvelope per capture (DB ingest path)
- ``rfobs.champions.<hostname>``  -- TODO: champion notifications + file refs
- ``rfobs.bursts.<hostname>``     -- TODO: BurstFingerprint events
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import nats

from rfobserver.models import StatsEnvelope

if TYPE_CHECKING:
    from nats.aio.client import Client as NatsClient
    from nats.js import JetStreamContext

    from rfobserver.models import ProcessedDataEnvelope

logger = logging.getLogger(__name__)

STREAM_CHAMPIONS = "rfobs.champions"
STREAM_BURSTS = "rfobs.bursts"
STREAM_STATS = "rfobs.stats"


class NatsProducer:
    """NATS JetStream publisher.

    The connection lifecycle is managed by the caller via ``connect()`` /
    ``close()``. ``connected`` reflects the current state; if a publish is
    attempted while disconnected it is dropped and counted as ``dropped``
    rather than raised, so transient broker outages don't take down the
    pipeline.
    """

    def __init__(self, url: str, token: str | None = None) -> None:
        self._url = url
        self._token = token
        self._nc: NatsClient | None = None
        self._js: JetStreamContext | None = None
        self._connected: bool = False
        self._stats_count: int = 0
        self._dropped: int = 0

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def stats_count(self) -> int:
        return self._stats_count

    @property
    def dropped(self) -> int:
        return self._dropped

    @property
    def url(self) -> str:
        return self._url

    async def connect(self) -> None:
        opts: dict[str, Any] = {
            "servers": [self._url],
            # Reconnect forever: a server-side broker outage must not
            # permanently disable publishing (nats.py's default of 60 attempts
            # gives up after ~2 min and never recovers even if the broker
            # returns). The callbacks keep ``connected`` accurate so publishes
            # drop immediately during an outage instead of hanging.
            "allow_reconnect": True,
            "max_reconnect_attempts": -1,
            "reconnect_time_wait": 2,
            "disconnected_cb": self._on_disconnected,
            "reconnected_cb": self._on_reconnected,
            "error_cb": self._on_error,
            "closed_cb": self._on_closed,
        }
        if self._token:
            opts["token"] = self._token

        self._nc = await nats.connect(**opts)
        self._js = self._nc.jetstream()

        # Ensure all three streams exist so consumers can subscribe even
        # before we publish to a category.
        for stream_name, subjects in [
            ("RFOBS_STATS", [f"{STREAM_STATS}.>"]),
            ("RFOBS_CHAMPIONS", [f"{STREAM_CHAMPIONS}.>"]),
            ("RFOBS_BURSTS", [f"{STREAM_BURSTS}.>"]),
        ]:
            try:
                await self._js.find_stream_name_by_subject(subjects[0])
            except nats.js.errors.NotFoundError:
                await self._js.add_stream(name=stream_name, subjects=subjects)
                logger.info("Created JetStream stream: %s", stream_name)

        self._connected = True
        logger.info("Connected to NATS at %s", self._url)

    async def close(self) -> None:
        self._connected = False
        if self._nc is not None:
            await self._nc.close()
            self._nc = None
            self._js = None
            logger.info("NATS connection closed")

    # -- connection lifecycle callbacks --------------------------------------

    async def _on_disconnected(self) -> None:
        self._connected = False
        logger.warning("NATS disconnected from %s; publishes drop until reconnect", self._url)

    async def _on_reconnected(self) -> None:
        self._connected = True
        logger.info("NATS reconnected to %s", self._url)

    async def _on_error(self, err: Exception) -> None:
        logger.warning("NATS error: %s", err)

    async def _on_closed(self) -> None:
        self._connected = False

    # -- typed publishers ----------------------------------------------------

    async def publish_stats(self, envelope: ProcessedDataEnvelope, hostname: str) -> bool:
        """Publish the stats-only projection on ``rfobs.stats.<hostname>``.

        Only IQ statistics + metadata are sent (``StatsEnvelope``), not the PSD
        powers array -- RFS doesn't need the PSD, and it would dominate the
        payload. Returns True on success, False on failure (counted as dropped).
        """
        subject = f"{STREAM_STATS}.{hostname}"
        payload = StatsEnvelope.from_envelope(envelope).model_dump_json().encode()
        return await self._publish(subject, payload)

    async def publish_champion(
        self,
        envelope: ProcessedDataEnvelope,  # noqa: ARG002
        hostname: str,  # noqa: ARG002
        categories: list[str],  # noqa: ARG002
    ) -> bool:
        """TODO: publish champion-of-category notification on ``rfobs.champions.<hostname>``.

        Will carry the won categories (loudest/quietest/rfi) and a reference
        to the IQ file that won, so a downstream consumer can pull the file
        from the sensor and archive it.
        """
        raise NotImplementedError("rfobs.champions stream not implemented yet")

    async def publish_burst(self, burst: Any, hostname: str) -> bool:  # noqa: ARG002
        """TODO: publish a BurstFingerprint on ``rfobs.bursts.<hostname>``.

        Will be emitted from the burst-detection thread once we wire up a
        consumer that wants per-event burst telemetry.
        """
        raise NotImplementedError("rfobs.bursts stream not implemented yet")

    # -- internals -----------------------------------------------------------

    async def _publish(self, subject: str, payload: bytes) -> bool:
        if not self._connected or self._js is None:
            self._dropped += 1
            logger.debug("NATS publish dropped (not connected): %s", subject)
            return False
        try:
            # Bounded so a publish issued just as the broker drops can't hang
            # the fire-and-forget task indefinitely.
            ack = await self._js.publish(subject, payload, timeout=5.0)
            if subject.startswith(f"{STREAM_STATS}."):
                self._stats_count += 1
            logger.debug("Published to %s (seq=%d)", subject, ack.seq)
            return True
        except Exception:
            self._dropped += 1
            logger.exception("NATS publish failed: %s", subject)
            return False
