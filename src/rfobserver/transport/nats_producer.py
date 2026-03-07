"""NATS JetStream publisher for outbound sensor data.

Publishes to three streams:
- rfobs.champions -- champion observations (loudest/quietest/rfi)
- rfobs.bursts    -- burst fingerprints
- rfobs.stats     -- periodic PSD/kurtosis summaries
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import nats

if TYPE_CHECKING:
    from nats.aio.client import Client as NatsClient
    from nats.js import JetStreamContext

logger = logging.getLogger(__name__)

STREAM_CHAMPIONS = "rfobs.champions"
STREAM_BURSTS = "rfobs.bursts"
STREAM_STATS = "rfobs.stats"


class NatsProducer:
    """NATS JetStream publisher."""

    def __init__(self, url: str, token: str | None = None) -> None:
        self._url = url
        self._token = token
        self._nc: NatsClient | None = None
        self._js: JetStreamContext | None = None

    async def connect(self) -> None:
        opts: dict[str, Any] = {"servers": [self._url]}
        if self._token:
            opts["token"] = self._token

        self._nc = await nats.connect(**opts)
        self._js = self._nc.jetstream()

        # Ensure streams exist
        for stream_name, subjects in [
            ("RFOBS_CHAMPIONS", [f"{STREAM_CHAMPIONS}.>"]),
            ("RFOBS_BURSTS", [f"{STREAM_BURSTS}.>"]),
            ("RFOBS_STATS", [f"{STREAM_STATS}.>"]),
        ]:
            try:
                await self._js.find_stream_name_by_subject(subjects[0])
            except nats.js.errors.NotFoundError:
                await self._js.add_stream(name=stream_name, subjects=subjects)
                logger.info("Created JetStream stream: %s", stream_name)

        logger.info("Connected to NATS at %s", self._url)

    async def publish(self, subject: str, data: bytes) -> None:
        if self._js is None:
            raise RuntimeError("Not connected to NATS")
        ack = await self._js.publish(subject, data)
        logger.debug("Published to %s (seq=%d)", subject, ack.seq)

    async def publish_json(self, subject: str, payload: dict[str, Any]) -> None:
        data = json.dumps(payload).encode()
        await self.publish(subject, data)

    async def close(self) -> None:
        if self._nc is not None:
            await self._nc.close()
            logger.info("NATS connection closed")
