"""Reading the sensors: one HTTP client per sensor, with limits and a cache.

Field sensors are sensitive to load (docs/debugging/2026-10-01_hcro-overflows-
storage-scans.md), so the fleet never sends a sensor more than one waterfall
and two other requests at a time; more wait. Answers are cached briefly, so
several browsers, or one page refreshing, cost a sensor one query. A sensor
that does not answer is marked offline and holds up nobody else.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from rfobserver.fleet.registry import Registry, Sensor

logger = logging.getLogger(__name__)

IDENTITY_INTERVAL_SEC = 30.0
TIMEOUT_SEC = 5.0
WATERFALL_TIMEOUT_SEC = 30.0
# Cached answers: briefly for a range reaching the present, longer for a
# closed one (it cannot change).
CACHE_OPEN_SEC = 10.0
CACHE_CLOSED_SEC = 600.0
CACHE_MAX_ENTRIES = 256
CACHE_MAX_BYTES = 256 * 1024**2
# A range whose end is within this of now counts as reaching the present.
_OPEN_RANGE_SEC = 120.0

# What the fleet may read through a sensor's /api: the Dashboard's data, never
# a write (only GET is proxied at all). Captures are read through their own
# routes (fleet/captures.py).
PROXY_PATHS = (
    "averaged/",
    "averaged",
    "iq-captures",
    "detections.json",
    "health",
    "identity",
    "ui-prefs",
)


def proxy_allowed(path: str) -> bool:
    return any(path == p or (p.endswith("/") and path.startswith(p)) for p in PROXY_PATHS)


@dataclass
class Status:
    online: bool = False
    identity: dict[str, Any] | None = None
    last_seen: float | None = None  # wall time of the last answer
    error: str | None = None
    address_used: str | None = None


# Headers kept from a sensor's answer (the capture list's total).
_KEPT_HEADERS = ("x-total-count",)
# Capture file downloads at a time per sensor; more wait.
DOWNLOADS_PER_SENSOR = 2


@dataclass
class _Cached:
    expires: float
    status: int
    content_type: str
    body: bytes
    headers: dict[str, str]


@dataclass
class Reply:
    status: int
    content_type: str
    body: bytes
    cached: bool = False
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class _Limits:
    waterfall: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(1))
    other: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(2))
    downloads: asyncio.Semaphore = field(
        default_factory=lambda: asyncio.Semaphore(DOWNLOADS_PER_SENSOR)
    )


def _cache_ttl(params: dict[str, str]) -> float:
    until = params.get("until")
    if not until:
        return CACHE_OPEN_SEC
    try:
        end = datetime.fromisoformat(until.replace("Z", "+00:00"))
    except ValueError:
        return CACHE_OPEN_SEC
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc).timestamp() - end.timestamp()
    return CACHE_CLOSED_SEC if age > _OPEN_RANGE_SEC else CACHE_OPEN_SEC


class SensorHub:
    """Clients, limits, cache and status for every sensor in the registry."""

    def __init__(self, registry: Registry, transport: httpx.AsyncBaseTransport | None = None):
        self.registry = registry
        self._client = httpx.AsyncClient(transport=transport, follow_redirects=False)
        self._limits: dict[str, _Limits] = {}
        self._status: dict[str, Status] = {}
        self._cache: OrderedDict[tuple[str, str, str], _Cached] = OrderedDict()
        self._cache_bytes = 0
        self._poll_task: asyncio.Task[None] | None = None

    # --- status ---

    def status(self, sensor_id: str) -> Status:
        return self._status.setdefault(sensor_id, Status())

    def describe(self, sensor: Sensor) -> dict[str, Any]:
        st = self.status(sensor.id)
        ident = st.identity or {}
        return {
            **sensor.public(),
            # What the pages show, and what the user set (empty: none).
            "label": self.display_label(sensor),
            "custom_label": sensor.label,
            "online": st.online,
            "last_seen": st.last_seen,
            "error": st.error,
            "version": ident.get("version"),
            "sensor_name": ident.get("sensor_name"),
            "tuning": {k: ident.get(k) for k in ("center_freq_hz", "sample_rate_hz", "gain_db")},
            "active": ident.get("active"),
            "features": ident.get("features"),  # None from a sensor that does not report them
            "psd_backend": ident.get("psd_backend"),
        }

    def display_label(self, sensor: Sensor) -> str:
        ident = self.status(sensor.id).identity or {}
        return sensor.label or ident.get("sensor_name") or sensor.id

    # --- requests ---

    def _limits_for(self, sensor_id: str) -> _Limits:
        return self._limits.setdefault(sensor_id, _Limits())

    async def get(
        self,
        sensor: Sensor,
        path: str,
        params: dict[str, str] | None = None,
        *,
        cache: bool = True,
        root: str = "api",
        heavy: bool = False,
        ttl: float | None = None,
    ) -> Reply:
        """GET ``/<root>/<path>`` on a sensor, through its limits and the
        cache. ``heavy`` reads share the waterfall's limit and timeout;
        ``ttl`` overrides how long the answer is cached. Raises
        httpx.HTTPError when the sensor cannot be reached."""
        params = dict(params or {})
        key = (sensor.id, f"{root}/{path}", str(sorted(params.items())))
        now = time.monotonic()
        if cache:
            hit = self._cache.get(key)
            if hit is not None and hit.expires > now:
                self._cache.move_to_end(key)
                return Reply(hit.status, hit.content_type, hit.body, True, hit.headers)
        heavy = heavy or (root == "api" and path == "averaged/waterfall")
        limits = self._limits_for(sensor.id)
        sem = limits.waterfall if heavy else limits.other
        timeout = WATERFALL_TIMEOUT_SEC if heavy else TIMEOUT_SEC
        async with sem:
            # Another request may have filled the cache while this one waited.
            if cache:
                hit = self._cache.get(key)
                if hit is not None and hit.expires > time.monotonic():
                    return Reply(hit.status, hit.content_type, hit.body, True, hit.headers)
            try:
                r = await self._client.get(
                    f"{sensor.base_url}/{root}/{path}", params=params, timeout=timeout
                )
            except httpx.HTTPError as exc:
                self._mark_down(sensor, exc)
                raise
        self._mark_up(sensor)
        kept = {h: r.headers[h] for h in _KEPT_HEADERS if h in r.headers}
        reply = Reply(r.status_code, r.headers.get("content-type", ""), r.content, False, kept)
        if cache and r.status_code == 200:
            self._store(key, reply, _cache_ttl(params) if ttl is None else ttl)
        return reply

    async def send(self, sensor: Sensor, method: str, path: str, body: Any = None) -> Reply:
        """``method`` ``/<path>`` on a sensor with a JSON ``body``, uncached,
        through its limits. Raises httpx.HTTPError when it cannot be reached."""
        async with self._limits_for(sensor.id).other:
            try:
                r = await self._client.request(
                    method, f"{sensor.base_url}/{path}", json=body, timeout=TIMEOUT_SEC
                )
            except httpx.HTTPError as exc:
                self._mark_down(sensor, exc)
                raise
        self._mark_up(sensor)
        return Reply(r.status_code, r.headers.get("content-type", ""), r.content, False, {})

    def _mark_down(self, sensor: Sensor, exc: Exception) -> None:
        st = self.status(sensor.id)
        st.online, st.error = False, f"{type(exc).__name__}: {exc}"

    def _mark_up(self, sensor: Sensor) -> None:
        st = self.status(sensor.id)
        st.online, st.last_seen, st.error = True, time.time(), None

    @contextlib.asynccontextmanager
    async def download(
        self, sensor: Sensor, path: str, method: str, headers: dict[str, str]
    ) -> AsyncIterator[httpx.Response]:
        """A streamed ``method`` request for a file at ``/<path>`` on a
        sensor, holding one of its download slots until the caller is done.
        Only connecting has a time limit, so a long download is not cut off."""
        async with self._limits_for(sensor.id).downloads:
            req = self._client.build_request(
                method,
                f"{sensor.base_url}/{path}",
                headers=headers,
                timeout=httpx.Timeout(TIMEOUT_SEC, read=None),
            )
            try:
                r = await self._client.send(req, stream=True)
            except httpx.HTTPError as exc:
                self._mark_down(sensor, exc)
                raise
            self._mark_up(sensor)
            try:
                yield r
            finally:
                await r.aclose()

    def _store(self, key: tuple[str, str, str], reply: Reply, ttl: float) -> None:
        old = self._cache.pop(key, None)
        if old is not None:
            self._cache_bytes -= len(old.body)
        expires = time.monotonic() + ttl
        self._cache[key] = _Cached(
            expires, reply.status, reply.content_type, reply.body, reply.headers
        )
        self._cache_bytes += len(reply.body)
        while self._cache and (
            len(self._cache) > CACHE_MAX_ENTRIES or self._cache_bytes > CACHE_MAX_BYTES
        ):
            _, dropped = self._cache.popitem(last=False)
            self._cache_bytes -= len(dropped.body)

    async def get_json(
        self, sensor: Sensor, path: str, params: dict[str, str] | None = None
    ) -> Any:
        import json

        reply = await self.get(sensor, path, params)
        if reply.status != 200:
            raise httpx.HTTPStatusError(
                f"{path}: HTTP {reply.status}",
                request=httpx.Request("GET", f"{sensor.base_url}/api/{path}"),
                response=httpx.Response(reply.status),
            )
        return json.loads(reply.body)

    # --- identity ---

    async def fetch_identity(self, base_url: str) -> dict[str, Any]:
        """``/api/identity`` of whatever answers at ``base_url`` (adding a sensor)."""
        r = await self._client.get(f"{base_url}/api/identity", timeout=TIMEOUT_SEC)
        r.raise_for_status()
        ident: dict[str, Any] = r.json()
        if not isinstance(ident, dict) or "id" not in ident:
            raise ValueError("not an RFObserver sensor (no id in /api/identity)")
        return ident

    async def refresh(self, sensor: Sensor) -> None:
        """Poll one sensor's identity at its best address that answers.

        An mDNS sensor's addresses are tried in preference order (this host's
        subnet first, see mdns.rank_addresses), so after falling back to
        another address while the best one was down, it moves back once that
        one answers again. A manual sensor tries its own address first."""
        st = self.status(sensor.id)
        candidates = [sensor.address, *sensor.alt_addresses]
        if sensor.source == "mdns":
            from rfobserver.mdns import rank_addresses

            candidates = rank_addresses(candidates)
        last_error: str | None = None
        for addr in candidates:
            base = sensor.base_url.replace(sensor.address, addr, 1)
            try:
                ident = await self.fetch_identity(base)
            except (httpx.HTTPError, ValueError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                continue
            if addr != sensor.address:
                self.registry.use_address(sensor.id, addr)
            st.identity, st.online, st.last_seen, st.error = ident, True, time.time(), None
            st.address_used = addr
            return
        if st.online:
            logger.warning("Fleet: %s went offline (%s)", sensor.id, last_error)
        st.online, st.error = False, last_error

    async def refresh_all(self) -> None:
        sensors = [s for s in self.registry.all() if s.enabled]
        await asyncio.gather(*(self.refresh(s) for s in sensors))

    async def _poll_loop(self) -> None:
        while True:
            try:
                await self.refresh_all()
            except Exception:
                logger.exception("Fleet: identity poll failed")
            await asyncio.sleep(IDENTITY_INTERVAL_SEC)

    def start(self) -> None:
        if self._poll_task is None:
            self._poll_task = asyncio.create_task(self._poll_loop())

    async def close(self) -> None:
        if self._poll_task is not None:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
            self._poll_task = None
        await self._client.aclose()
