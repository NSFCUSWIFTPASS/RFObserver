"""mDNS (zeroconf) for the fleet: sensors advertise, the fleet server browses.

A sensor advertises ``<hostname>._rfobserver._tcp.local.`` on its web port,
with every non-loopback IPv4 address of the host and TXT records ``id``
(the hostname, the fleet's key for a sensor) and ``version``. mDNS stays on
one subnet; sensors elsewhere are added to the fleet by address.

zeroconf is imported lazily and every failure is logged, never raised: the
sensor must run the same whether or not discovery works.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

SERVICE_TYPE = "_rfobserver._tcp.local."


@dataclass(frozen=True)
class Found:
    """A sensor seen on mDNS."""

    id: str
    addresses: tuple[str, ...]
    port: int
    version: str | None


# Interfaces whose addresses other hosts cannot reach: container and VM
# bridges, and the Jetson's USB gadget bridge (l4tbr0, 192.168.55.1).
_VIRTUAL_PREFIXES = ("docker", "br-", "veth", "virbr", "cni", "flannel", "l4tbr", "usb", "rndis")


def _primary_ipv4() -> str | None:
    """The source address of the default route (no packet is sent)."""
    import socket

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("192.0.2.1", 9))  # TEST-NET-1, never routed to
            addr: str = sock.getsockname()[0]
    except OSError:
        return None
    return None if addr.startswith("0.") else addr


def local_ipv4_addresses(bind_host: str = "0.0.0.0") -> list[str]:
    """The addresses to advertise: ``bind_host`` if the web server binds one,
    else the host's IPv4 addresses, the default route's first, leaving out
    loopback, link-local and virtual interfaces. The fleet uses the first and
    falls back to the others."""
    if bind_host not in ("", "0.0.0.0", "::"):
        return [bind_host]
    import ifaddr

    out: list[str] = []
    for adapter in ifaddr.get_adapters():
        name = str(adapter.nice_name or adapter.name)
        if name.startswith(_VIRTUAL_PREFIXES):
            continue
        for ip in adapter.ips:
            if not isinstance(ip.ip, str):
                continue  # IPv6 entries are tuples
            addr = ipaddress.IPv4Address(ip.ip)
            if not (addr.is_loopback or addr.is_link_local):
                out.append(str(addr))
    primary = _primary_ipv4()
    if primary in out:
        out.remove(primary)
        out.insert(0, primary)
    return out


def local_ipv4_networks() -> list[ipaddress.IPv4Network]:
    """The subnets of this host's real (non-virtual) IPv4 interfaces, the
    default route's first."""
    import ifaddr

    nets: list[ipaddress.IPv4Network] = []
    for adapter in ifaddr.get_adapters():
        if str(adapter.nice_name or adapter.name).startswith(_VIRTUAL_PREFIXES):
            continue
        for ip in adapter.ips:
            if isinstance(ip.ip, str) and not ip.ip.startswith("127."):
                nets.append(ipaddress.IPv4Network(f"{ip.ip}/{ip.network_prefix}", strict=False))
    primary = _primary_ipv4()
    if primary is not None:
        nets.sort(key=lambda n: ipaddress.IPv4Address(primary) not in n)
    return nets


def rank_addresses(
    addresses: list[str], networks: list[ipaddress.IPv4Network] | None = None
) -> list[str]:
    """``addresses`` ordered by the first of ``networks`` (default: this
    host's subnets, the default route's first) each is on; addresses on none
    go last. Ties keep the order given. mDNS does not keep the advertised
    order, so the fleet ranks what it receives."""
    if networks is None:
        try:
            networks = local_ipv4_networks()
        except Exception:
            networks = []

    def rank(a: str) -> int:
        try:
            ip = ipaddress.ip_address(a)
        except ValueError:
            return len(networks)
        for i, n in enumerate(networks):
            if ip.version == n.version and ip in n:
                return i
        return len(networks)

    return sorted(addresses, key=rank)


class Advertiser:
    """Registers this sensor's service until stopped."""

    def __init__(self, *, hostname: str, port: int, version: str, bind_host: str) -> None:
        self._hostname = hostname
        self._port = port
        self._version = version
        self._bind_host = bind_host
        self._zc: Any = None
        self._info: Any = None

    async def start(self) -> bool:
        try:
            from zeroconf import ServiceInfo
            from zeroconf.asyncio import AsyncZeroconf

            addresses = local_ipv4_addresses(self._bind_host)
            if not addresses:
                logger.warning("mDNS: no IPv4 address to advertise; not advertising")
                return False
            self._info = ServiceInfo(
                SERVICE_TYPE,
                f"{self._hostname}.{SERVICE_TYPE}",
                port=self._port,
                properties={"id": self._hostname, "version": self._version},
                server=f"{self._hostname}.local.",
                parsed_addresses=addresses,
            )
            self._zc = AsyncZeroconf()
            await self._zc.async_register_service(self._info, allow_name_change=True)
        except Exception:
            logger.warning(
                "mDNS advertisement failed; the fleet can still add this sensor by address",
                exc_info=True,
            )
            await self.stop()
            return False
        logger.info(
            "mDNS: advertising %s on %s port %d", self._hostname, ", ".join(addresses), self._port
        )
        return True

    async def run(self) -> None:
        """Advertise until cancelled (a pipeline worker)."""
        try:
            if await self.start():
                await asyncio.Event().wait()
        finally:
            await self.stop()

    async def stop(self) -> None:
        zc, self._zc = self._zc, None
        if zc is None:
            return
        try:
            if self._info is not None:
                await zc.async_unregister_service(self._info)
            await zc.async_close()
        except Exception:
            logger.debug("mDNS: closing the advertiser failed", exc_info=True)


class Browser:
    """Watches for sensors and reports each one found or updated.

    ``on_found`` runs on the event loop for every resolved service; ``on_lost``
    with the service's id when it is removed.
    """

    def __init__(
        self,
        on_found: Callable[[Found], None],
        on_lost: Callable[[str], None] | None = None,
    ) -> None:
        self._on_found = on_found
        self._on_lost = on_lost
        self._zc: Any = None
        self._browser: Any = None
        self._tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> bool:
        try:
            from zeroconf.asyncio import AsyncServiceBrowser, AsyncZeroconf

            self._zc = AsyncZeroconf()
            self._browser = AsyncServiceBrowser(
                self._zc.zeroconf, SERVICE_TYPE, handlers=[self._on_change]
            )
        except Exception:
            logger.warning("mDNS browsing failed; add sensors by address", exc_info=True)
            await self.stop()
            return False
        logger.info("mDNS: browsing for %s", SERVICE_TYPE)
        return True

    def _on_change(self, zeroconf: Any, service_type: str, name: str, state_change: Any) -> None:
        from zeroconf import ServiceStateChange

        if state_change is ServiceStateChange.Removed:
            if self._on_lost is not None:
                self._on_lost(name.split(".")[0])
            return
        task = asyncio.ensure_future(self._resolve(service_type, name))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _resolve(self, service_type: str, name: str) -> None:
        from zeroconf.asyncio import AsyncServiceInfo

        info = AsyncServiceInfo(service_type, name)
        try:
            if not await info.async_request(self._zc.zeroconf, 3000):
                return
        except Exception:
            logger.debug("mDNS: resolving %s failed", name, exc_info=True)
            return
        props = {
            (k.decode() if isinstance(k, bytes) else str(k)): (
                v.decode() if isinstance(v, bytes) else v
            )
            for k, v in (info.properties or {}).items()
        }
        sensor_id = props.get("id") or name.split(".")[0]
        found = Found(
            id=str(sensor_id),
            addresses=tuple(info.parsed_addresses()),
            port=int(info.port or 0),
            version=props.get("version"),
        )
        if found.addresses and found.port:
            self._on_found(found)

    async def stop(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        browser, self._browser = self._browser, None
        zc, self._zc = self._zc, None
        try:
            if browser is not None:
                await browser.async_cancel()
            if zc is not None:
                await zc.async_close()
        except Exception:
            logger.debug("mDNS: closing the browser failed", exc_info=True)
