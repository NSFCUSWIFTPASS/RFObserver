"""The fleet's sensor list: found on mDNS or added by address, kept in fleet.json.

A sensor's ``id`` is its hostname, from its ``/api/identity``; a sensor found
both ways is one entry. Each sensor keeps the colour slot it was given when
first added, so its colour never changes when others come or go.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# The categorical palette, dark-surface steps (the charts draw on a dark
# canvas in both themes). Fixed order; validated on #1a1a2e with the dataviz
# palette checker: worst adjacent colour-blind delta E 8.4, normal 19.3,
# all >= 3:1 contrast. A seventh sensor and later reuse the palette from the
# start, told apart by the legend and labels.
PALETTE = ("#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300")

DEFAULT_PORT = 80


@dataclass
class Sensor:
    id: str
    address: str
    port: int = DEFAULT_PORT
    label: str = ""
    slot: int = 0
    enabled: bool = True
    # How it got here: "mdns" or "manual". A manual entry is kept even when
    # mDNS also sees it, so removing it from mDNS does not drop it.
    source: str = "manual"
    # Other addresses mDNS advertised for it (tried if the main one fails).
    alt_addresses: list[str] = field(default_factory=list)

    @property
    def colour(self) -> str:
        return PALETTE[self.slot % len(PALETTE)]

    @property
    def base_url(self) -> str:
        host = f"[{self.address}]" if ":" in self.address else self.address
        return f"http://{host}" if self.port == 80 else f"http://{host}:{self.port}"

    def public(self) -> dict[str, Any]:
        d = asdict(self)
        d["colour"] = self.colour
        d["base_url"] = self.base_url
        d["label"] = self.label or self.id
        return d


def parse_address(text: str) -> tuple[str, int]:
    """``host``, ``host:port`` or ``http://host:port`` -> (host, port)."""
    t = text.strip()
    for prefix in ("http://", "https://"):
        if t.startswith(prefix):
            t = t[len(prefix) :]
    t = t.rstrip("/")
    if not t or "/" in t:
        raise ValueError(f"not an address: {text!r}")
    if t.startswith("["):  # [v6]:port
        host, _, rest = t[1:].partition("]")
        port = int(rest[1:]) if rest.startswith(":") else DEFAULT_PORT
    elif t.count(":") == 1:
        host, _, p = t.partition(":")
        port = int(p)
    else:
        host, port = t, DEFAULT_PORT
    if not host or not 0 < port < 65536:
        raise ValueError(f"not an address: {text!r}")
    return host, port


class Registry:
    """The sensor list, saved to ``path`` on every change."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._sensors: dict[str, Sensor] = {}
        self._load()

    def _load(self) -> None:
        try:
            raw = json.loads(self._path.read_text())
        except FileNotFoundError:
            return
        except (OSError, ValueError):
            logger.exception("Could not read %s; starting with no sensors", self._path)
            return
        for item in raw.get("sensors", []):
            try:
                s = Sensor(**item)
            except TypeError:
                logger.warning("Skipping a bad sensor entry in %s: %r", self._path, item)
                continue
            self._sensors[s.id] = s

    def _save_locked(self) -> None:
        data = {"sensors": [asdict(s) for s in self._sensors.values()]}
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self._path.parent, prefix=".fleet-", suffix=".json")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, self._path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def _next_slot_locked(self) -> int:
        used = {s.slot for s in self._sensors.values()}
        slot = 0
        while slot in used:
            slot += 1
        return slot

    def all(self) -> list[Sensor]:
        """Every sensor, in colour-slot order (the order they were added)."""
        with self._lock:
            return sorted(self._sensors.values(), key=lambda s: s.slot)

    def get(self, sensor_id: str) -> Sensor | None:
        with self._lock:
            return self._sensors.get(sensor_id)

    def add(self, sensor_id: str, address: str, port: int, *, source: str) -> Sensor:
        """Add a sensor, or update the address of the one with this id."""
        with self._lock:
            s = self._sensors.get(sensor_id)
            if s is None:
                s = Sensor(sensor_id, address, port, slot=self._next_slot_locked(), source=source)
                self._sensors[sensor_id] = s
                logger.info("Fleet: added %s at %s:%d (%s)", sensor_id, address, port, source)
            else:
                s.address, s.port = address, port
                if source == "manual":
                    s.source = "manual"
            self._save_locked()
            return s

    def use_address(self, sensor_id: str, address: str) -> Sensor | None:
        """Make one of a sensor's known addresses its main one, keeping the
        others (including the old main) as alternatives."""
        with self._lock:
            s = self._sensors.get(sensor_id)
            if s is None or s.address == address:
                return s
            known = [s.address, *s.alt_addresses]
            s.address = address
            s.alt_addresses = [a for a in known if a != address]
            self._save_locked()
            logger.info("Fleet: %s now at %s", sensor_id, address)
            return s

    def seen_on_mdns(self, sensor_id: str, addresses: list[str], port: int) -> Sensor:
        """Record an mDNS sighting; ``addresses`` best first (the caller ranks
        them). An mDNS sensor takes the best one; a manual sensor keeps the
        address it was added with."""
        with self._lock:
            s = self._sensors.get(sensor_id)
            if s is None:
                s = Sensor(
                    sensor_id,
                    addresses[0],
                    port,
                    slot=self._next_slot_locked(),
                    source="mdns",
                )
                self._sensors[sensor_id] = s
                logger.info("Fleet: found %s on mDNS at %s:%d", sensor_id, addresses[0], port)
            elif s.source == "mdns":
                s.address, s.port = addresses[0], port
            s.alt_addresses = [a for a in addresses if a != s.address]
            self._save_locked()
            return s

    def update(
        self, sensor_id: str, *, label: str | None = None, enabled: bool | None = None
    ) -> Sensor | None:
        with self._lock:
            s = self._sensors.get(sensor_id)
            if s is None:
                return None
            if label is not None:
                s.label = label.strip()
            if enabled is not None:
                s.enabled = enabled
            self._save_locked()
            return s

    def remove(self, sensor_id: str) -> bool:
        with self._lock:
            if self._sensors.pop(sensor_id, None) is None:
                return False
            self._save_locked()
            return True
