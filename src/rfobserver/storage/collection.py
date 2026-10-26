"""IQ collection statistics: raw IQ recorded, IQ deleted automatically and
live RF time, counted per UTC hour (``collection_hours``).

The recording-finalize path, the storage evictions and the receiver loop add
to one ``CollectionStats`` from their own threads; a pipeline loop flushes it
into the table, adding to each hour's row. Recorded figures never decrease:
deletions are counted separately, in the hour they happen. There is no
backfill: counting starts at the hour of the first flush
(``COLLECTION_SINCE_KEY``).
"""

from __future__ import annotations

import functools
import logging
import math
import threading
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone, tzinfo
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

if TYPE_CHECKING:
    from rfobserver.config import AppSettings
    from rfobserver.storage.database import SensorDatabase

logger = logging.getLogger(__name__)

HOUR = 3600

FIELDS = (
    "auto_captures",
    "manual_captures",
    "auto_bytes",
    "manual_bytes",
    "auto_seconds",
    "manual_seconds",
    "deleted_cap_captures",
    "deleted_cap_bytes",
    "deleted_space_captures",
    "deleted_space_bytes",
    "active_seconds",
)

_ORIGINS = ("auto", "manual")
_REASONS = ("cap", "space")


def _hour(ts: float) -> int:
    return int(math.floor(ts / HOUR)) * HOUR


class CollectionStats:
    """Pending per-hour additions, safe to add to from any thread."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending: defaultdict[int, defaultdict[str, float]] = defaultdict(
            lambda: defaultdict(float)
        )

    def add_recorded(self, origin: str, start_ts: float, stop_ts: float, nbytes: int) -> None:
        """A finished capture of ``nbytes`` raw IQ spanning [start, stop].
        Bytes and seconds are split over the hours in proportion to time, the
        count goes to the hour it started in."""
        if origin not in _ORIGINS:
            return
        stop_ts = max(stop_ts, start_ts)
        span = stop_ts - start_ts
        parts: list[tuple[int, float]] = []  # (hour, seconds in it)
        t = start_ts
        while True:
            h = _hour(t)
            end = min(stop_ts, h + HOUR)
            parts.append((h, end - t))
            if end >= stop_ts:
                break
            t = end
        with self._lock:
            self._pending[_hour(start_ts)][f"{origin}_captures"] += 1
            given = 0
            for i, (h, secs) in enumerate(parts):
                if i == len(parts) - 1:
                    b = nbytes - given  # the rest, so the parts sum exactly
                else:
                    b = round(nbytes * secs / span) if span > 0 else 0
                given += b
                row = self._pending[h]
                if b:
                    row[f"{origin}_bytes"] += b
                if secs:
                    row[f"{origin}_seconds"] += secs

    def add_deleted(self, reason: str, nbytes: int, at_ts: float) -> None:
        """A capture of ``nbytes`` raw IQ deleted automatically: ``reason`` is
        "cap" (the storage cap) or "space" (the low-free-space guard)."""
        if reason not in _REASONS:
            return
        with self._lock:
            row = self._pending[_hour(at_ts)]
            row[f"deleted_{reason}_captures"] += 1
            row[f"deleted_{reason}_bytes"] += nbytes

    def add_active(self, seconds: float, at_ts: float) -> None:
        """Seconds of live RF received."""
        with self._lock:
            self._pending[_hour(at_ts)]["active_seconds"] += seconds

    def take(self) -> dict[int, dict[str, float]]:
        """The pending additions by hour, cleared."""
        with self._lock:
            out = {h: dict(v) for h, v in self._pending.items() if v}
            self._pending.clear()
        return out

    def _put_back(self, rows: dict[int, dict[str, float]]) -> None:
        with self._lock:
            for h, fields in rows.items():
                for k, v in fields.items():
                    self._pending[h][k] += v

    async def flush(self, db: SensorDatabase, now: float) -> None:
        """Add the pending figures to the table. The first flush ever marks
        the hour of ``now`` as the start of counting. On a database error the
        figures are kept for the next flush and the error is raised."""
        from rfobserver.storage.database import COLLECTION_SINCE_KEY

        if await db.get_config(COLLECTION_SINCE_KEY) is None:
            await db.set_config(COLLECTION_SINCE_KEY, str(_hour(now)))
        rows = self.take()
        if not rows:
            return
        try:
            await db.add_collection_hours(rows)
        except BaseException:
            self._put_back(rows)
            raise


# --- days in the sensor's time zone ---

DEFAULT_ZONE = "America/Los_Angeles"  # a sensor with no location

_warned_zones: set[str] = set()
_whole_hour_ok: dict[tuple[str, int], ZoneInfo] = {}


def whole_hour_zone(name: str) -> ZoneInfo:
    """The IANA zone ``name``, if its UTC offset is a whole number of hours on
    every day of this year and next (days are built from UTC hours).
    Raises ValueError otherwise. A zone that passes is remembered for the year."""
    year = datetime.now(timezone.utc).year
    known = _whole_hour_ok.get((name, year))
    if known is not None:
        return known
    try:
        tz = ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"unknown time zone {name!r}") from exc
    day = datetime(year, 1, 1, 12, tzinfo=timezone.utc)
    for _ in range(731):
        offset = day.astimezone(tz).utcoffset()
        if offset is not None and offset.total_seconds() % HOUR:
            raise ValueError(f"time zone {name!r} is not a whole number of hours from UTC")
        day += timedelta(days=1)
    _whole_hour_ok[(name, year)] = tz
    return tz


def _lookup_zone_name(lat: float, lon: float) -> str:
    """The IANA zone at a location (offline, from tzfpy's bundled map)."""
    from tzfpy import get_tz

    return str(get_tz(lon, lat))


@functools.lru_cache(maxsize=16)
def location_zone(lat: float, lon: float) -> str:
    """The IANA zone name at a location, looked up once per location."""
    return _lookup_zone_name(lat, lon)


def _usable_zone_at(lat: float, lon: float) -> ZoneInfo | None:
    """The whole-hour zone at a location, or None (with a warning, once)
    when there is none: a fractional zone, or an unknown name."""
    name = location_zone(lat, lon)
    try:
        return whole_hour_zone(name)
    except ValueError as exc:
        if name not in _warned_zones:
            _warned_zones.add(name)
            logger.warning(
                "The zone at %.4f, %.4f (%s) is not usable (%s); using %s",
                lat,
                lon,
                name,
                exc,
                DEFAULT_ZONE,
            )
        return None


def _zone_at_location(settings: AppSettings) -> ZoneInfo | None:
    lat, lon = settings.LATITUDE, settings.LONGITUDE
    if lat is None or lon is None:
        return None
    try:
        return _usable_zone_at(float(lat), float(lon))
    except Exception:  # never stop the API over the lookup
        logger.exception("Time zone lookup for %s, %s failed", lat, lon)
        return None


def sensor_zone(settings: AppSettings, requested: str | None = None) -> tuple[tzinfo, str]:
    """The zone days are counted in, and where it came from: ``requested``
    ("request", ValueError if it is not a whole-hour zone), else the zone at
    the sensor's LATITUDE/LONGITUDE ("location"), else the TIMEZONE setting
    ("setting": set by hand, or the last zone found at a location), else
    Pacific ("default"). UTC is the built-in one when the host has no
    time-zone database."""
    if requested:
        return whole_hour_zone(requested), "request"
    at_location = _zone_at_location(settings)
    if at_location is not None:
        return at_location, "location"
    name = (settings.TIMEZONE or "").strip()
    if name:
        try:
            return whole_hour_zone(name), "setting"
        except ValueError as exc:
            if f"TIMEZONE={name}" not in _warned_zones:
                _warned_zones.add(f"TIMEZONE={name}")
                logger.warning("TIMEZONE %r is not usable (%s); ignoring it", name, exc)
    try:
        return whole_hour_zone(DEFAULT_ZONE), "default"
    except ValueError:
        return timezone.utc, "default"


class SensorZone:
    """The sensor's zone, resolved at startup and again whenever the
    settings it depends on change (Config apply calls ``refresh``), so a
    request never waits for the location lookup. A zone found at the
    location is written into TIMEZONE, so the setting shows it (and Config
    apply saves it to .env); TIMEZONE set by hand counts only without a
    location."""

    def __init__(self, settings: AppSettings) -> None:
        self._key: tuple[object, ...] | None = None
        self._zone: tuple[tzinfo, str] = (timezone.utc, "default")
        self.refresh(settings)

    @staticmethod
    def _key_of(settings: AppSettings) -> tuple[object, ...]:
        return (settings.TIMEZONE, settings.LATITUDE, settings.LONGITUDE)

    def refresh(self, settings: AppSettings) -> tuple[tzinfo, str]:
        if self._key_of(settings) != self._key:
            self._zone = tz, source = sensor_zone(settings)
            if source == "location" and zone_name(tz) != settings.TIMEZONE:
                object.__setattr__(settings, "TIMEZONE", zone_name(tz))
            self._key = self._key_of(settings)
            logger.info("Collection statistics time zone: %s (%s)", zone_name(tz), source)
        return self._zone

    def get(self, settings: AppSettings) -> tuple[tzinfo, str]:
        """The resolved zone (re-resolved if the settings changed some other way)."""
        return self.refresh(settings)

    def current(self) -> tuple[tzinfo, str]:
        return self._zone


def zone_name(tz: tzinfo) -> str:
    """The IANA name of a zone from sensor_zone ("UTC" for the built-in one)."""
    return str(getattr(tz, "key", "UTC"))


def local_day_bounds(day: date, tz: tzinfo) -> tuple[int, int]:
    """UTC epoch seconds of ``day``'s local midnight and the next one."""
    start = datetime(day.year, day.month, day.day, tzinfo=tz)
    nxt = day + timedelta(days=1)
    end = datetime(nxt.year, nxt.month, nxt.day, tzinfo=tz)
    return int(start.timestamp()), int(end.timestamp())
