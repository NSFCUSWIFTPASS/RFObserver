"""IQ collection statistics: raw IQ recorded, IQ deleted automatically and
live RF time, by hour, by day in a time zone (``tz``, else the zone at the
sensor's location, else Pacific; see storage.collection.sensor_zone), over a
date range, as a 7-day average, and the storage now. Read from
collection_hours (storage/collection.py); counting starts at
``covered_since``, with no backfill."""

from __future__ import annotations

import shutil
import time
from datetime import date, datetime, timedelta, timezone, tzinfo
from typing import Any, NamedTuple

from fastapi import APIRouter, HTTPException, Query, Request

from rfobserver.storage.collection import FIELDS, local_day_bounds, sensor_zone, zone_name

router = APIRouter()

MAX_RANGE_DAYS = 366
MAX_HOURLY_DAYS = 31
MAX_AVERAGE_DAYS = 60
# Dates outside these years cannot be turned into epoch seconds everywhere.
MIN_YEAR, MAX_YEAR = 1970, 9998

_COUNT_FIELDS = {f for f in FIELDS if not f.endswith("_seconds")}


def _now() -> float:
    return time.time()


def _db(request: Request) -> Any:
    db = getattr(request.app.state, "database", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    return db


def _zone(request: Request) -> tuple[tzinfo, str]:
    """The request's ``tz`` (400 if unusable), else the sensor's zone."""
    requested = request.query_params.get("tz") or None
    try:
        return sensor_zone(request.app.state.settings, requested)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"tz must be an IANA zone a whole number of hours from UTC: {exc}",
        ) from None


class _Context(NamedTuple):
    db: Any
    tz: tzinfo
    source: str  # where tz came from: "request", "location" or "default"
    since: int | None  # when counting began (epoch seconds), None before


async def _context(request: Request) -> _Context:
    from rfobserver.storage.database import COLLECTION_SINCE_KEY

    tz, source = _zone(request)
    db = _db(request)
    raw = await db.get_config(COLLECTION_SINCE_KEY)
    return _Context(db, tz, source, int(raw) if raw else None)


def _iso(ts: int, tz: tzinfo) -> str:
    return datetime.fromtimestamp(ts, tz).isoformat()


def _header(ctx: _Context) -> dict[str, Any]:
    return {
        "timezone": zone_name(ctx.tz),
        "timezone_source": ctx.source,
        "covered_since": None if ctx.since is None else _iso(ctx.since, ctx.tz),
    }


def _empty() -> dict[str, float]:
    return {f: 0 if f in _COUNT_FIELDS else 0.0 for f in FIELDS}


def _with_totals(fig: dict[str, Any]) -> dict[str, Any]:
    fig["captures"] = fig["auto_captures"] + fig["manual_captures"]
    fig["bytes"] = fig["auto_bytes"] + fig["manual_bytes"]
    fig["seconds"] = fig["auto_seconds"] + fig["manual_seconds"]
    return fig


def _parse_date(raw: str, name: str) -> date:
    try:
        day = date.fromisoformat(raw)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"{name} must be YYYY-MM-DD") from None
    if not MIN_YEAR <= day.year <= MAX_YEAR:
        raise HTTPException(
            status_code=400, detail=f"{name} must be in the years {MIN_YEAR} to {MAX_YEAR}"
        )
    return day


async def _days(
    db: Any, tz: tzinfo, since: int | None, first: date, last: date
) -> list[dict[str, Any]]:
    """Each local day in [first, last] with its figures, from one read."""
    bounds = [
        (first + timedelta(days=i), *local_day_bounds(first + timedelta(days=i), tz))
        for i in range((last - first).days + 1)
    ]
    rows = await db.collection_hours(bounds[0][1], bounds[-1][2])
    now = _now()
    out = []
    i = 0
    for day, lo, hi in bounds:
        fig = _empty()
        while i < len(rows) and rows[i]["hour"] < hi:
            if rows[i]["hour"] >= lo:
                for f in FIELDS:
                    fig[f] += rows[i][f]
            i += 1
        out.append(
            _with_totals(
                {
                    "date": day.isoformat(),
                    "start": _iso(lo, tz),
                    "end": _iso(hi, tz),
                    "hours": (hi - lo) // 3600,
                    "complete": hi <= now,
                    "covered": since is not None and since <= lo,
                    **fig,
                }
            )
        )
    return out


def _sum(days: list[dict[str, Any]]) -> dict[str, Any]:
    total = _empty()
    for d in days:
        for f in FIELDS:
            total[f] += d[f]
    return _with_totals(total)


@router.get("/daily")
async def daily(
    request: Request,
    date_: str | None = Query(None, alias="date"),
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    """One local day (``date``), or each day in [start, end] with a total."""
    if date_ is not None and start is None and end is None:
        day = _parse_date(date_, "date")
        ctx = await _context(request)
        (d,) = await _days(ctx.db, ctx.tz, ctx.since, day, day)
        return {**_header(ctx), **d}
    if date_ is None and start is not None and end is not None:
        first, last = _parse_date(start, "start"), _parse_date(end, "end")
        if last < first:
            raise HTTPException(status_code=400, detail="end is before start")
        if (last - first).days + 1 > MAX_RANGE_DAYS:
            raise HTTPException(status_code=400, detail=f"at most {MAX_RANGE_DAYS} days")
        ctx = await _context(request)
        days = await _days(ctx.db, ctx.tz, ctx.since, first, last)
        return {**_header(ctx), "days": days, "total": _sum(days)}
    raise HTTPException(status_code=400, detail="give date, or start and end")


@router.get("/average")
async def average(request: Request, days: str = "7") -> dict[str, Any]:
    """The mean per day over the last ``days`` complete local days; null until
    that many complete days have been counted."""
    try:
        n = int(days)
    except ValueError:
        n = 0
    if not 1 <= n <= MAX_AVERAGE_DAYS:
        raise HTTPException(
            status_code=400, detail=f"days must be a whole number from 1 to {MAX_AVERAGE_DAYS}"
        )
    days_n = n
    ctx = await _context(request)
    today = datetime.fromtimestamp(_now(), ctx.tz).date()
    window = await _days(
        ctx.db, ctx.tz, ctx.since, today - timedelta(days=days_n), today - timedelta(days=1)
    )
    counted = [d for d in window if d["covered"] and d["complete"]]
    avg = None
    if len(counted) == days_n:
        total = _sum(counted)
        avg = {k: v / days_n for k, v in total.items()}
    return {**_header(ctx), "days_counted": len(counted), "average": avg, "days": window}


@router.get("/hourly")
async def hourly(request: Request, since: str, until: str) -> dict[str, Any]:
    """Each counted UTC hour in [since, until) (ISO times)."""
    from rfobserver.web.routes.api import InvalidTimeFilterError, _opt_utc_dt

    try:
        lo_dt, hi_dt = _opt_utc_dt(since), _opt_utc_dt(until)
    except InvalidTimeFilterError:
        lo_dt = hi_dt = None
    if lo_dt is None or hi_dt is None:
        raise HTTPException(status_code=400, detail="since and until must be ISO times")
    if hi_dt <= lo_dt:
        raise HTTPException(status_code=400, detail="until must be after since")
    if hi_dt - lo_dt > timedelta(days=MAX_HOURLY_DAYS):
        raise HTTPException(status_code=400, detail=f"at most {MAX_HOURLY_DAYS} days")
    ctx = await _context(request)
    lo = int(lo_dt.timestamp()) // 3600 * 3600
    rows = await ctx.db.collection_hours(lo, int(hi_dt.timestamp()))
    hours = [
        {
            "hour_utc": _iso(r["hour"], timezone.utc),
            "hour_local": _iso(r["hour"], ctx.tz),
            **{f: r[f] for f in FIELDS},
        }
        for r in rows
    ]
    return {**_header(ctx), "hours": hours}


@router.get("/storage")
async def storage(request: Request) -> dict[str, Any]:
    """Captures and their footprint on disk now, the cap and the volume."""
    ls = getattr(request.app.state, "local_storage", None)
    if ls is None:
        raise HTTPException(status_code=503, detail="Storage not available")
    snap: dict[str, Any] = {**_header(await _context(request)), **ls.snapshot()}
    try:
        usage = shutil.disk_usage(ls.storage_path)
        snap["volume_free_bytes"], snap["volume_total_bytes"] = usage.free, usage.total
    except OSError:
        snap["volume_free_bytes"] = snap["volume_total_bytes"] = None
    return snap
