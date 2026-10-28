"""Scheduled IQ captures: /api/schedules.

Times are UTC (ISO 8601 with ms and ``Z``); each also comes as
``*_local`` in the sensor's time zone (the collection statistics' zone, from
its location) for convenience. See docs/schedules-api.md.
"""

from __future__ import annotations

import shutil
import time
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from rfobserver.schedules.book import ScheduleConflict
from rfobserver.schedules.model import (
    ScheduleError,
    first_run_from,
    iso_ms,
    parse_schedule,
    run_start_ms,
    schedule_bytes,
    to_json,
    total_runs,
)
from rfobserver.storage.collection import sensor_zone
from rfobserver.storage.governor import resolve_floor

if TYPE_CHECKING:
    from datetime import tzinfo

    from rfobserver.schedules.book import ScheduleBook
    from rfobserver.schedules.model import Schedule

router = APIRouter()

NEXT_RUNS = 5
RECENT_RUNS = 20
_SOURCES = ("api", "ui", "fleet")


def _now() -> float:
    return time.time()


def _book(request: Request) -> ScheduleBook:
    book = getattr(request.app.state, "schedules", None)
    if book is None:
        raise HTTPException(status_code=503, detail="Scheduled captures need the capture pipeline")
    return book  # type: ignore[no-any-return]


def _zone(request: Request) -> tzinfo:
    settings = request.app.state.settings
    resolved = getattr(request.app.state, "collection_zone", None)
    tz, _ = resolved.get(settings) if resolved is not None else sensor_zone(settings)
    return tz


def _zone_name(tz: tzinfo) -> str:
    return str(getattr(tz, "key", "UTC"))


def local_text(ms: int, tz: tzinfo) -> str:
    """``11:30:00.250 PDT``, with the date first when it differs from the
    UTC date."""
    dt = datetime.fromtimestamp(ms // 1000, tz=tz)
    text = f"{dt:%H:%M:%S}.{ms % 1000:03d} {dt.tzname()}"
    utc_date = datetime.fromtimestamp(ms // 1000, tz=timezone.utc).date()
    if dt.date() != utc_date:
        text = f"{dt:%Y-%m-%d} {text}"
    return text


def _instant(run: int, ms: int, tz: tzinfo) -> dict[str, Any]:
    return {"run": run, "start": iso_ms(ms), "start_local": local_text(ms, tz)}


def _next_runs(s: Schedule, now_ms: int, tz: tzinfo, limit: int) -> list[dict[str, Any]]:
    k = first_run_from(s, now_ms)
    if k is None:
        return []
    last = min(total_runs(s), k + limit)
    return [_instant(i, run_start_ms(s, i), tz) for i in range(k, last)]


def _run_row(r: dict[str, Any], tz: tzinfo) -> dict[str, Any]:
    return {**r, **_instant(r["run"], r["start_ms"], tz)}


def _warnings(request: Request) -> list[str]:
    s = request.app.state.settings
    if s.FREQUENCY_STEP > 0:
        return [
            f"This sensor is sweeping; windows longer than the dwell ({s.DURATION_SEC:g} s) "
            "will be cut at the retune."
        ]
    return []


def _check_space(request: Request, s: Schedule) -> None:
    settings = request.app.state.settings
    need = schedule_bytes(s, float(settings.BANDWIDTH))
    try:
        usage = shutil.disk_usage(settings.STORAGE_PATH)
    except OSError:
        return
    room = usage.free - resolve_floor(settings.DISK_MIN_FREE_GB, usage.total)
    if need > room:
        raise HTTPException(
            status_code=409,
            detail=(
                f"The schedule needs {need / 1e9:.1f} GB but only {max(room, 0) / 1e9:.1f} GB "
                "is free above the storage floor"
            ),
        )


@router.post("")
async def create(request: Request) -> JSONResponse:
    book = _book(request)
    try:
        body = await request.json()
    except ValueError:
        body = None
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="The body must be a JSON object")
    now_ms = int(_now() * 1000)
    source = str(body["source"]) if body.get("source") in _SOURCES else "api"
    settings = request.app.state.settings
    try:
        s = parse_schedule(
            body,
            now_ms=now_ms,
            grace_ms=int(settings.TRIGGER_PRE_SEC * 1000),
            source=source,
            new_id=lambda: uuid.uuid4().hex[:12],
        )
    except ScheduleError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    if book.get(s.id) is None:
        _check_space(request, s)
    try:
        stored, created = await book.add(s)
    except ScheduleConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    tz = _zone(request)
    return JSONResponse(
        status_code=201 if created else 200,
        content={
            "schedule": to_json(stored),
            "next_runs": _next_runs(stored, now_ms, tz, NEXT_RUNS),
            "warnings": _warnings(request),
        },
    )


@router.get("")
async def list_schedules(request: Request) -> dict[str, Any]:
    book = _book(request)
    tz = _zone(request)
    now_ms = int(_now() * 1000)
    _, schedules = book.snapshot()
    items = []
    recent: list[dict[str, Any]] = []
    for s in sorted(schedules, key=lambda s: (s.start_ms, s.id)):
        nxt = _next_runs(s, now_ms, tz, 1)
        runs = await book.runs(s.id, RECENT_RUNS)
        recent += [{"id": s.id, "label": s.label, **_run_row(r, tz)} for r in runs]
        items.append(
            {
                "schedule": to_json(s),
                "next_run": nxt[0] if nxt else None,
                "last_run": _run_row(runs[0], tz) if runs else None,
            }
        )
    recent.sort(key=lambda r: (r["start_ms"], r["id"]), reverse=True)
    return {
        "now": iso_ms(now_ms),
        "now_ms": now_ms,
        "now_local": local_text(now_ms, tz),
        "timezone": _zone_name(tz),
        "warnings": _warnings(request),
        "schedules": items,
        "recent": recent[:RECENT_RUNS],
    }


@router.get("/{schedule_id}")
async def get_schedule(request: Request, schedule_id: str) -> dict[str, Any]:
    book = _book(request)
    s = book.get(schedule_id)
    if s is None:
        raise HTTPException(status_code=404, detail=f"No schedule {schedule_id}")
    tz = _zone(request)
    now_ms = int(_now() * 1000)
    runs = await book.runs(schedule_id, 100)
    return {
        "schedule": to_json(s),
        "next_runs": _next_runs(s, now_ms, tz, NEXT_RUNS),
        "runs": [_run_row(r, tz) for r in runs],
    }


@router.delete("/{schedule_id}")
async def delete_schedule(request: Request, schedule_id: str) -> dict[str, Any]:
    book = _book(request)
    if not await book.delete(schedule_id):
        raise HTTPException(status_code=404, detail=f"No schedule {schedule_id}")
    return {"deleted": schedule_id}
