"""High-sensitivity mode: /api/high-sensitivity.

A run of at most 5 minutes with a lower burst threshold, the tone detector on
and a larger isolation budget (pipeline/high_sensitivity.py). Times are UTC
ISO 8601 with ms and ``Z``. See docs/high-sensitivity-api.md.
"""

from __future__ import annotations

import math
import re
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from rfobserver.pipeline.high_sensitivity import HighSensitivity, HSError, HSRun
from rfobserver.schedules.model import iso_ms

router = APIRouter()

# Run ids go in URL paths (and the fleet's rollback): plain characters only.
RUN_ID = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


def processor(app: Any) -> Any:
    """The running pipeline (the supervisor's, which follows a rebuild), or None."""
    sup = getattr(app.state, "supervisor", None)
    proc = getattr(sup, "processor", None) if sup is not None else None
    return proc if proc is not None else getattr(app.state, "processor", None)


def controller(app: Any) -> HighSensitivity | None:
    """The running pipeline's high-sensitivity controller, or None."""
    hs = getattr(processor(app), "high_sensitivity", None)
    return hs if isinstance(hs, HighSensitivity) else None


def _hs(request: Request) -> HighSensitivity:
    hs = controller(request.app)
    if hs is None:
        raise HTTPException(status_code=503, detail="pipeline not running")
    return hs


def run_json(run: HSRun, now: float) -> dict[str, Any]:
    return {
        "id": run.id,
        "source": run.source,
        "start": iso_ms(round(run.start * 1000)),
        "end": iso_ms(round(run.end * 1000)),
        "threshold_db": run.threshold_db,
        "seconds_left": max(0, math.ceil(run.end - now)) if run.ended is None else 0,
        "ended": None if run.ended is None else iso_ms(round(run.ended * 1000)),
        "end_reason": run.end_reason,
    }


def status(app: Any) -> dict[str, Any] | None:
    """The health block: whether a run is on, and which."""
    hs = controller(app)
    if hs is None:
        return None
    run = hs.current()
    return {"active": run is not None, "run": run_json(run, hs.now()) if run else None}


@router.post("")
async def start(request: Request) -> dict[str, Any]:
    hs = _hs(request)
    try:
        body = await request.json()
    except ValueError:
        body = None
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="The body must be a JSON object")
    run_id = body.get("id")
    if run_id is not None and (not isinstance(run_id, str) or not RUN_ID.match(run_id)):
        raise HTTPException(
            status_code=400, detail="id must be 1 to 64 letters, digits, '.', '-' or '_'"
        )
    cur = hs.current()
    db = getattr(request.app.state, "database", None)
    if (
        run_id is not None
        and (cur is None or cur.id != run_id)
        and db is not None
        and await db.hs_run_get(run_id) is not None
    ):
        raise HTTPException(status_code=409, detail=f"run id {run_id} was used before")
    try:
        run = hs.start(
            body.get("seconds"),
            run_id=run_id,
            threshold_db=body.get("threshold_db"),
            source="fleet" if body.get("source") == "fleet" else "api",
        )
    except HSError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from None
    return {"run": run_json(run, hs.now())}


@router.delete("/{run_id}")
async def stop(request: Request, run_id: str) -> dict[str, Any]:
    hs = _hs(request)
    try:
        run = hs.stop(run_id)
    except HSError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from None
    return {"run": run_json(run, hs.now())}


@router.get("")
async def current(request: Request) -> dict[str, Any]:
    hs = controller(request.app)
    run = hs.current() if hs is not None else None
    db = getattr(request.app.state, "database", None)
    runs = await db.hs_runs() if db is not None else []
    return {"run": run_json(run, hs.now()) if hs is not None and run else None, "runs": runs}


@router.get("/{run_id}")
async def one(request: Request, run_id: str) -> dict[str, Any]:
    db = getattr(request.app.state, "database", None)
    found = await db.hs_run_get(run_id) if db is not None else None
    if found is None:
        raise HTTPException(status_code=404, detail=f"No high-sensitivity run {run_id}")
    return found  # type: ignore[no-any-return]
