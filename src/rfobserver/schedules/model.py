"""A schedule (one window, or a repeating one) and the instants of its runs.

Run k starts at ``start_ms + k * every_ms``: counted from the schedule's
start, so every sensor given the same schedule records the same instants,
whenever it received it. All times are integer epoch milliseconds (UTC).
"""

from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

MAX_DURATION_MS = 600_000
# Repeats at most once a second, and at most this many runs: each run is a
# file, a database row and an outcome to report.
MIN_EVERY_MS = 1000
MAX_RUNS = 100_000
ID_RE = r"^[A-Za-z0-9_-]{1,64}$"
_ID = re.compile(ID_RE)
_LABEL_MAX = 200


class ScheduleError(ValueError):
    """An invalid schedule; the message is the API's ``detail``."""


@dataclass(frozen=True)
class Schedule:
    id: str
    start_ms: int
    duration_ms: int
    every_ms: int | None
    count: int | None
    until_ms: int | None
    label: str
    source: str
    created_ms: int


def iso_ms(ms: int) -> str:
    """``2026-10-28T18:30:00.250Z``."""
    dt = datetime.fromtimestamp(ms // 1000, tz=timezone.utc)
    return f"{dt:%Y-%m-%dT%H:%M:%S}.{ms % 1000:03d}Z"


def parse_instant(value: Any, name: str) -> int:
    """Epoch ms of an ISO 8601 string that carries its zone (``Z`` or an
    offset); a naive time is ambiguous and refused."""
    if not isinstance(value, str):
        raise ScheduleError(
            f"{name} must be an ISO 8601 time with a zone, e.g. 2026-10-28T18:30:00.250Z"
        )
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        dt = None
    if dt is None or dt.tzinfo is None:
        raise ScheduleError(
            f"{name} must be an ISO 8601 time with a zone, e.g. 2026-10-28T18:30:00.250Z"
        )
    return round(dt.timestamp() * 1000)


def _int(body: dict[str, Any], name: str, *, minimum: int) -> int | None:
    value = body.get(name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ScheduleError(f"{name} must be a whole number")
    if value < minimum:
        raise ScheduleError(f"{name} must be at least {minimum}")
    return value


def parse_schedule(
    body: dict[str, Any],
    *,
    now_ms: int,
    grace_ms: int,
    source: str,
    new_id: Callable[[], str],
    allow_past: bool = False,
) -> Schedule:
    """A Schedule from an API body, or ScheduleError with the reason.

    ``grace_ms``: how far in the past a run's start may be (the sensor still
    holds that much of the stream). A repeating schedule may start in the
    past while runs remain; it records from the next one. ``allow_past``
    skips the check (the same schedule sent again)."""
    sid = body.get("id")
    if sid is None:
        sid = new_id()
    if not isinstance(sid, str) or not _ID.match(sid):
        raise ScheduleError("id must be 1 to 64 letters, digits, '-' or '_'")

    if body.get("start_ms") is not None:
        start = _int(body, "start_ms", minimum=0)
    elif body.get("start") is not None:
        start = parse_instant(body["start"], "start")
    else:
        raise ScheduleError("start (ISO 8601 UTC) or start_ms (epoch ms) is required")
    assert start is not None

    duration = _int(body, "duration_ms", minimum=1)
    if duration is None:
        raise ScheduleError("duration_ms is required")
    if duration > MAX_DURATION_MS:
        raise ScheduleError(f"duration_ms must be at most {MAX_DURATION_MS}")

    every = _int(body, "every_ms", minimum=1)
    count = _int(body, "count", minimum=1)
    until_raw = body.get("until")
    until = None if until_raw is None else parse_instant(until_raw, "until")
    if every is None:
        if count is not None or until is not None:
            raise ScheduleError("count and until need every_ms")
    else:
        if every < MIN_EVERY_MS:
            raise ScheduleError(f"every_ms must be at least {MIN_EVERY_MS}")
        if every <= duration:
            raise ScheduleError("every_ms must be longer than duration_ms")
        if (count is None) == (until is None):
            raise ScheduleError("a repeating schedule needs count or until (one of them)")
        if until is not None and until <= start:
            raise ScheduleError("until must be after start")

    label = body.get("label") or ""
    if not isinstance(label, str):
        raise ScheduleError("label must be text")
    s = Schedule(
        id=sid,
        start_ms=start,
        duration_ms=duration,
        every_ms=every,
        count=count,
        until_ms=until,
        label=label.strip()[:_LABEL_MAX],
        source=source,
        created_ms=now_ms,
    )
    if total_runs(s) > MAX_RUNS:
        raise ScheduleError(f"a schedule may have at most {MAX_RUNS} runs")
    if not allow_past and first_run_from(s, now_ms - grace_ms) is None:
        if every is None:
            raise ScheduleError(
                f"start is {(now_ms - start) / 1000:.3f} s in the past; "
                f"at most {grace_ms / 1000:.3f} s is allowed"
            )
        raise ScheduleError("every run of this schedule is in the past")
    return s


def run_start_ms(s: Schedule, k: int) -> int:
    return s.start_ms + k * (s.every_ms or 0)


def total_runs(s: Schedule) -> int:
    if s.every_ms is None:
        return 1
    if s.count is not None:
        return s.count
    assert s.until_ms is not None
    # Runs whose start is before until.
    return max(0, math.ceil((s.until_ms - s.start_ms) / s.every_ms))


def first_run_from(s: Schedule, t_ms: int) -> int | None:
    """The first run starting at or after ``t_ms``, None past the last."""
    if t_ms <= s.start_ms:
        k = 0
    elif s.every_ms is None:
        return None
    else:
        k = math.ceil((t_ms - s.start_ms) / s.every_ms)
    return k if k < total_runs(s) else None


def runs_between(s: Schedule, t0_ms: int, t1_ms: int, limit: int) -> list[int]:
    """Runs starting in [t0_ms, t1_ms), the newest ``limit`` of them."""
    first = first_run_from(s, t0_ms)
    if first is None:
        return []
    end = first_run_from(s, t1_ms)
    last = (total_runs(s) if end is None else end) - 1
    if last < first:
        return []
    return list(range(max(first, last - limit + 1), last + 1))


def schedule_bytes(s: Schedule, rate_hz: float) -> int:
    """Bytes all runs write (sc16: 4 bytes a sample)."""
    return total_runs(s) * round(s.duration_ms / 1000 * rate_hz) * 4


def to_json(s: Schedule) -> dict[str, Any]:
    d: dict[str, Any] = asdict(s)
    d["start"] = iso_ms(s.start_ms)
    d["until"] = None if s.until_ms is None else iso_ms(s.until_ms)
    d["runs"] = total_runs(s)
    return d


def from_json(d: dict[str, Any]) -> Schedule:
    return Schedule(
        id=d["id"],
        start_ms=int(d["start_ms"]),
        duration_ms=int(d["duration_ms"]),
        every_ms=d.get("every_ms"),
        count=d.get("count"),
        until_ms=d.get("until_ms"),
        label=d.get("label") or "",
        source=d.get("source") or "api",
        created_ms=int(d.get("created_ms") or 0),
    )
