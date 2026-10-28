"""Scheduled captures: parsing a schedule and the instants of its runs."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from rfobserver.schedules.model import (
    MAX_DURATION_MS,
    ScheduleError,
    first_run_from,
    from_json,
    parse_schedule,
    run_start_ms,
    runs_between,
    schedule_bytes,
    to_json,
    total_runs,
)

T = 1793212200250  # 2026-10-28T18:30:00.250Z
NOW = T - 60_000


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def parse(body: dict[str, Any]) -> Any:
    return parse_schedule(body, now_ms=NOW, grace_ms=1000, source="api", new_id=lambda: "abc")


def test_one_off_parses_iso_with_ms() -> None:
    s = parse({"start": "2026-10-28T18:30:00.250Z", "duration_ms": 2000})
    assert s.id == "abc" and s.start_ms == T and s.duration_ms == 2000
    assert total_runs(s) == 1
    assert to_json(s)["start"] == "2026-10-28T18:30:00.250Z"


def test_start_ms_and_offset_forms_agree() -> None:
    a = parse({"start_ms": T, "duration_ms": 1})
    b = parse({"start": "2026-10-28T11:30:00.250-07:00", "duration_ms": 1})
    assert a.start_ms == b.start_ms == T


def test_repeat_instants_are_from_start() -> None:
    s = parse({"start_ms": T, "duration_ms": 500, "every_ms": 60000, "count": 3})
    assert [run_start_ms(s, k) for k in range(3)] == [T, T + 60000, T + 120000]
    assert first_run_from(s, T) == 0
    assert first_run_from(s, T + 1) == 1
    assert first_run_from(s, T + 120001) is None


def test_until_bounds_runs() -> None:
    s = parse({"start_ms": T, "duration_ms": 500, "every_ms": 1000, "until": iso(T + 3000)})
    # Runs start at T, T+1000, T+2000; T+3000 is not before until.
    assert total_runs(s) == 3
    assert to_json(s)["until"] == "2026-10-28T18:30:03.250Z"


def test_json_round_trip() -> None:
    s = parse(
        {"id": "x-1", "start_ms": T, "duration_ms": 5, "every_ms": 10, "count": 4, "label": "a"}
    )
    assert from_json(to_json(s)) == s


@pytest.mark.parametrize(
    ("body", "detail"),
    [
        ({"duration_ms": 1}, "start"),
        ({"start_ms": NOW - 1001, "duration_ms": 1}, "past"),
        ({"start_ms": T, "duration_ms": 0}, "duration_ms"),
        ({"start_ms": T, "duration_ms": MAX_DURATION_MS + 1}, "duration_ms"),
        ({"start_ms": T, "duration_ms": 500, "every_ms": 500, "count": 2}, "every_ms"),
        ({"start_ms": T, "duration_ms": 500, "every_ms": 1000}, "count or until"),
        (
            {
                "start_ms": T,
                "duration_ms": 500,
                "every_ms": 1000,
                "count": 2,
                "until": iso(T + 9000),
            },
            "count or until",
        ),
        ({"start_ms": T, "duration_ms": 500, "count": 2}, "every_ms"),
        ({"start": "2026-10-28 18:30", "duration_ms": 1}, "start"),
        ({"start": "soon", "duration_ms": 1}, "start"),
        ({"id": "bad id!", "start_ms": T, "duration_ms": 1}, "id"),
        ({"start_ms": T, "duration_ms": "2"}, "duration_ms"),
        ({"start_ms": T, "duration_ms": 1, "every_ms": 10, "count": 0}, "count"),
        ({"start_ms": T, "duration_ms": 1, "every_ms": 10, "until": iso(T)}, "until"),
    ],
)
def test_invalid_bodies(body: dict[str, Any], detail: str) -> None:
    with pytest.raises(ScheduleError, match=detail):
        parse(body)


def test_start_within_grace_is_accepted() -> None:
    assert parse({"start_ms": NOW - 1000, "duration_ms": 1}).start_ms == NOW - 1000


def test_runs_between_is_bounded_and_newest() -> None:
    s = parse({"start_ms": T, "duration_ms": 1, "every_ms": 10, "count": 10**9})
    ks = runs_between(s, T, T + 10**7, limit=500)
    assert len(ks) == 500 and ks[0] == 10**6 - 500 and ks[-1] == 10**6 - 1
    assert runs_between(s, T - 100, T + 25, limit=500) == [0, 1, 2]
    one = parse({"start_ms": T, "duration_ms": 1})
    assert runs_between(one, T + 1, T + 100, limit=5) == []


def test_schedule_bytes() -> None:
    s = parse({"start_ms": T, "duration_ms": 1000, "every_ms": 2000, "count": 3})
    assert schedule_bytes(s, 1e6) == 3 * 1_000_000 * 4
