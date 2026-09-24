"""StorageGovernor: the free-space ladder, as pure decisions over samples."""

from __future__ import annotations

import errno
import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from rfobserver.storage.governor import (
    DEGRADED_CONFIG_KEY,
    GB,
    LAST_WRITE_ERROR_CONFIG_KEY,
    StorageGovernor,
    StorageSample,
    VolumeSample,
    describe_write_error,
    is_disk_full_error,
    resolve_floor,
)

T0 = datetime(2026, 9, 23, 10, 0, tzinfo=timezone.utc)
VOL = 1000 * GB  # auto floor = 50 GB


def _s(free_gb: float, *, evictable: bool = True, db_free_gb: float | None = None) -> StorageSample:
    return StorageSample(
        data=VolumeSample(int(free_gb * GB), VOL),
        db_volume=None if db_free_gb is None else VolumeSample(int(db_free_gb * GB), 100 * GB),
        db_file_bytes=40 * GB,
        db_reusable_bytes=3 * GB,
        auto_bytes=480 * GB,
        manual_bytes=118 * GB,
        evictable_auto=evictable,
    )


def _run(gov: StorageGovernor, *samples: StorageSample, min_free_gb: float = 0.0):
    out = []
    for i, s in enumerate(samples):
        out.append(gov.tick(s, min_free_gb=min_free_gb, now=T0 + timedelta(seconds=10 * i)))
    return out


def test_auto_floor_is_5_percent_with_a_2_gb_minimum():
    assert resolve_floor(0, 916 * GB) == int(916 * GB * 0.05)
    assert resolve_floor(0, 20 * GB) == 2 * GB
    assert resolve_floor(12.5, 916 * GB) == int(12.5 * GB)
    assert resolve_floor(-1, 916 * GB) == int(916 * GB * 0.05)  # negative = auto


def test_healthy_is_step_0_with_no_actions():
    gov = StorageGovernor()
    (a,) = _run(gov, _s(200))
    assert gov.state.step == 0
    assert a.evict_to_free_bytes is None and not a.start_pressure_prune


def test_below_floor_with_evictable_iq_is_step_1_and_evicts_to_the_margin():
    gov = StorageGovernor()
    (a,) = _run(gov, _s(40))
    assert gov.state.step == 1
    assert a.evict_to_free_bytes == int(50 * GB * 1.15)
    assert not gov.state.refuse_recording


def test_below_floor_without_evictable_iq_is_step_2_then_3():
    gov = StorageGovernor()
    a1, a2 = _run(gov, _s(40, evictable=False), _s(40, evictable=False))
    assert a1.start_pressure_prune  # entering step 2 wakes retention now
    assert not a2.start_pressure_prune
    assert gov.state.step == 3  # pruning returned no disk
    assert gov.state.refuse_recording
    assert gov.state.degraded_since == T0 + timedelta(seconds=10)


def test_step_2_is_not_treated_as_reclaiming():
    """Pruning frees pages inside the DB file, not disk: a tick at step 2 that is
    still short must escalate, never sit at 2 waiting for space."""
    gov = StorageGovernor()
    _run(gov, *[_s(40, evictable=False)] * 5)
    assert gov.state.step == 3


def test_below_half_floor_is_step_4_even_with_evictable_iq():
    gov = StorageGovernor()
    (a,) = _run(gov, _s(20))
    assert gov.state.step == 4
    assert gov.state.skip_psd_blobs and gov.state.refuse_recording
    assert a.evict_to_free_bytes == int(50 * GB * 1.15)  # cumulative: still evicts


def test_recovery_needs_the_margin_for_3_consecutive_ticks():
    gov = StorageGovernor()
    _run(gov, _s(40))
    assert gov.state.step == 1
    _run(gov, _s(58), _s(58))  # 58 >= 57.5 for 2 ticks
    assert gov.state.step == 1
    _run(gov, _s(58))
    assert gov.state.step == 0


def test_no_flapping_at_the_floor():
    gov = StorageGovernor()
    steps = []
    for free in [49, 51, 49.5, 52, 49.9, 55, 50.1, 56]:
        _run(gov, _s(free))
        steps.append(gov.state.step)
    assert steps == [1] * 8  # never reached 57.5, so never left step 1


def test_recovery_streak_resets_on_a_short_tick():
    gov = StorageGovernor()
    _run(gov, _s(40), _s(60), _s(60), _s(56), _s(60), _s(60))
    assert gov.state.step == 1
    _run(gov, _s(60))
    assert gov.state.step == 0


def test_partial_recovery_does_not_leave_step_4():
    gov = StorageGovernor()
    _run(gov, _s(20, evictable=False))
    assert gov.state.step == 4
    _run(gov, *[_s(40, evictable=False)] * 5)  # above floor/2, below floor
    assert gov.state.step == 4
    _run(gov, *[_s(60, evictable=False)] * 3)
    assert gov.state.step == 0


def test_evicts_whenever_short_and_evictable_even_above_step_1():
    gov = StorageGovernor()
    _run(gov, _s(40, evictable=False), _s(40, evictable=False))
    assert gov.state.step == 3
    (a,) = _run(gov, _s(40, evictable=True))  # a capture finished meanwhile
    assert gov.state.step == 3
    assert a.evict_to_free_bytes == int(50 * GB * 1.15)


def test_db_volume_short_prunes_but_never_evicts():
    gov = StorageGovernor()
    (a,) = _run(gov, _s(500, db_free_gb=4))  # db floor = 5 GB (5% of 100)
    assert gov.state.step == 2
    assert a.evict_to_free_bytes is None
    assert a.start_pressure_prune
    assert gov.state.db_floor_bytes == 5 * GB


def test_explicit_floor_overrides_auto():
    gov = StorageGovernor()
    _run(gov, _s(40), min_free_gb=30)
    assert gov.state.step == 0


def test_step_since_moves_only_on_a_change():
    gov = StorageGovernor()
    _run(gov, _s(40), _s(41))
    assert gov.state.step_since == T0


def test_write_error_sets_the_sticky_flag_and_it_survives_recovery():
    gov = StorageGovernor()
    _run(gov, _s(200))
    gov.report_write_error("ENOSPC: No space left on device", now=T0)
    _run(gov, *[_s(200)] * 5)
    st = gov.state
    assert st.degraded and st.step == 0
    assert st.last_write_error == {"at": T0.isoformat(), "error": "ENOSPC: No space left on device"}
    gov.clear_degraded()
    assert not gov.state.degraded and gov.state.last_write_error is None


def test_clearing_at_step_3_does_not_reset_the_flag_until_step_3_is_entered_again():
    gov = StorageGovernor()
    _run(gov, _s(40, evictable=False), _s(40, evictable=False))
    gov.clear_degraded()
    _run(gov, _s(40, evictable=False))
    assert gov.state.degraded_since is None
    assert gov.state.degraded  # still step 3


def test_degraded_change_is_reported_once_for_persistence():
    gov = StorageGovernor()
    assert gov.take_degraded_change() == (False, {})
    gov.report_write_error("x", now=T0)
    changed, values = gov.take_degraded_change()
    assert changed
    assert values[DEGRADED_CONFIG_KEY] == T0.isoformat()
    assert json.loads(values[LAST_WRITE_ERROR_CONFIG_KEY]) == {"at": T0.isoformat(), "error": "x"}
    assert gov.take_degraded_change() == (False, {})
    gov.clear_degraded()
    assert gov.take_degraded_change() == (
        True,
        {DEGRADED_CONFIG_KEY: "", LAST_WRITE_ERROR_CONFIG_KEY: ""},
    )


def test_a_later_write_error_is_persisted_even_when_already_degraded():
    gov = StorageGovernor()
    gov.report_write_error("first", now=T0)
    gov.take_degraded_change()
    later = T0 + timedelta(minutes=5)
    gov.report_write_error("second", now=later)
    changed, values = gov.take_degraded_change()
    assert changed
    assert values[DEGRADED_CONFIG_KEY] == T0.isoformat()  # since the first
    assert json.loads(values[LAST_WRITE_ERROR_CONFIG_KEY])["error"] == "second"


def test_restore_degraded_parses_the_persisted_value():
    gov = StorageGovernor()
    gov.restore_degraded(T0.isoformat())
    assert gov.state.degraded_since == T0
    assert gov.take_degraded_change() == (False, {})  # already persisted
    gov2 = StorageGovernor()
    gov2.restore_degraded("")
    gov2.restore_degraded(None)
    gov2.restore_degraded("not a timestamp")
    assert gov2.state.degraded_since is None


def test_restore_brings_back_the_last_write_error():
    gov = StorageGovernor()
    gov.report_write_error("ENOSPC: No space left on device", now=T0)
    _, values = gov.take_degraded_change()
    fresh = StorageGovernor()
    fresh.restore_degraded(values[DEGRADED_CONFIG_KEY], values[LAST_WRITE_ERROR_CONFIG_KEY])
    assert fresh.state.last_write_error == gov.state.last_write_error
    assert fresh.state.degraded_since == T0
    assert fresh.take_degraded_change() == (False, {})


@pytest.mark.parametrize("raw", [None, "", "not json", "[1, 2]", '{"at": 1}', '{"error": "x"}'])
def test_restore_ignores_a_missing_or_garbled_write_error(raw):
    gov = StorageGovernor()
    gov.restore_degraded(T0.isoformat(), raw)
    assert gov.state.last_write_error is None
    assert gov.state.degraded_since == T0


def test_health_block_shape():
    gov = StorageGovernor()
    _run(gov, _s(40))
    h = gov.state.to_health()
    assert h["free_gb"] == 40.0 and h["floor_gb"] == 50.0 and h["volume_gb"] == 1000.0
    assert h["db_gb"] == 40.0 and h["db_reusable_gb"] == 3.0
    assert h["auto_gb"] == 480.0 and h["manual_gb"] == 118.0
    assert h["step"] == 1 and h["step_text"]
    assert h["step_since"] == T0.isoformat()
    assert h["last_write_error"] is None and h["degraded_since"] is None
    assert h["db_volume"] is None
    gov2 = StorageGovernor()
    _run(gov2, _s(500, db_free_gb=4))
    assert gov2.state.to_health()["db_volume"] == {"free_gb": 4.0, "floor_gb": 5.0}


def test_health_before_the_first_tick():
    h = StorageGovernor().state.to_health()
    assert h["step"] == 0 and h["free_gb"] is None


@pytest.mark.parametrize(
    "exc,expected",
    [
        (OSError(errno.ENOSPC, "No space left on device"), True),
        (OSError(errno.EDQUOT, "Disk quota exceeded"), True),
        (sqlite3.OperationalError("database or disk is full"), True),
        (OSError(errno.EIO, "Input/output error"), False),
        (sqlite3.OperationalError("database is locked"), False),
        (ValueError("x"), False),
    ],
)
def test_is_disk_full_error(exc, expected):
    assert is_disk_full_error(exc) is expected


def test_describe_write_error():
    assert describe_write_error(OSError(errno.ENOSPC, "No space left on device")) == (
        "ENOSPC: No space left on device"
    )
    assert describe_write_error(ValueError("bad")) == "ValueError: bad"


def test_ticks_count_every_completed_tick():
    gov = StorageGovernor()
    assert gov.ticks == 0
    _run(gov, _s(200), _s(40), _s(200))
    assert gov.ticks == 3
