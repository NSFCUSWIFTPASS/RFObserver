"""_storage_loop: sample, tick, evict, wake retention, persist the sticky flag."""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from types import SimpleNamespace

from rfobserver.config import AppSettings
from rfobserver.pipeline.app import (
    _active_capture_names,
    _restore_storage_flag,
    _storage_tick,
)
from rfobserver.storage.governor import (
    DEGRADED_CONFIG_KEY,
    GB,
    LAST_WRITE_ERROR_CONFIG_KEY,
    StorageGovernor,
    StorageSample,
    VolumeSample,
)


def _sample(free_gb: float, evictable: bool) -> StorageSample:
    return StorageSample(
        data=VolumeSample(int(free_gb * GB), 1000 * GB),
        db_volume=None,
        db_file_bytes=0,
        db_reusable_bytes=0,
        auto_bytes=0,
        manual_bytes=0,
        evictable_auto=evictable,
    )


class _LS:
    def __init__(
        self,
        samples: list[StorageSample],
        evict_ages: list[tuple[str, float]] | None = None,
    ) -> None:
        self.samples = samples
        self.evictions: list[tuple[int, set[str]]] = []
        self.active_seen: list[set[str]] = []
        self.not_after_seen: list[float | None] = []
        self.exclude_fn_seen: list[set[str]] = []
        # (capture name, age in seconds) reported to on_evict during eviction,
        # in eviction order -- stands in for the ages a real LocalStorage
        # would compute from each capture's mtime.
        self.evict_ages = evict_ages or []

    def sample(self, *, db_path, active_names, db_file_bytes, db_reusable_bytes, not_after=None):
        self.active_seen.append(set(active_names))
        self.not_after_seen.append(not_after)
        return self.samples.pop(0)

    def evict_until_free(
        self, target, *, exclude=(), exclude_fn=None, not_after=None, on_evict=None
    ):
        self.evictions.append((target, set(exclude)))
        self.not_after_seen.append(not_after)
        if exclude_fn is not None:
            self.exclude_fn_seen.append(set(exclude_fn()))
        if on_evict is not None:
            for name, age in self.evict_ages:
                on_evict(Path(name), age)
        return 0


class _DB:
    def __init__(self) -> None:
        self.config: dict[str, str] = {}

    async def file_stats(self):
        return 10, 2

    async def set_config(self, k: str, v: str) -> None:
        self.config[k] = v

    async def get_config(self, k: str) -> str | None:
        return self.config.get(k)


def _sup(state: str = "idle", file: str | None = None):
    proc = SimpleNamespace(recording_status=lambda: {"state": state, "file": file})
    return SimpleNamespace(processor=proc)


def test_active_capture_names():
    assert _active_capture_names(_sup("recording", "A.sc16")) == {"A.sc16"}
    assert _active_capture_names(_sup("finalizing", "A.sc16")) == {"A.sc16"}
    assert _active_capture_names(_sup("idle", "A.sc16")) == set()
    assert _active_capture_names(SimpleNamespace(processor=None)) == set()


async def test_tick_evicts_excluding_the_active_capture():
    s = AppSettings(_env_file=None)
    gov, ls, db, wake = StorageGovernor(), _LS([_sample(40, True)]), _DB(), asyncio.Event()
    await _storage_tick(s, gov, db, ls, _sup("recording", "A.sc16"), wake)
    assert gov.state.step == 1
    assert ls.evictions == [(int(50 * GB * 1.15), {"A.sc16"})]
    assert not wake.is_set()


async def test_entering_step_2_wakes_retention_and_step_3_persists_the_flag():
    s = AppSettings(_env_file=None)
    gov, db, wake = StorageGovernor(), _DB(), asyncio.Event()
    ls = _LS([_sample(40, False), _sample(40, False)])
    await _storage_tick(s, gov, db, ls, _sup(), wake)
    assert wake.is_set() and gov.state.step == 2
    assert DEGRADED_CONFIG_KEY not in db.config
    await _storage_tick(s, gov, db, ls, _sup(), wake)
    assert gov.state.step == 3
    assert db.config[DEGRADED_CONFIG_KEY] == gov.state.degraded_since.isoformat()


async def test_write_error_from_a_thread_is_persisted_on_the_next_tick():
    s = AppSettings(_env_file=None)
    gov, db, wake = StorageGovernor(), _DB(), asyncio.Event()
    gov.report_write_error("ENOSPC: No space left on device")
    await _storage_tick(s, gov, db, _LS([_sample(200, True)]), _sup(), wake)
    assert db.config[DEGRADED_CONFIG_KEY]
    assert "ENOSPC" in db.config[LAST_WRITE_ERROR_CONFIG_KEY]
    # A restart restores both the flag and its reason.
    fresh = StorageGovernor()
    fresh.restore_degraded(db.config[DEGRADED_CONFIG_KEY], db.config[LAST_WRITE_ERROR_CONFIG_KEY])
    assert fresh.state.last_write_error == gov.state.last_write_error
    assert fresh.state.degraded_since == gov.state.degraded_since


async def test_tick_uses_the_live_floor_setting():
    s = AppSettings(_env_file=None, DISK_MIN_FREE_GB=30)
    gov = StorageGovernor()
    await _storage_tick(s, gov, _DB(), _LS([_sample(40, True)]), _sup(), asyncio.Event())
    assert gov.state.step == 0


async def test_startup_restores_the_flag_and_the_last_write_error():
    db = _DB()
    gov = StorageGovernor()
    gov.report_write_error("ENOSPC: No space left on device")
    await _storage_tick(
        AppSettings(_env_file=None), gov, db, _LS([_sample(200, True)]), _sup(), asyncio.Event()
    )
    fresh = StorageGovernor()
    await _restore_storage_flag(fresh, db)
    assert fresh.state.degraded_since == gov.state.degraded_since
    assert fresh.state.last_write_error == gov.state.last_write_error


# --- final review I1: a capture begun during the tick is excluded -------------


async def test_a_capture_begun_during_the_tick_is_excluded_from_eviction():
    """file_stats can queue 10-30 s behind the writer; a capture that begins
    in that window must still be excluded at eviction time."""
    s = AppSettings(_env_file=None)
    status = {"state": "idle", "file": None}
    sup = SimpleNamespace(processor=SimpleNamespace(recording_status=lambda: dict(status)))

    class _SlowDB(_DB):
        async def file_stats(self):
            status.update(state="recording", file="B.sc16")  # begun meanwhile
            return 10, 2

    ls = _LS([_sample(40, True)])
    before = time.time()
    await _storage_tick(s, StorageGovernor(), _SlowDB(), ls, sup, asyncio.Event())
    assert ls.evictions and ls.exclude_fn_seen == [{"B.sc16"}]
    # Both the sample and the eviction got the tick's start as not_after.
    assert len(ls.not_after_seen) == 2
    assert all(t is not None and before <= t <= time.time() for t in ls.not_after_seen)


# --- final review M4: a failing file_stats does not skip the tick -------------


async def test_a_failing_file_stats_still_ticks_and_evicts():
    class _BadStatsDB(_DB):
        async def file_stats(self):
            raise OSError("database is locked")

    s = AppSettings(_env_file=None)
    gov, ls = StorageGovernor(), _LS([_sample(40, True)])
    await _storage_tick(s, gov, _BadStatsDB(), ls, _sup(), asyncio.Event())
    assert gov.ticks == 1 and gov.state.step == 1
    assert ls.evictions  # the disk decision still acted
    assert gov.state.sample is not None and gov.state.sample.db_file_bytes == 0


# --- final review M5: a failed persist is retried on the next tick ------------


async def test_a_failed_flag_persist_is_retried_on_the_next_tick():
    class _FlakyDB(_DB):
        fails = 1

        async def set_config(self, k: str, v: str) -> None:
            if self.fails:
                self.fails -= 1
                raise OSError("disk I/O error")
            await super().set_config(k, v)

    s = AppSettings(_env_file=None)
    gov, db = StorageGovernor(), _FlakyDB()
    gov.report_write_error("ENOSPC: No space left on device")
    await _storage_tick(s, gov, db, _LS([_sample(200, True)]), _sup(), asyncio.Event())
    assert DEGRADED_CONFIG_KEY not in db.config  # the first persist failed
    await _storage_tick(s, gov, db, _LS([_sample(200, True)]), _sup(), asyncio.Event())
    assert db.config[DEGRADED_CONFIG_KEY] == gov.state.degraded_since.isoformat()
    assert "ENOSPC" in db.config[LAST_WRITE_ERROR_CONFIG_KEY]


async def test_a_clear_during_an_in_flight_persist_is_not_overwritten():
    """A tick persisting the old flag is mid-write when the flag is cleared:
    the cleared values must land last."""
    from rfobserver.storage.governor import persist_degraded_change

    release = asyncio.Event()

    class _SlowDB(_DB):
        async def set_config(self, k: str, v: str) -> None:
            if v and not release.is_set():
                await release.wait()  # the tick's write of the old flag stalls
            await super().set_config(k, v)

    s = AppSettings(_env_file=None)
    gov, db = StorageGovernor(), _SlowDB()
    gov.report_write_error("ENOSPC: No space left on device")
    tick = asyncio.ensure_future(
        _storage_tick(s, gov, db, _LS([_sample(200, True)]), _sup(), asyncio.Event())
    )
    await asyncio.sleep(0.01)  # the tick is now inside its write
    gov.clear_degraded()  # what the clear route does, then it persists
    clear = asyncio.ensure_future(persist_degraded_change(gov, db))
    await asyncio.sleep(0.01)
    release.set()
    await asyncio.gather(tick, clear)
    assert db.config[DEGRADED_CONFIG_KEY] == ""
    assert db.config[LAST_WRITE_ERROR_CONFIG_KEY] == ""


# --- task 10: warn when step 1 evicts captures soon after recording -----------


async def test_evicting_a_capture_with_a_fresh_mtime_notes_a_young_eviction():
    s = AppSettings(_env_file=None)
    gov = StorageGovernor()
    ls = _LS([_sample(40, True)], evict_ages=[("A.sc16", 42.0)])
    await _storage_tick(s, gov, _DB(), ls, _sup(), asyncio.Event())
    st = gov.state
    assert st.evicting_young is True
    assert st.young_evictions == 1
    assert st.youngest_evicted_age_sec == 42.0


async def test_evicting_only_old_captures_does_not_note_a_young_eviction():
    s = AppSettings(_env_file=None)
    gov = StorageGovernor()
    ls = _LS([_sample(40, True)], evict_ages=[("OLD.sc16", 9999.0)])
    await _storage_tick(s, gov, _DB(), ls, _sup(), asyncio.Event())
    assert gov.state.evicting_young is False
    assert gov.state.young_evictions == 0


async def test_no_eviction_never_notes_a_young_eviction():
    s = AppSettings(_env_file=None)
    gov = StorageGovernor()
    ls = _LS([_sample(200, True)])  # step 0: nothing evicted
    await _storage_tick(s, gov, _DB(), ls, _sup(), asyncio.Event())
    assert gov.state.evicting_young is False


async def test_evicting_young_captures_logs_a_warning_naming_each_one(caplog):
    s = AppSettings(_env_file=None)
    gov = StorageGovernor()
    ls = _LS([_sample(40, True)], evict_ages=[("X.sc16", 42.0), ("Y.sc16", 97.0)])
    with caplog.at_level(logging.WARNING, logger="rfobserver.pipeline.app"):
        await _storage_tick(s, gov, _DB(), ls, _sup(), asyncio.Event())
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert any("X.sc16" in m and "42" in m and "Y.sc16" in m and "97" in m for m in warnings), (
        warnings
    )
