"""_storage_loop: sample, tick, evict, wake retention, persist the sticky flag."""

from __future__ import annotations

import asyncio
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
    def __init__(self, samples: list[StorageSample]) -> None:
        self.samples = samples
        self.evictions: list[tuple[int, set[str]]] = []
        self.active_seen: list[set[str]] = []

    def sample(self, *, db_path, active_names, db_file_bytes, db_reusable_bytes):
        self.active_seen.append(set(active_names))
        return self.samples.pop(0)

    def evict_until_free(self, target, *, exclude=()):
        self.evictions.append((target, set(exclude)))
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
