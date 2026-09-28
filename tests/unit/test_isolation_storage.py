"""Isolated bursts under the storage governor."""

from __future__ import annotations

import asyncio
import os
import shutil
from datetime import datetime, timezone
from types import SimpleNamespace

import numpy as np

from rfobserver.config import AppSettings
from rfobserver.pipeline.app import _storage_tick
from rfobserver.processing.isolate import IsolatedBurst
from rfobserver.storage.burst_archive import BurstArchive
from rfobserver.storage.governor import GB, StorageGovernor, StorageSample, VolumeSample
from rfobserver.storage.local import LocalStorage

T0 = datetime(2026, 9, 28, tzinfo=timezone.utc)


def _iso(bid):
    return IsolatedBurst(
        bid, np.zeros(2000, dtype="<i2").tobytes(), 1_000_000, [[]], 915e6, 0, 1, False
    )


def test_sample_counts_bursts_as_evictable_bytes(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    a = BurstArchive(tmp_path)
    a.save(_iso("b1"), {})
    s = ls.sample(db_path=tmp_path / "db", active_names=(), db_file_bytes=0, db_reusable_bytes=0)
    assert s.bursts_bytes == a.usage_bytes() > 0
    assert s.old_bursts_bytes == s.bursts_bytes  # no not_after: all of them can go
    # The governor, which knows the floor, decides whether bursts make step 1
    # possible; the capture flag stays about captures.
    assert s.evictable_auto is False


def test_sample_counts_only_bursts_written_before_not_after_as_old(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    a = BurstArchive(tmp_path)
    old = a.save(_iso("old"), {})
    os.utime(old, (1000, 1000))
    new = a.save(_iso("new"), {})
    os.utime(new, (3000, 3000))
    s = ls.sample(
        db_path=tmp_path / "db",
        active_names=(),
        db_file_bytes=0,
        db_reusable_bytes=0,
        not_after=2000,
    )
    assert s.bursts_bytes == a.usage_bytes()
    assert 0 < s.old_bursts_bytes < s.bursts_bytes
    assert s.old_bursts_bytes == BurstArchive._size(old)


def test_sample_without_bursts_does_not_create_the_folder(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    s = ls.sample(db_path=tmp_path / "db", active_names=(), db_file_bytes=0, db_reusable_bytes=0)
    assert s.bursts_bytes == 0 and s.old_bursts_bytes == 0
    assert s.evictable_auto is False
    assert not (tmp_path / "bursts").exists()


def _gov_sample(free_gb, old_bursts_gb, evictable_auto=False):
    return StorageSample(
        data=VolumeSample(int(free_gb * GB), 1000 * GB),
        db_volume=None,
        db_file_bytes=0,
        db_reusable_bytes=0,
        auto_bytes=0,
        manual_bytes=0,
        evictable_auto=evictable_auto,
        bursts_bytes=int(old_bursts_gb * GB),
        old_bursts_bytes=int(old_bursts_gb * GB),
    )


def test_bursts_that_can_reach_the_floor_make_step_1():
    gov = StorageGovernor()
    actions = gov.tick(_gov_sample(45, 6), min_free_gb=50, now=T0)
    assert gov.state.step == 1
    assert actions.evict_to_free_bytes == int(50 * GB * 1.15)


def test_bursts_that_cannot_reach_the_floor_do_not_hold_step_1():
    gov = StorageGovernor()
    actions = gov.tick(_gov_sample(45, 1), min_free_gb=50, now=T0)
    assert gov.state.step == 2
    # They are still deleted while short; they just do not stop escalation.
    assert actions.evict_to_free_bytes == int(50 * GB * 1.15)
    gov.tick(_gov_sample(45, 1), min_free_gb=50, now=T0)
    assert gov.state.step == 3


class _DB:
    async def file_stats(self):
        return 0, 0

    async def set_config(self, k, v):
        pass


async def test_storage_tick_evicts_bursts_before_captures(tmp_path, monkeypatch):
    s = AppSettings(_env_file=None, STORAGE_PATH=str(tmp_path), DISK_MIN_FREE_GB=10**6)
    ls = LocalStorage(str(tmp_path), max_gb=100)
    a = BurstArchive(tmp_path)
    for i in range(3):
        p = a.save(_iso(f"b{i}"), {})
        os.utime(p, (1000 + i, 1000 + i))
    order = []
    real_b = BurstArchive.evict_until_free
    real_c = LocalStorage.evict_until_free
    monkeypatch.setattr(
        BurstArchive,
        "evict_until_free",
        lambda self, *a, **k: order.append("bursts") or real_b(self, *a, **k),
    )
    monkeypatch.setattr(
        LocalStorage,
        "evict_until_free",
        lambda self, *a, **k: order.append("captures") or real_c(self, *a, **k),
    )
    sup = SimpleNamespace(processor=None)
    await _storage_tick(s, StorageGovernor(), _DB(), ls, sup, asyncio.Event())
    assert order[:2] == ["bursts", "captures"]
    assert BurstArchive(tmp_path).usage_bytes() == 0


async def test_storage_tick_keeps_captures_when_bursts_meet_the_target(tmp_path, monkeypatch):
    s = AppSettings(_env_file=None, STORAGE_PATH=str(tmp_path), DISK_MIN_FREE_GB=10**6)
    ls = LocalStorage(str(tmp_path), max_gb=100)
    BurstArchive(tmp_path).save(_iso("b1"), {})
    capture = ls.auto_dir / "old.sc16"
    capture.write_bytes(b"\0" * 4096)
    os.utime(capture, (1000, 1000))
    real_du = shutil.disk_usage
    bursts_root = tmp_path / "bursts"

    def fake_du(path):
        du = real_du(path)
        # Once the bursts are gone, report the volume as comfortably free.
        if not any(bursts_root.rglob("*.sigmf-data")):
            return du._replace(free=10**6 * 1024**3 * 2)
        return du

    monkeypatch.setattr(shutil, "disk_usage", fake_du)
    await _storage_tick(
        s, StorageGovernor(), _DB(), ls, SimpleNamespace(processor=None), asyncio.Event()
    )
    assert BurstArchive(tmp_path).usage_bytes() == 0
    assert capture.exists()


async def test_storage_tick_enforces_the_burst_cap(tmp_path):
    s = AppSettings(_env_file=None, STORAGE_PATH=str(tmp_path), BURST_ARCHIVE_MAX_GB=1e-9)
    ls = LocalStorage(str(tmp_path), max_gb=100)
    a = BurstArchive(tmp_path)
    a.save(_iso("b1"), {})
    await _storage_tick(
        s, StorageGovernor(), _DB(), ls, SimpleNamespace(processor=None), asyncio.Event()
    )
    assert a.usage_bytes() == 0


async def test_storage_tick_under_the_cap_does_not_walk_again(tmp_path, monkeypatch):
    s = AppSettings(_env_file=None, STORAGE_PATH=str(tmp_path), BURST_ARCHIVE_MAX_GB=2.0)
    ls = LocalStorage(str(tmp_path), max_gb=100)
    a = BurstArchive(tmp_path)
    a.save(_iso("b1"), {})
    calls = []
    monkeypatch.setattr(BurstArchive, "enforce_cap", lambda self, m: calls.append(m) or 0)
    await _storage_tick(
        s, StorageGovernor(), _DB(), ls, SimpleNamespace(processor=None), asyncio.Event()
    )
    assert calls == []
    assert a.usage_bytes() > 0


async def test_bursts_reappearing_every_tick_still_escalate_to_step_3(tmp_path):
    # Free space stays far below the floor while isolation keeps saving a few
    # small bursts between ticks: the ladder must still go 2 then 3 (which is
    # what stops burst saving), not sit at step 1 evicting a few KB a tick.
    # The floor is 1.5 x the real free space: short, but above step 4's half.
    floor_gb = shutil.disk_usage(tmp_path).free * 1.5 / GB
    s = AppSettings(_env_file=None, STORAGE_PATH=str(tmp_path), DISK_MIN_FREE_GB=floor_gb)
    ls = LocalStorage(str(tmp_path), max_gb=100)
    a = BurstArchive(tmp_path)
    gov, wake = StorageGovernor(), asyncio.Event()
    steps = []
    for tick in range(3):
        for i in range(3):
            p = a.save(_iso(f"t{tick}b{i}"), {})
            os.utime(p, (1000 + tick, 1000 + tick))
        await _storage_tick(s, gov, _DB(), ls, SimpleNamespace(processor=None), wake)
        steps.append(gov.state.step)
    assert steps == [2, 3, 3]
    assert wake.is_set()
    assert gov.state.refuse_recording
    # Old bursts were still deleted along the way.
    assert a.usage_bytes() == 0
