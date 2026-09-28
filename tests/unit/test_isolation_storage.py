"""Isolated bursts under the storage governor."""

from __future__ import annotations

import asyncio
import os
import shutil
from types import SimpleNamespace

import numpy as np

from rfobserver.config import AppSettings
from rfobserver.pipeline.app import _storage_tick
from rfobserver.processing.isolate import IsolatedBurst
from rfobserver.storage.burst_archive import BurstArchive
from rfobserver.storage.governor import StorageGovernor
from rfobserver.storage.local import LocalStorage


def _iso(bid):
    return IsolatedBurst(
        bid, np.zeros(2000, dtype="<i2").tobytes(), 1_000_000, [[]], 915e6, 0, 1, False
    )


def test_sample_counts_bursts_and_makes_them_evictable(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    a = BurstArchive(tmp_path)
    a.save(_iso("b1"), {})
    s = ls.sample(db_path=tmp_path / "db", active_names=(), db_file_bytes=0, db_reusable_bytes=0)
    assert s.bursts_bytes == a.usage_bytes() > 0
    assert s.evictable_auto is True  # no auto captures, but bursts can go


def test_sample_without_bursts_is_not_evictable(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    s = ls.sample(db_path=tmp_path / "db", active_names=(), db_file_bytes=0, db_reusable_bytes=0)
    assert s.bursts_bytes == 0
    assert s.evictable_auto is False


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
