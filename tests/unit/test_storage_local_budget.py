"""LocalStorage under the governor: sampling and eviction to a free target."""

from __future__ import annotations

import os
from pathlib import Path

from rfobserver.storage.local import LocalStorage, is_active_capture


def _cap(d: Path, name: str, size: int, mtime: float) -> Path:
    sc16 = d / f"{name}.sc16"
    sc16.write_bytes(b"\0" * size)
    (d / f"{name}.json").write_text("{}")
    (d / f"{name}.psd").write_bytes(b"\0" * 8)
    for p in (sc16, d / f"{name}.json", d / f"{name}.psd"):
        os.utime(p, (mtime, mtime))
    return sc16


class _Disk:
    """Free space that grows as captures are deleted."""

    def __init__(self, ls: LocalStorage, free: int) -> None:
        self.ls, self.base = ls, free
        self.start = self._used()

    def _used(self) -> int:
        return sum(p.stat().st_size for p in self.ls.storage_path.rglob("*") if p.is_file())

    def __call__(self) -> int:
        return self.base + (self.start - self._used())


def test_evicts_oldest_first_until_the_target(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    a = _cap(ls.auto_dir, "A", 1000, 1)
    b = _cap(ls.auto_dir, "B", 1000, 2)
    c = _cap(ls.auto_dir, "C", 1000, 3)
    disk = _Disk(ls, free=100)
    freed = ls.evict_until_free(1500, free_bytes=disk)
    assert not a.exists() and not b.exists() and c.exists()
    assert not (ls.auto_dir / "A.json").exists()  # companions go too
    assert freed == 2 * (1000 + 2 + 8)


def test_stops_when_nothing_is_left_to_evict(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    _cap(ls.auto_dir, "A", 1000, 1)
    freed = ls.evict_until_free(10**12, free_bytes=lambda: 0)
    assert freed > 0 and list(ls.auto_dir.glob("*.sc16")) == []


def test_manual_captures_are_never_evicted(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    m = _cap(ls.manual_dir, "M", 1000, 0)
    ls.evict_until_free(10**12, free_bytes=lambda: 0)
    assert m.exists()


def test_evict_never_takes_the_active_capture_or_its_drop_renamed_name(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    active = _cap(ls.auto_dir, "S-host-20260923T100000", 1000, 1)
    renamed = _cap(ls.auto_dir, "S-host-20260923T090000_drop3", 1000, 0)
    other = _cap(ls.auto_dir, "S-host-20260923T080000", 1000, 0)
    ls.evict_until_free(
        10**12,
        exclude={"S-host-20260923T100000.sc16", "S-host-20260923T090000.sc16"},
        free_bytes=lambda: 0,
    )
    assert active.exists() and renamed.exists() and not other.exists()


def test_is_active_capture():
    act = {"X-h-1.sc16"}
    assert is_active_capture("X-h-1.sc16", act)
    assert is_active_capture("X-h-1_drop12.sc16", act)
    assert not is_active_capture("X-h-10.sc16", act)
    assert not is_active_capture("X-h-1.sc16", set())


def test_sample_counts_auto_and_manual_and_evictability(tmp_path):
    ls = LocalStorage(str(tmp_path / "s"), max_gb=100)
    _cap(ls.auto_dir, "A", 1000, 1)
    _cap(ls.manual_dir, "M", 500, 1)
    s = ls.sample(
        db_path=tmp_path / "s" / "db.sqlite",
        active_names=set(),
        db_file_bytes=7,
        db_reusable_bytes=3,
    )
    assert s.auto_bytes == 1000 + 2 + 8
    assert s.manual_bytes == 500 + 2 + 8
    assert s.evictable_auto is True
    assert s.db_volume is None  # same device
    assert s.db_file_bytes == 7 and s.db_reusable_bytes == 3
    assert 0 < s.data.free_bytes <= s.data.total_bytes
    only_active = ls.sample(
        db_path=tmp_path / "s" / "db.sqlite",
        active_names={"A.sc16"},
        db_file_bytes=0,
        db_reusable_bytes=0,
    )
    assert only_active.evictable_auto is False


def test_sample_reports_a_db_on_another_device(tmp_path, monkeypatch):
    ls = LocalStorage(str(tmp_path / "s"), max_gb=100)
    real_stat = os.stat
    db_dir = tmp_path / "dbvol"
    db_dir.mkdir()

    class _S:
        def __init__(self, st, dev):
            self._st, self.st_dev = st, dev

        def __getattr__(self, k):
            return getattr(self._st, k)

    def fake_stat(p, *a, **k):
        st = real_stat(p, *a, **k)
        return _S(st, 999) if Path(p) == db_dir else st

    monkeypatch.setattr("rfobserver.storage.local.os.stat", fake_stat)
    s = ls.sample(db_path=db_dir / "x.db", active_names=(), db_file_bytes=0, db_reusable_bytes=0)
    assert s.db_volume is not None and s.db_volume.total_bytes > 0


def test_sample_tolerates_a_capture_deleted_mid_walk(tmp_path, monkeypatch):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    gone = _cap(ls.auto_dir, "GONE", 1000, 1)
    _cap(ls.auto_dir, "KEEP", 1000, 2)
    real = LocalStorage._capture_size

    def racing(self, p):
        if p.name == "GONE.sc16":
            gone.unlink()
            p.stat()  # raises FileNotFoundError, as a real race would
        return real(self, p)

    monkeypatch.setattr(LocalStorage, "_capture_size", racing)
    s = ls.sample(db_path=tmp_path / "db", active_names=(), db_file_bytes=0, db_reusable_bytes=0)
    assert s.auto_bytes == 1000 + 2 + 8


def test_manual_usage(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=100)
    _cap(ls.manual_dir, "M", 500, 1)
    assert ls.get_manual_usage_bytes() == 500 + 2 + 8
