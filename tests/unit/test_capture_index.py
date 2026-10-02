"""The in-memory capture index in LocalStorage.

Per-tick directory walks over a large archive held the GIL long enough to
starve the USB receiver (docs/debugging/2026-10-01_hcro-overflows-storage-scans.md).
The index replaces them: one scan when storage is opened, then tracking.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from rfobserver.storage.local import LocalStorage, _capture_base


def _write(d: Path, base: str, size: int, mtime: float, companions: bool = True) -> Path:
    sc16 = d / f"{base}.sc16"
    sc16.write_bytes(b"\0" * size)
    files = [sc16]
    if companions:
        for suf, n in ((".json", 3), (".psd", 16), (".psd.json", 5), (".detections.json", 2)):
            p = d / f"{base}{suf}"
            p.write_bytes(b"\0" * n)
            files.append(p)
    for p in files:
        os.utime(p, (mtime, mtime))
    return sc16


def _snapshot(ls: LocalStorage) -> tuple:
    return (
        ls.capture_count(),
        ls.get_usage_bytes(),
        ls.get_manual_usage_bytes(),
        [(e.path, e.mtime, e.size) for e in ls.captures_newest_first()],
    )


@pytest.mark.parametrize(
    ("name", "base"),
    [
        ("x.sc16", "x"),
        ("x.json", "x"),
        ("x.psd", "x"),
        ("x.psd.json", "x"),  # not "x.psd"
        ("x.detections.json", "x"),  # not "x.detections"
        ("x.npz", "x"),
        ("x.sigmf-meta", None),
        ("notes.txt", None),
    ],
)
def test_capture_base(name, base):
    assert _capture_base(name) == base


def test_startup_scan_counts_companions_and_ignores_orphans(tmp_path):
    root = tmp_path / "s"
    (root / "auto").mkdir(parents=True)
    (root / "manual").mkdir()
    _write(root / "auto", "a", 100, 1)
    _write(root / "manual", "m", 50, 2, companions=False)
    (root / "auto" / "orphan.psd").write_bytes(b"\0" * 999)  # no .sc16: not a capture
    ls = LocalStorage(str(root), max_gb=1.0)
    assert ls.capture_count() == 2
    assert ls.get_usage_bytes() == 100 + 3 + 16 + 5 + 2
    assert ls.get_manual_usage_bytes() == 50


def test_tracking_matches_a_fresh_scan_through_adds_evictions_and_resizes(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=1.0)
    for i in range(6):
        ls.track(_write(ls.auto_dir, f"a{i}", 100 + i, 10 + i))
    ls.track(_write(ls.manual_dir, "m0", 70, 5))
    # A companion written later (the deferred detections sidecar) is re-measured.
    (ls.auto_dir / "a5.detections.json").write_bytes(b"\0" * 40)
    ls.track(ls.auto_dir / "a5.sc16")
    # Cap to about three auto captures: the oldest go, manual is untouched.
    ls.max_bytes = 3 * 140
    ls.enforce_cap()
    assert _snapshot(ls) == _snapshot(LocalStorage(str(tmp_path), max_gb=1.0))
    assert not (ls.auto_dir / "a0.sc16").exists()
    assert (ls.manual_dir / "m0.sc16").exists()


def test_track_of_a_missing_capture_drops_it(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=1.0)
    sc16 = _write(ls.auto_dir, "a", 100, 1)
    ls.track(sc16)
    for p in ls.auto_dir.iterdir():
        p.unlink()
    assert ls.track(sc16) is None
    assert ls.capture_count() == 0 and ls.get_usage_bytes() == 0


def test_track_ignores_paths_outside_auto_and_manual(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=1.0)
    stray = tmp_path / "elsewhere.sc16"
    stray.write_bytes(b"\0" * 10)
    assert ls.track(stray) is None
    assert ls.capture_count() == 0


def test_newest_first_pages_across_auto_and_manual(tmp_path):
    ls = LocalStorage(str(tmp_path), max_gb=1.0)
    ls.track(_write(ls.auto_dir, "a1", 10, 1))
    ls.track(_write(ls.manual_dir, "m2", 10, 2))
    ls.track(_write(ls.auto_dir, "a3", 10, 3))
    ls.track(_write(ls.manual_dir, "m4", 10, 4))
    names = [e.path.name for e in ls.captures_newest_first()]
    assert names == ["m4.sc16", "a3.sc16", "m2.sc16", "a1.sc16"]
    page = ls.captures_newest_first(offset=1, limit=2)
    assert [(e.path.name, e.origin) for e in page] == [("a3.sc16", "auto"), ("m2.sc16", "manual")]


def test_hot_paths_never_walk_the_directories(tmp_path, monkeypatch):
    """sample, enforce_cap, evict_until_free, the counts and the listing read
    the index; only opening the storage scans."""
    ls = LocalStorage(str(tmp_path), max_gb=1.0)
    for i in range(5):
        ls.track(_write(ls.auto_dir, f"a{i}", 100, i + 1))

    def boom(*_a, **_k):
        raise AssertionError("directory walk on a hot path")

    monkeypatch.setattr(os, "scandir", boom)
    monkeypatch.setattr(Path, "glob", boom)
    monkeypatch.setattr(Path, "rglob", boom)
    monkeypatch.setattr(Path, "iterdir", boom)
    monkeypatch.setattr(
        "rfobserver.storage.local.BurstArchive.scan_usage", staticmethod(lambda *_a: (0, 0))
    )
    ls.sample(db_path=tmp_path / "db", active_names=(), db_file_bytes=0, db_reusable_bytes=0)
    ls.capture_count()
    ls.get_usage_bytes()
    ls.get_manual_usage_bytes()
    ls.captures_newest_first(0, 2)
    ls.max_bytes = 250
    ls.enforce_cap()
    ls.evict_until_free(10**15, free_bytes=lambda: 0)
    assert ls.capture_count() == 0  # evict_until_free may take even the newest


def test_enforce_cap_and_evict_never_double_delete(tmp_path):
    """A victim is claimed (removed from the index) before its files are
    deleted, so two evictors cannot both count the same capture as freed."""
    ls = LocalStorage(str(tmp_path), max_gb=1.0)
    for i in range(4):
        ls.track(_write(ls.auto_dir, f"a{i}", 100, i + 1, companions=False))
    first = ls.captures_newest_first()[-1].path
    assert ls._delete_capture(first) == 100
    assert ls._delete_capture(first) == 0  # already claimed and gone
    assert ls.get_usage_bytes() == 300
