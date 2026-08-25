"""Tests for rfobserver.storage.local."""

from rfobserver.storage.local import LocalStorage


def test_save_capture(tmp_path):
    storage = LocalStorage(str(tmp_path), max_gb=1.0)
    data = b"\x00" * 1000
    path = storage.save_capture("test.sc16", data)
    assert path.exists()
    assert path.read_bytes() == data


def test_get_usage(tmp_path):
    # Usage tracks the managed (auto) set, which is what the cap bounds.
    storage = LocalStorage(str(tmp_path), max_gb=1.0)
    assert storage.get_usage_bytes() == 0
    (storage.auto_dir / "test.sc16").write_bytes(b"\x00" * 500)
    assert storage.get_usage_bytes() == 500


def test_fifo_rotation(tmp_path):
    import time

    # 1 KB limit
    storage = LocalStorage(str(tmp_path), max_gb=1024 / (1024**3))
    storage.save_capture("a.sc16", b"\x00" * 400)
    time.sleep(0.05)  # ensure different mtime
    storage.save_capture("b.sc16", b"\x00" * 400)
    time.sleep(0.05)
    # This should rotate oldest file(s) to make room
    storage.save_capture("c.sc16", b"\x00" * 400)

    files = list(tmp_path.glob("*.sc16"))
    names = {f.name for f in files}
    # Oldest file(s) should be removed, newest should exist
    assert "c.sc16" in names
    total = sum(f.stat().st_size for f in files)
    assert total <= 1024


def test_creates_directory(tmp_path):
    path = tmp_path / "sub" / "dir"
    LocalStorage(str(path))
    assert path.exists()


def _write_capture(storage, base, sc16_bytes, sub="auto"):
    """Write a full capture set (.sc16 + companions) into a storage subdir."""
    d = getattr(storage, f"{sub}_dir")
    (d / f"{base}.sc16").write_bytes(b"\x00" * sc16_bytes)
    (d / f"{base}.psd").write_bytes(b"\x00" * sc16_bytes)  # grid is comparable in size
    (d / f"{base}.psd.json").write_text("{}")
    (d / f"{base}.json").write_text("{}")
    (d / f"{base}.detections.json").write_text("[]")


def test_creates_auto_and_manual_subdirs(tmp_path):
    storage = LocalStorage(str(tmp_path), max_gb=1.0)
    assert storage.auto_dir.is_dir()
    assert storage.manual_dir.is_dir()
    assert storage.auto_dir == tmp_path / "auto"
    assert storage.manual_dir == tmp_path / "manual"


def test_get_usage_counts_companions(tmp_path):
    """Usage must count the whole capture footprint, not just the .sc16."""
    storage = LocalStorage(str(tmp_path), max_gb=1.0)
    _write_capture(storage, "a", 500)
    # 500 (.sc16) + 500 (.psd) + 2 (.psd.json) + 2 (.json) + 2 (.detections.json)
    assert storage.get_usage_bytes() == 1006


def test_get_usage_counts_auto_only(tmp_path):
    """Manual captures are outside the managed budget."""
    storage = LocalStorage(str(tmp_path), max_gb=1.0)
    _write_capture(storage, "a", 500, sub="auto")
    _write_capture(storage, "m", 9000, sub="manual")
    assert storage.get_usage_bytes() == 1006  # only the auto capture


def test_enforce_cap_evicts_oldest_full_set(tmp_path):
    import time

    # Cap ~1.5 KB; two captures at ~1 KB each -> the oldest must go entirely.
    storage = LocalStorage(str(tmp_path), max_gb=1500 / (1024**3))
    _write_capture(storage, "old", 500)
    time.sleep(0.05)  # ensure a later mtime
    _write_capture(storage, "new", 500)

    storage.enforce_cap()

    names = {p.name for p in storage.auto_dir.iterdir()}
    # Every companion of the oldest capture is gone.
    for suf in (".sc16", ".psd", ".psd.json", ".json", ".detections.json"):
        assert f"old{suf}" not in names
    # The newest capture survives intact.
    assert "new.sc16" in names
    assert "new.psd" in names
    assert "new.detections.json" in names
    assert storage.get_usage_bytes() <= storage.max_bytes


def test_enforce_cap_keeps_newest_even_when_over_cap(tmp_path):
    # A single capture larger than the cap is never deleted (nothing older to evict).
    storage = LocalStorage(str(tmp_path), max_gb=100 / (1024**3))
    _write_capture(storage, "only", 500)
    storage.enforce_cap()
    assert (storage.auto_dir / "only.sc16").exists()


def test_enforce_cap_never_evicts_manual(tmp_path):
    import time

    # Auto over cap, plus a large manual capture that must be left untouched.
    storage = LocalStorage(str(tmp_path), max_gb=1500 / (1024**3))
    _write_capture(storage, "m", 9000, sub="manual")
    _write_capture(storage, "old", 500, sub="auto")
    time.sleep(0.05)
    _write_capture(storage, "new", 500, sub="auto")

    storage.enforce_cap()

    # Manual capture and all companions survive regardless of the cap.
    for suf in (".sc16", ".psd", ".psd.json", ".json", ".detections.json"):
        assert (storage.manual_dir / f"m{suf}").exists()
    # Oldest auto capture evicted; newest auto kept.
    assert not (storage.auto_dir / "old.sc16").exists()
    assert (storage.auto_dir / "new.sc16").exists()


def test_migrate_flat_captures_to_manual(tmp_path):
    # Pre-seed a legacy capture at the storage root, then construct.
    root = tmp_path
    root.mkdir(exist_ok=True)
    (root / "legacy.sc16").write_bytes(b"\x00" * 800)
    (root / "legacy.psd").write_bytes(b"\x00" * 400)
    (root / "legacy.json").write_text("{}")

    storage = LocalStorage(str(root), max_gb=1.0)  # migration runs at construction

    # Moved into manual/, root cleared.
    assert not (root / "legacy.sc16").exists()
    assert (storage.manual_dir / "legacy.sc16").exists()
    assert (storage.manual_dir / "legacy.psd").exists()
    assert (storage.manual_dir / "legacy.json").exists()
