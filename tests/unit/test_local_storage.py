"""Tests for rfobserver.storage.local."""

from rfobserver.storage.local import LocalStorage


def test_save_capture(tmp_path):
    storage = LocalStorage(str(tmp_path), max_gb=1.0)
    data = b"\x00" * 1000
    path = storage.save_capture("test.sc16", data)
    assert path.exists()
    assert path.read_bytes() == data


def test_get_usage(tmp_path):
    storage = LocalStorage(str(tmp_path), max_gb=1.0)
    assert storage.get_usage_bytes() == 0
    storage.save_capture("test.sc16", b"\x00" * 500)
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
    storage = LocalStorage(str(path))
    assert path.exists()
