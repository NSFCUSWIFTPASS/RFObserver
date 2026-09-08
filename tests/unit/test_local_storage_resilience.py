"""Resilience tests for LocalStorage eviction: unlink failures must not abort FIFO rotation."""

import os
from pathlib import Path

from rfobserver.storage.local import LocalStorage


def _mk_capture(d: Path, name: str, size: int = 1024) -> Path:
    p = d / name
    p.write_bytes(b"\x00" * size)
    return p


def test_enforce_cap_survives_unlink_error(tmp_path, monkeypatch):
    st = LocalStorage(str(tmp_path), max_gb=0.0)  # cap 0 -> evict all but newest
    auto = st.auto_dir
    auto.mkdir(parents=True, exist_ok=True)
    a = _mk_capture(auto, "a.sc16")
    b = _mk_capture(auto, "b.sc16")

    # Force an explicit mtime gap so eviction order is deterministic: a is
    # older and would normally be evicted first.
    now = os.stat(a).st_mtime
    os.utime(a, (now - 10, now - 10))
    os.utime(b, (now, now))

    real_unlink = Path.unlink

    def flaky_unlink(self, *args, **kwargs):
        if self.name == "a.sc16":
            raise OSError("Read-only file system")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", flaky_unlink)
    # Must not raise even though deleting a.sc16 fails.
    st.enforce_cap()
    # a could not be removed (unlink raised), but the call survived and did
    # not abort eviction.
    assert a.exists()
    # b is the newest capture; enforce_cap never deletes the single newest.
    assert b.exists()


def test_delete_capture_returns_freed_bytes_when_some_unlinks_fail(tmp_path, monkeypatch):
    st = LocalStorage(str(tmp_path), max_gb=50.0)
    auto = st.auto_dir
    auto.mkdir(parents=True, exist_ok=True)
    sc16 = _mk_capture(auto, "c.sc16", size=2048)
    json_companion = auto / "c.json"
    json_companion.write_bytes(b"\x00" * 100)

    real_unlink = Path.unlink

    def flaky_unlink(self, *args, **kwargs):
        if self.suffix == ".sc16":
            raise OSError("Device busy")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", flaky_unlink)

    freed = st._delete_capture(sc16)

    # freed reflects the size computed before deletion attempts, even though
    # the .sc16 itself survived the failed unlink.
    assert freed == 2048 + 100
    assert sc16.exists()  # unlink failed
    assert not json_companion.exists()  # companion unlink succeeded
