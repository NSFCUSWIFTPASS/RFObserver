"""Isolated bursts saved as SigMF, capped and evicted oldest first."""

from __future__ import annotations

import json
import os

import numpy as np

from rfobserver.modules.base import ParamDescriptor, UpstreamModule
from rfobserver.modules.manager import ModuleManager
from rfobserver.processing.isolate import IsolatedBurst
from rfobserver.storage.burst_archive import BurstArchive


def _iso(bid: str, n: int = 100) -> IsolatedBurst:
    cs16 = np.arange(2 * n, dtype="<i2").tobytes()
    return IsolatedBurst(bid, cs16, 1_600_000, [["-R", "383"]], 919.4e6, 1234, 5000, False)


def test_save_writes_a_loadable_sigmf_pair(tmp_path):
    a = BurstArchive(tmp_path)
    data = a.save(_iso("b1"), {"rfobs:snr_db": 40.0, "core:datetime": "2026-09-28T00:00:00Z"})
    assert data.name == "b1.sigmf-data" and data.parent.parent == a.root
    meta = json.loads(data.with_suffix(".sigmf-meta").read_text())
    g = meta["global"]
    assert g["core:datatype"] == "ci16_le" and g["core:sample_rate"] == 1_600_000
    assert g["rfobs:burst_id"] == "b1" and g["rfobs:snr_db"] == 40.0
    assert meta["captures"][0]["core:frequency"] == 919.4e6
    assert data.read_bytes() == _iso("b1").cs16
    import sigmf  # the official library

    rec = sigmf.sigmffile.fromfile(str(data.with_suffix(".sigmf-meta")))
    assert rec.get_global_field("core:sample_rate") == 1_600_000


def test_save_leaves_no_tmp_file_behind(tmp_path):
    a = BurstArchive(tmp_path)
    a.save(_iso("b1"), {})
    assert list(a.root.rglob("*.tmp")) == []


def test_replay_bursts_go_to_their_own_subdir(tmp_path):
    a = BurstArchive(tmp_path)
    p = a.save(_iso("b2"), {}, subdir="replay-feb4")
    assert p.parent == a.root / "replay-feb4"


def test_cap_deletes_oldest_pairs_first(tmp_path):
    a = BurstArchive(tmp_path)
    paths = []
    for i in range(4):
        p = a.save(_iso(f"b{i}", n=1000), {})
        os.utime(p, (1000 + i, 1000 + i))
        os.utime(p.with_suffix(".sigmf-meta"), (1000 + i, 1000 + i))
        paths.append(p)
    one = paths[0].stat().st_size + paths[0].with_suffix(".sigmf-meta").stat().st_size
    freed = a.enforce_cap(2 * one + 10)
    assert freed >= 2 * one - 10
    assert not paths[0].exists() and not paths[1].exists()
    assert not paths[0].with_suffix(".sigmf-meta").exists()
    assert paths[2].exists() and paths[3].exists()


def test_evict_until_free_stops_at_the_target(tmp_path):
    a = BurstArchive(tmp_path)
    for i in range(3):
        p = a.save(_iso(f"b{i}", n=1000), {})
        os.utime(p, (1000 + i, 1000 + i))
    state = {"free": 0}

    def free():
        return state["free"]

    orig = a._delete_pair

    def counting(p):
        n = orig(p)
        state["free"] += n
        return n

    a._delete_pair = counting
    a.evict_until_free(1, free_bytes=free)
    assert len(list(a.root.rglob("*.sigmf-data"))) == 2


def test_usage_counts_both_files(tmp_path):
    a = BurstArchive(tmp_path)
    p = a.save(_iso("b1"), {})
    assert a.usage_bytes() == p.stat().st_size + p.with_suffix(".sigmf-meta").stat().st_size


class _Rec(UpstreamModule):
    # Ruling A: base.py identifies module classes with `module_type`, not `kind`.
    module_type = "rec"

    def __init__(self):
        super().__init__({})
        self.got = []

    @classmethod
    def parameters(cls) -> list[ParamDescriptor]:
        return []

    def configure(self, params):
        pass

    def feed(self, sc16_buf, center_freq_hz, sample_rate):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def status(self):
        return {}

    def feed_burst(self, iq, sample_rate, meta):
        self.got.append((len(iq), sample_rate, meta["burst_id"]))


def test_modules_receive_isolated_bursts_and_default_is_a_noop():
    mm = ModuleManager()
    rec = _Rec()
    mm._modules["r"] = rec
    mm.feed_bursts(np.zeros(10, dtype=np.complex64), 1_600_000, {"burst_id": "b1"})
    assert rec.got == [(10, 1_600_000, "b1")]
    # A module without feed_burst (like fm_demod) is unaffected.
    UpstreamModule.feed_burst(rec, np.zeros(1, dtype=np.complex64), 1, {})
