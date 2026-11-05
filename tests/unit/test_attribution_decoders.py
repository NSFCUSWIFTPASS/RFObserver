"""Attribution decoders switched on and off one at a time (ATTRIBUTION_DECODERS)."""

from __future__ import annotations

import asyncio
import json

import pytest

from rfobserver.pipeline import attribution as attr
from rfobserver.pipeline.attribution import (
    ALL_DECODERS,
    AttributionItem,
    AttributionWorker,
    StrongestQueue,
    parse_decoders,
)
from rfobserver.processing.channelize import select_rate_and_protocols

SSM_FRAME = {"model": "SSM", "src_id": "0200574952470001", "bit_errors": 0}


def _item(passes):
    return AttributionItem("b1", b"\0\0" * 10, 1_600_000, passes, -30.0)


WIDE = select_rate_and_protocols(500_000)[1]  # -R 383, then the ssnmesh-like flex
NARROW = select_rate_and_protocols(50_000)[1]  # rtl_433's default set


def _recorder(seen):
    def decode(rtl, cs16, rate, passes):
        seen.append(passes)
        return []

    return decode


async def _run(item, enabled, rtl="/bin/true"):
    got, outcomes = [], []

    async def sink(r):
        got.append(r)

    q = StrongestQueue(4)
    w = AttributionWorker(
        None,
        rtl,
        queue=q,
        sinks=[sink],
        on_outcome=outcomes.append,
        decoders=lambda: frozenset(enabled),
    )
    q.put_nowait(item)
    task = asyncio.create_task(w.run())
    for _ in range(50):
        await asyncio.sleep(0.01)
        if got or outcomes:
            break
    task.cancel()
    return got, outcomes


def test_parse_decoders_ignores_case_spaces_and_unknown_names():
    assert parse_decoders(" SSM, ssnmesh ,bogus,") == frozenset({"ssm", "ssnmesh"})
    assert parse_decoders("") == frozenset()
    assert parse_decoders(",".join(ALL_DECODERS)) == frozenset(ALL_DECODERS)


async def test_ssm_off_does_not_run_the_ssm_decoder(monkeypatch):
    def no_ssm(*a):
        pytest.fail("SSM decoder is switched off")

    monkeypatch.setattr(attr, "decode_ssm", no_ssm)
    monkeypatch.setattr(attr, "decode_cs16", lambda *a, **k: [{"model": "SilverSpring-Mesh"}])
    got, _ = await _run(_item(WIDE), {"silverspring", "ssnmesh", "rtl433"})
    assert got[0].model == "SilverSpring-Mesh"


async def test_silverspring_off_drops_its_rtl433_pass(monkeypatch):
    seen = []
    monkeypatch.setattr(attr, "decode_ssm", lambda *a: None)
    monkeypatch.setattr(attr, "decode_cs16", _recorder(seen))
    await _run(_item(WIDE), {"ssm", "ssnmesh", "rtl433"})
    assert seen == [[p for p in WIDE if "-X" in p]]


async def test_ssnmesh_off_drops_the_flex_pass(monkeypatch):
    seen = []
    monkeypatch.setattr(attr, "decode_ssm", lambda *a: None)
    monkeypatch.setattr(attr, "decode_cs16", _recorder(seen))
    await _run(_item(WIDE), {"ssm", "silverspring", "rtl433"})
    assert seen == [[["-R", "383"]]]


async def test_rtl433_off_drops_the_default_set_on_narrow_bursts(monkeypatch):
    monkeypatch.setattr(attr, "decode_ssm", lambda *a: None)

    def no_rtl(*a, **k):
        pytest.fail("no rtl_433 pass is switched on for this burst")

    monkeypatch.setattr(attr, "decode_cs16", no_rtl)
    got, outcomes = await _run(_item(NARROW), {"ssm", "silverspring", "ssnmesh"})
    # SSM was tried, so the burst was attempted.
    assert got[0].outcome == "not_decoded" and outcomes == ["not_decoded"]


async def test_a_disabled_model_from_the_default_set_is_not_a_label(monkeypatch):
    monkeypatch.setattr(attr, "decode_ssm", lambda *a: None)
    monkeypatch.setattr(attr, "decode_cs16", lambda *a, **k: [{"model": "SilverSpring-Mesh"}])
    got, _ = await _run(_item(NARROW), {"ssm", "ssnmesh", "rtl433"})
    assert got[0].model is None and got[0].outcome == "not_decoded"


async def test_all_off_is_skipped_with_no_attempted_mark(monkeypatch):
    def never(*a, **k):
        pytest.fail("every decoder is switched off")

    monkeypatch.setattr(attr, "decode_ssm", never)
    monkeypatch.setattr(attr, "decode_cs16", never)
    got, outcomes = await _run(_item(WIDE), set())
    assert got == [] and outcomes == ["skipped"]


async def test_without_rtl433_ssm_still_decodes(monkeypatch):
    monkeypatch.setattr(attr, "decode_ssm", lambda *a: SSM_FRAME)
    got, _ = await _run(_item(WIDE), set(ALL_DECODERS), rtl=None)
    assert got[0].model == "SSM"


async def test_without_rtl433_an_ssm_miss_is_not_decoded(monkeypatch):
    monkeypatch.setattr(attr, "decode_ssm", lambda *a: None)

    def no_rtl(*a, **k):
        pytest.fail("rtl_433 is not installed")

    monkeypatch.setattr(attr, "decode_cs16", no_rtl)
    got, _ = await _run(_item(WIDE), set(ALL_DECODERS), rtl=None)
    assert got[0].outcome == "not_decoded"


async def test_the_switches_are_read_for_every_burst(monkeypatch):
    enabled = {"ssm"}
    monkeypatch.setattr(attr, "decode_ssm", lambda *a: SSM_FRAME)
    got, outcomes = [], []

    async def sink(r):
        got.append(r)

    q = StrongestQueue(4)
    w = AttributionWorker(
        None,
        "/bin/true",
        queue=q,
        sinks=[sink],
        on_outcome=outcomes.append,
        decoders=lambda: frozenset(enabled),
    )
    task = asyncio.create_task(w.run())
    q.put_nowait(_item(WIDE))
    for _ in range(50):
        await asyncio.sleep(0.01)
        if got:
            break
    enabled.clear()
    q.put_nowait(_item(WIDE))
    for _ in range(50):
        await asyncio.sleep(0.01)
        if len(outcomes) == 2:
            break
    task.cancel()
    assert outcomes == ["decoded", "skipped"]
    assert json.loads(got[0].attribution)["frames"] == [SSM_FRAME]
