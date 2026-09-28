"""Tests for the crash-give-up-to-process-exit handler."""

import asyncio

import pytest

from rfobserver.pipeline.app import make_give_up_handler


@pytest.mark.asyncio
async def test_give_up_handler_exits_91_after_delay() -> None:
    codes: list[int] = []
    handler = make_give_up_handler(True, exit_fn=codes.append, delay_sec=0.05)
    handler()
    assert codes == [], "exit must be delayed so health can report the give-up"
    await asyncio.sleep(0.15)
    assert codes == [91]


@pytest.mark.asyncio
async def test_give_up_handler_disabled_never_exits() -> None:
    codes: list[int] = []
    make_give_up_handler(False, exit_fn=codes.append, delay_sec=0.0)()
    await asyncio.sleep(0.05)
    assert codes == []
