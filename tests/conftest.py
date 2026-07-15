"""Global test fixtures -- mock UHD before any rfobserver imports."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

# UHD is only available on systems with USRP hardware drivers installed.
# Mock it at the module level so all tests can import rfobserver without UHD.
if "uhd" not in sys.modules:
    sys.modules["uhd"] = MagicMock()


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--runslow",
        action="store_true",
        default=False,
        help="run tests marked @pytest.mark.slow",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.getoption("--runslow"):
        return
    skip_slow = pytest.mark.skip(reason="need --runslow to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)
