"""Global test fixtures -- mock UHD before any rfobserver imports."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

# UHD is only available on systems with USRP hardware drivers installed.
# Mock it at the module level so all tests can import rfobserver without UHD.
if "uhd" not in sys.modules:
    sys.modules["uhd"] = MagicMock()
