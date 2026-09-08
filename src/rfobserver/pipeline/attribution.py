"""rtl_433 per-burst attribution: discovery, decode of a channelized .cs16 blob,
a bounded drop-strongest queue, and the async worker that drains + merges. The
decode step is synchronous (subprocess); the worker calls it off the event loop.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile

logger = logging.getLogger(__name__)

_KNOWN_BUILD = os.path.expanduser("~/rtl_433_build/build/src/rtl_433")


def find_rtl433(override: str | None = None) -> str | None:
    """Locate rtl_433: explicit override, $RTL433, the known build path, PATH."""
    for cand in (override, os.environ.get("RTL433"), _KNOWN_BUILD, shutil.which("rtl_433")):
        if cand and os.path.exists(cand):
            return cand
    return None


def decode_cs16(
    rtl_path: str,
    cs16_bytes: bytes,
    target_rate_hz: int,
    passes: list[list[str]],
    timeout_sec: float = 30.0,
) -> list[dict]:
    """Run rtl_433 over the .cs16 blob, one pass at a time, returning the first
    pass that decodes anything. Empty list if nothing decodes."""
    with tempfile.NamedTemporaryFile(suffix=".cs16", delete=True) as tf:
        tf.write(cs16_bytes)
        tf.flush()
        for extra in passes:
            cmd = [rtl_path, "-s", f"{target_rate_hz}", "-F", "json", *extra, "-r", tf.name]
            try:
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
            except subprocess.TimeoutExpired:
                logger.warning("rtl_433 timed out after %.0fs", timeout_sec)
                continue
            frames = [
                json.loads(line)
                for line in proc.stdout.splitlines()
                if line.strip().startswith("{")
            ]
            if frames:
                return frames
    return []
