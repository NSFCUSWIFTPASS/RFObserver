"""Local NVMe file storage with FIFO rotation.

Manages IQ capture files on local storage, enforcing a maximum disk usage
limit by deleting oldest files first.
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


# Suffixes of the companion files a streaming capture emits alongside its
# .sc16. A capture's full footprint (and what eviction must delete) is the
# .sc16 plus whichever of these exist. ".npz" is a legacy grid companion kept
# so old captures are also evicted cleanly.
_COMPANION_SUFFIXES = (".json", ".psd", ".psd.json", ".detections.json", ".npz")


class LocalStorage:
    """Manages IQ file storage with FIFO rotation."""

    def __init__(self, storage_path: str, max_gb: float = 50.0) -> None:
        self.storage_path = Path(storage_path)
        self.max_bytes = int(max_gb * 1024**3)
        self.storage_path.mkdir(parents=True, exist_ok=True)

    def save_capture(self, filename: str, data: bytes) -> Path:
        """Save raw IQ data to a file, rotating old files if needed."""
        self._enforce_limit(len(data))
        dest = self.storage_path / filename
        dest.write_bytes(data)
        logger.debug("Saved capture: %s (%d bytes)", filename, len(data))
        return dest

    def _companion_paths(self, sc16_path: Path) -> list[Path]:
        """Paths of the companion files for a capture (whether or not they exist)."""
        base = sc16_path.name[: -len(".sc16")]
        return [self.storage_path / f"{base}{suf}" for suf in _COMPANION_SUFFIXES]

    def _capture_size(self, sc16_path: Path) -> int:
        """Total bytes of a capture: its .sc16 plus every existing companion."""
        total = sc16_path.stat().st_size if sc16_path.exists() else 0
        for comp in self._companion_paths(sc16_path):
            if comp.exists():
                total += comp.stat().st_size
        return total

    def _delete_capture(self, sc16_path: Path) -> int:
        """Delete a capture and all its companions. Returns bytes freed."""
        freed = self._capture_size(sc16_path)
        sc16_path.unlink(missing_ok=True)
        for comp in self._companion_paths(sc16_path):
            comp.unlink(missing_ok=True)
        logger.info("Rotated old capture: %s (freed %d bytes)", sc16_path.name, freed)
        return freed

    def _enforce_limit(self, incoming_bytes: int) -> None:
        """Delete oldest captures until there's room for incoming data."""
        captures = sorted(self.storage_path.glob("*.sc16"), key=lambda f: f.stat().st_mtime)
        usage = sum(self._capture_size(c) for c in captures)
        while usage + incoming_bytes > self.max_bytes and captures:
            oldest = captures.pop(0)
            usage -= self._delete_capture(oldest)

    def enforce_cap(self) -> None:
        """Evict oldest captures until total footprint is within the disk cap.

        Unlike ``_enforce_limit`` (which reserves room for a pending write), this
        operates on captures already on disk and never deletes the single newest
        capture, so a just-finalized recording is always kept even if it alone
        exceeds the cap. Called after each streaming recording so continuous
        triggering stays bounded by ARCHIVE_MAX_GB.
        """
        captures = sorted(self.storage_path.glob("*.sc16"), key=lambda f: f.stat().st_mtime)
        usage = sum(self._capture_size(c) for c in captures)
        while usage > self.max_bytes and len(captures) > 1:
            oldest = captures.pop(0)
            usage -= self._delete_capture(oldest)

    def get_usage_bytes(self) -> int:
        """Return total bytes used by captures (.sc16 files and their companions)."""
        return sum(self._capture_size(c) for c in self.storage_path.glob("*.sc16"))

    def get_usage_gb(self) -> float:
        return self.get_usage_bytes() / (1024**3)
