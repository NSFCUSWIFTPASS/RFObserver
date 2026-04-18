"""Local NVMe file storage with FIFO rotation.

Manages IQ capture files on local storage, enforcing a maximum disk usage
limit by deleting oldest files first.
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


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

    def _enforce_limit(self, incoming_bytes: int) -> None:
        """Delete oldest files until there's room for incoming data."""
        files = sorted(self.storage_path.glob("*.sc16"), key=lambda f: f.stat().st_mtime)
        current_usage = sum(f.stat().st_size for f in files)

        while current_usage + incoming_bytes > self.max_bytes and files:
            oldest = files.pop(0)
            size = oldest.stat().st_size
            oldest.unlink()
            # Also remove companion .json and .npz if present
            oldest.with_suffix(".json").unlink(missing_ok=True)
            oldest.with_suffix(".npz").unlink(missing_ok=True)
            current_usage -= size
            logger.info("Rotated old file: %s (freed %d bytes)", oldest.name, size)

    def get_usage_bytes(self) -> int:
        """Return total bytes used by .sc16 files."""
        return sum(f.stat().st_size for f in self.storage_path.glob("*.sc16"))

    def get_usage_gb(self) -> float:
        return self.get_usage_bytes() / (1024**3)
