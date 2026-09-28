"""Isolated bursts on disk: one SigMF pair per burst under <storage>/bursts/.

Live bursts go into a folder per UTC day; replay bursts into
``replay-<capture stem>/`` so they never mix with the sensor's own. Bounded by
BURST_ARCHIVE_MAX_GB (oldest first), and evicted before automatic captures when
the storage governor needs space.
"""

from __future__ import annotations

import contextlib
import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rfobserver.storage.psd_grid import write_text_atomic
from rfobserver.storage.sigmf_export import SIGMF_VERSION

if TYPE_CHECKING:
    from collections.abc import Callable

    from rfobserver.processing.isolate import IsolatedBurst

logger = logging.getLogger(__name__)


class BurstArchive:
    def __init__(self, storage_path: str | Path) -> None:
        self.root = Path(storage_path) / "bursts"
        self.root.mkdir(parents=True, exist_ok=True)

    def save(self, iso: IsolatedBurst, meta: dict[str, Any], subdir: str | None = None) -> Path:
        folder = self.root / (subdir or datetime.now(timezone.utc).strftime("%Y%m%d"))
        folder.mkdir(parents=True, exist_ok=True)
        data = folder / f"{iso.burst_id}.sigmf-data"
        glob: dict[str, Any] = {
            "core:datatype": "ci16_le",
            "core:sample_rate": iso.rate_hz,
            "core:version": SIGMF_VERSION,
            "core:description": "RFObserver isolated burst (peak-normalized)",
            "rfobs:burst_id": iso.burst_id,
            "rfobs:truncated": iso.truncated,
            "rfobs:source_samples": iso.num_source_samples,
        }
        capture: dict[str, Any] = {"core:sample_start": 0, "core:frequency": iso.freq_hz}
        for k, v in meta.items():
            (capture if k == "core:datetime" else glob)[k] = v
        # Data first, meta last (atomically): a meta file must never exist
        # without its data, and a crash or full disk must never leave a
        # truncated meta next to a complete data file.
        data.write_bytes(iso.cs16)
        write_text_atomic(
            data.with_suffix(".sigmf-meta"),
            json.dumps({"global": glob, "captures": [capture], "annotations": []}, indent=2),
        )
        return data

    def _pairs(self) -> list[Path]:
        def mtime(p: Path) -> float:
            try:
                return p.stat().st_mtime
            except OSError:
                return 0.0

        return sorted(self.root.rglob("*.sigmf-data"), key=mtime)

    @staticmethod
    def _size(p: Path) -> int:
        total = 0
        for f in (p, p.with_suffix(".sigmf-meta")):
            with contextlib.suppress(OSError):
                total += f.stat().st_size
        return total

    def _delete_pair(self, p: Path) -> int:
        freed = self._size(p)
        for f in (p, p.with_suffix(".sigmf-meta")):
            try:
                f.unlink(missing_ok=True)
            except OSError:
                logger.warning("Could not delete burst file %s", f)
        return freed

    def usage_bytes(self) -> int:
        return sum(self._size(p) for p in self._pairs())

    def enforce_cap(self, max_bytes: int) -> int:
        pairs = self._pairs()
        usage = sum(self._size(p) for p in pairs)
        freed = 0
        while usage > max_bytes and pairs:
            n = self._delete_pair(pairs.pop(0))
            usage -= n
            freed += n
        return freed

    def evict_until_free(
        self, target_free_bytes: int, free_bytes: Callable[[], int] | None = None
    ) -> int:
        free = free_bytes or (lambda: shutil.disk_usage(self.root).free)
        pairs = self._pairs()
        freed = 0
        while pairs and free() < target_free_bytes:
            freed += self._delete_pair(pairs.pop(0))
        if freed:
            logger.warning("Storage floor: evicted %.1f MB of isolated bursts", freed / 1e6)
        return freed
