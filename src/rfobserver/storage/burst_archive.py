"""Isolated bursts on disk: one SigMF pair per burst under <storage>/bursts/.

Live bursts go into a folder per UTC day; replay bursts into
``replay-<capture stem>/`` so they never mix with the sensor's own. Bounded by
BURST_ARCHIVE_MAX_GB (oldest first), and evicted before automatic captures when
the storage governor needs space.

Bursts are tracked in an in-memory index (one per bursts/ root, shared by every
BurstArchive in the process), built by one directory walk on first use and kept
current by save and eviction. The storage governor's 10 s tick used to walk
bursts/ with pathlib (three stats per burst, up to three walks per tick); on the
field sensor that held the GIL long enough to delay the USB receiver past UHD's
buffer (docs/debugging/2026-10-01_hcro-overflows-storage-scans.md). As with
captures, files changed outside RFObserver are picked up at the next start.
"""

from __future__ import annotations

import bisect
import contextlib
import json
import logging
import os
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rfobserver.storage.psd_grid import write_text_atomic
from rfobserver.storage.sigmf_export import SIGMF_VERSION

if TYPE_CHECKING:
    from collections.abc import Callable

    from rfobserver.processing.isolate import IsolatedBurst

logger = logging.getLogger(__name__)


class _BurstIndex:
    """Bursts under one bursts/ root: data path -> (mtime, bytes of the pair)."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.lock = threading.Lock()
        self._entries: dict[str, tuple[float, int]] = {}
        self._order: list[tuple[float, str]] = []  # oldest first
        self.total = 0
        self._build()

    def _build(self) -> None:
        t0 = time.monotonic()
        sizes: dict[str, int] = {}
        mtimes: dict[str, float] = {}
        if self.root.is_dir():
            for dirpath, _dirs, files in os.walk(self.root):
                for name in files:
                    if name.endswith(".sigmf-data"):
                        stem = name[: -len(".sigmf-data")]
                    elif name.endswith(".sigmf-meta"):
                        stem = name[: -len(".sigmf-meta")]
                    else:
                        continue
                    try:
                        st = os.stat(os.path.join(dirpath, name))
                    except OSError:
                        continue
                    key = os.path.join(dirpath, stem + ".sigmf-data")
                    sizes[key] = sizes.get(key, 0) + st.st_size
                    if name.endswith(".sigmf-data"):
                        mtimes[key] = st.st_mtime
        with self.lock:
            for key, mtime in mtimes.items():
                self._entries[key] = (mtime, sizes[key])
                self._order.append((mtime, key))
                self.total += sizes[key]
            self._order.sort()
        logger.info(
            "Burst index: %d bursts (%.1f MB) in %.2f s",
            len(mtimes),
            self.total / 1e6,
            time.monotonic() - t0,
        )

    def put(self, data: Path, mtime: float, size: int) -> None:
        key = str(data)
        with self.lock:
            self._remove_locked(key)
            self._entries[key] = (mtime, size)
            bisect.insort(self._order, (mtime, key))
            self.total += size

    def _remove_locked(self, key: str) -> int | None:
        e = self._entries.pop(key, None)
        if e is None:
            return None
        i = bisect.bisect_left(self._order, (e[0], key))
        if i < len(self._order) and self._order[i] == (e[0], key):
            del self._order[i]
        self.total -= e[1]
        return e[1]

    def claim_oldest(self) -> tuple[Path, int] | None:
        """Remove the oldest burst from the index and return it (to delete)."""
        with self.lock:
            if not self._order:
                return None
            _, key = self._order[0]
            size = self._remove_locked(key)
            return Path(key), size or 0

    def remove(self, data: Path) -> int | None:
        with self.lock:
            return self._remove_locked(str(data))

    def split(self, not_after: float | None) -> tuple[int, int]:
        """(all bytes, bytes of bursts last written before ``not_after``)."""
        with self.lock:
            if not_after is None:
                return self.total, self.total
            new = 0
            for mtime, key in reversed(self._order):  # newest first: few are new
                if mtime < not_after:
                    break
                new += self._entries[key][1]
            return self.total, self.total - new

    def count(self) -> int:
        with self.lock:
            return len(self._entries)


_INDEXES: dict[Path, _BurstIndex] = {}
_INDEXES_LOCK = threading.Lock()


def burst_index(storage_path: str | Path) -> _BurstIndex:
    """The shared index for ``storage_path``/bursts, built on first use (one
    walk; blocking, so call it off the event loop the first time). Never
    creates the folder."""
    root = (Path(storage_path) / "bursts").resolve()
    with _INDEXES_LOCK:
        idx = _INDEXES.get(root)
        if idx is None:
            idx = _BurstIndex(root)
            _INDEXES[root] = idx
        return idx


class BurstArchive:
    def __init__(self, storage_path: str | Path) -> None:
        self.root = Path(storage_path) / "bursts"
        self.root.mkdir(parents=True, exist_ok=True)
        self._index = burst_index(storage_path)

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
        self.track(data)
        return data

    def track(self, data: Path) -> None:
        """Add or re-measure one burst pair in the index (stats only its files)."""
        try:
            mtime = data.stat().st_mtime
        except OSError:
            self._index.remove(data)
            return
        self._index.put(data, mtime, self._size(data))

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

    @staticmethod
    def scan_usage(storage_path: str | Path, not_after: float | None = None) -> tuple[int, int]:
        """(all burst bytes, bytes of bursts last written before ``not_after``)
        under ``storage_path``/bursts, from the index (no walk once it is
        built). Unlike the constructor this never creates the folder: a
        missing bursts/ is (0, 0)."""
        return burst_index(storage_path).split(not_after)

    def usage_bytes(self) -> int:
        return self._index.total

    def _evict_oldest(self) -> int | None:
        """Delete the oldest burst pair. Returns bytes freed, None when empty.
        The victim leaves the index before its files are deleted, so two
        evictors never both count it."""
        claimed = self._index.claim_oldest()
        if claimed is None:
            return None
        path, size = claimed
        self._delete_pair(path)
        return size

    def enforce_cap(self, max_bytes: int) -> int:
        freed = 0
        while self._index.total > max_bytes:
            n = self._evict_oldest()
            if n is None:
                break
            freed += n
        return freed

    def evict_until_free(
        self, target_free_bytes: int, free_bytes: Callable[[], int] | None = None
    ) -> int:
        free = free_bytes or (lambda: shutil.disk_usage(self.root).free)
        freed = 0
        while free() < target_free_bytes:
            n = self._evict_oldest()
            if n is None:
                break
            freed += n
        if freed:
            logger.warning("Storage floor: evicted %.1f MB of isolated bursts", freed / 1e6)
        return freed
