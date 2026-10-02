"""Local NVMe file storage with FIFO rotation.

Manages IQ capture files on local storage, enforcing a maximum disk usage
limit by deleting oldest files first.

Captures are tracked in an in-memory index (path -> mtime, footprint), built
by one directory scan when the storage is opened and kept current by the code
that adds and removes captures. Nothing walks the capture directories per
tick: on a sensor with tens of thousands of captures those walks held the GIL
long enough to starve the USB receiver thread (UHD overflows) and blocked the
event loop. See docs/debugging/2026-10-01_hcro-overflows-storage-scans.md.
Files changed outside RFObserver (a delete over SFTP) are picked up at the
next start; until then a tracked total can only run high, which makes the cap
evict a little early, and disk safety comes from the volume's free space.
"""

from __future__ import annotations

import bisect
import heapq
import itertools
import logging
import os
import shutil
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from rfobserver.storage.burst_archive import BurstArchive, burst_index
from rfobserver.storage.governor import StorageSample, VolumeSample

if TYPE_CHECKING:
    from collections.abc import Callable, Collection

logger = logging.getLogger(__name__)


# Suffixes of the companion files a streaming capture emits alongside its
# .sc16. A capture's full footprint (and what eviction must delete) is the
# .sc16 plus whichever of these exist. ".npz" is a legacy grid companion kept
# so old captures are also evicted cleanly.
_COMPANION_SUFFIXES = (".json", ".psd", ".psd.json", ".detections.json", ".npz")
# Longest first, so "x.psd.json" maps to base "x", not "x.psd".
_SUFFIXES_LONGEST_FIRST = tuple(sorted(_COMPANION_SUFFIXES, key=len, reverse=True))


@dataclass(frozen=True)
class CaptureEntry:
    """One tracked capture: its .sc16 path, the .sc16's mtime, and the bytes of
    the .sc16 plus every companion that existed when it was last measured."""

    path: Path
    mtime: float
    size: int

    @property
    def origin(self) -> str:
        return self.path.parent.name


def _capture_base(name: str) -> str | None:
    """The capture a file in a capture directory belongs to, or None."""
    if name.endswith(".sc16"):
        return name[: -len(".sc16")]
    for suf in _SUFFIXES_LONGEST_FIRST:
        if name.endswith(suf):
            return name[: -len(suf)]
    return None


def is_active_capture(name: str, active_names: Collection[str]) -> bool:
    """True for a capture being recorded, including the ``_drop<N>`` name the
    finalize step may rename it to while the governor is looking."""
    for active in active_names:
        stem = active[: -len(".sc16")] if active.endswith(".sc16") else active
        if name == active or (name.startswith(f"{stem}_drop") and name.endswith(".sc16")):
            return True
    return False


class LocalStorage:
    """Manages IQ file storage with FIFO rotation."""

    def __init__(self, storage_path: str, max_gb: float = 50.0) -> None:
        self.max_bytes = int(max_gb * 1024**3)
        # One lock for the index: the recording-control thread (enforce_cap),
        # the storage thread (evict_until_free, sample) and the web routes all
        # use it. File I/O happens outside it.
        self._lock = threading.RLock()
        self._entries: dict[Path, CaptureEntry] = {}
        # Per directory, (mtime, path) sorted oldest first.
        self._order: dict[Path, list[tuple[float, str]]] = {}
        self._bytes: dict[Path, int] = {}
        self.set_storage_path(storage_path)

    def set_storage_path(self, storage_path: str | Path) -> None:
        """Point at a storage root (creating auto/ and manual/) and index it.

        Blocking (one scan of the capture directories): call it off the event
        loop. Used at startup and when the storage path changes at runtime.
        """
        root = Path(storage_path)
        root.mkdir(parents=True, exist_ok=True)
        # Captures are split by origin: auto/ holds triggered + continuous
        # recordings (FIFO-evicted to stay within the cap); manual/ holds manual
        # and replay records and is never evicted.
        auto_dir = root / "auto"
        manual_dir = root / "manual"
        auto_dir.mkdir(exist_ok=True)
        manual_dir.mkdir(exist_ok=True)
        with self._lock:
            self.storage_path = root
            self.auto_dir = auto_dir
            self.manual_dir = manual_dir
            self.migrate_flat_captures_to_manual()
            self.rebuild_index()
        # The burst archive's index too, so its one walk happens here (off the
        # event loop, before streaming) rather than at the first storage tick.
        burst_index(root)

    # --- capture index ---

    def rebuild_index(self) -> int:
        """Rebuild the index with one scan of auto/ and manual/. Returns the
        number of captures found. One os.scandir pass per directory: each file
        is visited once, and a capture is the .sc16 plus its companions."""
        t0 = time.monotonic()
        entries: dict[Path, CaptureEntry] = {}
        for d in (self.auto_dir, self.manual_dir):
            entries.update(self._scan_dir(d))
        with self._lock:
            self._entries = entries
            self._order = {self.auto_dir: [], self.manual_dir: []}
            self._bytes = {self.auto_dir: 0, self.manual_dir: 0}
            for e in entries.values():
                self._order[e.path.parent].append((e.mtime, str(e.path)))
                self._bytes[e.path.parent] += e.size
            for lst in self._order.values():
                lst.sort()
        logger.info(
            "Capture index: %d captures (%.1f GB auto, %.1f GB manual) in %.2f s",
            len(entries),
            self._bytes[self.auto_dir] / 1024**3,
            self._bytes[self.manual_dir] / 1024**3,
            time.monotonic() - t0,
        )
        return len(entries)

    @staticmethod
    def _scan_dir(d: Path) -> dict[Path, CaptureEntry]:
        sizes: dict[str, int] = {}
        mtimes: dict[str, float] = {}
        try:
            it = os.scandir(d)
        except OSError:
            return {}
        with it:
            for de in it:
                base = _capture_base(de.name)
                if base is None:
                    continue
                try:
                    st = de.stat(follow_symlinks=False)
                except OSError:
                    continue
                sizes[base] = sizes.get(base, 0) + st.st_size
                if de.name.endswith(".sc16"):
                    mtimes[base] = st.st_mtime
        # Only bases with a .sc16 are captures; companions on their own are
        # left alone, as before.
        return {
            d / f"{base}.sc16": CaptureEntry(d / f"{base}.sc16", mtime, sizes[base])
            for base, mtime in mtimes.items()
        }

    def _index_dir(self, path: Path) -> Path | None:
        parent = path.parent
        return parent if parent in (self.auto_dir, self.manual_dir) else None

    def _remove_locked(self, path: Path) -> CaptureEntry | None:
        e = self._entries.pop(path, None)
        if e is not None:
            order = self._order[e.path.parent]
            key = (e.mtime, str(e.path))
            i = bisect.bisect_left(order, key)
            if i < len(order) and order[i] == key:
                del order[i]
            self._bytes[e.path.parent] -= e.size
        return e

    def track(self, sc16_path: Path) -> CaptureEntry | None:
        """Add or re-measure one capture (after it is written, or a companion
        is). Stats only that capture's files. A capture that no longer exists
        is dropped from the index. Returns its entry, or None."""
        sc16_path = Path(sc16_path)
        d = self._index_dir(sc16_path)
        if d is None:
            return None
        try:
            mtime = sc16_path.stat().st_mtime
            size = self._capture_size(sc16_path)
        except OSError:
            self.forget(sc16_path)
            return None
        entry = CaptureEntry(sc16_path, mtime, size)
        with self._lock:
            self._remove_locked(sc16_path)
            self._entries[sc16_path] = entry
            bisect.insort(self._order[d], (mtime, str(sc16_path)))
            self._bytes[d] += size
        return entry

    def forget(self, sc16_path: Path) -> None:
        """Drop a capture from the index (it was deleted or moved away)."""
        with self._lock:
            self._remove_locked(Path(sc16_path))

    def capture_count(self) -> int:
        with self._lock:
            return len(self._entries)

    def captures_newest_first(
        self, offset: int = 0, limit: int | None = None
    ) -> list[CaptureEntry]:
        """Tracked captures across auto/ and manual/, newest first."""
        with self._lock:
            # Both lists are sorted, so merging them newest first touches only
            # offset + limit items, not every capture.
            merged = heapq.merge(
                reversed(self._order[self.auto_dir]),
                reversed(self._order[self.manual_dir]),
                reverse=True,
            )
            stop = None if limit is None else offset + limit
            return [self._entries[Path(p)] for _, p in itertools.islice(merged, offset, stop)]

    def migrate_flat_captures_to_manual(self) -> None:
        """Move any legacy root-level captures into manual/ (one-time, at startup).

        Older builds wrote captures straight into the storage root; those are
        treated as manual (deliberate keeps) so the auto/ FIFO never evicts them.
        Only root-level ``*.sc16`` are touched; the subdirs are left alone.
        """
        for sc16 in self.storage_path.glob("*.sc16"):
            for src in [sc16, *self._companion_paths(sc16)]:
                if src.exists():
                    src.rename(self.manual_dir / src.name)

    def save_capture(self, filename: str, data: bytes) -> Path:
        """Save raw IQ data to a file, rotating old files if needed."""
        self._enforce_limit(len(data))
        dest = self.storage_path / filename
        dest.write_bytes(data)
        logger.debug("Saved capture: %s (%d bytes)", filename, len(data))
        return dest

    def _companion_paths(self, sc16_path: Path) -> list[Path]:
        """Paths of the companion files for a capture (whether or not they exist).

        Resolved relative to the .sc16's own directory so this works for a
        capture in auto/, manual/, or the legacy root alike.
        """
        base = sc16_path.name[: -len(".sc16")]
        return [sc16_path.parent / f"{base}{suf}" for suf in _COMPANION_SUFFIXES]

    def _capture_size(self, sc16_path: Path) -> int:
        """Total bytes of a capture: its .sc16 plus every existing companion."""
        total = sc16_path.stat().st_size if sc16_path.exists() else 0
        for comp in self._companion_paths(sc16_path):
            if comp.exists():
                total += comp.stat().st_size
        return total

    def _delete_capture(self, sc16_path: Path) -> int:
        """Delete a capture and all its companions. Returns bytes freed.

        Tolerant of unlink failures (e.g. a read-only-remounted or busy volume):
        a file that cannot be removed is logged and skipped rather than aborting
        eviction, so one bad file cannot silently stop FIFO rotation.
        """
        # The tracked size: measuring again would stat six files per capture.
        # enforce_cap and evict_until_free claim a victim by removing it from
        # the index under the lock, so they never both delete the same one.
        with self._lock:
            entry = self._remove_locked(sc16_path)
        freed = entry.size if entry is not None else self._size_or_zero(sc16_path)
        for p in [sc16_path, *self._companion_paths(sc16_path)]:
            try:
                p.unlink(missing_ok=True)
            except OSError:
                logger.warning("Could not unlink %s during eviction", p)
        logger.info("Rotated old capture: %s (freed %d bytes)", sc16_path.name, freed)
        return freed

    def _enforce_limit(self, incoming_bytes: int) -> None:
        """Delete oldest captures until there's room for incoming data."""
        captures = sorted(self.storage_path.glob("*.sc16"), key=self._mtime_or_zero)
        usage = sum(self._size_or_zero(c) for c in captures)
        while usage + incoming_bytes > self.max_bytes and captures:
            oldest = captures.pop(0)
            usage -= self._delete_capture(oldest)

    def enforce_cap(self) -> None:
        """Evict oldest captures until total footprint is within the disk cap.

        Unlike ``_enforce_limit`` (which reserves room for a pending write), this
        operates on captures already on disk and never deletes the single newest
        capture, so a just-finalized recording is always kept even if it alone
        exceeds the cap. Called after each streaming recording so continuous
        triggering stays bounded by ARCHIVE_MAX_GB. Only the auto/ set is
        considered -- manual captures are never counted and never evicted.
        """
        while True:
            with self._lock:
                order = self._order[self.auto_dir]
                if self._bytes[self.auto_dir] <= self.max_bytes or len(order) <= 1:
                    return
                oldest = Path(order[0][1])
            self._delete_capture(oldest)

    def get_usage_bytes(self) -> int:
        """Bytes used by the managed (auto) capture set, including companions.

        Manual captures are excluded -- they are outside the FIFO budget.
        """
        with self._lock:
            return self._bytes[self.auto_dir]

    def get_usage_gb(self) -> float:
        return self.get_usage_bytes() / (1024**3)

    def _size_or_zero(self, sc16_path: Path) -> int:
        """_capture_size tolerant of a capture deleted or renamed mid-walk
        (eviction, the drop-rename at finalize, a person deleting over SFTP)."""
        try:
            return self._capture_size(sc16_path)
        except OSError:
            return 0

    @staticmethod
    def _mtime_or_zero(p: Path) -> float:
        try:
            return p.stat().st_mtime
        except OSError:
            return 0.0

    def get_manual_usage_bytes(self) -> int:
        """Bytes used by manual/ captures. Reported only; never evicted."""
        with self._lock:
            return self._bytes[self.manual_dir]

    def _auto_oldest_first(self) -> list[CaptureEntry]:
        with self._lock:
            return [self._entries[Path(p)] for _, p in self._order[self.auto_dir]]

    def evict_until_free(
        self,
        target_free_bytes: int,
        *,
        exclude: Collection[str] = (),
        exclude_fn: Callable[[], Collection[str]] | None = None,
        not_after: float | None = None,
        free_bytes: Callable[[], int] | None = None,
        on_evict: Callable[[Path, float], None] | None = None,
    ) -> int:
        """Delete the oldest auto/ captures until the volume has
        ``target_free_bytes`` free or none is left to delete. Returns bytes freed.

        The governor's step 1. Unlike enforce_cap this may take the newest
        finished capture; it never takes the capture being recorded or anything
        in manual/. The capture being recorded is recognised three ways:
        named in ``exclude`` (a snapshot), named by ``exclude_fn`` (asked again
        right before each delete, since a recording can begin after the
        snapshot), or an mtime at or after ``not_after`` (a file still being
        written).

        ``on_evict``, if given, is called right before each capture is deleted
        with (its .sc16 path, its age in seconds = now - mtime). Runs on
        whatever thread calls evict_until_free (the storage worker thread via
        asyncio.to_thread); the caller collects ages there and reports them to
        the governor once this returns.
        """
        free = free_bytes or (lambda: shutil.disk_usage(self.storage_path).free)

        def protected(e: CaptureEntry) -> bool:
            if is_active_capture(e.path.name, exclude):
                return True
            if not_after is not None and e.mtime >= not_after:
                return True
            return exclude_fn is not None and is_active_capture(e.path.name, exclude_fn())

        freed = 0
        for victim in self._auto_oldest_first():
            if free() >= target_free_bytes:
                break
            if protected(victim):
                continue
            if on_evict is not None:
                on_evict(victim.path, time.time() - victim.mtime)
            freed += self._delete_capture(victim.path)
        if freed:
            logger.warning("Storage floor: evicted %.1f GB of automatic captures", freed / 1024**3)
        return freed

    def sample(
        self,
        *,
        db_path: Path,
        active_names: Collection[str],
        db_file_bytes: int,
        db_reusable_bytes: int,
        not_after: float | None = None,
    ) -> StorageSample:
        """The filesystem half of a governor sample (blocking: call in a thread).

        A capture counts as evictable when it is not active and, given
        ``not_after``, was last written before it (evict_until_free skips the
        rest, so they must not make step 1 look possible). Burst files are
        reported separately; the governor decides whether they make step 1
        possible. Never creates bursts/."""
        du = shutil.disk_usage(self.storage_path)
        db_volume = None
        db_dir = Path(db_path).resolve().parent
        try:
            if os.stat(db_dir).st_dev != os.stat(self.storage_path).st_dev:
                ddu = shutil.disk_usage(db_dir)
                db_volume = VolumeSample(ddu.free, ddu.total)
        except OSError:
            db_volume = None
        # The one walk of bursts/ per tick; the storage loop reuses it for the
        # cap check and burst eviction.
        bursts_bytes, old_bursts_bytes = BurstArchive.scan_usage(self.storage_path, not_after)
        with self._lock:
            # Oldest first: the first non-active capture old enough decides it,
            # usually the very first one.
            evictable = any(
                not is_active_capture(Path(p).name, active_names)
                and (not_after is None or mtime < not_after)
                for mtime, p in self._order[self.auto_dir]
            )
            auto_bytes = self._bytes[self.auto_dir]
            manual_bytes = self._bytes[self.manual_dir]
        return StorageSample(
            data=VolumeSample(du.free, du.total),
            db_volume=db_volume,
            db_file_bytes=db_file_bytes,
            db_reusable_bytes=db_reusable_bytes,
            auto_bytes=auto_bytes,
            manual_bytes=manual_bytes,
            evictable_auto=evictable,
            bursts_bytes=bursts_bytes,
            old_bursts_bytes=old_bursts_bytes,
        )
