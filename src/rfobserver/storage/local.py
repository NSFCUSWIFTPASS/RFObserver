"""Local NVMe file storage with FIFO rotation.

Manages IQ capture files on local storage, enforcing a maximum disk usage
limit by deleting oldest files first.
"""

from __future__ import annotations

import logging
import os
import shutil
import time
from pathlib import Path
from typing import TYPE_CHECKING

from rfobserver.storage.governor import StorageSample, VolumeSample

if TYPE_CHECKING:
    from collections.abc import Callable, Collection

logger = logging.getLogger(__name__)


# Suffixes of the companion files a streaming capture emits alongside its
# .sc16. A capture's full footprint (and what eviction must delete) is the
# .sc16 plus whichever of these exist. ".npz" is a legacy grid companion kept
# so old captures are also evicted cleanly.
_COMPANION_SUFFIXES = (".json", ".psd", ".psd.json", ".detections.json", ".npz")


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
        self.storage_path = Path(storage_path)
        self.max_bytes = int(max_gb * 1024**3)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        # Captures are split by origin: auto/ holds triggered + continuous
        # recordings (FIFO-evicted to stay within the cap); manual/ holds manual
        # and replay records and is never evicted.
        self.auto_dir = self.storage_path / "auto"
        self.manual_dir = self.storage_path / "manual"
        self.auto_dir.mkdir(exist_ok=True)
        self.manual_dir.mkdir(exist_ok=True)
        self.migrate_flat_captures_to_manual()

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
        # Tolerant size: enforce_cap (recording-control thread) and
        # evict_until_free (storage thread) can race on the same capture.
        freed = self._size_or_zero(sc16_path)
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
        captures = sorted(self.auto_dir.glob("*.sc16"), key=self._mtime_or_zero)
        usage = sum(self._size_or_zero(c) for c in captures)
        while usage > self.max_bytes and len(captures) > 1:
            oldest = captures.pop(0)
            usage -= self._delete_capture(oldest)

    def get_usage_bytes(self) -> int:
        """Bytes used by the managed (auto) capture set, including companions.

        Manual captures are excluded -- they are outside the FIFO budget.
        """
        return sum(self._size_or_zero(c) for c in self.auto_dir.glob("*.sc16"))

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
        return sum(self._size_or_zero(c) for c in self.manual_dir.glob("*.sc16"))

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

        def protected(c: Path) -> bool:
            if is_active_capture(c.name, exclude):
                return True
            if not_after is not None and self._mtime_or_zero(c) >= not_after:
                return True
            return exclude_fn is not None and is_active_capture(c.name, exclude_fn())

        captures = sorted(
            (c for c in self.auto_dir.glob("*.sc16") if not protected(c)),
            key=self._mtime_or_zero,
        )
        freed = 0
        while captures and free() < target_free_bytes:
            victim = captures.pop(0)
            if protected(victim):
                continue
            if on_evict is not None:
                on_evict(victim, time.time() - self._mtime_or_zero(victim))
            freed += self._delete_capture(victim)
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
        rest, so they must not make step 1 look possible)."""
        du = shutil.disk_usage(self.storage_path)
        db_volume = None
        db_dir = Path(db_path).resolve().parent
        try:
            if os.stat(db_dir).st_dev != os.stat(self.storage_path).st_dev:
                ddu = shutil.disk_usage(db_dir)
                db_volume = VolumeSample(ddu.free, ddu.total)
        except OSError:
            db_volume = None
        autos = list(self.auto_dir.glob("*.sc16"))
        return StorageSample(
            data=VolumeSample(du.free, du.total),
            db_volume=db_volume,
            db_file_bytes=db_file_bytes,
            db_reusable_bytes=db_reusable_bytes,
            auto_bytes=sum(self._size_or_zero(c) for c in autos),
            manual_bytes=self.get_manual_usage_bytes(),
            evictable_auto=any(
                not is_active_capture(c.name, active_names)
                and (not_after is None or self._mtime_or_zero(c) < not_after)
                for c in autos
            ),
        )
