"""Champion file archiver.

Ported from rf_processor.archiver. Archives IQ capture files that win
champion categories (loudest, quietest, rfi) to a date-organized directory
structure on the local NVMe.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
from pathlib import Path

from rfobserver.models import MetadataRecord

logger = logging.getLogger(__name__)


class ChampionArchiver:
    """Archives champion IQ files to date-organized directories."""

    def __init__(self, storage_path: str) -> None:
        self.storage_path = Path(storage_path)

    async def archive(self, metadata: MetadataRecord, categories: list[str]) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._archive_blocking, metadata, categories)

    def _archive_blocking(self, metadata: MetadataRecord, categories: list[str]) -> None:
        source = metadata.source_path
        ts = metadata.timestamp

        for category in categories:
            archive_dir = (
                self.storage_path
                / str(ts.year)
                / f"{ts.month:02d}"
                / f"{ts.day:02d}"
                / f"{ts.hour:02d}"
                / category
            )
            archive_dir.mkdir(parents=True, exist_ok=True)

            # Remove old champion for same config
            for old_json in archive_dir.glob(f"*{metadata.hostname}*.json"):
                try:
                    old_meta = MetadataRecord.model_validate_json(old_json.read_text())
                    if _config_matches(metadata, old_meta):
                        old_json.with_suffix(".sc16").unlink(missing_ok=True)
                        old_json.unlink(missing_ok=True)
                        logger.debug("Removed old champion: %s", old_json.name)
                except Exception as e:
                    logger.error("Could not parse old champion %s: %s", old_json, e)

            dest_sc16 = archive_dir / source.name
            dest_json = dest_sc16.with_suffix(".json")

            shutil.copy(source, dest_sc16)
            dest_json.write_text(metadata.model_dump_json(indent=2))

            logger.info("Archived %s to category '%s'", source.name, category)


def _config_matches(new: MetadataRecord, old: MetadataRecord) -> bool:
    """Check if two metadata records have the same capture configuration."""
    return (
        new.hostname == old.hostname
        and new.frequency == old.frequency
        and new.interval == old.interval
        and new.length == old.length
        and new.gain == old.gain
        and new.sampling_rate == old.sampling_rate
    )
