"""Shared data models -- vendored from rf-shared plus new burst types."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from pathlib import Path

# ---------------------------------------------------------------------------
# Vendored from rf-shared (MetadataRecord, IQStatistics, PSDData, Envelope)
# ---------------------------------------------------------------------------


class MetadataRecord(BaseModel):
    """Metadata for a single IQ capture file."""

    hostname: str
    organization: str
    gcs: str = ""
    serial: str = ""
    frequency: int
    timestamp: datetime
    source_path: Path
    checksum: str = ""
    gain: int
    sampling_rate: int
    interval: int = 0
    length: float = 0.0
    bit_depth: int = 16
    group: str = Field(default_factory=lambda: str(uuid4()))

    def validate_checksum(self, computed: str) -> None:
        if self.checksum and self.checksum != computed:
            raise ValueError(f"Checksum mismatch: expected {self.checksum}, got {computed}")


class IQStatistics(BaseModel):
    """Power statistics computed from an IQ capture."""

    average: float
    max: float
    median: float
    std: float
    kurtosis: float


class PSDData(BaseModel):
    """Power spectral density result from Welch method."""

    powers: list[float]
    frequencies: list[float]
    center_freq: float
    sample_rate: int
    num_bins: int


class Envelope(BaseModel):
    """NATS message envelope."""

    source_path: str = ""
    message_id: str = Field(default_factory=lambda: str(uuid4()))
    payload: dict[str, Any] = Field(default_factory=dict)


class ProcessedDataEnvelope(BaseModel):
    """Container for processed capture results."""

    metadata: MetadataRecord
    statistics: IQStatistics
    psd_data: PSDData
    message_id: str = Field(default_factory=lambda: str(uuid4()))


# ---------------------------------------------------------------------------
# New models for RFObserver burst detection
# ---------------------------------------------------------------------------


class BurstFingerprint(BaseModel):
    """Five-parameter fingerprint for a detected RF burst."""

    burst_id: str = Field(default_factory=lambda: str(uuid4()))
    start_time: datetime
    stop_time: datetime
    center_freq_hz: float
    bandwidth_hz: float
    peak_power_db: float
    duration_ms: float = 0.0
    detection_timestamp: datetime = Field(default_factory=datetime.utcnow)


class ChampionRecord(BaseModel):
    """A capture that won a champion category (loudest, quietest, rfi)."""

    metadata: MetadataRecord
    statistics: IQStatistics
    categories: list[str]
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class SensorStatus(BaseModel):
    """Current sensor health and operational status."""

    hostname: str
    uptime_sec: float = 0.0
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    disk_used_gb: float = 0.0
    disk_total_gb: float = 0.0
    sdr_temperature_c: float | None = None
    capture_count: int = 0
    detection_count: int = 0
    last_capture_time: datetime | None = None
    pipeline_running: bool = False
