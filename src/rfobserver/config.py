from __future__ import annotations

import socket
from dataclasses import dataclass

from pydantic import Field, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


@dataclass
class ZmsSettingsGroup:
    """Resolved ZMS configuration (only constructed when all required fields are set)."""

    zmc_http: str
    identity_http: str
    token: str
    monitor_id: str
    monitor_name: str = "RFObserver"
    dst_http: str | None = None
    monitor_schema_path: str | None = None
    metric_id: str | None = None

    @property
    def dst_or_zmc(self) -> str:
        return self.dst_http or self.zmc_http


class AppSettings(BaseSettings):
    """Application settings loaded from environment variables with RFOBS_ prefix."""

    model_config = SettingsConfigDict(
        env_prefix="RFOBS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Receiver (defaults to WiFi 2.4 GHz, B200-mini full BW)
    FREQUENCY_START: int = 2_437_000_000
    FREQUENCY_END: int = 2_437_000_000
    FREQUENCY_STEP: int = 0
    BANDWIDTH: int = 56_000_000
    DURATION_SEC: float = 0.5
    GAIN: int = 40

    # Sweep
    CYCLES: int = 0  # 0 = infinite
    RECORDS_PER_STEP: int = 1
    INTERVAL_SEC: int = 10

    # Trigger
    TRIGGER_ENABLED: bool = False
    TRIGGER_THRESHOLD_DB: float = -40.0
    TRIGGER_HYSTERESIS: int = 3
    TRIGGER_PRE_SEC: float = 1.0
    TRIGGER_DETECT_SEC: float = 0.5

    # Burst detection
    BURST_THRESHOLD_HIGH_DB: float = 10.0
    BURST_THRESHOLD_LOW_RATIO: float = 0.6
    BURST_MERGE_FREQ_BINS: int = 5
    BURST_MERGE_TIME_MS: float = 3.0

    # Identity
    HOSTNAME: str = Field(default_factory=socket.gethostname)
    ORGANIZATION: str = "DefaultOrg"
    COORDINATES: str = "0.0N,0.0W"

    # NATS
    NATS_ENABLED: bool = False
    NATS_HOST: str = "localhost"
    NATS_PORT: int = 4222
    NATS_TOKEN: SecretStr | None = None

    # Storage
    STORAGE_PATH: str = "/tmp/rfobserver"
    DB_PATH: str = "/tmp/rfobserver/rfobserver.db"
    ARCHIVE_MAX_GB: float = 50.0
    HISTORY_DAYS: int = 7

    # WebUI
    WEB_HOST: str = "0.0.0.0"
    WEB_PORT: int = 8080

    # Processing
    NUM_FFT_BINS: int = 1024
    PSD_TIME_RESOLUTION_MS: float = 0.2  # internal PSD grid time resolution

    # Streaming pipeline
    STREAMING_CHUNK_SLICES: int = 200  # PSD time slices per recv chunk
    BURST_WINDOW_ROWS: int = 500  # rolling burst detection window (rows)
    BURST_EVAL_INTERVAL_ROWS: int = 250  # how often to run burst detection (rows)

    # Recording
    RECORDING_MAX_SEC: float = 30.0  # auto-stop after this duration (0 = no limit)
    RECORDING_RAM_BUFFER: bool = False  # buffer entire capture in RAM, flush on stop

    # Metrics
    METRICS_ENABLED: bool = False
    METRICS_PORT: int = 9090

    # Development
    MOCK_RECEIVER: bool = False
    LOG_LEVEL: str = "INFO"

    # ZMS (optional)
    #
    # ZMS_ENABLED is the user-intent flag — True means "start the monitor at
    # boot if settings.zms is also valid". Default True so existing deployments
    # with the four required URLs/tokens populated keep behaving as before.
    # The /api/zms/{enable,disable} endpoints persist this through .env.
    ZMS_ENABLED: bool = True
    ZMS_ZMC_HTTP: str | None = None
    ZMS_DST_HTTP: str | None = None
    ZMS_IDENTITY_HTTP: str | None = None
    ZMS_TOKEN: SecretStr | None = None
    ZMS_MONITOR_ID: str | None = None
    ZMS_MONITOR_NAME: str = "RFObserver"
    ZMS_MONITOR_SCHEMA_PATH: str | None = None
    ZMS_METRIC_ID: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def NATS_URL(self) -> str:
        return f"nats://{self.NATS_HOST}:{self.NATS_PORT}"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def zms(self) -> ZmsSettingsGroup | None:
        """Constructs ZMS settings if all required env vars are set."""
        if self.ZMS_ZMC_HTTP and self.ZMS_IDENTITY_HTTP and self.ZMS_TOKEN and self.ZMS_MONITOR_ID:
            return ZmsSettingsGroup(
                zmc_http=self.ZMS_ZMC_HTTP,
                dst_http=self.ZMS_DST_HTTP,
                identity_http=self.ZMS_IDENTITY_HTTP,
                token=self.ZMS_TOKEN.get_secret_value(),
                monitor_id=self.ZMS_MONITOR_ID,
                monitor_name=self.ZMS_MONITOR_NAME,
                monitor_schema_path=self.ZMS_MONITOR_SCHEMA_PATH,
                metric_id=self.ZMS_METRIC_ID,
            )
        return None
