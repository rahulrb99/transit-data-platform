from pathlib import Path
from typing import Literal, Optional

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_environment: Literal["development", "production", "test"] = "development"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "transit"
    postgres_user: str = "transit"
    postgres_password: str = "transit"

    raw_data_root: Path = Path("data/raw")
    raw_archive_root: Path = Path("data/archive/raw")
    processed_data_root: Path = Path("data/processed")
    mbta_gtfs_static_url: str = "https://cdn.mbta.com/MBTA_GTFS.zip"
    mbta_vehicle_positions_url: str = (
        "https://cdn.mbta.com/realtime/VehiclePositions.pb"
    )
    mbta_poll_interval_seconds: float = 15.0
    mbta_http_timeout_seconds: float = 30.0
    kafka_bootstrap_servers: str = "localhost:19092"
    vehicle_positions_topic: str = "vehicle_positions"
    vehicle_positions_consumer_group: str = "vehicle-position-postgres-writer"
    mbta_consumer_batch_size: int = 100
    mbta_realtime_stale_threshold_seconds: float = 60.0
    dashboard_refresh_interval_seconds: int = 30
    dashboard_vehicle_max_age_seconds: int = 90
    runtime_health_max_age_seconds: int = Field(default=300, gt=0)
    realtime_retention_days: int = Field(default=14, gt=0)
    metrics_retention_days: int = Field(default=30, gt=0)
    raw_payload_retention_days: int = Field(default=7, gt=0)
    retention_batch_size: int = Field(default=1000, gt=0, le=10000)
    retention_max_batches: int = Field(default=100, gt=0)
    archive_min_free_bytes: int = Field(default=2147483648, ge=0)
    archive_backend: Literal["local", "s3"] = "local"
    archive_status_path: Path = Path("data/archive/raw/.status/s3-archive.json")
    backup_status_path: Path = Path("backups/.s3-upload-status.json")
    s3_bucket_name: Optional[str] = None
    aws_region: Optional[str] = None
    s3_raw_archive_prefix: str = "raw-archives"
    s3_postgres_backup_prefix: str = "postgres-backups"
    s3_backup_max_age_hours: int = Field(default=26, gt=0)

    @model_validator(mode="after")
    def validate_production_settings(self) -> "Settings":
        if self.app_environment.lower() != "production":
            return self

        insecure_values = {"", "transit", "password", "changeme", "change-me"}

        if self.postgres_password.lower() in insecure_values:
            raise ValueError("POSTGRES_PASSWORD must be explicitly set for production")

        if self.postgres_host.lower() in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("POSTGRES_HOST must not be localhost in production")

        if self.archive_backend != "s3":
            raise ValueError("ARCHIVE_BACKEND must be s3 in production")

        if not self.s3_bucket_name or not self.aws_region:
            raise ValueError("S3_BUCKET_NAME and AWS_REGION are required for production")

        return self

    # .env also contains Compose-only settings such as DASHBOARD_PORT.
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


def get_settings() -> Settings:
    return Settings()
