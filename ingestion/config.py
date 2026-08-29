from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "transit"
    postgres_user: str = "transit"
    postgres_password: str = "transit"

    raw_data_root: Path = Path("data/raw")
    processed_data_root: Path = Path("data/processed")
    mbta_gtfs_static_url: str = "https://cdn.mbta.com/MBTA_GTFS.zip"
    mbta_vehicle_positions_url: str = "https://cdn.mbta.com/realtime/VehiclePositions.pb"
    kafka_bootstrap_servers: str = "localhost:19092"
    vehicle_positions_topic: str = "vehicle_positions"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


def get_settings() -> Settings:
    return Settings()
