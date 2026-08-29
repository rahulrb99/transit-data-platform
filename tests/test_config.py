from pathlib import Path

from ingestion.config import Settings


def test_default_raw_data_root() -> None:
    settings = Settings()
    assert settings.raw_data_root == Path("data/raw")

