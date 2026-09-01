from pathlib import Path

import pytest
from pydantic import ValidationError

from ingestion.config import Settings


def test_default_raw_data_root() -> None:
    settings = Settings()
    assert settings.raw_data_root == Path("data/raw")


def test_default_dashboard_refresh_interval() -> None:
    settings = Settings()
    assert settings.dashboard_refresh_interval_seconds == 30


def test_default_dashboard_vehicle_max_age() -> None:
    settings = Settings()
    assert settings.dashboard_vehicle_max_age_seconds == 90


def test_compose_only_environment_settings_are_allowed(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("DASHBOARD_PORT=8502\nRUNTIME_HEALTH_MAX_AGE_SECONDS=120\n")
    settings = Settings(_env_file=env_file)
    assert settings.runtime_health_max_age_seconds == 120


def test_production_rejects_default_database_password(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "APP_ENVIRONMENT=production\n"
        "POSTGRES_HOST=postgres\n"
        "POSTGRES_PASSWORD=transit\n"
    )

    with pytest.raises(ValidationError, match="POSTGRES_PASSWORD"):
        Settings(_env_file=env_file)


def test_production_rejects_localhost_database_host(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "APP_ENVIRONMENT=production\n"
        "POSTGRES_HOST=localhost\n"
        "POSTGRES_PASSWORD=not-a-default-password\n"
    )

    with pytest.raises(ValidationError, match="POSTGRES_HOST"):
        Settings(_env_file=env_file)


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "::1"])
def test_production_rejects_loopback(host):
    with pytest.raises(ValidationError, match="POSTGRES_HOST"):
        Settings(_env_file=None, app_environment="production", postgres_host=host,
                 postgres_password="a-real-configured-secret")


def test_unknown_environment_does_not_bypass_production_validation():
    with pytest.raises(ValidationError):
        Settings(_env_file=None, app_environment="prodution")


def test_production_requires_s3_archive_configuration():
    with pytest.raises(ValidationError, match="ARCHIVE_BACKEND"):
        Settings(
            _env_file=None,
            app_environment="production",
            postgres_host="postgres",
            postgres_password="configured-secret",
        )


def test_valid_production_s3_configuration():
    settings = Settings(
        _env_file=None,
        app_environment="production",
        postgres_host="postgres",
        postgres_password="configured-secret",
        archive_backend="s3",
        s3_bucket_name="transit-private",
        aws_region="us-east-1",
    )
    assert settings.s3_raw_archive_prefix == "raw-archives"
