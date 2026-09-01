from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SYSTEMD_DIR = ROOT / "infrastructure" / "systemd"


def test_backup_service_runs_existing_production_workflow_as_instance_user() -> None:
    service = (SYSTEMD_DIR / "transit-postgres-backup@.service").read_text(
        encoding="utf-8"
    )

    assert "User=%i" in service
    assert "SupplementaryGroups=docker" in service
    assert "WorkingDirectory=/opt/transit-data-platform" in service
    assert (
        "ExecStart=/opt/transit-data-platform/.venv/bin/python "
        "-m scripts.postgres_backup --production backup "
        "--output /opt/transit-data-platform/backups"
    ) in service
    assert "UMask=0077" in service
    assert "TimeoutStartSec=2h" in service
    assert "ReadWritePaths=/opt/transit-data-platform/backups" in service
    assert "AWS_ACCESS_KEY_ID" not in service
    assert "AWS_SECRET_ACCESS_KEY" not in service


def test_backup_timer_is_daily_persistent_and_jittered() -> None:
    timer = (SYSTEMD_DIR / "transit-postgres-backup@.timer").read_text(
        encoding="utf-8"
    )

    assert "OnCalendar=*-*-* 03:15:00 UTC" in timer
    assert "RandomizedDelaySec=30m" in timer
    assert "Persistent=true" in timer
    assert "Unit=transit-postgres-backup@%i.service" in timer


def test_operations_lock_contains_backup_runtime_dependencies() -> None:
    requirements = (ROOT / "requirements-ops.lock").read_text(encoding="utf-8")

    assert "boto3==" in requirements
    assert "pydantic-settings==" in requirements
    assert "streamlit==" not in requirements
    assert "dbt-postgres==" not in requirements
