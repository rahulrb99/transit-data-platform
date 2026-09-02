import re
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
    pinned_requirements = [
        line
        for line in requirements.splitlines()
        if line and not line.startswith("#")
    ]

    assert "boto3==1.42.97" in pinned_requirements
    assert "botocore==1.42.97" in pinned_requirements
    assert "annotated-types==0.7.0" in pinned_requirements
    assert "pydantic-settings==2.11.0" in pinned_requirements
    assert "python-dotenv==1.2.1" in pinned_requirements
    assert "s3transfer==0.16.0" in pinned_requirements
    assert "typing-inspection==0.4.2" in pinned_requirements
    assert "urllib3==1.26.20" in pinned_requirements
    assert all(
        re.fullmatch(r"[A-Za-z0-9_.-]+==[^=\s]+", line)
        for line in pinned_requirements
    )
    assert "streamlit==" not in requirements
    assert "dbt-postgres==" not in requirements


def test_ci_validates_host_backup_runtime_on_python39() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )

    assert "host-backup-python39:" in workflow
    assert 'python-version: "3.9"' in workflow
    assert "python -m pip install -r requirements-ops.lock" in workflow
    assert "import scripts.postgres_backup, ingestion.s3_archive" in workflow
