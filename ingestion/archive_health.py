"""Operational verification for S3 raw archives and PostgreSQL backups."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from ingestion.config import get_settings
from ingestion.s3_archive import S3BackupArchive, S3UploadError, build_s3_store


def load_status(path: Path) -> dict[str, object] | None:
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def check_archive_health(*, store=None, now: datetime | None = None) -> list[str]:
    settings = get_settings()
    if settings.archive_backend != "s3":
        return ["S3 archive: disabled (local backend)"]
    resolved_store = store or build_s3_store(settings)
    messages: list[str] = []
    failed = False
    raw_status = load_status(settings.archive_status_path)
    if raw_status is None:
        messages.append("Raw S3 archive: no upload attempt recorded yet")
    elif raw_status.get("success") is True:
        messages.append(
            f"Raw S3 archive: latest upload succeeded at {raw_status.get('checked_at')}"
        )
    else:
        failed = True
        messages.append(
            f"Raw S3 archive: UPLOAD FAILED at {raw_status.get('checked_at')}: "
            f"{raw_status.get('error')}"
        )

    backup = S3BackupArchive(
        resolved_store,
        settings.s3_postgres_backup_prefix,
        settings.backup_status_path,
    )
    try:
        head = resolved_store.head(backup.latest_manifest_key)
    except S3UploadError as error:
        failed = True
        messages.append(f"PostgreSQL S3 backup: latest-success manifest unavailable: {error}")
    else:
        last_modified = head.get("LastModified")
        if not isinstance(last_modified, datetime):
            failed = True
            messages.append("PostgreSQL S3 backup: manifest has no LastModified timestamp")
        else:
            age_hours = ((now or datetime.now(UTC)) - last_modified).total_seconds() / 3600
            if age_hours > settings.s3_backup_max_age_hours:
                failed = True
                messages.append(
                    f"PostgreSQL S3 backup: latest manifest is stale ({age_hours:.1f} hours old)"
                )
        messages.append(
            f"PostgreSQL S3 backup: latest successful manifest updated {last_modified}"
        )
    if failed:
        raise RuntimeError("\n".join(messages))
    return messages


def main() -> None:
    try:
        messages = check_archive_health()
    except RuntimeError as error:
        print(error)
        raise SystemExit(1) from error
    print("\n".join(messages))


if __name__ == "__main__":
    main()
