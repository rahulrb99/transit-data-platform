"""Binary-safe local Compose backups and restores into a NEW database only."""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from ingestion.config import Settings
from ingestion.s3_archive import S3BackupArchive, build_s3_store, write_status

UTC = timezone.utc  # noqa: UP017 - datetime.UTC is unavailable on Python 3.9.


class BackupUploader(Protocol):
    def upload_backup(self, dump: Path, checksum_sidecar: Path) -> str: ...


def digest(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def run(compose: list[str], command: str, *, stdin=None, stdout=None):
    return subprocess.run(
        [*compose, "exec", "-T", "postgres", "sh", "-eu", "-c", command],
        stdin=stdin, stdout=stdout, check=True,
    )


def backup(
    compose: list[str], output: Path, uploader: BackupUploader | None = None
) -> Path:
    output.mkdir(parents=True, exist_ok=True)
    name = (
        "transit_"
        + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        + ".dump"
    )
    target = output / name
    pending = target.with_suffix(".partial")
    try:
        with pending.open("xb") as stream:
            run(compose, 'pg_dump -Fc --no-owner --no-acl -U "$POSTGRES_USER" '
                         '-d "$POSTGRES_DB"', stdout=stream)
            stream.flush()
            os.fsync(stream.fileno())
        with pending.open("rb") as stream:
            run(compose, "pg_restore --list >/dev/null", stdin=stream)
        checksum = digest(pending)
        pending.rename(target)
        sidecar = target.with_suffix(".dump.sha256")
        sidecar.write_text(checksum + "\n", encoding="ascii")
        if uploader is not None:
            uploader.upload_backup(target, sidecar)
        return target
    finally:
        pending.unlink(missing_ok=True)


def restore(compose: list[str], source: Path, target_database: str) -> None:
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,47}_restore", target_database):
        raise ValueError("Restore target must be a new lower-case name ending in _restore")
    if digest(source) != source.with_suffix(".dump.sha256").read_text().strip():
        raise ValueError("Backup checksum mismatch")
    with source.open("rb") as stream:
        run(compose, "pg_restore --list >/dev/null", stdin=stream)
    # createdb refuses an existing database; never drop or overwrite the working database.
    run(compose, f'createdb -U "$POSTGRES_USER" {target_database}')
    with source.open("rb") as stream:
        run(compose, 'pg_restore --exit-on-error --single-transaction --no-owner --no-acl '
                     f'-U "$POSTGRES_USER" -d {target_database}', stdin=stream)
    run(compose, f'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d {target_database} '
                 '-c "SELECT count(*) AS observations FROM realtime_vehicle_positions"')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--production", action="store_true")
    parser.add_argument("--upload-s3", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)
    save = commands.add_parser("backup")
    save.add_argument("--output", type=Path, default=Path("backups"))
    load = commands.add_parser("restore")
    load.add_argument("backup", type=Path)
    load.add_argument("--database", required=True)
    args = parser.parse_args()
    compose = ["docker", "compose"]
    if args.production:
        compose += ["--env-file", ".env.prod", "-f", "docker-compose.yml",
                    "-f", "docker-compose.prod.yml"]
    if args.command == "backup":
        settings = Settings(_env_file=".env.prod" if args.production else ".env")
        use_s3 = args.production or args.upload_s3
        uploader = None
        if use_s3:
            if settings.archive_backend != "s3":
                raise RuntimeError("S3 backup requested but ARCHIVE_BACKEND is not s3")
            uploader = S3BackupArchive(
                build_s3_store(settings),
                settings.s3_postgres_backup_prefix,
                settings.backup_status_path,
            )
        try:
            completed = backup(compose, args.output, uploader=uploader)
        except Exception as error:
            if use_s3:
                write_status(
                    settings.backup_status_path,
                    success=False,
                    key="postgres-backup",
                    error=error,
                )
            raise
        print(completed)
    else:
        restore(compose, args.backup, args.database)


if __name__ == "__main__":
    main()
