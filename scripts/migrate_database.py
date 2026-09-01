from __future__ import annotations

import hashlib
import re
from pathlib import Path

from ingestion.db import connect

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "infrastructure/postgres/migrations"


def run_migrations() -> list[str]:
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migration_files or any(
        not re.fullmatch(r"\d{3}_[a-z0-9_]+", p.stem) for p in migration_files
    ):
        raise ValueError("Missing migrations or invalid NNN_description.sql filename")
    if len({p.name[:3] for p in migration_files}) != len(migration_files):
        raise ValueError("Duplicate migration version")
    completed: list[str] = []
    with connect() as connection:
        connection.execute("SET LOCAL lock_timeout = '30s'")
        connection.execute("SELECT pg_advisory_xact_lock(74120901)")
        connection.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                checksum_sha256 TEXT
            )
        """)
        connection.execute(
            "ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum_sha256 TEXT"
        )
        applied = dict(connection.execute(
            "SELECT version, checksum_sha256 FROM schema_migrations"
        ).fetchall())
        if set(applied) - {p.stem for p in migration_files}:
            raise ValueError("Database has migrations missing from this release; refuse downgrade")
        for migration_file in migration_files:
            version = migration_file.stem
            content = migration_file.read_text(encoding="utf-8")
            checksum = hashlib.sha256(content.encode()).hexdigest()
            if version in applied:
                if applied[version] not in (None, checksum):
                    raise ValueError(f"Applied migration was modified: {migration_file.name}")
                connection.execute(
                    "UPDATE schema_migrations SET checksum_sha256 = %s WHERE version = %s",
                    (checksum, version),
                )
                continue
            if applied and version < max(applied):
                raise ValueError(f"Out-of-order migration: {version}")
            with connection.transaction():
                connection.execute(content)
                connection.execute(
                    "INSERT INTO schema_migrations (version, checksum_sha256) VALUES (%s, %s)",
                    (version, checksum),
                )
            completed.append(version)
    return completed


def main() -> None:
    completed = run_migrations()
    if completed:
        print("Applied migrations: " + ", ".join(completed))
    else:
        print("No pending migrations")


if __name__ == "__main__":
    main()
