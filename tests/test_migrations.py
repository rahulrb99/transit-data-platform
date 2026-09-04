from __future__ import annotations

from pathlib import Path
from typing import Self

from scripts import migrate_database

ROOT = Path(__file__).resolve().parents[1]


class FakeResult:
    def __init__(self, rows: list[tuple[str]] | None = None) -> None:
        self._rows = rows or []

    def fetchall(self) -> list[tuple[str]]:
        return self._rows


class FakeTransaction:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class FakeConnection:
    def __init__(
        self,
        applied: set[str],
        executed: list[tuple[str, tuple[object, ...] | None]],
    ) -> None:
        self.applied = applied
        self.executed = executed

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> FakeResult:
        self.executed.append((query, params))
        if "SELECT version FROM schema_migrations" in query:
            return FakeResult([(version,) for version in sorted(self.applied)])
        if "SELECT version, checksum_sha256 FROM schema_migrations" in query:
            return FakeResult([(version, None) for version in sorted(self.applied)])
        if "INSERT INTO schema_migrations" in query and params:
            self.applied.add(str(params[0]))
        return FakeResult()


def test_run_migrations_applies_only_pending_files(tmp_path: Path, monkeypatch) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "001_initial_schema.sql").write_text("SELECT 1;", encoding="utf-8")
    (migrations_dir / "002_retention_index.sql").write_text("SELECT 2;", encoding="utf-8")
    applied = {"001_initial_schema"}
    executed: list[tuple[str, tuple[object, ...] | None]] = []

    monkeypatch.setattr(migrate_database, "MIGRATIONS_DIR", migrations_dir)
    monkeypatch.setattr(
        migrate_database,
        "connect",
        lambda: FakeConnection(applied, executed),
    )

    completed = migrate_database.run_migrations()

    assert completed == ["002_retention_index"]
    assert applied == {"001_initial_schema", "002_retention_index"}
    assert any(query == "SELECT 2;" for query, _ in executed)
    assert not any(query == "SELECT 1;" for query, _ in executed)


def test_reliability_metric_migration_matches_fresh_database_schema() -> None:
    migration = (
        ROOT / "infrastructure/postgres/migrations/003_pipeline_reliability_metrics.sql"
    ).read_text(encoding="utf-8")
    initial_schema = (ROOT / "infrastructure/postgres/init.sql").read_text(
        encoding="utf-8"
    )

    for column in ("retry_count", "failed_batch_count"):
        assert column in migration
        assert column in initial_schema
