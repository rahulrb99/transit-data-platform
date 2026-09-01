"""Real PostgreSQL transaction tests, restricted to the deterministic CI database."""
import gzip
import json
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from ingestion.config import get_settings
from ingestion.db import connect
from ingestion.raw_archive import RawArchive
from ingestion.retention import archive_delete_batch
from ingestion.static.load_static_gtfs import _load_static_gtfs_atomically
from scripts import migrate_database

pytestmark = pytest.mark.integration


@pytest.fixture
def database():
    if not get_settings().postgres_db.endswith("_ci"):
        pytest.skip("Readiness integration tests require an isolated *_ci database")
    with connect() as connection, connection.transaction(force_rollback=True):
        yield connection


def test_static_reload_preserves_dependent_view_and_rollback(database):
    original = database.execute("SELECT route_id FROM raw.routes ORDER BY route_id").fetchall()
    database.execute("CREATE TEMP VIEW dependent_routes AS SELECT route_id FROM raw.routes")
    with database.transaction(force_rollback=True):
        _load_static_gtfs_atomically(database, {"routes": b"route_id,route_type\nreplacement,3\n"})
        assert database.execute("SELECT * FROM dependent_routes").fetchall() == [("replacement",)]
    assert database.execute("SELECT * FROM dependent_routes ORDER BY route_id").fetchall() == original


def test_static_copy_failure_leaves_previous_data(database):
    original = database.execute("SELECT * FROM raw.routes").fetchall()
    with pytest.raises(psycopg.Error), database.transaction():
        _load_static_gtfs_atomically(database, {
            "routes": b"route_id,route_type\nnew,3\n",
            "stops": b"stop_id,stop_name\nbad,too,many,columns\n",
        })
    assert database.execute("SELECT * FROM raw.routes").fetchall() == original


def test_static_activation_failure_rolls_back_earlier_tables(database):
    original = database.execute("SELECT * FROM raw.routes").fetchall()
    database.execute("ALTER TABLE raw.stops ADD CONSTRAINT readiness_reject CHECK (stop_id <> 'bad')")
    with pytest.raises(psycopg.errors.CheckViolation), database.transaction():
        _load_static_gtfs_atomically(database, {
            "routes": b"route_id,route_type\nreplacement,3\n",
            "stops": b"stop_id,stop_name\nbad,rejected\n",
        })
    assert database.execute("SELECT * FROM raw.routes").fetchall() == original


def test_retention_archives_exact_rows_and_is_repeatable(database, tmp_path):
    database.execute("CREATE TEMP TABLE retention_fixture (id INT PRIMARY KEY, ingested_at TIMESTAMPTZ)")
    now = datetime.now(UTC)
    database.execute("INSERT INTO retention_fixture VALUES (1, %s), (2, %s)",
                     (now - timedelta(days=30), now))
    archive = RawArchive(tmp_path, min_free_bytes=0)
    args = (database, archive, "retention_fixture", "id", "ingested_at", now - timedelta(days=14), 1)
    assert archive_delete_batch(*args) == 1
    assert archive_delete_batch(*args) == 0
    assert database.execute("SELECT id FROM retention_fixture").fetchall() == [(2,)]
    records = [json.loads(line) for line in gzip.decompress(next(tmp_path.rglob("*.gz")).read_bytes()).splitlines()]
    assert [row["id"] for row in records] == [1]


def test_migrations_are_transactional_and_detect_changed_history(database, tmp_path, monkeypatch):
    database.execute("CREATE SCHEMA readiness_migrations")
    database.execute("SET LOCAL search_path TO readiness_migrations")
    @contextmanager
    def connection():
        with database.transaction():
            yield database
    monkeypatch.setattr(migrate_database, "connect", connection)
    monkeypatch.setattr(migrate_database, "MIGRATIONS_DIR", tmp_path)
    first = tmp_path / "001_first.sql"
    first.write_text("CREATE TABLE example (id INTEGER);")
    assert migrate_database.run_migrations() == ["001_first"]
    assert migrate_database.run_migrations() == []
    second = tmp_path / "002_failure.sql"
    second.write_text("CREATE TABLE should_rollback (id INTEGER); SELECT missing_column;")
    with pytest.raises(psycopg.Error):
        migrate_database.run_migrations()
    assert database.execute("SELECT to_regclass('should_rollback')").fetchone()[0] is None
    assert database.execute("SELECT count(*) FROM schema_migrations").fetchone()[0] == 1
    second.unlink()
    first.write_text("CREATE TABLE changed (id INTEGER);")
    with pytest.raises(ValueError, match="modified"):
        migrate_database.run_migrations()
