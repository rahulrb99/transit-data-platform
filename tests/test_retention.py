from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Self

import pytest

from ingestion import retention
from ingestion.raw_archive import RawArchive


class FakeConnection:
    def __init__(self) -> None:
        self.queries: list[tuple[str, object]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query, params=None):
        self.queries.append((str(query), params))
        if "SELECT" in str(query) and "to_jsonb" in str(query):
            return SimpleNamespace(fetchall=lambda: [(1, json.dumps({"id": 1}))])
        return SimpleNamespace(rowcount=1)


def test_postgres_retention_deletes_rows_before_configured_cutoffs(monkeypatch, tmp_path) -> None:
    fake_connection = FakeConnection()
    monkeypatch.setattr(retention, "connect", lambda: fake_connection)
    monkeypatch.setattr(
        retention,
        "get_settings",
        lambda: SimpleNamespace(realtime_retention_days=14, metrics_retention_days=30,
                                retention_max_batches=1, retention_batch_size=1000),
    )

    realtime_deleted, metrics_deleted = retention.enforce_postgres_retention(
        now=datetime(2026, 8, 31, tzinfo=UTC),
        archive=RawArchive(tmp_path, min_free_bytes=0),
    )

    assert realtime_deleted == 1
    assert metrics_deleted == 1
    selections = [p for q, p in fake_connection.queries if "to_jsonb" in q]
    assert selections[0][0] == datetime(2026, 8, 17, tzinfo=UTC)
    assert selections[1][0] == datetime(2026, 8, 1, tzinfo=UTC)
    assert len(list(tmp_path.rglob("*.gz"))) == 2


def test_archive_failure_prevents_database_delete(tmp_path):
    connection = FakeConnection()
    class BrokenArchive:
        def archive_file(self, *args):
            raise OSError("disk full")
    with pytest.raises(OSError, match="disk full"):
        retention.archive_delete_batch(connection, BrokenArchive(), "realtime_vehicle_positions",
                                       "id", "ingested_at", datetime.now(UTC), 100)
    assert not any("DELETE" in q for q, _ in connection.queries)


def test_archive_is_idempotent_and_rejects_collision_and_escape(tmp_path):
    source = tmp_path / "source.pb"
    source.write_bytes(b"original")
    archive = RawArchive(tmp_path / "archive", min_free_bytes=0)
    first = archive.archive_file(source)
    assert archive.archive_file(source) == first
    with pytest.raises(ValueError, match="inside"):
        archive.archive_file(source, Path("../escape.pb"))
    source.write_bytes(b"changed")
    with pytest.raises(ValueError, match="collision"):
        archive.archive_file(source)
    assert first.archive_path.read_bytes() == b"original"


def test_disk_reserve_preserves_source(tmp_path):
    source = tmp_path / "source.pb"
    source.write_bytes(b"original")
    with pytest.raises(OSError, match="reserve"):
        RawArchive(tmp_path / "archive", min_free_bytes=10**30).archive_file(source)
    assert source.read_bytes() == b"original"


def test_raw_payload_retention_archives_before_delete(tmp_path: Path, monkeypatch) -> None:
    raw_root = tmp_path / "raw" / "realtime" / "vehicle_positions"
    old_file = raw_root / "2026-08-01" / "vehicle_positions_old.pb"
    new_file = raw_root / "2026-08-31" / "vehicle_positions_new.pb"
    old_file.parent.mkdir(parents=True)
    new_file.parent.mkdir(parents=True)
    old_file.write_bytes(b"old")
    new_file.write_bytes(b"new")

    old_timestamp = datetime(2026, 8, 1, tzinfo=UTC).timestamp()
    new_timestamp = datetime(2026, 8, 31, tzinfo=UTC).timestamp()
    os.utime(old_file, (old_timestamp, old_timestamp))
    os.utime(new_file, (new_timestamp, new_timestamp))

    monkeypatch.setattr(
        retention,
        "get_settings",
        lambda: SimpleNamespace(raw_payload_retention_days=7),
    )
    archived, deleted = retention.enforce_raw_payload_retention(
        raw_root=raw_root,
        archive=RawArchive(tmp_path / "archive"),
        now=datetime(2026, 8, 31, tzinfo=UTC),
    )

    assert (archived, deleted) == (1, 1)
    assert not old_file.exists()
    assert new_file.exists()
    assert (tmp_path / "archive" / "vehicle_positions" / "2026-08-01" / old_file.name).read_bytes() == b"old"
