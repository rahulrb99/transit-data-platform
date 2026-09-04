import ast
from pathlib import Path

import pytest

from scripts import postgres_backup

ROOT = Path(__file__).resolve().parents[1]


class FakeUploader:
    def __init__(self, fail=False):
        self.fail = fail
        self.calls = []

    def upload_backup(
        self, dump, checksum_sidecar, *, backup_started_at_monotonic=None
    ):
        del backup_started_at_monotonic
        self.calls.append((dump, checksum_sidecar))
        if self.fail:
            raise RuntimeError("upload failed")
        return "s3://bucket/backups/example.dump"


def test_backup_preserves_binary_and_records_checksum(tmp_path, monkeypatch):
    payload = b"PGDMP\x00\xff\r\n"
    def run(compose, command, *, stdin=None, stdout=None):
        if stdout:
            stdout.write(payload)
        else:
            assert stdin.read() == payload
    monkeypatch.setattr(postgres_backup, "run", run)
    result = postgres_backup.backup([], tmp_path)
    assert result.read_bytes() == payload
    assert result.with_suffix(".dump.sha256").read_text().strip() == postgres_backup.digest(result)


def test_failed_backup_does_not_publish_partial_file(tmp_path, monkeypatch):
    def fail(*args, **kwargs):
        kwargs["stdout"].write(b"partial")
        raise RuntimeError("dump failed")
    monkeypatch.setattr(postgres_backup, "run", fail)
    with pytest.raises(RuntimeError):
        postgres_backup.backup([], tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_backup_is_not_successful_when_s3_upload_fails(tmp_path, monkeypatch):
    payload = b"PGDMP"
    def run(compose, command, *, stdin=None, stdout=None):
        if stdout:
            stdout.write(payload)
    monkeypatch.setattr(postgres_backup, "run", run)
    uploader = FakeUploader(fail=True)
    with pytest.raises(RuntimeError, match="upload failed"):
        postgres_backup.backup([], tmp_path, uploader=uploader)
    assert len(uploader.calls) == 1
    assert uploader.calls[0][0].exists()


def test_backup_uploads_dump_and_checksum_before_returning(tmp_path, monkeypatch):
    payload = b"PGDMP"
    def run(compose, command, *, stdin=None, stdout=None):
        if stdout:
            stdout.write(payload)
    monkeypatch.setattr(postgres_backup, "run", run)
    uploader = FakeUploader()
    result = postgres_backup.backup([], tmp_path, uploader=uploader)
    assert uploader.calls == [(result, result.with_suffix(".dump.sha256"))]


def test_restore_refuses_working_database_and_corruption(tmp_path):
    with pytest.raises(ValueError, match="new"):
        postgres_backup.restore([], Path("unused"), "transit")
    source = tmp_path / "test.dump"
    source.write_bytes(b"corrupt")
    source.with_suffix(".dump.sha256").write_text("wrong")
    with pytest.raises(ValueError, match="checksum"):
        postgres_backup.restore([], source, "validation_restore")


def test_restore_reports_successful_validation_duration(tmp_path, monkeypatch):
    source = tmp_path / "test.dump"
    source.write_bytes(b"valid")
    source.with_suffix(".dump.sha256").write_text(
        postgres_backup.digest(source),
        encoding="ascii",
    )
    commands = []
    monkeypatch.setattr(
        postgres_backup,
        "run",
        lambda compose, command, **kwargs: commands.append(command),
    )
    timestamps = iter((10.0, 12.5))
    monkeypatch.setattr(postgres_backup.time, "monotonic", lambda: next(timestamps))

    duration = postgres_backup.restore([], source, "validation_restore")

    assert duration == 2.5
    assert len(commands) == 4
    assert "createdb" in commands[1]


def test_host_backup_import_path_is_python_39_compatible() -> None:
    for relative_path in (
        "scripts/postgres_backup.py",
        "ingestion/s3_archive.py",
        "ingestion/raw_archive.py",
        "ingestion/config.py",
    ):
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        ast.parse(source, filename=relative_path, feature_version=(3, 9))
        assert "from datetime import UTC" not in source
        assert "hashlib.file_digest" not in source
