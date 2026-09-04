from __future__ import annotations

import base64
import hashlib
import io
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from ingestion import archive_health
from ingestion.s3_archive import (
    S3BackupArchive,
    S3ObjectStore,
    S3RawArchive,
    S3UploadError,
    normalize_key,
)


class FakeS3Client:
    def __init__(
        self, *, fail_upload: bool = False, corrupt_head: bool = False,
        composite_checksum: bool = False,
    ) -> None:
        self.fail_upload = fail_upload
        self.corrupt_head = corrupt_head
        self.objects = {}
        self.uploads = []
        self.composite_checksum = composite_checksum

    def upload_file(self, filename, bucket, key, ExtraArgs):
        if self.fail_upload:
            raise RuntimeError("simulated S3 outage")
        content = Path(filename).read_bytes()
        checksum = hashlib.sha256(content).digest()
        self.objects[(bucket, key)] = {
            "ContentLength": len(content) + (1 if self.corrupt_head else 0),
            "ServerSideEncryption": ExtraArgs["ServerSideEncryption"],
            "Metadata": ExtraArgs["Metadata"],
            "ChecksumSHA256": (
                base64.b64encode(checksum).decode("ascii") + "-2"
                if self.composite_checksum else base64.b64encode(checksum).decode("ascii")
            ),
            "LastModified": datetime(2026, 8, 31, 12, tzinfo=UTC),
        }
        self.uploads.append((bucket, key, ExtraArgs, content))

    def head_object(self, Bucket, Key, **kwargs):
        return self.objects[(Bucket, Key)]

    def get_object(self, Bucket, Key):
        content = next(row[3] for row in self.uploads if row[0:2] == (Bucket, Key))
        return {"Body": io.BytesIO(content)}


def test_default_s3_client_uses_sdk_credential_chain(monkeypatch):
    calls = []
    fake_client = FakeS3Client()

    def client(service, **kwargs):
        calls.append((service, kwargs))
        return fake_client

    boto3_module = ModuleType("boto3")
    boto3_module.client = client
    botocore_module = ModuleType("botocore")
    config_module = ModuleType("botocore.config")
    config_module.Config = lambda **kwargs: kwargs
    monkeypatch.setitem(sys.modules, "boto3", boto3_module)
    monkeypatch.setitem(sys.modules, "botocore", botocore_module)
    monkeypatch.setitem(sys.modules, "botocore.config", config_module)
    store = S3ObjectStore("transit-private", "us-east-1")

    assert store.client is fake_client
    assert calls[0][0] == "s3"
    assert calls[0][1]["region_name"] == "us-east-1"
    assert not {
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
    } & calls[0][1].keys()


def test_s3_upload_requests_encryption_and_verifies_checksum(tmp_path):
    source = tmp_path / "vehicle.pb"
    source.write_bytes(b"protobuf")
    client = FakeS3Client()
    store = S3ObjectStore("transit-private", "us-east-1", client=client)

    result = store.upload_verified(source, "raw/vehicle.pb")

    _, _, args, content = client.uploads[0]
    assert content == b"protobuf"
    assert args["ServerSideEncryption"] == "AES256"
    assert args["ChecksumAlgorithm"] == "SHA256"
    assert result.uri == "s3://transit-private/raw/vehicle.pb"


def test_s3_verification_failure_is_visible(tmp_path):
    source = tmp_path / "vehicle.pb"
    source.write_bytes(b"protobuf")
    store = S3ObjectStore(
        "transit-private", "us-east-1", client=FakeS3Client(corrupt_head=True)
    )
    with pytest.raises(S3UploadError, match="verification failed"):
        store.upload_verified(source, "raw/vehicle.pb")


def test_multipart_composite_checksum_streams_object_for_verification(tmp_path):
    source = tmp_path / "backup.dump"
    source.write_bytes(b"database dump")
    store = S3ObjectStore(
        "transit-private", "us-east-1", client=FakeS3Client(composite_checksum=True)
    )
    assert store.upload_verified(source, "backups/backup.dump").bytes_uploaded == 13


def test_raw_archive_failure_records_status_and_preserves_source(tmp_path):
    source = tmp_path / "vehicle.pb"
    source.write_bytes(b"protobuf")
    status = tmp_path / "status.json"
    archive = S3RawArchive(
        S3ObjectStore("transit-private", "us-east-1", client=FakeS3Client(fail_upload=True)),
        "raw-archives",
        status,
    )
    with pytest.raises(S3UploadError, match="simulated S3 outage"):
        archive.archive_file(source, Path("vehicle_positions/vehicle.pb"))
    assert source.exists()
    payload = archive_health.load_status(status)
    assert payload["success"] is False
    assert "simulated S3 outage" in payload["error"]


def test_backup_manifest_is_uploaded_last(tmp_path):
    dump = tmp_path / "backup.dump"
    sidecar = tmp_path / "backup.dump.sha256"
    dump.write_bytes(b"database")
    sidecar.write_text(hashlib.sha256(b"database").hexdigest())
    status = tmp_path / "backup-status.json"
    client = FakeS3Client()
    archive = S3BackupArchive(
        S3ObjectStore("transit-private", "us-east-1", client=client),
        "postgres-backups",
        status,
    )

    uri = archive.upload_backup(dump, sidecar)

    assert uri.endswith("/backup.dump")
    assert client.uploads[-1][1] == "postgres-backups/_status/latest-success.json"
    payload = archive_health.load_status(status)
    assert payload["success"] is True
    assert payload["backup_size_bytes"] == 8
    assert payload["backup_duration_seconds"] >= 0
    assert payload["checksum_verified"] is True
    manifest = json.loads(client.uploads[-1][3])
    assert manifest["bytes"] == 8
    assert manifest["checksum_verified"] is True


def test_archive_health_reports_failure_and_stale_backup(tmp_path, monkeypatch):
    status = tmp_path / "raw-status.json"
    status.write_text('{"success": false, "checked_at": "now", "error": "denied"}')
    settings = SimpleNamespace(
        archive_backend="s3",
        archive_status_path=status,
        backup_status_path=tmp_path / "backup-status.json",
        s3_postgres_backup_prefix="postgres-backups",
        s3_backup_max_age_hours=26,
    )
    monkeypatch.setattr(archive_health, "get_settings", lambda: settings)
    store = SimpleNamespace(
        head=lambda key: {"LastModified": datetime.now(UTC) - timedelta(hours=30)}
    )
    with pytest.raises(RuntimeError, match="UPLOAD FAILED") as error:
        archive_health.check_archive_health(store=store)
    assert "stale" in str(error.value)


def test_archive_health_reports_recent_success(tmp_path, monkeypatch):
    status = tmp_path / "raw-status.json"
    status.write_text(
        '{"success": true, "checked_at": "2026-08-31T12:00:00+00:00"}'
    )
    settings = SimpleNamespace(
        archive_backend="s3",
        archive_status_path=status,
        backup_status_path=tmp_path / "backup-status.json",
        s3_postgres_backup_prefix="postgres-backups",
        s3_backup_max_age_hours=26,
    )
    monkeypatch.setattr(archive_health, "get_settings", lambda: settings)
    now = datetime(2026, 8, 31, 13, tzinfo=UTC)
    store = SimpleNamespace(
        head=lambda key: {"LastModified": now - timedelta(hours=1)}
    )

    messages = archive_health.check_archive_health(store=store, now=now)

    assert any("latest upload succeeded" in message for message in messages)
    assert any("latest successful manifest" in message for message in messages)


@pytest.mark.parametrize("key", ["../secret", "raw/../secret", r"raw\secret"])
def test_s3_keys_cannot_escape_configured_prefix(key):
    with pytest.raises(ValueError):
        normalize_key(key)
