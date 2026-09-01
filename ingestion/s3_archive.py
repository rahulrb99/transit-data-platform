"""Verified S3 archive implementations using the default AWS credential chain."""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from ingestion.config import Settings
from ingestion.raw_archive import ArchiveResult, _checksum_file

LOGGER = logging.getLogger(__name__)
UTC = timezone.utc  # noqa: UP017 - datetime.UTC is unavailable on Python 3.9.


class S3UploadError(RuntimeError):
    pass


@dataclass(frozen=True)
class S3UploadResult:
    bucket: str
    key: str
    checksum_sha256: str
    bytes_uploaded: int

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


class S3ObjectStore:
    def __init__(self, bucket: str, region: str, *, client: Any | None = None) -> None:
        if not bucket or not region:
            raise ValueError("S3 bucket and AWS region are required")
        self.bucket = bucket
        self.region = region
        if client is None:
            import boto3
            from botocore.config import Config

            # No credentials are passed: EC2 uses its attached instance role through IMDSv2.
            client = boto3.client(
                "s3",
                region_name=region,
                config=Config(
                    connect_timeout=10,
                    read_timeout=120,
                    retries={"mode": "standard", "max_attempts": 5},
                ),
            )
        self.client = client

    def upload_verified(self, source: Path, key: str) -> S3UploadResult:
        if not source.is_file():
            raise FileNotFoundError(source)
        normalized_key = normalize_key(key)
        checksum_hex = _checksum_file(source)
        checksum_base64 = base64.b64encode(bytes.fromhex(checksum_hex)).decode("ascii")
        size = source.stat().st_size
        self.client.upload_file(
            str(source),
            self.bucket,
            normalized_key,
            ExtraArgs={
                "ServerSideEncryption": "AES256",
                "ChecksumAlgorithm": "SHA256",
                "Metadata": {"sha256": checksum_hex},
            },
        )
        head = self.client.head_object(
            Bucket=self.bucket,
            Key=normalized_key,
            ChecksumMode="ENABLED",
        )
        if (
            int(head.get("ContentLength", -1)) != size
            or head.get("ServerSideEncryption") != "AES256"
            or head.get("Metadata", {}).get("sha256") != checksum_hex
        ):
            raise S3UploadError(f"S3 verification failed for s3://{self.bucket}/{normalized_key}")
        if head.get("ChecksumSHA256") != checksum_base64:
            # Multipart SHA-256 may be composite. Verify the actual object stream instead.
            response = self.client.get_object(Bucket=self.bucket, Key=normalized_key)
            body = response["Body"]
            digest = hashlib.sha256()
            try:
                for chunk in iter(lambda: body.read(1024 * 1024), b""):
                    digest.update(chunk)
            finally:
                body.close()
            if digest.hexdigest() != checksum_hex:
                raise S3UploadError(
                    f"S3 content verification failed for s3://{self.bucket}/{normalized_key}"
                )
        return S3UploadResult(self.bucket, normalized_key, checksum_hex, size)

    def head(self, key: str) -> dict[str, Any]:
        try:
            return self.client.head_object(Bucket=self.bucket, Key=normalize_key(key))
        except Exception as error:
            raise S3UploadError(f"S3 HeadObject failed: {error}") from error


class S3RawArchive:
    def __init__(self, store: S3ObjectStore, prefix: str, status_path: Path) -> None:
        self.store = store
        self.prefix = normalize_prefix(prefix)
        self.status_path = status_path

    def archive_file(
        self, source_path: Path, relative_path: Path | None = None
    ) -> ArchiveResult:
        relative = relative_path or Path(source_path.name)
        key = join_key(self.prefix, relative.as_posix())
        try:
            result = self.store.upload_verified(source_path, key)
        except Exception as error:
            write_status(self.status_path, success=False, key=key, error=error)
            LOGGER.exception("S3 archive upload failed bucket=%s key=%s", self.store.bucket, key)
            raise S3UploadError(f"S3 archive upload failed for {key}: {error}") from error
        write_status(self.status_path, success=True, key=key)
        return ArchiveResult(
            source_path=source_path,
            destination=result.uri,
            checksum_sha256=result.checksum_sha256,
            bytes_archived=result.bytes_uploaded,
        )


class S3BackupArchive:
    def __init__(self, store: S3ObjectStore, prefix: str, status_path: Path) -> None:
        self.store = store
        self.prefix = normalize_prefix(prefix)
        self.status_path = status_path

    @property
    def latest_manifest_key(self) -> str:
        return join_key(self.prefix, "_status/latest-success.json")

    def upload_backup(self, dump: Path, checksum_sidecar: Path) -> str:
        date_prefix = datetime.now(UTC).strftime("%Y/%m/%d")
        dump_key = join_key(self.prefix, date_prefix, dump.name)
        sidecar_key = dump_key + ".sha256"
        try:
            result = self.store.upload_verified(dump, dump_key)
            self.store.upload_verified(checksum_sidecar, sidecar_key)
            manifest = {
                "completed_at": datetime.now(UTC).isoformat(),
                "dump_uri": result.uri,
                "checksum_sha256": result.checksum_sha256,
                "bytes": result.bytes_uploaded,
            }
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "latest-success.json"
                path.write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8")
                self.store.upload_verified(path, self.latest_manifest_key)
        except Exception as error:
            write_status(self.status_path, success=False, key=dump_key, error=error)
            LOGGER.exception("PostgreSQL backup S3 upload failed bucket=%s key=%s", self.store.bucket, dump_key)
            raise S3UploadError(f"PostgreSQL backup upload failed: {error}") from error
        write_status(self.status_path, success=True, key=dump_key)
        return result.uri


def build_s3_store(settings: Settings) -> S3ObjectStore:
    return S3ObjectStore(
        bucket=settings.s3_bucket_name or "",
        region=settings.aws_region or "",
    )


def normalize_prefix(value: str) -> str:
    normalized = value.strip("/")
    if not normalized:
        raise ValueError("S3 prefix cannot be empty")
    normalize_key(normalized)
    return normalized


def normalize_key(*parts: str) -> str:
    if any("\\" in part or "\x00" in part for part in parts):
        raise ValueError("S3 keys must use POSIX separators and contain no NUL bytes")
    key = PurePosixPath(*[part.strip("/") for part in parts if part]).as_posix()
    if key in {"", "."} or key.startswith(("../", "/")) or "/../" in key:
        raise ValueError("S3 key must stay inside its configured prefix")
    return key


def join_key(*parts: str) -> str:
    return normalize_key(*parts)


def write_status(path: Path, *, success: bool, key: str, error: Exception | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "checked_at": datetime.now(UTC).isoformat(),
        "success": success,
        "key": key,
        "error": f"{type(error).__name__}: {error}" if error else None,
    }
    fd, temporary_name = tempfile.mkstemp(prefix=".s3-status-", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
