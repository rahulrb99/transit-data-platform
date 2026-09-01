from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from ingestion.config import get_settings


@dataclass(frozen=True)
class ArchiveResult:
    source_path: Path
    destination: str
    checksum_sha256: str
    bytes_archived: int

    @property
    def archive_path(self) -> Path:
        """Compatibility accessor for the local filesystem backend."""
        return Path(self.destination)


class ArchiveBackend(Protocol):
    """Durable archive contract. A future S3 backend must verify before returning."""

    def archive_file(
        self, source_path: Path, relative_path: Path | None = None
    ) -> ArchiveResult: ...


class RawArchive:
    """Filesystem archive boundary for raw payloads before local deletion."""

    def __init__(self, root: Path | None = None, *, min_free_bytes: int | None = None) -> None:
        self.root = root or get_settings().raw_archive_root
        self.min_free_bytes = (
            get_settings().archive_min_free_bytes if min_free_bytes is None else min_free_bytes
        )

    def archive_file(self, source_path: Path, relative_path: Path | None = None) -> ArchiveResult:
        if not source_path.is_file():
            raise FileNotFoundError(f"Raw payload does not exist: {source_path}")

        root = self.root.resolve()
        archive_path = (root / (relative_path or source_path.name)).resolve()
        if not archive_path.is_relative_to(root) or archive_path == source_path.resolve():
            raise ValueError("Archive must be a distinct file inside the archive root")
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        checksum = _checksum_file(source_path)
        if archive_path.exists():
            if _checksum_file(archive_path) != checksum:
                raise ValueError(f"Archive collision: {archive_path}")
        else:
            if shutil.disk_usage(root).free < source_path.stat().st_size + self.min_free_bytes:
                raise OSError("Archive disk reserve would be exhausted; source retained")
            fd, name = tempfile.mkstemp(prefix=".archive-", dir=archive_path.parent)
            temporary = Path(name)
            try:
                with os.fdopen(fd, "wb") as target, source_path.open("rb") as source:
                    shutil.copyfileobj(source, target)
                    target.flush()
                    os.fsync(target.fileno())
                if _checksum_file(temporary) != checksum or _checksum_file(source_path) != checksum:
                    raise OSError("Archive verification failed; source retained")
                try:
                    os.link(temporary, archive_path)
                except FileExistsError:
                    if _checksum_file(archive_path) != checksum:
                        raise ValueError(f"Archive collision: {archive_path}") from None
                if os.name != "nt":
                    directory = os.open(archive_path.parent, os.O_RDONLY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
            finally:
                temporary.unlink(missing_ok=True)
        return ArchiveResult(
            source_path=source_path,
            destination=str(archive_path),
            checksum_sha256=checksum,
            bytes_archived=archive_path.stat().st_size,
        )


def _checksum_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_archive_backend() -> ArchiveBackend:
    settings = get_settings()
    if settings.archive_backend == "local":
        return RawArchive()
    from ingestion.s3_archive import S3RawArchive, build_s3_store

    return S3RawArchive(
        store=build_s3_store(settings),
        prefix=settings.s3_raw_archive_prefix,
        status_path=settings.archive_status_path,
    )
