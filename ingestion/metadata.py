from dataclasses import dataclass
from pathlib import Path

from ingestion.db import connect


@dataclass(frozen=True)
class IngestionMetadata:
    source: str
    file_name: str
    record_count: int | None
    checksum_sha256: str
    raw_path: Path


def record_ingestion(metadata: IngestionMetadata) -> None:
    sql = """
        INSERT INTO raw.ingestion_metadata (
            source,
            file_name,
            record_count,
            checksum_sha256,
            raw_path
        )
        VALUES (%s, %s, %s, %s, %s)
    """
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    metadata.source,
                    metadata.file_name,
                    metadata.record_count,
                    metadata.checksum_sha256,
                    str(metadata.raw_path),
                ),
            )

