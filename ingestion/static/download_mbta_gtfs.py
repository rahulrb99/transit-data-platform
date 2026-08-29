from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import requests

from ingestion.config import get_settings
from ingestion.metadata import IngestionMetadata, record_ingestion


def checksum(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_mbta_static_gtfs() -> Path:
    settings = get_settings()
    run_date = datetime.now(UTC).date().isoformat()
    output_dir = settings.raw_data_root / "static" / run_date
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "mbta_gtfs.zip"
    response = requests.get(settings.mbta_gtfs_static_url, timeout=60)
    response.raise_for_status()
    output_path.write_bytes(response.content)

    record_ingestion(
        IngestionMetadata(
            source=settings.mbta_gtfs_static_url,
            file_name=output_path.name,
            record_count=None,
            checksum_sha256=checksum(output_path),
            raw_path=output_path,
        )
    )
    return output_path


if __name__ == "__main__":
    path = download_mbta_static_gtfs()
    print(f"Downloaded MBTA GTFS static feed to {path}")

