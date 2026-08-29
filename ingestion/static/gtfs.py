from ingestion.static.download_mbta_gtfs import download_mbta_static_gtfs
from ingestion.static.load_static_gtfs import load_static_gtfs


def ingest_static_gtfs() -> None:
    zip_path = download_mbta_static_gtfs()
    load_static_gtfs(zip_path)


if __name__ == "__main__":
    ingest_static_gtfs()

