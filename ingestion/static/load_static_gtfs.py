import csv
from pathlib import Path
from zipfile import ZipFile

from psycopg import sql

from ingestion.db import connect

GTFS_TABLES = [
    "agency",
    "routes",
    "stops",
    "trips",
    "stop_times",
    "calendar",
    "calendar_dates",
]


def latest_static_zip(raw_root: Path = Path("data/raw/static")) -> Path:
    candidates = sorted(raw_root.glob("*/mbta_gtfs.zip"))
    if not candidates:
        raise FileNotFoundError("No MBTA GTFS ZIP found under data/raw/static")
    return candidates[-1]


def recreate_raw_table(table_name: str, columns: list[str]) -> None:
    column_defs = [sql.SQL("{} TEXT").format(sql.Identifier(column)) for column in columns]
    drop_query = sql.SQL("DROP TABLE IF EXISTS raw.{}").format(sql.Identifier(table_name))
    create_query = sql.SQL("CREATE TABLE raw.{} ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(column_defs),
    )
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(drop_query)
            cur.execute(create_query)


def load_csv_bytes(table_name: str, raw_bytes: bytes) -> int:
    header_line = raw_bytes.splitlines()[0].decode("utf-8-sig")
    columns = next(csv.reader([header_line]))
    recreate_raw_table(table_name, columns)

    copy_query = sql.SQL(
        "COPY raw.{} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
    ).format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
    )

    with connect() as conn:
        with conn.cursor() as cur:
            with cur.copy(copy_query) as copy:
                copy.write(raw_bytes)
            cur.execute(
                sql.SQL("SELECT count(*) FROM raw.{}").format(sql.Identifier(table_name))
            )
            row_count = cur.fetchone()[0]
    return row_count


def load_static_gtfs(zip_path: Path | None = None) -> None:
    source_zip = zip_path or latest_static_zip()
    with ZipFile(source_zip) as gtfs_zip:
        for table_name in GTFS_TABLES:
            member = f"{table_name}.txt"
            if member not in gtfs_zip.namelist():
                continue
            row_count = load_csv_bytes(table_name, gtfs_zip.read(member))
            print(f"Loaded raw.{table_name}: {row_count:,} rows")


if __name__ == "__main__":
    load_static_gtfs()
    print("Loaded selected MBTA GTFS static files into raw schema")
