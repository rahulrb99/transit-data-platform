import csv
import io
import json
from collections.abc import Mapping
from contextlib import ExitStack
from pathlib import Path
from typing import BinaryIO
from zipfile import ZipFile

from psycopg import Connection, Cursor, sql

from ingestion.config import get_settings
from ingestion.db import connect
from ingestion.raw_archive import ArchiveBackend, _checksum_file, build_archive_backend

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


def _columns_from_csv(raw_bytes: bytes) -> list[str]:
    header_line = raw_bytes.splitlines()[0].decode("utf-8-sig")
    return next(csv.reader([header_line]))


def _create_table(cur: Cursor, table_name: str, columns: list[str]) -> None:
    column_defs = [sql.SQL("{} TEXT").format(sql.Identifier(column)) for column in columns]
    create_query = sql.SQL("CREATE TABLE raw.{} ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(column_defs),
    )
    cur.execute(create_query)


def _copy_csv_bytes(cur: Cursor, table_name: str, columns: list[str], raw_bytes: bytes) -> int:
    copy_query = sql.SQL(
        "COPY raw.{} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
    ).format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
    )
    with cur.copy(copy_query) as copy:
        copy.write(raw_bytes)
    cur.execute(sql.SQL("SELECT count(*) FROM raw.{}").format(sql.Identifier(table_name)))
    return cur.fetchone()[0]


def _stage_table_name(table_name: str) -> str:
    return f"__gtfs_load_{table_name}"


def _load_static_gtfs_atomically(
    conn: Connection, table_payloads: Mapping[str, bytes | BinaryIO]
) -> dict[str, int]:
    row_counts: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = '30s'")
        cur.execute("SELECT pg_advisory_xact_lock(74120902)")
        for table_name in table_payloads:
            if table_name not in GTFS_TABLES:
                raise ValueError(f"Unsupported GTFS table: {table_name}")
            cur.execute(sql.SQL("DROP TABLE IF EXISTS raw.{}").format(sql.Identifier(_stage_table_name(table_name))))

        columns_by_table = {}
        for table_name, payload in table_payloads.items():
            stream = io.BytesIO(payload) if isinstance(payload, bytes) else payload
            stage_table = _stage_table_name(table_name)
            columns = next(csv.reader([stream.readline().decode("utf-8-sig")]))
            if not columns or len(set(columns)) != len(columns) or any(not c for c in columns):
                raise ValueError(f"Invalid CSV header: {table_name}")
            columns_by_table[table_name] = columns
            stream.seek(0)
            _create_table(cur, stage_table, columns)
            with cur.copy(sql.SQL(
                "COPY raw.{} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
            ).format(sql.Identifier(stage_table))) as copy:
                for chunk in iter(lambda stream=stream: stream.read(1024 * 1024), b""):
                    copy.write(chunk)
            cur.execute(sql.SQL("SELECT count(*) FROM raw.{}").format(sql.Identifier(stage_table)))
            row_counts[table_name] = cur.fetchone()[0]
            if not row_counts[table_name] and table_name not in {"calendar", "calendar_dates"}:
                raise ValueError(f"Empty required GTFS table: {table_name}")

        # Keep live table OIDs: DROP/RENAME breaks dependent dbt views. MVCC readers retain
        # the previous committed rows while all tables are activated in one transaction.
        for table_name in table_payloads:
            columns = columns_by_table[table_name]
            cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS raw.{} (LIKE raw.{})").format(
                sql.Identifier(table_name), sql.Identifier(_stage_table_name(table_name))
            ))
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'raw' AND table_name = %s", (table_name,)
            )
            existing_columns = {row[0] for row in cur.fetchall()}
            for column in set(columns) - existing_columns:
                cur.execute(sql.SQL("ALTER TABLE raw.{} ADD COLUMN IF NOT EXISTS {} TEXT").format(
                    sql.Identifier(table_name), sql.Identifier(column)
                ))
            cur.execute(sql.SQL("DELETE FROM raw.{}").format(sql.Identifier(table_name)))
            names = sql.SQL(", ").join(map(sql.Identifier, columns))
            cur.execute(sql.SQL("INSERT INTO raw.{} ({}) SELECT {} FROM raw.{}").format(
                sql.Identifier(table_name), names, names, sql.Identifier(_stage_table_name(table_name))
            ))
            cur.execute(sql.SQL("DROP TABLE raw.{}").format(
                sql.Identifier(_stage_table_name(table_name))
            ))
        for optional in {"calendar", "calendar_dates"} - set(table_payloads):
            cur.execute("SELECT to_regclass(%s)", (f"raw.{optional}",))
            if cur.fetchone()[0]:
                cur.execute(sql.SQL("DELETE FROM raw.{}").format(sql.Identifier(optional)))
    return row_counts


def create_static_gtfs_indexes(cur: Cursor | None = None) -> None:
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_raw_stop_times_trip_sequence ON raw.stop_times (trip_id, stop_sequence)",
        "CREATE INDEX IF NOT EXISTS idx_raw_stop_times_trip_stop ON raw.stop_times (trip_id, stop_id)",
        "CREATE INDEX IF NOT EXISTS idx_raw_trips_trip_id ON raw.trips (trip_id)",
        "CREATE INDEX IF NOT EXISTS idx_raw_trips_route_id ON raw.trips (route_id)",
        "CREATE INDEX IF NOT EXISTS idx_raw_routes_route_id ON raw.routes (route_id)",
        "CREATE INDEX IF NOT EXISTS idx_raw_stops_stop_id ON raw.stops (stop_id)",
    ]
    if cur is not None:
        for statement in index_statements:
            cur.execute(statement)
        return

    with connect() as conn, conn.cursor() as index_cur:
        for statement in index_statements:
            index_cur.execute(statement)


def load_static_gtfs(
    zip_path: Path | None = None, archive: ArchiveBackend | None = None
) -> None:
    source_zip = zip_path or latest_static_zip(get_settings().raw_data_root / "static")
    checksum = _checksum_file(source_zip)
    archived = (archive or build_archive_backend()).archive_file(
        source_zip, Path("static") / f"{checksum}.zip"
    )
    with ZipFile(source_zip) as gtfs_zip, ExitStack() as streams:
        required = {f"{name}.txt" for name in GTFS_TABLES[:5]}
        if not required.issubset(gtfs_zip.namelist()):
            raise ValueError("GTFS feed is missing required tables")
        if not {"calendar.txt", "calendar_dates.txt"}.intersection(gtfs_zip.namelist()):
            raise ValueError("GTFS feed needs calendar.txt or calendar_dates.txt")
        table_payloads = {
            table_name: streams.enter_context(gtfs_zip.open(f"{table_name}.txt"))
            for table_name in GTFS_TABLES
            if f"{table_name}.txt" in gtfs_zip.namelist()
        }
        with connect() as conn, conn.transaction(), conn.cursor() as cur:
            row_counts = _load_static_gtfs_atomically(conn, table_payloads)
            create_static_gtfs_indexes(cur)
            cur.execute("""
                INSERT INTO raw.static_load_history (checksum_sha256, archive_path, row_counts)
                VALUES (%s, %s, %s::jsonb)
            """, (checksum, archived.destination, json.dumps(row_counts)))
    for table_name, row_count in row_counts.items():
        print(f"Loaded raw.{table_name}: {row_count:,} rows")


def load_static_gtfs_legacy(zip_path: Path | None = None) -> None:
    load_static_gtfs(zip_path)


if __name__ == "__main__":
    load_static_gtfs()
    print("Loaded selected MBTA GTFS static files into raw schema")
