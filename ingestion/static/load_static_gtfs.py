from pathlib import Path
from zipfile import ZipFile

import pandas as pd
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


def create_raw_table(table_name: str, columns: list[str]) -> None:
    column_defs = [sql.SQL("{} TEXT").format(sql.Identifier(column)) for column in columns]
    query = sql.SQL("CREATE TABLE IF NOT EXISTS raw.{} ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(column_defs),
    )
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(query)


def load_dataframe(table_name: str, dataframe: pd.DataFrame) -> None:
    if dataframe.empty:
        return

    create_raw_table(table_name, list(dataframe.columns))
    columns = [sql.Identifier(column) for column in dataframe.columns]
    insert = sql.SQL("INSERT INTO raw.{} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(columns),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    rows = dataframe.astype(object).where(pd.notnull(dataframe), None).itertuples(index=False, name=None)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(insert, rows)


def load_static_gtfs(zip_path: Path | None = None) -> None:
    source_zip = zip_path or latest_static_zip()
    with ZipFile(source_zip) as gtfs_zip:
        for table_name in GTFS_TABLES:
            member = f"{table_name}.txt"
            if member not in gtfs_zip.namelist():
                continue
            with gtfs_zip.open(member) as file:
                dataframe = pd.read_csv(file, dtype=str)
            load_dataframe(table_name, dataframe)


if __name__ == "__main__":
    load_static_gtfs()
    print("Loaded selected MBTA GTFS static files into raw schema")

