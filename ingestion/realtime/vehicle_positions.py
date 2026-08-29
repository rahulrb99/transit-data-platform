from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import requests
from google.transit import gtfs_realtime_pb2
from psycopg import sql

from ingestion.config import get_settings
from ingestion.db import connect
from ingestion.metadata import IngestionMetadata, record_ingestion


@dataclass(frozen=True)
class VehiclePositionRecord:
    event_id: str
    ingested_at: datetime
    feed_timestamp: datetime | None
    entity_id: str | None
    vehicle_id: str | None
    vehicle_label: str | None
    trip_id: str | None
    route_id: str | None
    direction_id: int | None
    start_time: str | None
    start_date: str | None
    stop_id: str | None
    current_stop_sequence: int | None
    current_status: str | None
    latitude: float | None
    longitude: float | None
    bearing: float | None
    speed: float | None
    occupancy_status: str | None
    source: str


def datetime_to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _has_field(message: object, field_name: str) -> bool:
    try:
        return message.HasField(field_name)
    except ValueError:
        return False


def _enum_name(enum_type: object, value: int, default: str | None = None) -> str | None:
    try:
        return enum_type.Name(value)
    except ValueError:
        return default


def _timestamp_to_datetime(timestamp: int | None) -> datetime | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, UTC)


def checksum_bytes(content: bytes) -> str:
    return sha256(content).hexdigest()


def build_event_id(
    source: str,
    entity_id: str | None,
    vehicle_id: str | None,
    feed_timestamp: datetime | None,
) -> str:
    event_key = {
        "source": source,
        "entity_id": entity_id,
        "vehicle_id": vehicle_id,
        "feed_timestamp": datetime_to_iso(feed_timestamp),
    }
    return sha256(json.dumps(event_key, sort_keys=True).encode("utf-8")).hexdigest()


def save_raw_feed(content: bytes, ingested_at: datetime) -> Path:
    settings = get_settings()
    output_dir = settings.raw_data_root / "realtime" / "vehicle_positions" / ingested_at.date().isoformat()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"vehicle_positions_{ingested_at:%Y%m%dT%H%M%SZ}.pb"
    output_path.write_bytes(content)
    return output_path


def download_vehicle_positions() -> tuple[bytes, Path, datetime]:
    settings = get_settings()
    ingested_at = datetime.now(UTC)
    response = requests.get(settings.mbta_vehicle_positions_url, timeout=30)
    response.raise_for_status()
    content = response.content
    raw_path = save_raw_feed(content, ingested_at)
    return content, raw_path, ingested_at


def fetch_feed() -> tuple[bytes, Path, datetime]:
    return download_vehicle_positions()


def parse_vehicle_positions(
    content: bytes,
    ingested_at: datetime,
    source: str,
) -> list[VehiclePositionRecord]:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(content)

    feed_timestamp = _timestamp_to_datetime(feed.header.timestamp) if feed.header.timestamp else None
    records: list[VehiclePositionRecord] = []

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue

        vehicle_position = entity.vehicle
        trip = vehicle_position.trip if _has_field(vehicle_position, "trip") else None
        vehicle = vehicle_position.vehicle if _has_field(vehicle_position, "vehicle") else None
        position = vehicle_position.position if _has_field(vehicle_position, "position") else None
        entity_id = entity.id or None
        vehicle_id = vehicle.id or None if vehicle else None

        records.append(
            VehiclePositionRecord(
                event_id=build_event_id(source, entity_id, vehicle_id, feed_timestamp),
                ingested_at=ingested_at,
                feed_timestamp=feed_timestamp,
                entity_id=entity_id,
                vehicle_id=vehicle_id,
                vehicle_label=vehicle.label or None if vehicle else None,
                trip_id=trip.trip_id or None if trip else None,
                route_id=trip.route_id or None if trip else None,
                direction_id=trip.direction_id if trip and _has_field(trip, "direction_id") else None,
                start_time=trip.start_time or None if trip else None,
                start_date=trip.start_date or None if trip else None,
                stop_id=vehicle_position.stop_id or None,
                current_stop_sequence=(
                    vehicle_position.current_stop_sequence
                    if _has_field(vehicle_position, "current_stop_sequence")
                    else None
                ),
                current_status=(
                    _enum_name(
                        gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus,
                        vehicle_position.current_status,
                    )
                    if _has_field(vehicle_position, "current_status")
                    else None
                ),
                latitude=position.latitude if position and _has_field(position, "latitude") else None,
                longitude=position.longitude if position and _has_field(position, "longitude") else None,
                bearing=position.bearing if position and _has_field(position, "bearing") else None,
                speed=position.speed if position and _has_field(position, "speed") else None,
                occupancy_status=(
                    _enum_name(
                        gtfs_realtime_pb2.VehiclePosition.OccupancyStatus,
                        vehicle_position.occupancy_status,
                    )
                    if _has_field(vehicle_position, "occupancy_status")
                    else None
                ),
                source=source,
            )
        )

    return records


def parse_feed(content: bytes, ingested_at: datetime, source: str) -> list[VehiclePositionRecord]:
    return parse_vehicle_positions(content, ingested_at, source)


def record_to_event(record: VehiclePositionRecord) -> dict[str, object]:
    return {
        "event_id": record.event_id,
        "ingested_at": datetime_to_iso(record.ingested_at),
        "feed_timestamp": datetime_to_iso(record.feed_timestamp),
        "entity_id": record.entity_id,
        "vehicle_id": record.vehicle_id,
        "vehicle_label": record.vehicle_label,
        "trip_id": record.trip_id,
        "route_id": record.route_id,
        "direction_id": record.direction_id,
        "start_time": record.start_time,
        "start_date": record.start_date,
        "stop_id": record.stop_id,
        "current_stop_sequence": record.current_stop_sequence,
        "current_status": record.current_status,
        "latitude": record.latitude,
        "longitude": record.longitude,
        "bearing": record.bearing,
        "speed": record.speed,
        "occupancy_status": record.occupancy_status,
        "source": record.source,
    }


def event_to_record(event: dict[str, object]) -> VehiclePositionRecord:
    def optional_str(key: str) -> str | None:
        value = event.get(key)
        return value if isinstance(value, str) else None

    def optional_int(key: str) -> int | None:
        value = event.get(key)
        return value if isinstance(value, int) else None

    def optional_float(key: str) -> float | None:
        value = event.get(key)
        return float(value) if isinstance(value, int | float) else None

    return VehiclePositionRecord(
        event_id=str(event["event_id"]),
        ingested_at=parse_datetime(optional_str("ingested_at")) or datetime.now(UTC),
        feed_timestamp=parse_datetime(optional_str("feed_timestamp")),
        entity_id=optional_str("entity_id"),
        vehicle_id=optional_str("vehicle_id"),
        vehicle_label=optional_str("vehicle_label"),
        trip_id=optional_str("trip_id"),
        route_id=optional_str("route_id"),
        direction_id=optional_int("direction_id"),
        start_time=optional_str("start_time"),
        start_date=optional_str("start_date"),
        stop_id=optional_str("stop_id"),
        current_stop_sequence=optional_int("current_stop_sequence"),
        current_status=optional_str("current_status"),
        latitude=optional_float("latitude"),
        longitude=optional_float("longitude"),
        bearing=optional_float("bearing"),
        speed=optional_float("speed"),
        occupancy_status=optional_str("occupancy_status"),
        source=str(event["source"]),
    )


def ensure_table() -> None:
    query = """
        CREATE TABLE IF NOT EXISTS realtime_vehicle_positions (
            id BIGSERIAL PRIMARY KEY,
            event_id TEXT UNIQUE,
            ingested_at TIMESTAMPTZ NOT NULL,
            feed_timestamp TIMESTAMPTZ,
            entity_id TEXT,
            vehicle_id TEXT,
            vehicle_label TEXT,
            trip_id TEXT,
            route_id TEXT,
            direction_id INTEGER,
            start_time TEXT,
            start_date TEXT,
            stop_id TEXT,
            current_stop_sequence INTEGER,
            current_status TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            bearing DOUBLE PRECISION,
            speed DOUBLE PRECISION,
            occupancy_status TEXT,
            source TEXT NOT NULL
        )
    """
    with connect() as conn, conn.cursor() as cur:
        cur.execute(query)
        cur.execute("ALTER TABLE realtime_vehicle_positions ADD COLUMN IF NOT EXISTS event_id TEXT")
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_realtime_vehicle_positions_event_id
                ON realtime_vehicle_positions (event_id)
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_rvp_event_id_unique
                ON realtime_vehicle_positions (event_id)
            """
        )


def insert_vehicle_positions(records: list[VehiclePositionRecord]) -> int:
    if not records:
        return 0

    ensure_table()
    columns = [
        "event_id",
        "ingested_at",
        "feed_timestamp",
        "entity_id",
        "vehicle_id",
        "vehicle_label",
        "trip_id",
        "route_id",
        "direction_id",
        "start_time",
        "start_date",
        "stop_id",
        "current_stop_sequence",
        "current_status",
        "latitude",
        "longitude",
        "bearing",
        "speed",
        "occupancy_status",
        "source",
    ]
    insert_query = sql.SQL(
        """
        INSERT INTO realtime_vehicle_positions ({}) VALUES ({})
        ON CONFLICT (event_id) DO NOTHING
        """
    ).format(
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    rows = [tuple(getattr(record, column) for column in columns) for record in records]

    with connect() as conn, conn.cursor() as cur:
        cur.executemany(insert_query, rows)
        inserted_count = cur.rowcount

    return inserted_count


def verify_latest(limit: int = 20) -> list[tuple]:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM realtime_vehicle_positions
            ORDER BY ingested_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def ingest_vehicle_positions() -> int:
    settings = get_settings()
    content, raw_path, ingested_at = download_vehicle_positions()
    records = parse_vehicle_positions(content, ingested_at, settings.mbta_vehicle_positions_url)
    inserted_count = insert_vehicle_positions(records)
    record_ingestion(
        IngestionMetadata(
            source=settings.mbta_vehicle_positions_url,
            file_name=raw_path.name,
            record_count=inserted_count,
            checksum_sha256=checksum_bytes(content),
            raw_path=raw_path,
        )
    )
    return inserted_count


if __name__ == "__main__":
    count = ingest_vehicle_positions()
    print(f"Inserted {count:,} realtime vehicle position records")
    print("Latest realtime_vehicle_positions rows:")
    for row in verify_latest():
        print(row)
