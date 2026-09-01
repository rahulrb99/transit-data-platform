from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
from psycopg.rows import dict_row

from ingestion.db import connect

RECENT_OBSERVATION_SCAN_LIMIT = 5_000
CURRENT_VEHICLE_LIMIT = 1_000
RECENT_ACTIVITY_LIMIT = 25
DEFAULT_VEHICLE_MAX_AGE_SECONDS = 90


def query_dataframe(
    query: str,
    params: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    with connect() as connection, connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(query, params)
        columns = [column.name for column in cursor.description or []]
        return pd.DataFrame(cursor.fetchall(), columns=columns)


def query_record(query: str) -> dict[str, object] | None:
    with connect() as connection, connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(query)
        return cursor.fetchone()


def build_route_filter(route_id: str | None, alias: str) -> tuple[str, dict[str, Any]]:
    if route_id is None:
        return "TRUE", {}
    return f"{alias}.route_id = %(route_id)s", {"route_id": route_id}


def vehicle_cutoff(
    max_age_seconds: int,
    as_of: datetime | None = None,
) -> datetime:
    if max_age_seconds <= 0:
        raise ValueError("max_age_seconds must be positive")
    reference_time = as_of or datetime.now(UTC)
    if reference_time.tzinfo is None:
        reference_time = reference_time.replace(tzinfo=UTC)
    return reference_time - timedelta(seconds=max_age_seconds)


def load_static_summary() -> dict[str, int]:
    row = query_record(
        """
        SELECT
            (SELECT count(*) FROM raw.routes) AS routes,
            (SELECT count(*) FROM raw.stops) AS stops,
            (SELECT count(*) FROM raw.trips) AS trips,
            (SELECT count(*) FROM raw.stop_times) AS stop_arrivals
        """
    )
    return {key: int(value) for key, value in (row or {}).items()}


def load_realtime_event_count() -> int:
    row = query_record("SELECT count(*) AS events_ingested FROM realtime_vehicle_positions")
    return int(row["events_ingested"]) if row else 0


def load_dead_letter_count() -> int:
    row = query_record(
        "SELECT count(*) AS dead_letter_records "
        "FROM realtime_vehicle_position_dead_letters"
    )
    return int(row["dead_letter_records"]) if row else 0


def load_latest_realtime_event() -> dict[str, object] | None:
    return query_record(
        """
        SELECT
            coalesce(vehicle_timestamp, feed_timestamp, ingested_at) AS event_timestamp,
            ingested_at
        FROM realtime_vehicle_positions
        ORDER BY id DESC
        LIMIT 1
        """
    )


def load_route_options(
    max_age_seconds: int = DEFAULT_VEHICLE_MAX_AGE_SECONDS,
    as_of: datetime | None = None,
) -> pd.DataFrame:
    return query_dataframe(
        f"""
        WITH bounded AS (
            SELECT
                route_id,
                coalesce(vehicle_timestamp, feed_timestamp, ingested_at)
                    AS event_timestamp
            FROM realtime_vehicle_positions
            ORDER BY id DESC
            LIMIT {RECENT_OBSERVATION_SCAN_LIMIT}
        ),
        recent AS (
            SELECT route_id
            FROM bounded
            WHERE event_timestamp >= %(cutoff_timestamp)s
        )
        SELECT DISTINCT
            recent.route_id,
            routes.route_short_name,
            routes.route_long_name
        FROM recent
        LEFT JOIN raw.routes AS routes
            ON routes.route_id = recent.route_id
        WHERE recent.route_id IS NOT NULL
          AND btrim(recent.route_id) <> ''
        ORDER BY routes.route_short_name NULLS LAST,
                 routes.route_long_name NULLS LAST,
                 recent.route_id
        """,
        {"cutoff_timestamp": vehicle_cutoff(max_age_seconds, as_of)},
    )


def load_current_vehicles(
    route_id: str | None = None,
    max_age_seconds: int = DEFAULT_VEHICLE_MAX_AGE_SECONDS,
    as_of: datetime | None = None,
) -> pd.DataFrame:
    route_clause, params = build_route_filter(route_id, "latest")
    params["cutoff_timestamp"] = vehicle_cutoff(max_age_seconds, as_of)
    return query_dataframe(
        f"""
        WITH bounded AS (
            SELECT
                id,
                coalesce(nullif(vehicle_id, ''), nullif(entity_id, ''), id::text)
                    AS vehicle_key,
                coalesce(vehicle_timestamp, feed_timestamp, ingested_at)
                    AS event_timestamp,
                vehicle_id,
                vehicle_label,
                entity_id,
                trip_id,
                route_id,
                ingested_at,
                stop_id,
                current_status,
                latitude,
                longitude
            FROM realtime_vehicle_positions
            ORDER BY id DESC
            LIMIT {RECENT_OBSERVATION_SCAN_LIMIT}
        ),
        recent AS (
            SELECT *
            FROM bounded
            WHERE event_timestamp >= %(cutoff_timestamp)s
        ),
        latest AS (
            SELECT DISTINCT ON (vehicle_key)
                id,
                vehicle_key,
                event_timestamp,
                vehicle_id,
                vehicle_label,
                entity_id,
                trip_id,
                route_id,
                ingested_at,
                stop_id,
                current_status,
                latitude,
                longitude
            FROM recent
            ORDER BY vehicle_key, event_timestamp DESC, id DESC
        )
        SELECT
            latest.event_timestamp,
            latest.vehicle_id,
            latest.vehicle_label,
            latest.entity_id,
            latest.trip_id,
            trips.trip_headsign,
            latest.route_id,
            routes.route_short_name,
            routes.route_long_name,
            latest.stop_id,
            stops.stop_name,
            latest.current_status,
            latest.ingested_at,
            latest.latitude,
            latest.longitude
        FROM latest
        LEFT JOIN raw.routes AS routes
            ON routes.route_id = latest.route_id
        LEFT JOIN raw.trips AS trips
            ON trips.trip_id = latest.trip_id
        LEFT JOIN raw.stops AS stops
            ON stops.stop_id = latest.stop_id
        WHERE {route_clause}
        ORDER BY latest.event_timestamp DESC, latest.id DESC
        LIMIT {CURRENT_VEHICLE_LIMIT}
        """,
        params,
    )


def load_recent_realtime_events(route_id: str | None = None) -> pd.DataFrame:
    route_clause, params = build_route_filter(route_id, "positions")
    return query_dataframe(
        f"""
        SELECT
            coalesce(
                positions.vehicle_timestamp,
                positions.feed_timestamp,
                positions.ingested_at
            ) AS event_timestamp,
            positions.vehicle_id,
            positions.vehicle_label,
            positions.entity_id,
            positions.trip_id,
            positions.route_id,
            routes.route_short_name,
            routes.route_long_name,
            positions.stop_id,
            stops.stop_name,
            positions.current_status,
            positions.latitude,
            positions.longitude
        FROM realtime_vehicle_positions AS positions
        LEFT JOIN raw.routes AS routes
            ON routes.route_id = positions.route_id
        LEFT JOIN raw.stops AS stops
            ON stops.stop_id = positions.stop_id
        WHERE {route_clause}
        ORDER BY positions.id DESC
        LIMIT {RECENT_ACTIVITY_LIMIT}
        """,
        params,
    )


def load_route_summary() -> pd.DataFrame:
    return query_dataframe(
        """
        SELECT
            routes.route_short_name,
            routes.route_long_name,
            count(DISTINCT trips.trip_id) AS trips
        FROM raw.routes AS routes
        LEFT JOIN raw.trips AS trips
            ON routes.route_id = trips.route_id
        GROUP BY 1, 2
        ORDER BY trips DESC
        LIMIT 25
        """
    )
