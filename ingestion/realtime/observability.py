"""Bounded, read-only production metrics derived from existing pipeline data."""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from ingestion.config import get_settings
from ingestion.db import connect

LATENCY_WINDOW_MINUTES = 5
RATE_WINDOW_MINUTES = 5
MAX_VALID_LATENCY_SECONDS = 86_400
LATENCY_SCAN_LIMIT = 25_000
STATIC_TABLES = (
    "routes",
    "stops",
    "trips",
    "stop_times",
    "calendar",
    "calendar_dates",
)


@dataclass(frozen=True)
class Distribution:
    count: int
    average: float | None
    p50: float | None
    p95: float | None
    p99: float | None
    maximum: float | None


@dataclass(frozen=True)
class ProductionMetricsSnapshot:
    generated_at: datetime
    rate_window_minutes: int
    latency_window_minutes: int
    realtime_events_received_total: int
    realtime_events_persisted_total: int
    realtime_batches_processed_total: int
    realtime_batches_failed_total: int
    realtime_events_dead_lettered_total: int
    duplicate_records_detected_total: int
    retry_count_total: int
    consumer_processing_success_rate: float | None
    current_ingestion_rate_events_per_minute: float
    peak_ingestion_rate_events_per_minute: float
    ingestion_latency_seconds: Distribution
    event_age_seconds: Distribution
    invalid_realtime_records_total: int
    records_missing_required_identifiers_total: int
    records_rejected_by_validation_total: int
    schema_validation_failures_total: int
    realtime_table_row_estimate: int
    database_size_bytes: int
    realtime_table_size_bytes: int
    latest_persisted_event_timestamp: datetime | None
    postgres_uptime_seconds: float
    static_gtfs_counts: dict[str, int | None]
    static_gtfs_count_source: str
    metrics_query_duration_seconds: float

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["generated_at"] = self.generated_at.isoformat()
        if self.latest_persisted_event_timestamp is not None:
            payload["latest_persisted_event_timestamp"] = (
                self.latest_persisted_event_timestamp.isoformat()
            )
        return payload


def safe_success_rate(succeeded: int, failed: int) -> float | None:
    attempts = succeeded + failed
    return succeeded / attempts if attempts else None


def distribution_from_row(values: tuple[object, ...]) -> Distribution:
    def optional_float(value: object) -> float | None:
        return None if value is None else float(value)

    return Distribution(
        count=int(values[0] or 0),
        average=optional_float(values[1]),
        p50=optional_float(values[2]),
        p95=optional_float(values[3]),
        p99=optional_float(values[4]),
        maximum=optional_float(values[5]),
    )


def _distribution_sql(expression: str) -> str:
    return f"""
        COUNT({expression}),
        AVG({expression}),
        percentile_cont(0.50) WITHIN GROUP (ORDER BY {expression}),
        percentile_cont(0.95) WITHIN GROUP (ORDER BY {expression}),
        percentile_cont(0.99) WITHIN GROUP (ORDER BY {expression}),
        MAX({expression})
    """


def load_production_metrics(
    *,
    now: datetime | None = None,
    rate_window_minutes: int = RATE_WINDOW_MINUTES,
    latency_window_minutes: int = LATENCY_WINDOW_MINUTES,
) -> ProductionMetricsSnapshot:
    if rate_window_minutes < 1 or latency_window_minutes < 1:
        raise ValueError("Metrics windows must be at least one minute")

    generated_at = now or datetime.now(UTC)
    started_at = time.monotonic()
    metrics_cutoff = generated_at - timedelta(days=get_settings().metrics_retention_days)
    rate_cutoff = generated_at - timedelta(minutes=rate_window_minutes)
    latency_cutoff = generated_at - timedelta(minutes=latency_window_minutes)

    with connect() as connection:
        connection.execute("SET LOCAL statement_timeout = '10s'")
        throughput = connection.execute(
            """
            WITH retained_metrics AS (
                SELECT *
                FROM realtime_pipeline_metrics
                WHERE metric_timestamp >= %s
                  AND metric_timestamp <= %s
            ),
            minute_rates AS (
                SELECT
                    date_trunc('minute', metric_timestamp) AS minute,
                    SUM(events_persisted) AS events
                FROM retained_metrics
                WHERE metric_kind = 'valid_batch'
                GROUP BY 1
            )
            SELECT
                COALESCE(SUM(events_received), 0),
                COALESCE(SUM(events_persisted), 0),
                COUNT(*) FILTER (WHERE metric_kind = 'valid_batch'),
                COALESCE(SUM(failed_batch_count), 0),
                COALESCE(SUM(dead_letter_events), 0),
                COALESCE(SUM(duplicate_events), 0),
                COALESCE(SUM(retry_count), 0),
                COALESCE(
                    SUM(events_persisted) FILTER (
                        WHERE metric_timestamp >= %s AND metric_timestamp <= %s
                    )
                        / %s::double precision,
                    0
                ),
                COALESCE((SELECT MAX(events) FROM minute_rates), 0)
            FROM retained_metrics
            """,
            (
                metrics_cutoff,
                generated_at,
                rate_cutoff,
                generated_at,
                rate_window_minutes,
            ),
        ).fetchone()

        latency = connection.execute(
            f"""
            WITH bounded AS (
                SELECT ingested_at, feed_timestamp, vehicle_timestamp
                FROM realtime_vehicle_positions
                ORDER BY id DESC
                LIMIT {LATENCY_SCAN_LIMIT}
            ),
            valid_events AS (
                SELECT
                    CASE
                        WHEN feed_timestamp IS NOT NULL
                         AND feed_timestamp <= ingested_at
                         AND feed_timestamp >= ingested_at - make_interval(secs => %s)
                        THEN EXTRACT(EPOCH FROM (ingested_at - feed_timestamp))
                    END AS ingestion_latency,
                    CASE
                        WHEN COALESCE(vehicle_timestamp, feed_timestamp) IS NOT NULL
                         AND COALESCE(vehicle_timestamp, feed_timestamp) <= %s
                         AND COALESCE(vehicle_timestamp, feed_timestamp)
                                >= %s - make_interval(secs => %s)
                        THEN EXTRACT(EPOCH FROM (
                            %s - COALESCE(vehicle_timestamp, feed_timestamp)
                        ))
                    END AS event_age
                FROM bounded
                WHERE ingested_at >= %s
                  AND ingested_at <= %s
            )
            SELECT
                {_distribution_sql('ingestion_latency')},
                {_distribution_sql('event_age')}
            FROM valid_events
            """,
            (
                MAX_VALID_LATENCY_SECONDS,
                generated_at,
                generated_at,
                MAX_VALID_LATENCY_SECONDS,
                generated_at,
                latency_cutoff,
                generated_at,
            ),
        ).fetchone()

        database = connection.execute(
            """
            SELECT
                GREATEST(COALESCE(c.reltuples::bigint, 0), 0),
                pg_database_size(current_database()),
                pg_total_relation_size('realtime_vehicle_positions'),
                (
                    SELECT ingested_at
                    FROM realtime_vehicle_positions
                    ORDER BY id DESC
                    LIMIT 1
                ),
                EXTRACT(EPOCH FROM (%s - pg_postmaster_start_time()))
            FROM pg_class AS c
            WHERE c.oid = 'realtime_vehicle_positions'::regclass
            """,
            (generated_at,),
        ).fetchone()

        static_row = connection.execute(
            """
            SELECT row_counts
            FROM raw.static_load_history
            ORDER BY load_id DESC
            LIMIT 1
            """
        ).fetchone()

        static_estimates = connection.execute(
            """
            SELECT relname, GREATEST(n_live_tup, 0)::bigint
            FROM pg_stat_user_tables
            WHERE schemaname = 'raw' AND relname = ANY(%s)
            """,
            (list(STATIC_TABLES),),
        ).fetchall()

        quality = connection.execute(
            """
            SELECT
                COUNT(*),
                COUNT(*) FILTER (
                    WHERE error_reason LIKE
                        'Missing or invalid required event fields:%%'
                ),
                COUNT(*) FILTER (WHERE error_type = 'ValueError'),
                COUNT(*) FILTER (
                    WHERE error_type IN (
                        'JSONDecodeError', 'UnicodeDecodeError', 'TypeError'
                    )
                )
            FROM realtime_vehicle_position_dead_letters
            WHERE processed_at >= %s
              AND processed_at <= %s
            """,
            (metrics_cutoff, generated_at),
        ).fetchone()

    batches_processed = int(throughput[2])
    batches_failed = int(throughput[3])
    has_static_manifest = bool(static_row and isinstance(static_row[0], dict))
    estimated_counts = dict(static_estimates)
    has_useful_estimates = any(value > 0 for value in estimated_counts.values())
    row_counts = (
        static_row[0]
        if has_static_manifest
        else estimated_counts if has_useful_estimates else {}
    )
    return ProductionMetricsSnapshot(
        generated_at=generated_at,
        rate_window_minutes=rate_window_minutes,
        latency_window_minutes=latency_window_minutes,
        realtime_events_received_total=int(throughput[0]),
        realtime_events_persisted_total=int(throughput[1]),
        realtime_batches_processed_total=batches_processed,
        realtime_batches_failed_total=batches_failed,
        realtime_events_dead_lettered_total=int(throughput[4]),
        duplicate_records_detected_total=int(throughput[5]),
        retry_count_total=int(throughput[6]),
        consumer_processing_success_rate=safe_success_rate(
            batches_processed,
            batches_failed,
        ),
        current_ingestion_rate_events_per_minute=float(throughput[7]),
        peak_ingestion_rate_events_per_minute=float(throughput[8]),
        ingestion_latency_seconds=distribution_from_row(latency[:6]),
        event_age_seconds=distribution_from_row(latency[6:]),
        invalid_realtime_records_total=int(quality[0]),
        records_missing_required_identifiers_total=int(quality[1]),
        records_rejected_by_validation_total=int(quality[2]),
        schema_validation_failures_total=int(quality[3]),
        realtime_table_row_estimate=int(database[0]),
        database_size_bytes=int(database[1]),
        realtime_table_size_bytes=int(database[2]),
        latest_persisted_event_timestamp=database[3],
        postgres_uptime_seconds=max(float(database[4]), 0.0),
        static_gtfs_counts={name: row_counts.get(name) for name in STATIC_TABLES},
        static_gtfs_count_source=(
            "load_manifest"
            if has_static_manifest
            else "postgres_estimate" if has_useful_estimates else "unavailable"
        ),
        metrics_query_duration_seconds=max(time.monotonic() - started_at, 0.0),
    )
