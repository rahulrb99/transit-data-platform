from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from confluent_kafka import Consumer, TopicPartition

from ingestion.config import get_settings
from ingestion.db import connect


@dataclass(frozen=True)
class PartitionLag:
    partition: int
    consumer_offset: int | None
    high_water_offset: int
    lag: int


@dataclass(frozen=True)
class PipelineHealthSummary:
    latest_source_event_timestamp: datetime | None
    latest_ingestion_timestamp: datetime | None
    events_received: int
    events_persisted: int
    duplicate_events: int
    dead_letter_events: int
    error_count: int
    recent_batch_size: int | None
    recent_processing_duration_seconds: float | None


def calculate_freshness(
    latest_source_event_timestamp: datetime | None,
    now: datetime,
    stale_threshold_seconds: float,
) -> tuple[float | None, str]:
    if latest_source_event_timestamp is None:
        return None, "UNKNOWN"
    age_seconds = max((now - latest_source_event_timestamp).total_seconds(), 0.0)
    status = "HEALTHY" if age_seconds <= stale_threshold_seconds else "STALE"
    return age_seconds, status


def calculate_partition_lag(
    consumer_offset: int | None,
    high_water_offset: int,
    low_water_offset: int = 0,
) -> int:
    effective_offset = (
        max(consumer_offset, low_water_offset)
        if consumer_offset is not None and consumer_offset >= 0
        else low_water_offset
    )
    return max(high_water_offset - effective_offset, 0)


def load_health_summary() -> PipelineHealthSummary:
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.realtime_pipeline_metrics')")
        metrics_table_exists = cur.fetchone()[0] is not None
        if metrics_table_exists:
            cur.execute(
                """
                SELECT
                    MAX(latest_source_event_timestamp),
                    MAX(latest_ingestion_timestamp),
                    COALESCE(SUM(events_received), 0),
                    COALESCE(SUM(events_persisted), 0),
                    COALESCE(SUM(duplicate_events), 0),
                    COALESCE(SUM(error_count), 0)
                FROM realtime_pipeline_metrics
                """
            )
            aggregate = cur.fetchone()
            cur.execute(
                """
                SELECT batch_size, processing_duration_seconds
                FROM realtime_pipeline_metrics
                WHERE metric_kind = 'valid_batch'
                ORDER BY metric_timestamp DESC, metric_id DESC
                LIMIT 1
                """
            )
            recent_batch = cur.fetchone()
        else:
            aggregate = (None, None, 0, 0, 0, 0)
            recent_batch = None

        cur.execute("SELECT to_regclass('public.realtime_vehicle_position_dead_letters')")
        dead_letter_table_exists = cur.fetchone()[0] is not None
        if dead_letter_table_exists:
            cur.execute("SELECT COUNT(*) FROM realtime_vehicle_position_dead_letters")
            dead_letter_events = cur.fetchone()[0]
        else:
            dead_letter_events = 0

    return PipelineHealthSummary(
        latest_source_event_timestamp=aggregate[0],
        latest_ingestion_timestamp=aggregate[1],
        events_received=int(aggregate[2]),
        events_persisted=int(aggregate[3]),
        duplicate_events=int(aggregate[4]),
        dead_letter_events=int(dead_letter_events),
        error_count=int(aggregate[5]),
        recent_batch_size=int(recent_batch[0]) if recent_batch else None,
        recent_processing_duration_seconds=float(recent_batch[1]) if recent_batch else None,
    )


def load_consumer_lag() -> list[PartitionLag]:
    settings = get_settings()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.vehicle_positions_consumer_group,
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(settings.vehicle_positions_topic, timeout=10.0)
        partitions = sorted(metadata.topics[settings.vehicle_positions_topic].partitions)
        requested = [
            TopicPartition(settings.vehicle_positions_topic, partition)
            for partition in partitions
        ]
        committed = consumer.committed(requested, timeout=10.0)
        lag_rows: list[PartitionLag] = []
        for offset in committed:
            low_water, high_water = consumer.get_watermark_offsets(offset, timeout=10.0)
            consumer_offset = offset.offset if offset.offset >= 0 else None
            lag_rows.append(
                PartitionLag(
                    partition=offset.partition,
                    consumer_offset=consumer_offset,
                    high_water_offset=high_water,
                    lag=calculate_partition_lag(consumer_offset, high_water, low_water),
                )
            )
        return lag_rows
    finally:
        consumer.close()


def format_health_summary(
    summary: PipelineHealthSummary,
    lag_rows: list[PartitionLag],
    *,
    now: datetime,
    stale_threshold_seconds: float,
) -> str:
    freshness_age, status = calculate_freshness(
        summary.latest_source_event_timestamp,
        now,
        stale_threshold_seconds,
    )
    latest_event = (
        summary.latest_source_event_timestamp.isoformat()
        if summary.latest_source_event_timestamp
        else "unavailable"
    )
    latest_ingestion = (
        summary.latest_ingestion_timestamp.isoformat()
        if summary.latest_ingestion_timestamp
        else "unavailable"
    )
    freshness = f"{freshness_age:.1f} seconds" if freshness_age is not None else "unavailable"
    recent_batch = str(summary.recent_batch_size) if summary.recent_batch_size is not None else "unavailable"
    recent_duration = (
        f"{summary.recent_processing_duration_seconds:.3f} seconds"
        if summary.recent_processing_duration_seconds is not None
        else "unavailable"
    )
    lines = [
        "Realtime Pipeline Health",
        "------------------------",
        f"Latest event:        {latest_event}",
        f"Latest ingestion:    {latest_ingestion}",
        f"Freshness:           {freshness}",
        f"Status:              {status}",
        "",
        f"Events received:     {summary.events_received:,}",
        f"Events persisted:    {summary.events_persisted:,}",
        f"Duplicate events:    {summary.duplicate_events:,}",
        f"Dead-letter events:  {summary.dead_letter_events:,}",
        f"Processing errors:   {summary.error_count:,}",
        f"Recent batch size:   {recent_batch}",
        f"Recent duration:     {recent_duration}",
        "",
        "Consumer lag:",
    ]
    if lag_rows:
        for row in lag_rows:
            offset = str(row.consumer_offset) if row.consumer_offset is not None else "uncommitted"
            lines.append(
                f"  partition {row.partition}: lag={row.lag:,} "
                f"consumer={offset} high_water={row.high_water_offset}"
            )
        lines.append(f"  total:       {sum(row.lag for row in lag_rows):,}")
    else:
        lines.append("  unavailable")
    return "\n".join(lines)


def main() -> None:
    settings = get_settings()
    summary = load_health_summary()
    lag_rows = load_consumer_lag()
    print(
        format_health_summary(
            summary,
            lag_rows,
            now=datetime.now(UTC),
            stale_threshold_seconds=settings.mbta_realtime_stale_threshold_seconds,
        )
    )


if __name__ == "__main__":
    main()
