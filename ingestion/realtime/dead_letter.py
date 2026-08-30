from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from psycopg import Cursor

from ingestion.db import connect
from ingestion.realtime.metrics import (
    MetricPersistenceResult,
    PipelineMetricContext,
    build_pipeline_metric,
    insert_pipeline_metric,
)


@dataclass(frozen=True)
class DeadLetterRecord:
    original_event_id: str | None
    topic: str
    partition: int
    kafka_offset: int
    processed_at: datetime
    error_type: str
    error_reason: str
    raw_payload: bytes
    vehicle_id: str | None
    trip_id: str | None


def _ensure_dead_letter_table(cur: Cursor) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS realtime_vehicle_position_dead_letters (
            dead_letter_id BIGSERIAL PRIMARY KEY,
            original_event_id TEXT,
            topic TEXT NOT NULL,
            partition INTEGER NOT NULL,
            kafka_offset BIGINT NOT NULL,
            processed_at TIMESTAMPTZ NOT NULL,
            error_type TEXT NOT NULL,
            error_reason TEXT NOT NULL,
            raw_payload BYTEA NOT NULL,
            vehicle_id TEXT,
            trip_id TEXT,
            CONSTRAINT uq_realtime_vehicle_position_dead_letter_record
                UNIQUE (topic, partition, kafka_offset)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_realtime_vehicle_position_dead_letters_processed_at
            ON realtime_vehicle_position_dead_letters (processed_at DESC)
        """
    )


def _insert_dead_letter_row(cur: Cursor, record: DeadLetterRecord) -> int:
    cur.execute(
        """
        INSERT INTO realtime_vehicle_position_dead_letters (
            original_event_id,
            topic,
            partition,
            kafka_offset,
            processed_at,
            error_type,
            error_reason,
            raw_payload,
            vehicle_id,
            trip_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (topic, partition, kafka_offset) DO NOTHING
        """,
        (
            record.original_event_id,
            record.topic,
            record.partition,
            record.kafka_offset,
            record.processed_at,
            record.error_type,
            record.error_reason,
            record.raw_payload,
            record.vehicle_id,
            record.trip_id,
        ),
    )
    return cur.rowcount


def insert_dead_letter(record: DeadLetterRecord) -> int:
    with connect() as conn, conn.cursor() as cur:
        _ensure_dead_letter_table(cur)
        inserted_count = _insert_dead_letter_row(cur, record)
    return inserted_count


def insert_dead_letter_with_metric(
    record: DeadLetterRecord,
    metric_context: PipelineMetricContext,
) -> MetricPersistenceResult:
    with connect() as conn, conn.cursor() as cur:
        _ensure_dead_letter_table(cur)
        inserted_count = _insert_dead_letter_row(cur, record)
        metric = build_pipeline_metric(
            metric_context,
            persisted_count=0,
            dead_letter_count=inserted_count,
            error_count=1,
        )
        insert_pipeline_metric(cur, metric)
    return MetricPersistenceResult(
        inserted_count=inserted_count,
        processing_duration_seconds=metric.processing_duration_seconds,
    )
