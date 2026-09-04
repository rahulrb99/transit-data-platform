from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime

from psycopg import Cursor


@dataclass(frozen=True)
class PipelineMetricContext:
    metric_kind: str
    topic: str
    batch_size: int
    latest_source_event_timestamp: datetime | None
    latest_ingestion_timestamp: datetime | None
    started_at_monotonic: float
    retry_count: int = 0
    failed_batch_count: int = 0


@dataclass(frozen=True)
class PipelineMetric:
    metric_timestamp: datetime
    metric_kind: str
    topic: str
    batch_size: int
    events_received: int
    events_persisted: int
    duplicate_events: int
    dead_letter_events: int
    error_count: int
    retry_count: int
    failed_batch_count: int
    processing_duration_seconds: float
    latest_source_event_timestamp: datetime | None
    latest_ingestion_timestamp: datetime | None


@dataclass(frozen=True)
class MetricPersistenceResult:
    inserted_count: int
    processing_duration_seconds: float


def build_pipeline_metric(
    context: PipelineMetricContext,
    *,
    persisted_count: int,
    dead_letter_count: int = 0,
    error_count: int = 0,
    metric_timestamp: datetime | None = None,
    completed_at_monotonic: float | None = None,
) -> PipelineMetric:
    completed_at = time.monotonic() if completed_at_monotonic is None else completed_at_monotonic
    duration_seconds = max(completed_at - context.started_at_monotonic, 0.0)
    duplicate_count = max(context.batch_size - persisted_count - dead_letter_count, 0)
    return PipelineMetric(
        metric_timestamp=metric_timestamp or datetime.now(UTC),
        metric_kind=context.metric_kind,
        topic=context.topic,
        batch_size=context.batch_size,
        events_received=context.batch_size,
        events_persisted=persisted_count,
        duplicate_events=duplicate_count,
        dead_letter_events=dead_letter_count,
        error_count=error_count,
        retry_count=context.retry_count,
        failed_batch_count=context.failed_batch_count,
        processing_duration_seconds=duration_seconds,
        latest_source_event_timestamp=context.latest_source_event_timestamp,
        latest_ingestion_timestamp=context.latest_ingestion_timestamp,
    )


def _ensure_pipeline_metrics_table(cur: Cursor) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS realtime_pipeline_metrics (
            metric_id BIGSERIAL PRIMARY KEY,
            metric_timestamp TIMESTAMPTZ NOT NULL,
            metric_kind TEXT NOT NULL,
            topic TEXT NOT NULL,
            batch_size INTEGER NOT NULL,
            events_received INTEGER NOT NULL,
            events_persisted INTEGER NOT NULL,
            duplicate_events INTEGER NOT NULL,
            dead_letter_events INTEGER NOT NULL,
            error_count INTEGER NOT NULL,
            retry_count INTEGER NOT NULL DEFAULT 0,
            failed_batch_count INTEGER NOT NULL DEFAULT 0,
            processing_duration_seconds DOUBLE PRECISION NOT NULL,
            latest_source_event_timestamp TIMESTAMPTZ,
            latest_ingestion_timestamp TIMESTAMPTZ
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_realtime_pipeline_metrics_timestamp
            ON realtime_pipeline_metrics (metric_timestamp DESC)
        """
    )


def insert_pipeline_metric(cur: Cursor, metric: PipelineMetric) -> None:
    _ensure_pipeline_metrics_table(cur)
    cur.execute(
        """
        INSERT INTO realtime_pipeline_metrics (
            metric_timestamp,
            metric_kind,
            topic,
            batch_size,
            events_received,
            events_persisted,
            duplicate_events,
            dead_letter_events,
            error_count,
            retry_count,
            failed_batch_count,
            processing_duration_seconds,
            latest_source_event_timestamp,
            latest_ingestion_timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            metric.metric_timestamp,
            metric.metric_kind,
            metric.topic,
            metric.batch_size,
            metric.events_received,
            metric.events_persisted,
            metric.duplicate_events,
            metric.dead_letter_events,
            metric.error_count,
            metric.retry_count,
            metric.failed_batch_count,
            metric.processing_duration_seconds,
            metric.latest_source_event_timestamp,
            metric.latest_ingestion_timestamp,
        ),
    )
