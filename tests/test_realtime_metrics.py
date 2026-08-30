from datetime import UTC, datetime, timedelta

from ingestion.realtime.health import (
    PartitionLag,
    PipelineHealthSummary,
    calculate_freshness,
    calculate_partition_lag,
    format_health_summary,
)
from ingestion.realtime.metrics import PipelineMetricContext, build_pipeline_metric


def test_successful_batch_metric_counts_and_duration() -> None:
    timestamp = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    context = PipelineMetricContext(
        metric_kind="valid_batch",
        topic="vehicle_positions",
        batch_size=5,
        latest_source_event_timestamp=timestamp - timedelta(seconds=2),
        latest_ingestion_timestamp=timestamp - timedelta(seconds=1),
        started_at_monotonic=10.0,
    )

    metric = build_pipeline_metric(
        context,
        persisted_count=3,
        metric_timestamp=timestamp,
        completed_at_monotonic=10.25,
    )

    assert metric.events_received == 5
    assert metric.events_persisted == 3
    assert metric.duplicate_events == 2
    assert metric.dead_letter_events == 0
    assert metric.processing_duration_seconds == 0.25
    assert metric.latest_source_event_timestamp == timestamp - timedelta(seconds=2)
    assert metric.latest_ingestion_timestamp == timestamp - timedelta(seconds=1)


def test_dead_letter_metric_counts_error() -> None:
    context = PipelineMetricContext(
        metric_kind="dead_letter",
        topic="vehicle_positions",
        batch_size=1,
        latest_source_event_timestamp=None,
        latest_ingestion_timestamp=None,
        started_at_monotonic=20.0,
    )

    metric = build_pipeline_metric(
        context,
        persisted_count=0,
        dead_letter_count=1,
        error_count=1,
        completed_at_monotonic=20.1,
    )

    assert metric.dead_letter_events == 1
    assert metric.duplicate_events == 0
    assert metric.error_count == 1


def test_freshness_healthy_stale_and_unknown() -> None:
    now = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)

    assert calculate_freshness(now - timedelta(seconds=8), now, 60) == (8.0, "HEALTHY")
    assert calculate_freshness(now - timedelta(seconds=97), now, 60) == (97.0, "STALE")
    assert calculate_freshness(None, now, 60) == (None, "UNKNOWN")


def test_partition_lag_uses_real_offset_boundaries() -> None:
    assert calculate_partition_lag(95, 100, 0) == 5
    assert calculate_partition_lag(None, 100, 20) == 80
    assert calculate_partition_lag(105, 100, 0) == 0


def test_health_command_formatting() -> None:
    now = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    summary = PipelineHealthSummary(
        latest_source_event_timestamp=now - timedelta(seconds=8),
        latest_ingestion_timestamp=now - timedelta(seconds=5),
        events_received=125,
        events_persisted=120,
        duplicate_events=5,
        dead_letter_events=2,
        error_count=2,
        recent_batch_size=25,
        recent_processing_duration_seconds=0.18,
    )
    lag_rows = [PartitionLag(0, 100, 103, 3), PartitionLag(1, 200, 200, 0)]

    output = format_health_summary(
        summary,
        lag_rows,
        now=now,
        stale_threshold_seconds=60,
    )

    assert "Status:              HEALTHY" in output
    assert "Events persisted:    120" in output
    assert "Dead-letter events:  2" in output
    assert "Recent duration:     0.180 seconds" in output
    assert "partition 0: lag=3 consumer=100 high_water=103" in output
    assert "total:       3" in output
