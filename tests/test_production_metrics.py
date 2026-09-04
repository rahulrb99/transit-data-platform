from __future__ import annotations

from datetime import UTC, datetime
from typing import Self, cast

import pytest

from ingestion.realtime import observability

NOW = datetime(2026, 9, 2, 12, tzinfo=UTC)


class FakeResult:
    def __init__(self, row: object = None) -> None:
        self.row = row

    def fetchone(self) -> tuple[object, ...] | None:
        return cast("tuple[object, ...] | None", self.row)

    def fetchall(self) -> list[tuple[object, ...]]:
        return cast("list[tuple[object, ...]]", self.row)


class FakeConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.results = iter(
            [
                None,
                (1000, 900, 10, 2, 3, 97, 4, 180.0, 240.0),
                (100, 2.0, 1.0, 4.0, 5.0, 6.0, 100, 3.0, 2.0, 5.0, 6.0, 7.0),
                (50000, 1_000_000, 750_000, NOW, 3600.0),
                (
                    {
                        "routes": 100,
                        "stops": 200,
                        "trips": 300,
                        "stop_times": 400,
                        "calendar": 5,
                        "calendar_dates": 6,
                    },
                ),
                [
                    ("routes", 100),
                    ("stops", 200),
                    ("trips", 300),
                    ("stop_times", 400),
                ],
                (3, 1, 2, 1),
            ]
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(
        self,
        query: str,
        params: tuple[object, ...] | None = None,
    ) -> FakeResult:
        del params
        self.queries.append(query)
        return FakeResult(next(self.results))


def test_production_snapshot_calculates_rates_reliability_and_scale(monkeypatch) -> None:
    connection = FakeConnection()
    monkeypatch.setattr(observability, "connect", lambda: connection)

    snapshot = observability.load_production_metrics(now=NOW)

    assert snapshot.realtime_events_received_total == 1000
    assert snapshot.realtime_events_persisted_total == 900
    assert snapshot.realtime_batches_processed_total == 10
    assert snapshot.realtime_batches_failed_total == 2
    assert snapshot.consumer_processing_success_rate == pytest.approx(10 / 12)
    assert snapshot.current_ingestion_rate_events_per_minute == 180.0
    assert snapshot.peak_ingestion_rate_events_per_minute == 240.0
    assert snapshot.ingestion_latency_seconds.p95 == 4.0
    assert snapshot.event_age_seconds.p99 == 6.0
    assert snapshot.realtime_table_row_estimate == 50000
    assert snapshot.static_gtfs_counts["calendar_dates"] == 6
    assert snapshot.static_gtfs_count_source == "load_manifest"
    assert snapshot.records_missing_required_identifiers_total == 1
    assert snapshot.to_dict()["generated_at"] == NOW.isoformat()
    latency_query = next(query for query in connection.queries if "valid_events" in query)
    throughput_query = next(query for query in connection.queries if "minute_rates" in query)
    quality_query = next(
        query for query in connection.queries if "error_reason LIKE" in query
    )
    assert "metric_timestamp >= %s" in throughput_query
    assert "processed_at >= %s" in quality_query
    assert f"LIMIT {observability.LATENCY_SCAN_LIMIT}" in latency_query
    assert "feed_timestamp <= ingested_at" in latency_query
    assert "COALESCE(vehicle_timestamp, feed_timestamp)" in latency_query
    assert "make_interval" in latency_query


def test_distribution_handles_no_valid_timestamps() -> None:
    distribution = observability.distribution_from_row((0, None, None, None, None, None))

    assert distribution.count == 0
    assert distribution.average is None
    assert distribution.p99 is None


def test_success_rate_is_unavailable_without_attempts() -> None:
    assert observability.safe_success_rate(0, 0) is None


@pytest.mark.parametrize("rate_window,latency_window", [(0, 15), (5, 0), (-1, 15)])
def test_metrics_windows_must_be_positive(rate_window, latency_window) -> None:
    with pytest.raises(ValueError, match="at least one minute"):
        observability.load_production_metrics(
            rate_window_minutes=rate_window,
            latency_window_minutes=latency_window,
        )
