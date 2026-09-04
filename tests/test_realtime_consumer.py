from __future__ import annotations

import json
import signal
import threading
from collections.abc import Callable
from types import SimpleNamespace
from typing import Self

import psycopg
import pytest

from ingestion.realtime import consumer, vehicle_positions
from ingestion.realtime.metrics import MetricPersistenceResult, PipelineMetricContext


def valid_event(event_id: str = "event-1") -> dict[str, object]:
    return {
        "event_id": event_id,
        "ingested_at": "2026-08-30T12:00:00+00:00",
        "feed_timestamp": "2026-08-30T11:59:58+00:00",
        "vehicle_timestamp": "2026-08-30T11:59:59+00:00",
        "entity_id": f"entity-{event_id}",
        "vehicle_id": f"vehicle-{event_id}",
        "vehicle_label": "1",
        "trip_id": "trip-1",
        "route_id": "7",
        "direction_id": 1,
        "start_time": "08:00:00",
        "start_date": "20260830",
        "stop_id": "stop-1",
        "current_stop_sequence": 4,
        "current_status": "IN_TRANSIT_TO",
        "latitude": 42.35,
        "longitude": -71.06,
        "bearing": 90.0,
        "speed": 8.0,
        "occupancy_status": "MANY_SEATS_AVAILABLE",
        "source": "https://cdn.mbta.com/realtime/VehiclePositions.pb",
    }


def metric_result(inserted_count: int, duration: float = 0.01) -> MetricPersistenceResult:
    return MetricPersistenceResult(inserted_count, duration)


class FakeMessage:
    def __init__(
        self,
        event: object,
        partition: int = 0,
        offset: int = 0,
    ) -> None:
        self._value = json.dumps(event).encode("utf-8")
        self._partition = partition
        self._offset = offset

    def value(self) -> bytes:
        return self._value

    def error(self) -> None:
        return None

    def topic(self) -> str:
        return "vehicle_positions"

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class FakeConsumer:
    def __init__(
        self,
        messages: list[FakeMessage] | None = None,
        on_empty_poll: Callable[[], None] | None = None,
        config: dict[str, object] | None = None,
    ) -> None:
        self.messages = list(messages or [])
        self.on_empty_poll = on_empty_poll
        self.config = config or {}
        self.commits: list[tuple[list[object], bool]] = []
        self.subscriptions: list[str] = []
        self.on_assign: object = None
        self.closed = False

    def subscribe(self, topics: list[str], on_assign: object = None) -> None:
        self.subscriptions = topics
        self.on_assign = on_assign

    def poll(self, timeout: float) -> FakeMessage | None:
        del timeout
        if self.messages:
            return self.messages.pop(0)
        if self.on_empty_poll is not None:
            self.on_empty_poll()
        return None

    def commit(self, offsets: list[object], asynchronous: bool) -> None:
        self.commits.append((offsets, asynchronous))

    def close(self) -> None:
        self.closed = True


class ControlledStopEvent:
    def __init__(self) -> None:
        self.delays: list[float] = []
        self.stopped = False

    def is_set(self) -> bool:
        return self.stopped

    def wait(self, delay: float) -> bool:
        self.delays.append(delay)
        self.stopped = True
        return True


def run_with_fake_consumer(
    monkeypatch: pytest.MonkeyPatch,
    kafka_consumer: FakeConsumer,
    *,
    max_messages: int | None,
    batch_size: int,
    stop_event: threading.Event | ControlledStopEvent | None = None,
) -> int:
    monkeypatch.setattr(consumer, "build_consumer", lambda group_id: kafka_consumer)
    return consumer.consume_to_postgres(
        max_messages=max_messages,
        batch_size=batch_size,
        stop_event=stop_event,  # type: ignore[arg-type]
    )


def test_full_batch_uses_one_persistence_call_before_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [FakeMessage(valid_event(f"event-{index}"), offset=index) for index in range(3)]
    kafka_consumer = FakeConsumer(messages)
    calls: list[tuple[str, int]] = []
    metric_contexts: list[PipelineMetricContext] = []

    def insert(
        records: list[object],
        metric_context: PipelineMetricContext,
    ) -> MetricPersistenceResult:
        metric_contexts.append(metric_context)
        calls.append(("persist", len(records)))
        return metric_result(len(records))

    def commit(offsets: list[object], asynchronous: bool) -> None:
        calls.append(("commit", len(offsets)))
        assert asynchronous is False

    monkeypatch.setattr(consumer, "insert_vehicle_positions_with_metric", insert)
    kafka_consumer.commit = commit  # type: ignore[method-assign]

    inserted = run_with_fake_consumer(
        monkeypatch,
        kafka_consumer,
        max_messages=3,
        batch_size=3,
    )

    assert inserted == 3
    assert calls == [("persist", 3), ("commit", 1)]
    assert metric_contexts[0].metric_kind == "valid_batch"
    assert metric_contexts[0].batch_size == 3
    assert metric_contexts[0].latest_source_event_timestamp is not None
    assert metric_contexts[0].latest_ingestion_timestamp is not None
    assert kafka_consumer.closed is True


def test_partial_batch_flushes_on_empty_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    messages = [FakeMessage(valid_event(f"event-{index}"), offset=index) for index in range(2)]
    kafka_consumer = FakeConsumer(messages)
    persisted_sizes: list[int] = []
    monkeypatch.setattr(
        consumer,
        "insert_vehicle_positions_with_metric",
        lambda records, metric_context: (
            persisted_sizes.append(len(records)) or metric_result(len(records))
        ),
    )

    inserted = run_with_fake_consumer(
        monkeypatch,
        kafka_consumer,
        max_messages=3,
        batch_size=3,
    )

    assert inserted == 2
    assert persisted_sizes == [2]
    assert len(kafka_consumer.commits) == 1


def test_database_failure_results_in_no_offset_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kafka_consumer = FakeConsumer([FakeMessage(valid_event())])
    stop_event = ControlledStopEvent()
    monkeypatch.setattr(
        consumer,
        "insert_vehicle_positions_with_metric",
        lambda records, metric_context: (_ for _ in ()).throw(
            psycopg.OperationalError("database unavailable")
        ),
    )

    inserted = run_with_fake_consumer(
        monkeypatch,
        kafka_consumer,
        max_messages=1,
        batch_size=1,
        stop_event=stop_event,
    )

    assert inserted == 0
    assert stop_event.delays == [1.0]
    assert kafka_consumer.commits == []


def test_duplicate_batch_is_idempotently_committed(monkeypatch: pytest.MonkeyPatch) -> None:
    kafka_consumer = FakeConsumer(
        [FakeMessage(valid_event("duplicate"), offset=0), FakeMessage(valid_event("duplicate"), offset=1)]
    )
    monkeypatch.setattr(
        consumer,
        "insert_vehicle_positions_with_metric",
        lambda records, metric_context: metric_result(1),
    )

    inserted = run_with_fake_consumer(
        monkeypatch,
        kafka_consumer,
        max_messages=2,
        batch_size=2,
    )

    assert inserted == 1
    assert len(kafka_consumer.commits) == 1


def test_multiple_partitions_commit_next_offset_per_partition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [
        FakeMessage(valid_event("a"), partition=0, offset=3),
        FakeMessage(valid_event("b"), partition=1, offset=7),
        FakeMessage(valid_event("c"), partition=0, offset=4),
    ]
    kafka_consumer = FakeConsumer(messages)
    monkeypatch.setattr(
        consumer,
        "insert_vehicle_positions_with_metric",
        lambda records, metric_context: metric_result(3),
    )

    run_with_fake_consumer(monkeypatch, kafka_consumer, max_messages=3, batch_size=3)

    offsets, asynchronous = kafka_consumer.commits[0]
    actual = {(item.topic, item.partition): item.offset for item in offsets}
    assert asynchronous is False
    assert actual == {("vehicle_positions", 0): 5, ("vehicle_positions", 1): 8}


def test_partial_batch_flushes_during_graceful_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop_event = threading.Event()
    kafka_consumer = FakeConsumer(
        [FakeMessage(valid_event("partial"))],
        on_empty_poll=stop_event.set,
    )
    persisted_sizes: list[int] = []
    monkeypatch.setattr(
        consumer,
        "insert_vehicle_positions_with_metric",
        lambda records, metric_context: (
            persisted_sizes.append(len(records)) or metric_result(len(records))
        ),
    )

    inserted = run_with_fake_consumer(
        monkeypatch,
        kafka_consumer,
        max_messages=None,
        batch_size=100,
        stop_event=stop_event,
    )

    assert inserted == 1
    assert persisted_sizes == [1]
    assert len(kafka_consumer.commits) == 1


def test_mixed_valid_and_malformed_events_continue_safely(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [
        FakeMessage(valid_event("valid"), offset=0),
        FakeMessage({"event_id": "malformed"}, offset=1),
        FakeMessage(valid_event("unread"), offset=2),
    ]
    kafka_consumer = FakeConsumer(messages)
    persisted_sizes: list[int] = []
    dead_letters: list[object] = []
    monkeypatch.setattr(
        consumer,
        "insert_vehicle_positions_with_metric",
        lambda records, metric_context: (
            persisted_sizes.append(len(records)) or metric_result(len(records))
        ),
    )
    monkeypatch.setattr(
        consumer,
        "insert_dead_letter_with_metric",
        lambda record, metric_context: dead_letters.append(record) or metric_result(1),
    )

    inserted = run_with_fake_consumer(
        monkeypatch,
        kafka_consumer,
        max_messages=3,
        batch_size=100,
    )

    assert inserted == 2
    assert persisted_sizes == [1, 1]
    assert len(dead_letters) == 1
    assert len(kafka_consumer.commits) == 3
    assert [offsets[0].offset for offsets, _ in kafka_consumer.commits] == [1, 2, 3]
    assert kafka_consumer.messages == []


def test_dead_letter_is_persisted_before_offset_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kafka_consumer = FakeConsumer([FakeMessage({"event_id": "bad"}, offset=4)])
    calls: list[str] = []
    metric_contexts: list[PipelineMetricContext] = []

    def insert_dead_letter(
        record: object,
        metric_context: PipelineMetricContext,
    ) -> MetricPersistenceResult:
        metric_contexts.append(metric_context)
        calls.append("dead-letter")
        return metric_result(1)

    def commit(offsets: list[object], asynchronous: bool) -> None:
        calls.append("commit")
        assert offsets[0].offset == 5
        assert asynchronous is False

    monkeypatch.setattr(consumer, "insert_dead_letter_with_metric", insert_dead_letter)
    kafka_consumer.commit = commit  # type: ignore[method-assign]

    inserted = run_with_fake_consumer(
        monkeypatch,
        kafka_consumer,
        max_messages=1,
        batch_size=100,
    )

    assert inserted == 0
    assert calls == ["dead-letter", "commit"]
    assert metric_contexts[0].metric_kind == "dead_letter"
    assert metric_contexts[0].batch_size == 1


def test_metrics_persistence_failure_prevents_offset_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kafka_consumer = FakeConsumer([FakeMessage(valid_event())])
    stop_event = ControlledStopEvent()
    monkeypatch.setattr(
        consumer,
        "insert_vehicle_positions_with_metric",
        lambda records, metric_context: (_ for _ in ()).throw(
            psycopg.OperationalError("metrics insert failed")
        ),
    )

    run_with_fake_consumer(
        monkeypatch,
        kafka_consumer,
        max_messages=1,
        batch_size=1,
        stop_event=stop_event,
    )

    assert kafka_consumer.commits == []


def test_successful_retry_records_failed_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contexts: list[PipelineMetricContext] = []

    def persist(
        records: list[object],
        metric_context: PipelineMetricContext,
    ) -> MetricPersistenceResult:
        del records
        contexts.append(metric_context)
        if len(contexts) == 1:
            raise psycopg.OperationalError("temporary outage")
        return metric_result(1)

    monkeypatch.setattr(consumer, "insert_vehicle_positions_with_metric", persist)
    stop_event = threading.Event()
    monkeypatch.setattr(stop_event, "wait", lambda delay: False)
    context = PipelineMetricContext(
        metric_kind="valid_batch",
        topic="vehicle_positions",
        batch_size=1,
        latest_source_event_timestamp=None,
        latest_ingestion_timestamp=None,
        started_at_monotonic=1.0,
    )

    result = consumer.persist_batch_with_retry([object()], context, stop_event)

    assert result is not None
    assert contexts[0].retry_count == 0
    assert contexts[1].retry_count == 1
    assert contexts[1].failed_batch_count == 1


def test_dead_letter_database_failure_prevents_offset_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kafka_consumer = FakeConsumer([FakeMessage({"event_id": "bad"}, offset=4)])
    stop_event = ControlledStopEvent()
    monkeypatch.setattr(
        consumer,
        "insert_dead_letter_with_metric",
        lambda record, metric_context: (_ for _ in ()).throw(
            psycopg.OperationalError("database unavailable")
        ),
    )

    inserted = run_with_fake_consumer(
        monkeypatch,
        kafka_consumer,
        max_messages=1,
        batch_size=100,
        stop_event=stop_event,
    )

    assert inserted == 0
    assert stop_event.delays == [1.0]
    assert kafka_consumer.commits == []


def test_replayed_malformed_event_is_idempotently_committed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kafka_consumer = FakeConsumer([FakeMessage({"event_id": "bad"}, offset=4)])
    monkeypatch.setattr(
        consumer,
        "insert_dead_letter_with_metric",
        lambda record, metric_context: metric_result(0),
    )

    run_with_fake_consumer(
        monkeypatch,
        kafka_consumer,
        max_messages=1,
        batch_size=100,
    )

    assert len(kafka_consumer.commits) == 1
    offsets, _ = kafka_consumer.commits[0]
    assert offsets[0].offset == 5


def test_malformed_event_on_another_partition_preserves_partition_offsets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [
        FakeMessage(valid_event("valid-a"), partition=0, offset=2),
        FakeMessage({"event_id": "bad"}, partition=1, offset=8),
        FakeMessage(valid_event("valid-b"), partition=0, offset=3),
    ]
    kafka_consumer = FakeConsumer(messages)
    monkeypatch.setattr(
        consumer,
        "insert_vehicle_positions_with_metric",
        lambda records, metric_context: metric_result(len(records)),
    )
    monkeypatch.setattr(
        consumer,
        "insert_dead_letter_with_metric",
        lambda record, metric_context: metric_result(1),
    )

    run_with_fake_consumer(monkeypatch, kafka_consumer, max_messages=3, batch_size=100)

    committed = [
        {(item.topic, item.partition): item.offset for item in offsets}
        for offsets, _ in kafka_consumer.commits
    ]
    assert committed == [
        {("vehicle_positions", 0): 3},
        {("vehicle_positions", 1): 9},
        {("vehicle_positions", 0): 4},
    ]


def test_shutdown_during_dead_letter_work_commits_successful_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop_event = threading.Event()
    kafka_consumer = FakeConsumer([FakeMessage({"event_id": "bad"})])

    def insert_dead_letter(record: object, metric_context: object) -> MetricPersistenceResult:
        del metric_context
        stop_event.set()
        return metric_result(1)

    monkeypatch.setattr(consumer, "insert_dead_letter_with_metric", insert_dead_letter)

    run_with_fake_consumer(
        monkeypatch,
        kafka_consumer,
        max_messages=None,
        batch_size=100,
        stop_event=stop_event,
    )

    assert len(kafka_consumer.commits) == 1
    assert kafka_consumer.closed is True


def test_configured_batch_size_is_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    messages = [FakeMessage(valid_event(f"event-{index}"), offset=index) for index in range(5)]
    kafka_consumer = FakeConsumer(messages)
    persisted_sizes: list[int] = []
    monkeypatch.setattr(
        consumer,
        "insert_vehicle_positions_with_metric",
        lambda records, metric_context: (
            persisted_sizes.append(len(records)) or metric_result(len(records))
        ),
    )

    inserted = run_with_fake_consumer(
        monkeypatch,
        kafka_consumer,
        max_messages=5,
        batch_size=2,
    )

    assert inserted == 5
    assert persisted_sizes == [2, 2, 1]


def test_invalid_batch_size_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    kafka_consumer = FakeConsumer()
    monkeypatch.setattr(consumer, "build_consumer", lambda group_id: kafka_consumer)

    with pytest.raises(ValueError, match="at least 1"):
        consumer.consume_to_postgres(batch_size=0)

    assert kafka_consumer.closed is False


def test_bulk_insert_uses_one_connection_and_executemany(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection_count = 0

    class FakeCursor:
        rowcount = 0

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: object) -> None:
            del query

        def executemany(self, query: object, rows: list[tuple[object, ...]]) -> None:
            del query
            self.rowcount = len(rows)

    class FakeConnection:
        def __init__(self) -> None:
            self.cursor_instance = FakeCursor()

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def cursor(self) -> FakeCursor:
            return self.cursor_instance

    fake_connection = FakeConnection()

    def connect() -> FakeConnection:
        nonlocal connection_count
        connection_count += 1
        return fake_connection

    monkeypatch.setattr(vehicle_positions, "connect", connect)
    records = [vehicle_positions.event_to_record(valid_event(f"event-{index}")) for index in range(3)]

    inserted = vehicle_positions.insert_vehicle_positions(records)

    assert inserted == 3
    assert connection_count == 1
    assert fake_connection.cursor_instance.rowcount == 3


def test_sigint_and_sigterm_request_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    handlers: dict[signal.Signals, object] = {}
    stop_event = threading.Event()
    monkeypatch.setattr(signal, "signal", lambda signum, handler: handlers.update({signum: handler}))

    consumer.install_shutdown_handlers(stop_event)
    for signum in (signal.SIGINT, signal.SIGTERM):
        stop_event.clear()
        handler = handlers[signum]
        assert callable(handler)
        handler(signum, None)
        assert stop_event.is_set()


def test_keyboard_interrupt_exits_without_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        consumer,
        "parse_args",
        lambda: SimpleNamespace(
            group_id="test-group",
            max_messages=None,
            batch_size=100,
        ),
    )
    monkeypatch.setattr(consumer, "install_shutdown_handlers", lambda stop_event: None)
    monkeypatch.setattr(
        consumer,
        "consume_to_postgres",
        lambda **kwargs: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    consumer.main()


def test_consumer_group_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[FakeConsumer] = []

    def create(config: dict[str, object]) -> FakeConsumer:
        instance = FakeConsumer(config=config)
        created.append(instance)
        return instance

    monkeypatch.setattr(consumer, "Consumer", create)

    built = consumer.build_consumer("reliable-writer")

    assert built is created[0]
    assert created[0].config["group.id"] == "reliable-writer"
    assert created[0].config["enable.auto.commit"] is False
    assert created[0].config["enable.auto.offset.store"] is False
    assert created[0].subscriptions == ["vehicle_positions"]
