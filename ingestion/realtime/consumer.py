from __future__ import annotations

import argparse
import json
import logging
import signal
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime
from types import FrameType

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition

from ingestion.config import get_settings
from ingestion.realtime.dead_letter import DeadLetterRecord, insert_dead_letter_with_metric
from ingestion.realtime.metrics import (
    MetricPersistenceResult,
    PipelineMetricContext,
)
from ingestion.realtime.vehicle_positions import (
    VehiclePositionRecord,
    event_to_record,
    insert_vehicle_positions_with_metric,
)

LOGGER = logging.getLogger(__name__)
INITIAL_DATABASE_RETRY_DELAY_SECONDS = 1.0
MAX_DATABASE_RETRY_DELAY_SECONDS = 30.0
MAX_POLL_INTERVAL_MS = 900_000
MAX_DEAD_LETTER_ERROR_REASON_LENGTH = 2_000

BatchItem = tuple[Message, VehiclePositionRecord]


def build_consumer(group_id: str | None = None) -> Consumer:
    settings = get_settings()
    resolved_group_id = group_id or settings.vehicle_positions_consumer_group
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": resolved_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "max.poll.interval.ms": MAX_POLL_INTERVAL_MS,
        }
    )

    def log_assignment(
        assigned_consumer: Consumer,
        partitions: list[TopicPartition],
    ) -> None:
        del assigned_consumer
        assignment = [
            {"topic": item.topic, "partition": item.partition, "offset": item.offset}
            for item in partitions
        ]
        LOGGER.info("Assigned topic partitions: %s", assignment)

    consumer.subscribe([settings.vehicle_positions_topic], on_assign=log_assignment)
    return consumer


def validate_event(event: dict[str, object]) -> None:
    required_string_fields = ["event_id", "vehicle_id", "ingested_at", "entity_id", "source"]
    invalid_fields = [
        field
        for field in required_string_fields
        if not isinstance(event.get(field), str) or not str(event[field]).strip()
    ]
    if invalid_fields:
        raise ValueError(f"Missing or invalid required event fields: {', '.join(invalid_fields)}")

    try:
        timestamp = datetime.fromisoformat(str(event["ingested_at"]))
    except ValueError as error:
        raise ValueError("ingested_at must be a valid ISO-8601 timestamp") from error
    if timestamp.tzinfo is None:
        raise ValueError("ingested_at must include a timezone")


def consume_event(message: Message) -> dict[str, object]:
    value = message.value()
    if value is None:
        raise ValueError("Kafka message has no value")

    event = json.loads(value.decode("utf-8"))
    if not isinstance(event, dict):
        raise TypeError("Kafka message value must decode to a JSON object")

    validate_event(event)
    return event


def build_dead_letter_record(message: Message, error: Exception) -> DeadLetterRecord:
    value = message.value()
    if isinstance(value, bytes):
        raw_payload = value
    elif value is None:
        raw_payload = b""
    else:
        raw_payload = repr(value).encode("utf-8", errors="replace")

    context: dict[str, object] = {}
    try:
        decoded = json.loads(raw_payload.decode("utf-8"))
        if isinstance(decoded, dict):
            context = decoded
    except (UnicodeDecodeError, json.JSONDecodeError):
        pass

    def optional_context_string(field: str) -> str | None:
        candidate = context.get(field)
        return candidate if isinstance(candidate, str) and candidate else None

    return DeadLetterRecord(
        original_event_id=optional_context_string("event_id"),
        topic=message.topic(),
        partition=message.partition(),
        kafka_offset=message.offset(),
        processed_at=datetime.now(UTC),
        error_type=type(error).__name__,
        error_reason=str(error)[:MAX_DEAD_LETTER_ERROR_REASON_LENGTH],
        raw_payload=raw_payload,
        vehicle_id=optional_context_string("vehicle_id"),
        trip_id=optional_context_string("trip_id"),
    )


def persist_batch_with_retry(
    records: list[VehiclePositionRecord],
    metric_context: PipelineMetricContext,
    stop_event: threading.Event,
) -> MetricPersistenceResult | None:
    retry_delay = INITIAL_DATABASE_RETRY_DELAY_SECONDS
    retry_count = 0
    while True:
        try:
            return insert_vehicle_positions_with_metric(
                records,
                replace(
                    metric_context,
                    retry_count=retry_count,
                    failed_batch_count=retry_count,
                ),
            )
        except psycopg.Error as error:
            retry_count += 1
            LOGGER.warning(
                "PostgreSQL batch persistence failed for %s events; retrying in %.1f seconds: %s",
                len(records),
                retry_delay,
                error,
            )
            if stop_event.is_set() or stop_event.wait(retry_delay):
                return None
            retry_delay = min(retry_delay * 2, MAX_DATABASE_RETRY_DELAY_SECONDS)


def persist_dead_letter_with_retry(
    record: DeadLetterRecord,
    metric_context: PipelineMetricContext,
    stop_event: threading.Event,
) -> MetricPersistenceResult | None:
    retry_delay = INITIAL_DATABASE_RETRY_DELAY_SECONDS
    retry_count = 0
    while True:
        try:
            return insert_dead_letter_with_metric(
                record,
                replace(metric_context, retry_count=retry_count),
            )
        except psycopg.Error as error:
            retry_count += 1
            LOGGER.warning(
                "Dead-letter persistence failed topic=%s partition=%s offset=%s; "
                "retrying in %.1f seconds: %s",
                record.topic,
                record.partition,
                record.kafka_offset,
                retry_delay,
                error,
            )
            if stop_event.is_set() or stop_event.wait(retry_delay):
                return None
            retry_delay = min(retry_delay * 2, MAX_DATABASE_RETRY_DELAY_SECONDS)


def offsets_for_batch(batch: list[BatchItem]) -> list[TopicPartition]:
    next_offsets: dict[tuple[str, int], int] = {}
    for message, record in batch:
        del record
        key = (message.topic(), message.partition())
        next_offsets[key] = max(next_offsets.get(key, 0), message.offset() + 1)
    return [
        TopicPartition(topic, partition, offset)
        for (topic, partition), offset in sorted(next_offsets.items())
    ]


def persist_and_commit_batch(
    consumer: Consumer,
    batch: list[BatchItem],
    stop_event: threading.Event,
) -> tuple[bool, int]:
    started_at = time.monotonic()
    records = [record for message, record in batch]
    latest_source_event_timestamp = max(
        (
            record.vehicle_timestamp or record.feed_timestamp
            for record in records
            if record.vehicle_timestamp is not None or record.feed_timestamp is not None
        ),
        default=None,
    )
    latest_ingestion_timestamp = max(record.ingested_at for record in records)
    metric_context = PipelineMetricContext(
        metric_kind="valid_batch",
        topic=batch[0][0].topic(),
        batch_size=len(batch),
        latest_source_event_timestamp=latest_source_event_timestamp,
        latest_ingestion_timestamp=latest_ingestion_timestamp,
        started_at_monotonic=started_at,
    )
    result = persist_batch_with_retry(records, metric_context, stop_event)
    if result is None:
        return False, 0
    inserted_count = result.inserted_count

    offsets = offsets_for_batch(batch)
    try:
        consumer.commit(offsets=offsets, asynchronous=False)
    except KafkaException as error:
        LOGGER.error(
            "Batch offset commit failed after PostgreSQL success; stopping for safe replay: %s",
            error,
        )
        return False, inserted_count

    duplicate_count = max(len(batch) - inserted_count, 0)
    LOGGER.info(
        "Persisted batch size=%s inserted=%s duplicates=%s partitions=%s duration=%.3fs",
        len(batch),
        inserted_count,
        duplicate_count,
        len(offsets),
        result.processing_duration_seconds,
    )
    return True, inserted_count


def dead_letter_and_commit(
    consumer: Consumer,
    message: Message,
    error: Exception,
    stop_event: threading.Event,
) -> tuple[bool, int]:
    started_at = time.monotonic()
    record = build_dead_letter_record(message, error)
    metric_context = PipelineMetricContext(
        metric_kind="dead_letter",
        topic=record.topic,
        batch_size=1,
        latest_source_event_timestamp=None,
        latest_ingestion_timestamp=None,
        started_at_monotonic=started_at,
    )
    result = persist_dead_letter_with_retry(record, metric_context, stop_event)
    if result is None:
        return False, 0
    inserted_count = result.inserted_count

    offset = TopicPartition(record.topic, record.partition, record.kafka_offset + 1)
    try:
        consumer.commit(offsets=[offset], asynchronous=False)
    except KafkaException as commit_error:
        LOGGER.error(
            "Dead-letter offset commit failed after PostgreSQL success topic=%s "
            "partition=%s offset=%s; stopping for safe replay: %s",
            record.topic,
            record.partition,
            record.kafka_offset,
            commit_error,
        )
        return False, inserted_count

    LOGGER.warning(
        "Dead-lettered event topic=%s partition=%s offset=%s event_id=%s reason=%s",
        record.topic,
        record.partition,
        record.kafka_offset,
        record.original_event_id,
        record.error_reason,
    )
    return True, inserted_count


def consume_to_postgres(
    group_id: str | None = None,
    max_messages: int | None = None,
    stop_event: threading.Event | None = None,
    batch_size: int | None = None,
) -> int:
    settings = get_settings()
    resolved_batch_size = (
        settings.mbta_consumer_batch_size if batch_size is None else batch_size
    )
    if resolved_batch_size < 1:
        raise ValueError("Consumer batch size must be at least 1")

    shutdown = stop_event or threading.Event()
    consumer = build_consumer(group_id)
    batch: list[BatchItem] = []
    inserted_count = 0
    committed_count = 0
    dead_letter_count = 0
    should_continue = True
    resolved_group_id = group_id or settings.vehicle_positions_consumer_group
    LOGGER.info(
        "Starting consumer group=%s topic=%s batch_size=%s",
        resolved_group_id,
        settings.vehicle_positions_topic,
        resolved_batch_size,
    )

    def flush_batch() -> bool:
        nonlocal inserted_count, committed_count
        if not batch:
            return True
        batch_count = len(batch)
        succeeded, inserted = persist_and_commit_batch(consumer, batch, shutdown)
        inserted_count += inserted
        batch.clear()
        if succeeded:
            committed_count += batch_count
        return succeeded

    try:
        while should_continue and not shutdown.is_set():
            if max_messages is not None and committed_count + len(batch) >= max_messages:
                should_continue = flush_batch()
                break

            message = consumer.poll(5.0)
            if message is None:
                should_continue = flush_batch()
                if max_messages is not None:
                    break
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            try:
                event = consume_event(message)
                record = event_to_record(event)
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
                if not flush_batch():
                    should_continue = False
                    break
                handled, dead_letter_inserted = dead_letter_and_commit(
                    consumer,
                    message,
                    error,
                    shutdown,
                )
                dead_letter_count += dead_letter_inserted
                if not handled:
                    should_continue = False
                    break
                committed_count += 1
                continue

            batch.append((message, record))
            if len(batch) >= resolved_batch_size:
                should_continue = flush_batch()
    finally:
        if batch:
            flush_batch()
        consumer.close()
        LOGGER.info(
            "Consumer stopped; committed %s events, inserted %s new rows, "
            "and wrote %s dead letters",
            committed_count,
            inserted_count,
            dead_letter_count,
        )

    return inserted_count


def install_shutdown_handlers(stop_event: threading.Event) -> None:
    def request_shutdown(signum: int, frame: FrameType | None) -> None:
        del frame
        LOGGER.info("Received signal %s; stopping consumer", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, request_shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_shutdown)


def parse_args(
    default_group_id: str | None = None,
    default_batch_size: int | None = None,
) -> argparse.Namespace:
    settings = get_settings()
    if default_group_id is None:
        default_group_id = settings.vehicle_positions_consumer_group
    if default_batch_size is None:
        default_batch_size = settings.mbta_consumer_batch_size
    parser = argparse.ArgumentParser(description="Consume vehicle position events into Postgres.")
    parser.add_argument("--group-id", default=default_group_id)
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=default_batch_size)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args()
    shutdown_event = threading.Event()
    install_shutdown_handlers(shutdown_event)
    try:
        consume_to_postgres(
            group_id=args.group_id,
            max_messages=args.max_messages,
            stop_event=shutdown_event,
            batch_size=args.batch_size,
        )
    except KeyboardInterrupt:
        shutdown_event.set()
        LOGGER.info("Consumer interrupted; shutdown complete")


if __name__ == "__main__":
    main()
