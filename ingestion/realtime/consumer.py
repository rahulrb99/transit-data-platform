from __future__ import annotations

import argparse
import json

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from ingestion.config import get_settings
from ingestion.realtime.vehicle_positions import event_to_record, insert_vehicle_positions


def build_consumer(group_id: str) -> Consumer:
    settings = get_settings()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.vehicle_positions_topic])
    return consumer


def validate_event(event: dict[str, object]) -> None:
    required_fields = ["event_id", "ingested_at", "entity_id", "source"]
    missing_fields = [field for field in required_fields if event.get(field) in (None, "")]
    if missing_fields:
        raise ValueError(f"Missing required event fields: {', '.join(missing_fields)}")


def consume_event(message: Message) -> dict[str, object]:
    value = message.value()
    if value is None:
        raise ValueError("Kafka message has no value")

    event = json.loads(value.decode("utf-8"))
    if not isinstance(event, dict):
        raise TypeError("Kafka message value must decode to a JSON object")

    validate_event(event)
    return event


def consume_to_postgres(group_id: str, max_messages: int | None = None) -> int:
    consumer = build_consumer(group_id)
    inserted_count = 0
    consumed_count = 0

    try:
        while max_messages is None or consumed_count < max_messages:
            message = consumer.poll(5.0)
            if message is None:
                if max_messages is not None:
                    break
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            event = consume_event(message)
            inserted_count += insert_vehicle_positions([event_to_record(event)])
            consumed_count += 1
            consumer.commit(message=message, asynchronous=False)

            if consumed_count % 100 == 0:
                print(f"Consumed {consumed_count:,} events; inserted {inserted_count:,} new rows")
    finally:
        consumer.close()

    print(f"Consumed {consumed_count:,} events; inserted {inserted_count:,} new rows")
    return inserted_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume vehicle position events into Postgres.")
    parser.add_argument("--group-id", default="vehicle-position-postgres-writer")
    parser.add_argument("--max-messages", type=int, default=None)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    consume_to_postgres(group_id=args.group_id, max_messages=args.max_messages)
