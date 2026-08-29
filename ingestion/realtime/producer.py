from __future__ import annotations

import argparse
import json
import time
from collections.abc import Iterable

from confluent_kafka import Producer

from ingestion.config import get_settings
from ingestion.metadata import IngestionMetadata, record_ingestion
from ingestion.realtime.vehicle_positions import (
    checksum_bytes,
    fetch_feed,
    parse_feed,
    record_to_event,
)


def delivery_report(error: object, message: object) -> None:
    if error is not None:
        raise RuntimeError(f"Failed to deliver event: {error}")


def produce_events(events: Iterable[dict[str, object]]) -> int:
    settings = get_settings()
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    count = 0

    for event in events:
        event_id = str(event["event_id"])
        producer.produce(
            topic=settings.vehicle_positions_topic,
            key=event_id.encode("utf-8"),
            value=json.dumps(event, sort_keys=True).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
        count += 1

    producer.flush()
    return count


def fetch_parse_produce_once() -> int:
    settings = get_settings()
    content, raw_path, ingested_at = fetch_feed()
    records = parse_feed(content, ingested_at, settings.mbta_vehicle_positions_url)
    events = [record_to_event(record) for record in records]
    published_count = produce_events(events)

    record_ingestion(
        IngestionMetadata(
            source=settings.mbta_vehicle_positions_url,
            file_name=raw_path.name,
            record_count=published_count,
            checksum_sha256=checksum_bytes(content),
            raw_path=raw_path,
        )
    )
    return published_count


def run_continuously(interval_seconds: int) -> None:
    while True:
        published_count = fetch_parse_produce_once()
        print(f"Published {published_count:,} vehicle position events")
        time.sleep(interval_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish MBTA vehicle positions to Redpanda.")
    parser.add_argument("--once", action="store_true", help="Run one fetch/produce cycle and exit.")
    parser.add_argument("--interval-seconds", type=int, default=15)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.once:
        count = fetch_parse_produce_once()
        print(f"Published {count:,} vehicle position events")
    else:
        run_continuously(args.interval_seconds)

