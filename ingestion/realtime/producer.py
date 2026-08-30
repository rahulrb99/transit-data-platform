from __future__ import annotations

import argparse
import json
import logging
import signal
import threading
from collections.abc import Iterable
from types import FrameType

import requests
from confluent_kafka import KafkaException, Producer
from google.protobuf.message import DecodeError

from ingestion.config import get_settings
from ingestion.metadata import IngestionMetadata, record_ingestion
from ingestion.realtime.vehicle_positions import (
    checksum_bytes,
    fetch_feed,
    parse_feed,
    record_to_event,
)

LOGGER = logging.getLogger(__name__)
INITIAL_RETRY_DELAY_SECONDS = 1.0
MAX_RETRY_DELAY_SECONDS = 60.0


def _is_transient_http_error(error: Exception) -> bool:
    if isinstance(error, requests.Timeout | requests.ConnectionError):
        return True
    if not isinstance(error, requests.HTTPError):
        return False
    status_code = error.response.status_code if error.response is not None else None
    return status_code is None or status_code in {408, 425, 429} or status_code >= 500


def produce_events(
    events: Iterable[dict[str, object]],
    producer: Producer | None = None,
) -> int:
    settings = get_settings()
    kafka_producer = producer or Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    delivery_errors: list[object] = []

    def delivery_report(error: object, message: object) -> None:
        if error is not None:
            delivery_errors.append(error)

    count = 0

    for event in events:
        event_id = str(event["event_id"])
        kafka_producer.produce(
            topic=settings.vehicle_positions_topic,
            key=event_id.encode("utf-8"),
            value=json.dumps(event, sort_keys=True).encode("utf-8"),
            callback=delivery_report,
        )
        kafka_producer.poll(0)
        count += 1

    undelivered_count = kafka_producer.flush()
    if delivery_errors or undelivered_count:
        reason = delivery_errors[0] if delivery_errors else "flush timed out"
        raise RuntimeError(
            f"Failed to deliver {len(delivery_errors) or undelivered_count} event(s): {reason}"
        )
    return count


def fetch_parse_produce_once(
    session: requests.Session | None = None,
    producer: Producer | None = None,
) -> int:
    settings = get_settings()
    content, raw_path, ingested_at = fetch_feed(
        session=session,
        timeout_seconds=settings.mbta_http_timeout_seconds,
    )
    records = parse_feed(content, ingested_at, settings.mbta_vehicle_positions_url)
    events = [record_to_event(record) for record in records]
    published_count = produce_events(events, producer=producer)

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


def run_continuously(
    interval_seconds: float,
    stop_event: threading.Event | None = None,
) -> None:
    settings = get_settings()
    shutdown = stop_event or threading.Event()
    retry_delay = INITIAL_RETRY_DELAY_SECONDS

    with requests.Session() as session:
        producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
        try:
            while not shutdown.is_set():
                try:
                    published_count = fetch_parse_produce_once(
                        session=session,
                        producer=producer,
                    )
                except (requests.RequestException, DecodeError, KafkaException, RuntimeError) as error:
                    transient = _is_transient_http_error(error)
                    delay = retry_delay if transient else interval_seconds
                    if transient:
                        retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY_SECONDS)
                    else:
                        retry_delay = INITIAL_RETRY_DELAY_SECONDS

                    LOGGER.warning(
                        "Vehicle-position poll failed; retrying in %.1f seconds: %s: %s",
                        delay,
                        type(error).__name__,
                        error,
                    )
                    shutdown.wait(delay)
                    continue

                retry_delay = INITIAL_RETRY_DELAY_SECONDS
                LOGGER.info(
                    "Published %s vehicle-position events; next poll in %.1f seconds",
                    published_count,
                    interval_seconds,
                )
                shutdown.wait(interval_seconds)
        finally:
            producer.flush()


def install_shutdown_handlers(stop_event: threading.Event) -> None:
    def request_shutdown(signum: int, frame: FrameType | None) -> None:
        del frame
        LOGGER.info("Received signal %s; stopping producer", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, request_shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_shutdown)


def parse_args(default_interval_seconds: float | None = None) -> argparse.Namespace:
    if default_interval_seconds is None:
        default_interval_seconds = get_settings().mbta_poll_interval_seconds
    parser = argparse.ArgumentParser(description="Publish MBTA vehicle positions to Redpanda.")
    parser.add_argument("--once", action="store_true", help="Run one fetch/produce cycle and exit.")
    parser.add_argument("--interval-seconds", type=float, default=default_interval_seconds)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args()
    try:
        if args.once:
            with requests.Session() as http_session:
                count = fetch_parse_produce_once(session=http_session)
            print(f"Published {count:,} vehicle position events")
        else:
            shutdown_event = threading.Event()
            install_shutdown_handlers(shutdown_event)
            run_continuously(args.interval_seconds, shutdown_event)
    except KeyboardInterrupt:
        LOGGER.info("Producer interrupted; shutdown complete")


if __name__ == "__main__":
    main()
