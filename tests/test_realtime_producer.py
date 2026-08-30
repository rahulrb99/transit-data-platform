from __future__ import annotations

import signal
import threading
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Self

import pytest
import requests
from google.protobuf.message import DecodeError

from ingestion.realtime import producer


class FakeSession:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class FakeKafkaProducer:
    def __init__(self, config: dict[str, object]) -> None:
        self.config = config
        self.flush_count = 0

    def flush(self) -> int:
        self.flush_count += 1
        return 0


class ControlledStopEvent:
    def __init__(self, waits_before_stop: int) -> None:
        self.waits_before_stop = waits_before_stop
        self.delays: list[float] = []
        self.stopped = False

    def is_set(self) -> bool:
        return self.stopped

    def wait(self, delay: float) -> bool:
        self.delays.append(delay)
        if len(self.delays) >= self.waits_before_stop:
            self.stopped = True
        return self.stopped


def configure_continuous_test(monkeypatch: pytest.MonkeyPatch) -> FakeKafkaProducer:
    kafka_producer = FakeKafkaProducer({})
    monkeypatch.setattr(producer.requests, "Session", FakeSession)
    monkeypatch.setattr(producer, "Producer", lambda config: kafka_producer)
    return kafka_producer


def test_successful_feed_polling(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    ingested_at = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    raw_path = tmp_path / "vehicle_positions.pb"
    raw_path.write_bytes(b"feed")
    session = FakeSession()
    kafka_producer = FakeKafkaProducer({})
    captured: dict[str, object] = {}

    def fake_fetch_feed(**kwargs: object) -> tuple[bytes, Path, datetime]:
        captured.update(kwargs)
        return b"feed", raw_path, ingested_at

    monkeypatch.setattr(producer, "fetch_feed", fake_fetch_feed)
    monkeypatch.setattr(producer, "parse_feed", lambda *args: [object(), object()])
    monkeypatch.setattr(
        producer,
        "record_to_event",
        lambda record: {"event_id": str(id(record))},
    )
    monkeypatch.setattr(producer, "produce_events", lambda events, producer=None: len(events))
    monkeypatch.setattr(producer, "record_ingestion", lambda metadata: None)

    count = producer.fetch_parse_produce_once(session=session, producer=kafka_producer)

    assert count == 2
    assert captured["session"] is session
    assert captured["timeout_seconds"] == 30.0


def test_transient_http_failures_use_exponential_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kafka_producer = configure_continuous_test(monkeypatch)
    stop_event = ControlledStopEvent(waits_before_stop=3)
    outcomes: list[Exception | int] = [requests.Timeout("slow feed"), requests.ConnectionError("reset"), 5]

    def poll_once(**kwargs: object) -> int:
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(producer, "fetch_parse_produce_once", poll_once)

    producer.run_continuously(15.0, stop_event)  # type: ignore[arg-type]

    assert stop_event.delays == [1.0, 2.0, 15.0]
    assert kafka_producer.flush_count == 1


def test_malformed_feed_waits_for_normal_poll_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_continuous_test(monkeypatch)
    stop_event = ControlledStopEvent(waits_before_stop=1)
    monkeypatch.setattr(
        producer,
        "fetch_parse_produce_once",
        lambda **kwargs: (_ for _ in ()).throw(DecodeError("invalid protobuf")),
    )

    producer.run_continuously(15.0, stop_event)  # type: ignore[arg-type]

    assert stop_event.delays == [15.0]


def test_permanent_http_failure_is_not_immediately_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_continuous_test(monkeypatch)
    stop_event = ControlledStopEvent(waits_before_stop=1)
    response = requests.Response()
    response.status_code = 404
    error = requests.HTTPError("not found", response=response)
    monkeypatch.setattr(
        producer,
        "fetch_parse_produce_once",
        lambda **kwargs: (_ for _ in ()).throw(error),
    )

    producer.run_continuously(15.0, stop_event)  # type: ignore[arg-type]

    assert stop_event.delays == [15.0]


def test_shutdown_signal_stops_polling(monkeypatch: pytest.MonkeyPatch) -> None:
    handlers: dict[signal.Signals, object] = {}
    stop_event = threading.Event()
    monkeypatch.setattr(signal, "signal", lambda signum, handler: handlers.update({signum: handler}))

    producer.install_shutdown_handlers(stop_event)
    sigint_handler = handlers[signal.SIGINT]
    assert callable(sigint_handler)
    sigint_handler(signal.SIGINT, None)

    assert stop_event.is_set()


def test_keyboard_interrupt_exits_without_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        producer,
        "parse_args",
        lambda: SimpleNamespace(once=False, interval_seconds=15.0),
    )
    monkeypatch.setattr(producer, "install_shutdown_handlers", lambda stop_event: None)
    monkeypatch.setattr(
        producer,
        "run_continuously",
        lambda *args: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    producer.main()
