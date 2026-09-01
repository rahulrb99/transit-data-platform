from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import psycopg
import pytest

from ingestion.realtime import runtime_health

NOW = datetime(2026, 8, 30, 12, tzinfo=UTC)


@pytest.mark.parametrize("age,expected", [(0, True), (300, True), (301, False), (-1, False)])
def test_progress_threshold(monkeypatch, age, expected):
    monkeypatch.setattr(runtime_health, "latest_progress", lambda _: NOW - timedelta(seconds=age))
    monkeypatch.setattr(
        runtime_health, "get_settings", lambda: SimpleNamespace(runtime_health_max_age_seconds=300)
    )
    assert runtime_health.check_progress("producer", now=NOW) is expected


def test_missing_progress_is_not_healthy(monkeypatch):
    monkeypatch.setattr(runtime_health, "latest_progress", lambda _: None)
    assert not runtime_health.check_progress("consumer", now=NOW)


def test_database_failure_is_unhealthy_without_leaking_credentials(monkeypatch, caplog):
    def fail(_):
        raise psycopg.OperationalError("password=do-not-print")

    monkeypatch.setattr(runtime_health, "latest_progress", fail)
    assert not runtime_health.check_progress("producer", now=NOW)
    assert "PostgreSQL probe failed" in caplog.text
    assert "do-not-print" not in caplog.text


@pytest.mark.parametrize("component", ["producer", "consumer"])
def test_probe_reads_existing_progress_tables_and_closes_connection(monkeypatch, component):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.execute.return_value.fetchone.return_value = (NOW,)
    monkeypatch.setattr(runtime_health, "connect", lambda: connection)
    assert runtime_health.latest_progress(component) == NOW
    query = connection.execute.call_args.args[0]
    assert "LIMIT 1" in query
    assert ("raw.ingestion_metadata" if component == "producer" else "realtime_pipeline_metrics") in query
    if component == "producer":
        assert connection.execute.call_args.args[1] == (
            runtime_health.get_settings().mbta_vehicle_positions_url,
        )
    connection.__exit__.assert_called_once()


@pytest.mark.parametrize("healthy,exit_code", [(True, 0), (False, 1)])
def test_health_command_exit_status(monkeypatch, healthy, exit_code):
    monkeypatch.setattr("sys.argv", ["runtime_health", "consumer"])
    monkeypatch.setattr(runtime_health, "check_progress", lambda _: healthy)
    with pytest.raises(SystemExit) as result:
        runtime_health.main()
    assert result.value.code == exit_code
