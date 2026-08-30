import pytest
from psycopg.errors import UniqueViolation

from ingestion.db import connect

pytestmark = pytest.mark.integration


def test_ci_schema_and_static_fixture_are_queryable() -> None:
    with connect() as connection:
        route_count = connection.execute("SELECT count(*) FROM raw.routes").fetchone()[0]
        stop_time_count = connection.execute("SELECT count(*) FROM raw.stop_times").fetchone()[0]
        realtime_count = connection.execute(
            "SELECT count(*) FROM realtime_vehicle_positions"
        ).fetchone()[0]

    assert route_count == 1
    assert stop_time_count == 5
    assert realtime_count == 2


def test_realtime_event_identity_is_enforced_by_postgres() -> None:
    with connect() as connection, pytest.raises(UniqueViolation), connection.transaction():
        connection.execute(
            """
            INSERT INTO realtime_vehicle_positions (
                event_id, ingested_at, source
            ) VALUES (
                'ci-event-current', NOW(), 'ci-integration-test'
            )
            """
        )
