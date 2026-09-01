import pytest
from psycopg.errors import UniqueViolation

from ingestion.db import connect

pytestmark = pytest.mark.integration


def require_isolated_ci_fixture() -> None:
    with connect() as connection:
        fixture_rows = connection.execute(
            "SELECT count(*) FROM realtime_vehicle_positions WHERE source = 'ci-fixture'"
        ).fetchone()[0]
    if fixture_rows != 2:
        pytest.skip("requires the isolated database initialized with ci-fixtures.sql")


def test_ci_schema_and_static_fixture_are_queryable() -> None:
    require_isolated_ci_fixture()
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
    require_isolated_ci_fixture()
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
