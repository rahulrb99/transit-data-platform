"""Read-only progress probes for the Compose streaming services."""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime

import psycopg

from ingestion.config import get_settings
from ingestion.db import connect

LOGGER = logging.getLogger(__name__)


def latest_progress(component: str) -> datetime | None:
    settings = get_settings()
    with connect() as connection:
        connection.execute("SET statement_timeout = '5s'")
        if component == "producer":
            row = connection.execute(
                """
                SELECT ingestion_timestamp FROM raw.ingestion_metadata
                WHERE source = %s
                ORDER BY ingestion_id DESC LIMIT 1
                """,
                (settings.mbta_vehicle_positions_url,),
            ).fetchone()
        elif component == "consumer":
            row = connection.execute(
                """
                SELECT metric_timestamp FROM realtime_pipeline_metrics
                ORDER BY metric_timestamp DESC LIMIT 1
                """
            ).fetchone()
        else:
            raise ValueError(f"Unknown component: {component}")
    return row[0] if row else None


def check_progress(component: str, *, now: datetime | None = None) -> bool:
    try:
        timestamp = latest_progress(component)
    except psycopg.Error:
        # Do not include connection strings or credentials in Docker health output.
        LOGGER.warning("%s progress unavailable: PostgreSQL probe failed", component)
        return False
    if timestamp is None:
        LOGGER.warning("%s has no recorded successful work", component)
        return False
    age = ((now or datetime.now(UTC)) - timestamp).total_seconds()
    healthy = 0 <= age <= get_settings().runtime_health_max_age_seconds
    if not healthy:
        LOGGER.warning("%s last successful work is %.1f seconds old", component, age)
    return healthy


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("component", choices=("producer", "consumer"))
    args = parser.parse_args()
    raise SystemExit(0 if check_progress(args.component) else 1)


if __name__ == "__main__":
    main()
