from datetime import timedelta

import pytest

from dashboard.data import (
    load_current_vehicles,
    load_latest_realtime_event,
    load_recent_realtime_events,
    load_route_options,
    load_static_summary,
)

pytestmark = pytest.mark.integration


def test_dashboard_queries_and_route_filter_work_against_postgres() -> None:
    latest = load_latest_realtime_event()
    assert latest is not None
    as_of = latest["event_timestamp"]

    summary = load_static_summary()
    route_options = load_route_options(max_age_seconds=3600, as_of=as_of)
    vehicles = load_current_vehicles(max_age_seconds=3600, as_of=as_of)

    assert summary["routes"] > 0
    assert summary["stops"] > 0
    assert not route_options.empty
    assert not vehicles.empty

    selected_route = str(vehicles.iloc[0]["route_id"])
    filtered_vehicles = load_current_vehicles(
        selected_route,
        max_age_seconds=3600,
        as_of=as_of,
    )
    filtered_events = load_recent_realtime_events(selected_route)

    assert not filtered_vehicles.empty
    assert set(filtered_vehicles["route_id"]) == {selected_route}
    assert set(filtered_events["route_id"]) == {selected_route}


def test_dashboard_route_filter_handles_an_empty_result() -> None:
    latest = load_latest_realtime_event()
    assert latest is not None
    vehicles = load_current_vehicles(
        "__route_that_does_not_exist__",
        max_age_seconds=3600,
        as_of=latest["event_timestamp"],
    )
    events = load_recent_realtime_events("__route_that_does_not_exist__")

    assert vehicles.empty
    assert events.empty


def test_dashboard_current_vehicle_query_excludes_stale_records() -> None:
    latest = load_latest_realtime_event()
    assert latest is not None
    stale_reference = latest["event_timestamp"] + timedelta(hours=1)

    vehicles = load_current_vehicles(max_age_seconds=90, as_of=stale_reference)

    assert vehicles.empty
