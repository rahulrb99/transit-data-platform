from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from dashboard.data import build_route_filter, vehicle_cutoff
from dashboard.map_view import (
    VEHICLE_LAYER_ID,
    build_vehicle_deck,
    map_error_message,
    prepare_vehicle_map_data,
    route_marker_color,
    selected_vehicle,
)
from dashboard.presentation import (
    UNKNOWN_VALUE,
    prepare_vehicle_table,
    route_option_labels,
)


def test_route_filter_uses_parameterized_route_id() -> None:
    clause, params = build_route_filter("Red", "latest")

    assert clause == "latest.route_id = %(route_id)s"
    assert params == {"route_id": "Red"}


def test_all_routes_filter_does_not_add_parameters() -> None:
    clause, params = build_route_filter(None, "latest")

    assert clause == "TRUE"
    assert params == {}


def test_vehicle_table_formats_names_status_and_missing_values() -> None:
    vehicles = pd.DataFrame(
        [
            {
                "event_timestamp": datetime(2026, 8, 30, 16, 0, tzinfo=UTC),
                "vehicle_id": "vehicle-1",
                "vehicle_label": "Unavailable",
                "entity_id": "entity-1",
                "trip_id": None,
                "route_id": "Red",
                "route_short_name": None,
                "route_long_name": "Red Line",
                "stop_id": "place-dwnxg",
                "stop_name": None,
                "current_status": "IN_TRANSIT_TO",
                "latitude": None,
                "longitude": None,
            }
        ]
    )

    table = prepare_vehicle_table(vehicles)

    assert table.iloc[0]["Route"] == "Red Line"
    assert table.iloc[0]["Destination"] == UNKNOWN_VALUE
    assert table.iloc[0]["Vehicle"] == "vehicle-1"
    assert table.iloc[0]["Trip"] == UNKNOWN_VALUE
    assert table.iloc[0]["Current stop"] == "place-dwnxg"
    assert table.iloc[0]["Status"] == "In transit"
    assert table.iloc[0]["Latitude"] == UNKNOWN_VALUE


def test_empty_vehicle_table_has_stable_columns() -> None:
    table = prepare_vehicle_table(pd.DataFrame())

    assert table.empty
    assert list(table.columns) == [
        "Route",
        "Destination",
        "Vehicle",
        "Trip",
        "Current stop",
        "Status",
        "Last update",
        "Latitude",
        "Longitude",
    ]


def test_map_data_returns_valid_vehicle_and_excludes_bad_coordinates() -> None:
    vehicles = pd.DataFrame(
        [
            {
                "latitude": 42.35,
                "longitude": -71.06,
                "route_id": "Red",
                "route_long_name": "Red Line",
                "trip_headsign": "Alewife",
                "vehicle_label": "Vehicle 1",
                "current_status": "IN_TRANSIT_TO",
                "event_timestamp": datetime(2026, 8, 30, 16, 0, tzinfo=UTC),
                "ingested_at": datetime(2026, 8, 30, 16, 0, 2, tzinfo=UTC),
            },
            {"latitude": None, "longitude": -71.07},
            {"latitude": 95.0, "longitude": -71.08},
            {"latitude": 42.36, "longitude": 181.0},
        ]
    )

    positions = prepare_vehicle_map_data(vehicles)

    assert len(positions) == 1
    assert positions.iloc[0]["route"] == "Red Line"
    assert positions.iloc[0]["destination"] == "Alewife"
    assert positions.iloc[0]["vehicle"] == "Vehicle 1"
    assert positions.iloc[0]["vehicle_id"] == UNKNOWN_VALUE
    assert positions.iloc[0]["vehicle_label"] == "Vehicle 1"
    assert positions.iloc[0]["status"] == "In transit"


def test_vehicle_cutoff_rejects_stale_records_boundary() -> None:
    as_of = datetime(2026, 8, 30, 16, 0, tzinfo=UTC)

    assert vehicle_cutoff(90, as_of) == as_of - timedelta(seconds=90)
    with pytest.raises(ValueError, match="must be positive"):
        vehicle_cutoff(0, as_of)


def test_vehicle_marker_color_is_stable_by_route() -> None:
    assert route_marker_color("Red") == route_marker_color("Red")
    assert route_marker_color("Red")[:3] == [218, 41, 28]
    assert route_marker_color("Green-D")[:3] == [0, 132, 61]
    assert len(route_marker_color("Red")) == 4


def test_vehicle_deck_has_selectable_vehicle_layer_and_tooltip() -> None:
    map_data = pd.DataFrame(
        [
            {
                "latitude": 42.35,
                "longitude": -71.06,
                "route": "Red Line",
                "destination": "Alewife",
                "vehicle": "Vehicle 1",
                "vehicle_id": "vehicle-1",
                "vehicle_label": "Vehicle 1",
                "status": "In transit",
                "last_update": "Aug 30, 12:00:00 PM EDT",
                "ingested_at": "Aug 30, 12:00:02 PM EDT",
                "marker_color": [230, 57, 70, 220],
            }
        ]
    )

    deck = build_vehicle_deck(map_data)
    deck_json = deck.to_json()

    assert VEHICLE_LAYER_ID in deck_json
    assert '"pickable": true' in deck_json
    assert "Toward {destination}" in deck._tooltip["html"]


def test_selected_vehicle_reads_pydeck_selection() -> None:
    vehicle = {"route": "Red Line", "vehicle": "Vehicle 1"}
    selection = {
        "selection": {"objects": {VEHICLE_LAYER_ID: [vehicle]}, "indices": {}}
    }

    assert selected_vehicle(selection) == vehicle
    assert selected_vehicle({"selection": {"objects": {}, "indices": {}}}) is None


def test_map_database_error_message_includes_last_success() -> None:
    last_success = datetime(2026, 8, 30, 16, 0, tzinfo=UTC)

    message = map_error_message(last_success)

    assert "temporarily unavailable" in message
    assert "Aug 30, 12:00:00 PM EDT" in message


def test_route_options_fall_back_to_route_id() -> None:
    options = pd.DataFrame(
        [
            {
                "route_id": "Shuttle-Generic",
                "route_short_name": None,
                "route_long_name": None,
            }
        ]
    )

    assert route_option_labels(options) == {"Shuttle-Generic": "Shuttle-Generic"}
