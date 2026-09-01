from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

DISPLAY_TIMEZONE = ZoneInfo("America/New_York")
UNKNOWN_VALUE = "Unknown"
VEHICLE_COLUMNS = [
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

STATUS_LABELS = {
    "IN_TRANSIT_TO": "In transit",
    "INCOMING_AT": "Approaching stop",
    "STOPPED_AT": "Stopped",
}


def friendly_value(*values: object) -> str:
    for value in values:
        if value is None or pd.isna(value):
            continue
        text = str(value).strip()
        if text and text.lower() not in {"none", "nan", "unavailable"}:
            return text
    return UNKNOWN_VALUE


def route_label(row: pd.Series) -> str:
    short_name = friendly_value(row.get("route_short_name"))
    long_name = friendly_value(row.get("route_long_name"))
    if short_name != UNKNOWN_VALUE and long_name != UNKNOWN_VALUE:
        if short_name.casefold() == long_name.casefold():
            return short_name
        return f"{short_name} - {long_name}"
    return friendly_value(
        row.get("route_short_name"),
        row.get("route_long_name"),
        row.get("route_id"),
    )


def route_option_labels(route_options: pd.DataFrame) -> dict[str, str]:
    if route_options.empty:
        return {}
    return {
        str(row["route_id"]): route_label(row)
        for _, row in route_options.iterrows()
    }


def friendly_status(value: object) -> str:
    raw_status = friendly_value(value)
    if raw_status == UNKNOWN_VALUE:
        return raw_status
    return STATUS_LABELS.get(raw_status, raw_status.replace("_", " ").title())


def format_timestamp(value: object, *, include_date: bool = True) -> str:
    if not isinstance(value, (datetime, pd.Timestamp)) or pd.isna(value):
        return UNKNOWN_VALUE
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    local_value = timestamp.tz_convert(DISPLAY_TIMEZONE)
    pattern = "%b %d, %I:%M:%S %p %Z" if include_date else "%I:%M:%S %p %Z"
    return local_value.strftime(pattern)


def prepare_vehicle_table(vehicles: pd.DataFrame) -> pd.DataFrame:
    if vehicles.empty:
        return pd.DataFrame(columns=VEHICLE_COLUMNS)

    records: list[dict[str, object]] = []
    for _, row in vehicles.iterrows():
        latitude = pd.to_numeric(row.get("latitude"), errors="coerce")
        longitude = pd.to_numeric(row.get("longitude"), errors="coerce")
        records.append(
            {
                "Route": route_label(row),
                "Destination": friendly_value(row.get("trip_headsign")),
                "Vehicle": friendly_value(
                    row.get("vehicle_label"),
                    row.get("vehicle_id"),
                    row.get("entity_id"),
                ),
                "Trip": friendly_value(row.get("trip_id")),
                "Current stop": friendly_value(row.get("stop_name"), row.get("stop_id")),
                "Status": friendly_status(row.get("current_status")),
                "Last update": format_timestamp(row.get("event_timestamp")),
                "Latitude": round(float(latitude), 5) if pd.notna(latitude) else UNKNOWN_VALUE,
                "Longitude": round(float(longitude), 5) if pd.notna(longitude) else UNKNOWN_VALUE,
            }
        )
    return pd.DataFrame.from_records(records, columns=VEHICLE_COLUMNS)


def prepare_map_positions(vehicles: pd.DataFrame) -> pd.DataFrame:
    if vehicles.empty:
        return pd.DataFrame(columns=["latitude", "longitude"])

    positions = vehicles.copy()
    positions["latitude"] = pd.to_numeric(positions["latitude"], errors="coerce")
    positions["longitude"] = pd.to_numeric(positions["longitude"], errors="coerce")
    valid_coordinates = (
        positions["latitude"].between(-90, 90)
        & positions["longitude"].between(-180, 180)
    )
    return positions.loc[valid_coordinates, ["latitude", "longitude"]].reset_index(
        drop=True
    )
