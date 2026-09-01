from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from zlib import crc32

import pandas as pd
import pydeck as pdk

if __package__:
    from .presentation import (
        UNKNOWN_VALUE,
        format_timestamp,
        friendly_status,
        friendly_value,
        route_label,
    )
else:
    from presentation import (
        UNKNOWN_VALUE,
        format_timestamp,
        friendly_status,
        friendly_value,
        route_label,
    )

BOSTON_LATITUDE = 42.3601
BOSTON_LONGITUDE = -71.0589
BOSTON_MAP_ZOOM = 10
VEHICLE_LAYER_ID = "mbta-current-vehicles"
ROUTE_PALETTE = (
    (230, 57, 70),
    (42, 157, 143),
    (244, 162, 97),
    (69, 123, 157),
    (239, 71, 111),
    (118, 104, 175),
    (38, 166, 154),
    (241, 143, 1),
)
MBTA_ROUTE_FAMILY_COLORS = {
    "RED": (218, 41, 28),
    "ORANGE": (237, 139, 0),
    "BLUE": (0, 61, 165),
    "GREEN": (0, 132, 61),
    "SILVER": (124, 135, 142),
    "COMMUTER_RAIL": (128, 0, 128),
    "FERRY": (0, 142, 170),
    "BUS": (255, 199, 44),
}


def route_marker_color(route_id: object) -> list[int]:
    route_key = friendly_value(route_id)
    if route_key == UNKNOWN_VALUE:
        return [120, 120, 120, 210]
    normalized = route_key.upper()
    if normalized.startswith(("RED", "MATTAPAN")):
        color = MBTA_ROUTE_FAMILY_COLORS["RED"]
    elif normalized.startswith("ORANGE"):
        color = MBTA_ROUTE_FAMILY_COLORS["ORANGE"]
    elif normalized.startswith("BLUE"):
        color = MBTA_ROUTE_FAMILY_COLORS["BLUE"]
    elif normalized.startswith("GREEN"):
        color = MBTA_ROUTE_FAMILY_COLORS["GREEN"]
    elif normalized.startswith("SL"):
        color = MBTA_ROUTE_FAMILY_COLORS["SILVER"]
    elif normalized.startswith("CR-"):
        color = MBTA_ROUTE_FAMILY_COLORS["COMMUTER_RAIL"]
    elif "FERRY" in normalized or normalized.startswith("BOAT"):
        color = MBTA_ROUTE_FAMILY_COLORS["FERRY"]
    elif normalized[0].isdigit():
        color = MBTA_ROUTE_FAMILY_COLORS["BUS"]
    else:
        color = ROUTE_PALETTE[crc32(route_key.encode("utf-8")) % len(ROUTE_PALETTE)]
    return [*color, 220]


def prepare_vehicle_map_data(vehicles: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "latitude",
        "longitude",
        "route",
        "destination",
        "vehicle",
        "vehicle_id",
        "vehicle_label",
        "status",
        "last_update",
        "ingested_at",
        "marker_color",
    ]
    if vehicles.empty:
        return pd.DataFrame(columns=columns)

    records: list[dict[str, object]] = []
    for _, row in vehicles.iterrows():
        latitude = pd.to_numeric(row.get("latitude"), errors="coerce")
        longitude = pd.to_numeric(row.get("longitude"), errors="coerce")
        if (
            pd.isna(latitude)
            or pd.isna(longitude)
            or not -90 <= float(latitude) <= 90
            or not -180 <= float(longitude) <= 180
        ):
            continue
        records.append(
            {
                "latitude": float(latitude),
                "longitude": float(longitude),
                "route": route_label(row),
                "destination": friendly_value(row.get("trip_headsign")),
                "vehicle": friendly_value(
                    row.get("vehicle_label"),
                    row.get("vehicle_id"),
                    row.get("entity_id"),
                ),
                "vehicle_id": friendly_value(row.get("vehicle_id")),
                "vehicle_label": friendly_value(row.get("vehicle_label")),
                "status": friendly_status(row.get("current_status")),
                "last_update": format_timestamp(row.get("event_timestamp")),
                "ingested_at": format_timestamp(row.get("ingested_at")),
                "marker_color": route_marker_color(row.get("route_id")),
            }
        )
    return pd.DataFrame.from_records(records, columns=columns)


def build_vehicle_deck(map_data: pd.DataFrame) -> pdk.Deck:
    vehicle_layer = pdk.Layer(
        "ScatterplotLayer",
        id=VEHICLE_LAYER_ID,
        data=map_data,
        get_position="[longitude, latitude]",
        get_fill_color="marker_color",
        get_line_color=[255, 255, 255, 210],
        get_radius=65,
        radius_min_pixels=5,
        radius_max_pixels=12,
        line_width_min_pixels=1,
        pickable=True,
        auto_highlight=True,
        stroked=True,
    )
    return pdk.Deck(
        map_style=None,
        initial_view_state=pdk.ViewState(
            latitude=BOSTON_LATITUDE,
            longitude=BOSTON_LONGITUDE,
            zoom=BOSTON_MAP_ZOOM,
            pitch=0,
            bearing=0,
        ),
        layers=[vehicle_layer],
        tooltip={
            "html": (
                "<b>{route}</b><br/>"
                "Toward {destination}<br/>"
                "Vehicle: {vehicle}<br/>"
                "Status: {status}<br/>"
                "Updated: {last_update}"
            ),
            "style": {
                "backgroundColor": "#111827",
                "color": "#f9fafb",
                "fontSize": "13px",
            },
        },
    )


def selected_vehicle(selection_state: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not selection_state:
        return None
    selection = selection_state.get("selection", {})
    objects = selection.get("objects", {}) if isinstance(selection, Mapping) else {}
    layer_objects = objects.get(VEHICLE_LAYER_ID, {}) if isinstance(objects, Mapping) else {}
    if not isinstance(layer_objects, list) or not layer_objects:
        return None
    selected = layer_objects[0]
    return dict(selected) if isinstance(selected, Mapping) else None


def map_error_message(last_successful_update: object) -> str:
    return (
        "The vehicle map is temporarily unavailable. Last successful map refresh: "
        f"{format_timestamp(last_successful_update)}."
    )


def route_legend(map_data: pd.DataFrame, limit: int = 6) -> list[tuple[str, str, int]]:
    if map_data.empty:
        return []
    grouped = (
        map_data.groupby("route", dropna=False)
        .agg(marker_color=("marker_color", "first"), vehicles=("route", "size"))
        .sort_values("vehicles", ascending=False)
        .head(limit)
    )
    return [
        (
            str(route),
            "#{:02x}{:02x}{:02x}".format(*color[:3]),
            int(row["vehicles"]),
        )
        for route, row in grouped.iterrows()
        for color in [row["marker_color"]]
    ]
