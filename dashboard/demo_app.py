from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

from dashboard.map_view import (
    build_vehicle_deck,
    prepare_vehicle_map_data,
    route_legend,
    selected_vehicle,
)
from dashboard.presentation import (
    UNKNOWN_VALUE,
    prepare_vehicle_table,
    route_option_labels,
)

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "demo_data"
MBTA_API_BASE = "https://api-v3.mbta.com"
REQUEST_TIMEOUT_SECONDS = 12

st.set_page_config(
    page_title="MBTA Transit Data Platform | Demo",
    page_icon="🚇",
    layout="wide",
)


@st.cache_data(ttl=30, show_spinner=False)
def load_live_vehicles() -> tuple[pd.DataFrame, str]:
    """Fetch public live vehicle data for this temporary demo deployment."""
    response = requests.get(
        f"{MBTA_API_BASE}/vehicles",
        params={
            "include": "trip,route,stop",
            "page[limit]": 500,
        },
        headers={"User-Agent": "MBTA-Transit-Data-Platform-Portfolio-Demo"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()

    included: dict[tuple[str, str], dict] = {}
    for resource in payload.get("included", []):
        if resource.get("type") and resource.get("id"):
            included[(resource["type"], resource["id"])] = resource.get("attributes", {})

    fetched_at = datetime.now(UTC).isoformat()
    records: list[dict[str, object]] = []
    for resource in payload.get("data", []):
        attributes = resource.get("attributes") or {}
        relationships = resource.get("relationships") or {}

        def related_id(name: str) -> str | None:
            relation = relationships.get(name) or {}
            data = relation.get("data")
            if isinstance(data, dict):
                value = data.get("id")
                return str(value) if value is not None else None
            return None

        route_id = related_id("route")
        trip_id = related_id("trip")
        stop_id = related_id("stop")
        route = included.get(("route", route_id), {}) if route_id else {}
        trip = included.get(("trip", trip_id), {}) if trip_id else {}
        stop = included.get(("stop", stop_id), {}) if stop_id else {}

        records.append({
            "event_timestamp": attributes.get("updated_at"),
            "vehicle_id": resource.get("id"),
            "vehicle_label": attributes.get("label"),
            "entity_id": resource.get("id"),
            "trip_id": trip_id,
            "trip_headsign": trip.get("headsign"),
            "route_id": route_id,
            "route_short_name": route.get("short_name"),
            "route_long_name": route.get("long_name"),
            "stop_id": stop_id,
            "stop_name": stop.get("name"),
            "current_status": attributes.get("current_status"),
            "ingested_at": fetched_at,
            "latitude": attributes.get("latitude"),
            "longitude": attributes.get("longitude"),
        })

    return pd.DataFrame.from_records(records), fetched_at


@st.cache_data
def load_fallback_csv(name: str) -> pd.DataFrame:
    path = DATA_DIR / name
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


@st.cache_data
def load_metrics() -> dict:
    path = DATA_DIR / "metrics.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def metric(value: object) -> str:
    if value is None or value == "":
        return "Unavailable"
    try:
        return f"{int(value):,}"
    except (ValueError, TypeError):
        return str(value)


metrics = load_metrics()
snapshot_at = metrics.get("snapshot_at", "date not recorded")
api_error: str | None = None
api_fetched_at: str | None = None
try:
    vehicles, api_fetched_at = load_live_vehicles()
    api_status = "Connected"
    data_mode = "Live MBTA API"
except (requests.RequestException, ValueError, KeyError) as exc:
    api_error = type(exc).__name__
    vehicles = load_fallback_csv("vehicles.csv")
    api_status = "Unavailable"
    data_mode = "Saved snapshot" if not vehicles.empty else "API unavailable"

st.title("MBTA Transit Data Platform")
st.caption("Portfolio dashboard · temporary demo deployment")

st.info(
    "The full production pipeline (producer → Redpanda → consumer → PostgreSQL) "
    "is separate from this no-cost demo. The map below reads public MBTA V3 API data "
    "directly; historical engineering metrics are explicitly labeled with their snapshot date."
)

with st.sidebar:
    st.header("Dashboard controls")
    st.caption(f"Vehicle data source: {data_mode}")
    if st.button("Refresh vehicle data", icon=":material/refresh:", width="stretch"):
        load_live_vehicles.clear()
        st.rerun()
    if api_fetched_at:
        st.caption(f"API response received: {api_fetched_at}")
    st.caption("Live responses are cached for 30 seconds.")

# API payloads contain the route catalog for vehicles returned in this response.
active_routes = vehicles[
    [column for column in ("route_id", "route_short_name", "route_long_name") if column in vehicles]
].drop_duplicates() if not vehicles.empty else pd.DataFrame(
    columns=["route_id", "route_short_name", "route_long_name"]
)
route_labels = route_option_labels(active_routes)
route_ids: list[str | None] = [None, *route_labels.keys()]
with st.sidebar:
    selected_route_id = st.selectbox(
        "Route filter",
        route_ids,
        format_func=lambda route_id: (
            "All routes" if route_id is None else route_labels.get(route_id, route_id)
        ),
    )

st.subheader("System status")
status_cols = st.columns(4)
status_cols[0].metric("Vehicle data source", data_mode)
status_cols[1].metric("MBTA API", api_status)
status_cols[2].metric("Routes in production GTFS", metric(metrics.get("routes")))
status_cols[3].metric("Historical snapshot", snapshot_at)
if api_error:
    st.warning(
        f"The live MBTA API could not be reached ({api_error}). "
        "Showing a bundled vehicle snapshot if one is available; no live status is inferred."
    )

st.subheader("Key metrics")
primary = st.columns(3)
primary[0].metric("Vehicles returned by API", metric(len(vehicles)) if api_status == "Connected" else metric(None))
primary[1].metric(
    "Historical events persisted",
    metric(metrics.get("realtime_events_persisted_total")),
)
primary[2].metric(
    "Historical dead-letter records",
    metric(metrics.get("dead_letter_records")),
)

secondary = st.columns(3)
secondary[0].metric("Routes", metric(metrics.get("routes")))
secondary[1].metric("Stops", metric(metrics.get("stops")))
secondary[2].metric("Trips", metric(metrics.get("trips")))

with st.expander("Engineering metrics · historical production snapshot", expanded=True):
    eng = st.columns(4)
    eng[0].metric("Ingestion rate", metrics.get("current_ingestion_rate", "Unavailable"))
    eng[1].metric("Peak rate", metrics.get("peak_ingestion_rate", "Unavailable"))
    eng[2].metric("P95 ingestion latency", metrics.get("p95_latency", "Unavailable"))
    eng[3].metric("Consumer success", metrics.get("consumer_success_rate", "Unavailable"))
    st.caption(
        f"Point-in-time metrics captured {snapshot_at}. These are not live measurements "
        "from this temporary demo deployment."
    )

filtered = vehicles.copy()
if selected_route_id is not None and not filtered.empty and "route_id" in filtered:
    filtered = filtered[filtered["route_id"].astype(str) == str(selected_route_id)]

st.subheader("Vehicle activity")
if filtered.empty:
    st.warning(
        "No vehicle observations are available right now. Try refreshing the vehicle data. "
        "The dashboard layout and historical production metrics remain available."
    )
else:
    map_data = prepare_vehicle_map_data(filtered)
    table_data = prepare_vehicle_table(filtered)
    map_col, table_col = st.columns([1.55, 1], gap="large")
    with map_col:
        st.markdown("**Vehicle map**")
        if map_data.empty:
            st.info("Vehicle observations have no valid coordinates for mapping.")
        else:
            entries = route_legend(map_data)
            if entries:
                legend = " · ".join(f"{route} ({count})" for route, _, count in entries)
                st.caption(legend)
            event = st.pydeck_chart(
                build_vehicle_deck(map_data),
                width="stretch",
                height=520,
                selection_mode="single-object",
                on_select="rerun",
                key=f"demo-vehicle-map-{selected_route_id or 'all'}",
            )
            selected = selected_vehicle(event)
            if selected:
                st.markdown("**Selected vehicle**")
                st.json(selected)
    with table_col:
        st.markdown("**Vehicles in current response**")
        st.dataframe(table_data, hide_index=True, width="stretch", height=520)
    st.caption(
        f"Vehicle positions are from the MBTA API; response retrieved at "
        f"{api_fetched_at or 'the saved-snapshot timestamp'}."
    )

st.subheader("Recent activity")
if not filtered.empty:
    st.dataframe(
        prepare_vehicle_table(filtered.head(25)),
        hide_index=True,
        width="stretch",
    )
else:
    st.caption("Recent vehicle activity is unavailable.")

st.subheader("How data reaches the production dashboard")
st.markdown(
    """
    <div style="display:flex;flex-wrap:wrap;gap:.5rem;margin:.75rem 0 1.25rem">
      <div style="flex:1;min-width:100px;padding:.9rem;border:1px solid #888;border-top:3px solid #e63946;border-radius:6px;text-align:center"><b>MBTA feeds</b><br/><small>GTFS + GTFS-Realtime</small></div>
      <div style="align-self:center">→</div>
      <div style="flex:1;min-width:100px;padding:.9rem;border:1px solid #888;border-top:3px solid #6c757d;border-radius:6px;text-align:center"><b>Python producer</b><br/><small>Fetch, parse, publish</small></div>
      <div style="align-self:center">→</div>
      <div style="flex:1;min-width:100px;padding:.9rem;border:1px solid #888;border-top:3px solid #f4a261;border-radius:6px;text-align:center"><b>Redpanda</b><br/><small>Durable event stream</small></div>
      <div style="align-self:center">→</div>
      <div style="flex:1;min-width:100px;padding:.9rem;border:1px solid #888;border-top:3px solid #2a9d8f;border-radius:6px;text-align:center"><b>PostgreSQL</b><br/><small>Operational storage</small></div>
      <div style="align-self:center">→</div>
      <div style="flex:1;min-width:100px;padding:.9rem;border:1px solid #888;border-top:3px solid #ef476f;border-radius:6px;text-align:center"><b>Streamlit</b><br/><small>Dashboard</small></div>
    </div>
    """,
    unsafe_allow_html=True,
)
st.caption(
    "Demo hosting bypasses the production ingestion/database path only for live vehicle "
    "display. The streaming architecture and its historical metrics are documented separately."
)

st.subheader("Routes represented by current vehicle data")
if vehicles.empty:
    st.caption("Route detail is unavailable until live vehicle data is reachable.")
else:
    route_catalog = (
        vehicles.groupby(
            ["route_id", "route_short_name", "route_long_name"],
            dropna=False,
        )
        .size()
        .reset_index(name="Vehicles observed")
        .sort_values("Vehicles observed", ascending=False)
        .rename(columns={
            "route_id": "Route ID",
            "route_short_name": "Route",
            "route_long_name": "Route name",
        })
    )
    st.dataframe(route_catalog, hide_index=True, width="stretch", height=430)
