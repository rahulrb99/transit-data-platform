from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
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

st.set_page_config(
    page_title="MBTA Transit Data Platform | Demo",
    page_icon="🚇",
    layout="wide",
)

@st.cache_data
def load_csv(name: str) -> pd.DataFrame:
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

vehicles = load_csv("vehicles.csv")
routes = load_csv("routes.csv")
metrics = load_metrics()
snapshot_at = metrics.get("snapshot_at", "date not recorded")

st.title("MBTA Transit Data Platform")
st.caption("Portfolio demo · Historical snapshot mode")

st.info(
    f"This is a read-only dashboard snapshot captured at {snapshot_at}. "
    "The live ingestion pipeline is not running on this demo host; vehicle positions "
    "and engineering metrics are historical and should not be interpreted as live."
)

with st.sidebar:
    st.header("Dashboard controls")
    st.caption("Snapshot mode")
    route_labels = route_option_labels(routes)
    route_ids = [None, *route_labels.keys()]
    selected_route_id = st.selectbox(
        "Route filter",
        route_ids,
        format_func=lambda route_id: (
            "All routes" if route_id is None else route_labels.get(route_id, route_id)
        ),
    )
    st.caption("Refresh the browser to reload the published snapshot.")

st.subheader("System status")
status_cols = st.columns(4)
status_cols[0].metric("Data mode", "Historical snapshot")
status_cols[1].metric("PostgreSQL", "Offline on demo host")
status_cols[2].metric("Routes", metric(metrics.get("routes")))
status_cols[3].metric("Snapshot captured", snapshot_at)

st.subheader("Key metrics")
primary = st.columns(3)
primary[0].metric("Vehicles in snapshot", metric(len(vehicles)))
primary[1].metric("Realtime events persisted", metric(metrics.get("realtime_events_persisted_total")))
primary[2].metric("Dead-letter records", metric(metrics.get("dead_letter_records")))

secondary = st.columns(3)
secondary[0].metric("Routes", metric(metrics.get("routes")))
secondary[1].metric("Stops", metric(metrics.get("stops")))
secondary[2].metric("Trips", metric(metrics.get("trips")))

with st.expander("Engineering metrics", expanded=True):
    eng = st.columns(4)
    eng[0].metric("Ingestion rate", metrics.get("current_ingestion_rate", "Unavailable"))
    eng[1].metric("Peak rate", metrics.get("peak_ingestion_rate", "Unavailable"))
    eng[2].metric("P95 ingestion latency", metrics.get("p95_latency", "Unavailable"))
    eng[3].metric("Consumer success", metrics.get("consumer_success_rate", "Unavailable"))
    st.caption(
        "These values are copied from the historical production snapshot, if provided. "
        "Missing values are shown as unavailable, not inferred."
    )

filtered = vehicles.copy()
if selected_route_id is not None and not filtered.empty and "route_id" in filtered:
    filtered = filtered[filtered["route_id"].astype(str) == str(selected_route_id)]

st.subheader("Vehicle activity")
if filtered.empty:
    st.warning(
        "No vehicle snapshot is bundled with this deployment yet. The map and layout "
        "remain available, but no markers can be shown until a real exported snapshot "
        "is added to dashboard/demo_data/vehicles.csv."
    )
else:
    map_data = prepare_vehicle_map_data(filtered)
    table_data = prepare_vehicle_table(filtered)
    map_col, table_col = st.columns([1.55, 1], gap="large")
    with map_col:
        st.markdown("**Vehicle map**")
        if map_data.empty:
            st.info("The snapshot has no valid coordinates.")
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
    with map_col:
        st.caption("Map positions are frozen at the snapshot timestamp.")
    with table_col:
        st.markdown("**Vehicles in snapshot**")
        st.dataframe(table_data, hide_index=True, width="stretch", height=520)

st.subheader("Recent activity")
if not filtered.empty:
    st.dataframe(
        prepare_vehicle_table(filtered.head(25)),
        hide_index=True,
        width="stretch",
    )
else:
    st.caption("Recent activity is unavailable until a snapshot is added.")

st.subheader("How data reaches this dashboard")
st.markdown(
    """
    <div style="display:flex;flex-wrap:wrap;gap:.5rem;margin:.75rem 0 1.25rem">
      <div style="flex:1;min-width:100px;padding:.9rem;border:1px solid #888;border-top:3px solid #e63946;border-radius:6px;text-align:center"><b>MBTA feeds</b><br/><small>GTFS + GTFS-Realtime</small></div>
      <div style="align-self:center">→</div>
      <div style="flex:1;min-width:100px;padding:.9rem;border:1px solid #888;border-top:3px solid #6c757d;border-radius:6px;text-align:center"><b>Python ingestion</b><br/><small>Fetch, parse, validate</small></div>
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

st.subheader("Route catalog")
if routes.empty:
    st.caption(
        "The route catalog is unavailable until routes.csv is exported from the existing "
        "PostgreSQL database or static GTFS data."
    )
else:
    display_routes = routes.rename(
        columns={
            "route_short_name": "Route",
            "route_long_name": "Route name",
            "trips": "Scheduled trips",
        }
    ).fillna(UNKNOWN_VALUE)
    st.dataframe(display_routes, hide_index=True, width="stretch", height=430)

st.caption(
    "The demo host is independent of AWS. The full streaming architecture and deployment "
    "remain documented in the repository; live telemetry is intentionally not simulated."
)
