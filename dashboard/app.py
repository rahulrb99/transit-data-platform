from __future__ import annotations

from datetime import UTC, datetime, timedelta
from html import escape

import pandas as pd
import psycopg
import streamlit as st
from map_view import (
    build_vehicle_deck,
    map_error_message,
    prepare_vehicle_map_data,
    route_legend,
    selected_vehicle,
)
from presentation import (
    UNKNOWN_VALUE,
    format_timestamp,
    prepare_vehicle_table,
    route_option_labels,
)

from data import (
    load_current_vehicles,
    load_dead_letter_count,
    load_latest_realtime_event,
    load_realtime_event_count,
    load_recent_realtime_events,
    load_route_options,
    load_route_summary,
    load_static_summary,
)
from ingestion.config import get_settings
from ingestion.realtime.health import calculate_freshness

REFRESH_OPTIONS_SECONDS = (15, 30, 60, 120, 300)
settings = get_settings()


@st.cache_data(ttl=3600, show_spinner=False)
def cached_static_summary() -> dict[str, int]:
    return load_static_summary()


@st.cache_data(ttl=60, show_spinner=False)
def cached_realtime_event_count() -> int:
    return load_realtime_event_count()


@st.cache_data(ttl=60, show_spinner=False)
def cached_dead_letter_count() -> int:
    return load_dead_letter_count()


@st.cache_data(ttl=60, show_spinner=False)
def cached_route_options(max_age_seconds: int) -> pd.DataFrame:
    return load_route_options(max_age_seconds)


@st.cache_data(ttl=10, show_spinner=False)
def cached_current_vehicles(route_id: str | None, max_age_seconds: int) -> pd.DataFrame:
    return load_current_vehicles(route_id, max_age_seconds)


@st.cache_data(ttl=10, show_spinner=False)
def cached_recent_events(route_id: str | None) -> pd.DataFrame:
    return load_recent_realtime_events(route_id)


@st.cache_data(ttl=3600, show_spinner=False)
def cached_route_summary() -> pd.DataFrame:
    return load_route_summary()


def format_age(age_seconds: float | None) -> str:
    if age_seconds is None:
        return UNKNOWN_VALUE
    if age_seconds < 60:
        return f"{age_seconds:.0f} seconds"
    return f"{age_seconds / 60:.1f} minutes"


def metric_value(value: int | None) -> str:
    return f"{value:,}" if value is not None else "Unavailable"


def closest_refresh_option(configured_seconds: int) -> int:
    return min(REFRESH_OPTIONS_SECONDS, key=lambda option: abs(option - configured_seconds))


def show_section_error(section_name: str, message: str) -> None:
    st.warning(f"{section_name} is unavailable. {message}")


def render_map_legend(entries: list[tuple[str, str, int]]) -> None:
    if not entries:
        return
    items = "".join(
        (
            '<span class="map-legend-item">'
            f'<span class="map-legend-swatch" style="background:{color}"></span>'
            f"{escape(route)} <small>{count}</small></span>"
        )
        for route, color, count in entries
    )
    st.markdown(f'<div class="map-legend">{items}</div>', unsafe_allow_html=True)


def render_selected_vehicle(vehicle: dict[str, object]) -> None:
    st.markdown("**Selected vehicle**")
    with st.container(border=True):
        first_row = st.columns(3)
        first_row[0].markdown(f"**Route**  \n{vehicle.get('route', UNKNOWN_VALUE)}")
        first_row[1].markdown(
            f"**Destination**  \n{vehicle.get('destination', UNKNOWN_VALUE)}"
        )
        first_row[2].markdown(f"**Status**  \n{vehicle.get('status', UNKNOWN_VALUE)}")
        second_row = st.columns(3)
        second_row[0].markdown(
            f"**Vehicle label**  \n{vehicle.get('vehicle_label', UNKNOWN_VALUE)}"
        )
        second_row[1].markdown(
            f"**Vehicle ID**  \n{vehicle.get('vehicle_id', UNKNOWN_VALUE)}"
        )
        second_row[2].markdown(
            f"**Last vehicle update**  \n{vehicle.get('last_update', UNKNOWN_VALUE)}"
        )


st.set_page_config(
    page_title="Live Overview | MBTA Transit Data Platform",
    page_icon=":material/directions_transit:",
    layout="wide",
)

st.markdown(
    """
    <style>
    .block-container { padding-top: 2rem; padding-bottom: 3rem; }
    .pipeline-flow {
        display: flex;
        align-items: stretch;
        gap: 0.55rem;
        margin: 0.75rem 0 1.25rem;
    }
    .pipeline-stage {
        flex: 1 1 8rem;
        min-width: 7.5rem;
        padding: 0.8rem 0.65rem;
        border: 1px solid rgba(128, 128, 128, 0.35);
        border-top: 3px solid var(--stage-color);
        border-radius: 6px;
        text-align: center;
        background: rgba(128, 128, 128, 0.06);
    }
    .pipeline-stage strong { display: block; font-size: 0.93rem; }
    .pipeline-stage span {
        display: block;
        margin-top: 0.2rem;
        font-size: 0.76rem;
        opacity: 0.75;
    }
    .pipeline-arrow {
        display: flex;
        align-items: center;
        opacity: 0.55;
        font-size: 1.2rem;
    }
    .map-legend {
        display: flex;
        flex-wrap: wrap;
        gap: 0.4rem 0.8rem;
        margin: 0.35rem 0 0.7rem;
        font-size: 0.8rem;
    }
    .map-legend-item { display: inline-flex; align-items: center; gap: 0.3rem; }
    .map-legend-swatch {
        width: 0.65rem;
        height: 0.65rem;
        border-radius: 50%;
        border: 1px solid rgba(255, 255, 255, 0.65);
    }
    .map-legend small { opacity: 0.65; }
    @media (max-width: 900px) {
        .pipeline-flow { flex-wrap: wrap; }
        .pipeline-arrow { display: none; }
        .pipeline-stage { flex-basis: 30%; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("MBTA Transit Data Platform")
st.caption("Live MBTA vehicle monitoring powered by a tested streaming data pipeline.")

with st.sidebar:
    st.header("Dashboard controls")
    auto_refresh = st.toggle(
        "Auto-refresh realtime data",
        value=True,
        help="Refreshes the live monitoring section without reloading static catalog data.",
    )
    refresh_interval_seconds = st.selectbox(
        "Refresh interval",
        options=REFRESH_OPTIONS_SECONDS,
        index=REFRESH_OPTIONS_SECONDS.index(
            closest_refresh_option(settings.dashboard_refresh_interval_seconds)
        ),
        format_func=lambda seconds: f"{seconds} seconds",
        disabled=not auto_refresh,
    )
    if st.button("Refresh now", icon=":material/refresh:", width="stretch"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Realtime queries are bounded and cached briefly to protect PostgreSQL.")


@st.fragment(
    run_every=(
        timedelta(seconds=refresh_interval_seconds)
        if auto_refresh
        else None
    )
)
def render_live_monitoring() -> None:
    dashboard_refreshed_at = datetime.now(UTC)

    try:
        latest = load_latest_realtime_event()
    except psycopg.Error:
        st.subheader("System status")
        st.error(
            "PostgreSQL is unavailable. Live metrics and vehicle monitoring cannot "
            "be loaded right now."
        )
        st.caption(f"Last dashboard refresh: {format_timestamp(dashboard_refreshed_at)}")
        return

    last_event_timestamp = latest.get("event_timestamp") if latest else None
    latest_ingestion_timestamp = latest.get("ingested_at") if latest else None
    freshness_age, freshness_status = calculate_freshness(
        last_event_timestamp if isinstance(last_event_timestamp, datetime) else None,
        dashboard_refreshed_at,
        settings.mbta_realtime_stale_threshold_seconds,
    )
    realtime_status = {
        "HEALTHY": "Receiving",
        "STALE": "Stale",
        "UNKNOWN": "No data",
    }[freshness_status]

    st.subheader("System status")
    health_state = "complete" if freshness_status == "HEALTHY" else "error"
    health_label = (
        "Systems connected and realtime data is receiving"
        if freshness_status == "HEALTHY"
        else f"PostgreSQL connected; realtime data is {realtime_status.lower()}"
    )
    with st.status(health_label, state=health_state, expanded=True):
        health_columns = st.columns(4)
        health_columns[0].markdown("**Realtime status**  \n" + realtime_status)
        health_columns[1].markdown("**PostgreSQL**  \nHealthy")
        health_columns[2].markdown(
            f"**Event freshness**  \n{format_age(freshness_age)}"
        )
        health_columns[3].markdown(
            f"**Last ingestion**  \n{format_timestamp(latest_ingestion_timestamp)}"
        )

    try:
        static_summary = cached_static_summary()
    except psycopg.Error:
        static_summary = {}
        show_section_error("Static GTFS metrics", "Confirm the raw GTFS tables are loaded.")

    try:
        realtime_event_count = cached_realtime_event_count()
    except psycopg.Error:
        realtime_event_count = None
        show_section_error("Realtime total", "The realtime observation table could not be read.")

    try:
        dead_letter_count = cached_dead_letter_count()
    except psycopg.Error:
        dead_letter_count = None
        show_section_error("Dead-letter total", "The dead-letter table could not be read.")

    try:
        route_options = cached_route_options(settings.dashboard_vehicle_max_age_seconds)
    except psycopg.Error:
        route_options = pd.DataFrame()
        show_section_error("Route filter", "Current route options could not be loaded.")

    route_labels = route_option_labels(route_options)
    route_ids: list[str | None] = [None, *route_labels]
    selected_route_id = st.selectbox(
        "Route filter",
        route_ids,
        format_func=lambda route_id: (
            "All routes" if route_id is None else route_labels.get(route_id, route_id)
        ),
        help="Filters the current vehicle view, map, and recent activity.",
    )

    try:
        with st.spinner("Loading current vehicle positions..."):
            current_vehicles = cached_current_vehicles(
                selected_route_id,
                settings.dashboard_vehicle_max_age_seconds,
            )
    except psycopg.Error:
        current_vehicles = None
        show_section_error(
            "Live vehicle activity",
            "PostgreSQL could not return current vehicle positions. Retrying on refresh.",
        )
    else:
        st.session_state["map_last_successful_update"] = dashboard_refreshed_at

    st.subheader("Key metrics")
    primary_metrics = st.columns(3)
    primary_metrics[0].metric(
        "Vehicles currently observed",
        metric_value(len(current_vehicles) if current_vehicles is not None else None),
        help="Latest observation per vehicle within the bounded realtime window.",
    )
    primary_metrics[1].metric("Realtime events", metric_value(realtime_event_count))
    primary_metrics[2].metric("Dead-letter records", metric_value(dead_letter_count))
    static_metrics = st.columns(3)
    static_metrics[0].metric("Routes", metric_value(static_summary.get("routes")))
    static_metrics[1].metric("Stops", metric_value(static_summary.get("stops")))
    static_metrics[2].metric("Trips", metric_value(static_summary.get("trips")))

    st.subheader("Live vehicle activity")
    selected_route_name = (
        "all routes"
        if selected_route_id is None
        else route_labels.get(selected_route_id, selected_route_id)
    )
    st.caption(
        f"Vehicles updated within the last "
        f"{settings.dashboard_vehicle_max_age_seconds} seconds on {selected_route_name}. "
        "Hover for a summary or select a marker for details."
    )

    if freshness_status != "HEALTHY":
        st.warning(
            "Realtime data is stale. Old vehicle markers are hidden until fresh "
            "observations reach PostgreSQL."
        )

    if current_vehicles is None:
        last_map_update = st.session_state.get("map_last_successful_update")
        st.error(map_error_message(last_map_update))
    else:
        if current_vehicles.empty:
            st.info("No fresh vehicle observations match this route.")
        else:
            map_data = prepare_vehicle_map_data(current_vehicles)
            vehicle_table = prepare_vehicle_table(current_vehicles)
            map_column, table_column = st.columns([1.55, 1], gap="large")
            with map_column:
                st.markdown("**Vehicle map**")
                if map_data.empty:
                    st.info("No valid coordinates are available for these vehicles.")
                else:
                    render_map_legend(route_legend(map_data))
                    st.caption(
                        "Colors group familiar MBTA route families; marker labels "
                        "identify the exact route."
                    )
                    map_event = st.pydeck_chart(
                        build_vehicle_deck(map_data),
                        width="stretch",
                        height=520,
                        selection_mode="single-object",
                        on_select="rerun",
                        key=f"vehicle-map-{selected_route_id or 'all'}",
                    )
                    selected = selected_vehicle(map_event)
                    if selected:
                        render_selected_vehicle(selected)
                    missing_positions = len(current_vehicles) - len(map_data)
                    if missing_positions:
                        st.caption(
                            f"{missing_positions:,} vehicle observations without valid "
                            "coordinates are omitted from the map."
                        )
            with table_column:
                st.markdown("**Current vehicles**")
                st.dataframe(
                    vehicle_table,
                    hide_index=True,
                    width="stretch",
                    height=520,
                )

        st.caption(
            "Map data last refreshed successfully: "
            f"{format_timestamp(st.session_state.get('map_last_successful_update'))}"
        )

    st.subheader("Recent activity")
    try:
        recent_events = cached_recent_events(selected_route_id)
    except psycopg.Error:
        show_section_error("Recent activity", "Recent observations could not be loaded.")
    else:
        recent_table = prepare_vehicle_table(recent_events)
        if recent_table.empty:
            st.info("No recent vehicle observations match this route.")
        else:
            st.dataframe(
                recent_table,
                hide_index=True,
                width="stretch",
                height=330,
            )

    st.caption(
        f"Last realtime event: {format_timestamp(last_event_timestamp)} | "
        f"Last dashboard refresh: {format_timestamp(dashboard_refreshed_at)}"
    )


render_live_monitoring()

st.subheader("How data reaches this page")
st.markdown(
    """
    <div class="pipeline-flow">
      <div class="pipeline-stage" style="--stage-color:#e63946"><strong>MBTA feeds</strong><span>GTFS and GTFS-Realtime</span></div>
      <div class="pipeline-arrow">&rarr;</div>
      <div class="pipeline-stage" style="--stage-color:#6c757d"><strong>Python ingestion</strong><span>Fetch, parse, validate</span></div>
      <div class="pipeline-arrow">&rarr;</div>
      <div class="pipeline-stage" style="--stage-color:#f4a261"><strong>Redpanda</strong><span>Durable event stream</span></div>
      <div class="pipeline-arrow">&rarr;</div>
      <div class="pipeline-stage" style="--stage-color:#2a9d8f"><strong>PostgreSQL</strong><span>Raw and realtime history</span></div>
      <div class="pipeline-arrow">&rarr;</div>
      <div class="pipeline-stage" style="--stage-color:#e76f51"><strong>dbt</strong><span>Tested analytics models</span></div>
      <div class="pipeline-arrow">&rarr;</div>
      <div class="pipeline-stage" style="--stage-color:#ef476f"><strong>Streamlit</strong><span>Transit monitoring</span></div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.subheader("Route catalog")
try:
    static_summary = cached_static_summary()
    route_summary = cached_route_summary()
except psycopg.Error:
    show_section_error("Route catalog", "Confirm PostgreSQL and static GTFS are available.")
else:
    st.caption(
        f"Current static GTFS snapshot: {static_summary['stop_arrivals']:,} "
        "scheduled stop arrivals."
    )
    route_summary = route_summary.rename(
        columns={
            "route_short_name": "Route",
            "route_long_name": "Route name",
            "trips": "Scheduled trips",
        }
    ).fillna(UNKNOWN_VALUE)
    st.dataframe(route_summary, hide_index=True, width="stretch", height=430)
