import pandas as pd
import psycopg
import streamlit as st

from ingestion.db import connect


def query_dataframe(sql: str) -> pd.DataFrame:
    with connect() as conn:
        return pd.read_sql(sql, conn)


st.set_page_config(page_title="MBTA Transit Data Platform", layout="wide")
st.title("MBTA Transit Data Platform")

try:
    overview = query_dataframe(
        """
        select
            (select count(*) from raw.routes) as routes,
            (select count(*) from raw.stops) as stops,
            (select count(*) from raw.trips) as trips,
            (select count(*) from raw.stop_times) as stop_arrivals
        """
    )
except psycopg.errors.UndefinedTable:
    st.info("Run static GTFS ingestion to populate the dashboard.")
    st.stop()
except psycopg.OperationalError as error:
    st.error("Could not connect to PostgreSQL. Confirm the project database is running.")
    st.caption(str(error))
    st.stop()

routes, stops, trips, stop_arrivals = st.columns(4)
routes.metric("Routes", f"{overview.loc[0, 'routes']:,}")
stops.metric("Stops", f"{overview.loc[0, 'stops']:,}")
trips.metric("Trips", f"{overview.loc[0, 'trips']:,}")
stop_arrivals.metric("Scheduled Arrivals", f"{overview.loc[0, 'stop_arrivals']:,}")

st.subheader("Route Overview")
route_summary = query_dataframe(
    """
    select
        r.route_short_name,
        r.route_long_name,
        count(distinct t.trip_id) as trips
    from raw.routes r
    left join raw.trips t
        on r.route_id = t.route_id
    group by 1, 2
    order by trips desc
    limit 25
    """
)
st.dataframe(route_summary, use_container_width=True, hide_index=True)
