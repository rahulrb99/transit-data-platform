{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='observation_id',
        on_schema_change='fail'
    )
}}

select
    id as observation_id,
    event_id,
    ingested_at,
    feed_timestamp,
    vehicle_timestamp,
    coalesce(vehicle_timestamp, feed_timestamp, ingested_at) as event_timestamp,
    entity_id,
    vehicle_id,
    vehicle_label,
    trip_id,
    route_id,
    direction_id,
    start_time,
    start_date,
    stop_id,
    current_stop_sequence::integer as current_stop_sequence,
    current_status,
    latitude,
    longitude,
    bearing,
    speed,
    occupancy_status,
    source
from public.realtime_vehicle_positions
{% if is_incremental() %}
where id > coalesce((select max(observation_id) from {{ this }}), 0)
{% endif %}
