{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='observation_id',
        on_schema_change='fail'
    )
}}

select
    observation_id,
    event_id,
    ingested_at,
    feed_timestamp,
    vehicle_timestamp,
    event_timestamp as prediction_timestamp,
    vehicle_id,
    vehicle_label,
    trip_id,
    route_id,
    direction_id,
    start_time,
    start_date,
    observed_stop_id,
    scheduled_stop_id,
    observed_stop_matches_schedule,
    current_stop_sequence,
    current_status,
    latitude,
    longitude,
    bearing,
    speed,
    occupancy_status,
    scheduled_arrival_time,
    scheduled_departure_time,
    scheduled_arrival_timestamp,
    scheduled_arrival_seconds,
    scheduled_departure_seconds,
    scheduled_dwell_time_sec,
    scheduled_travel_time_to_next_stop_sec,
    hour,
    day_of_week,
    is_weekend,
    current_delay_sec,
    previous_observed_delay_sec,
    historical_route_delay_sec,
    historical_stop_delay_sec,
    target_observation_id,
    target_event_timestamp,
    target_delay_4_stops_sec,
    target_generation_method,
    current_timestamp as training_row_created_at
from {{ ref('int_delay_prediction_features') }}
where has_target_delay_4_stops
{% if is_incremental() %}
    and event_timestamp >= coalesce(
        (
            select max(prediction_timestamp)
                - make_interval(hours => {{ var('realtime_incremental_lookback_hours', 24) }})
            from {{ this }}
        ),
        '1900-01-01'::timestamptz
    )
{% endif %}
