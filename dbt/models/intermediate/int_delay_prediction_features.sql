with scheduled_stop_times as (
    select
        trip_id,
        stop_id as scheduled_stop_id,
        stop_sequence,
        arrival_time,
        departure_time,
        (
            split_part(arrival_time, ':', 1)::integer * 3600
            + split_part(arrival_time, ':', 2)::integer * 60
            + split_part(arrival_time, ':', 3)::integer
        ) as scheduled_arrival_seconds,
        (
            split_part(departure_time, ':', 1)::integer * 3600
            + split_part(departure_time, ':', 2)::integer * 60
            + split_part(departure_time, ':', 3)::integer
        ) as scheduled_departure_seconds
    from {{ ref('stg_stop_times') }}
),

scheduled_features as (
    select
        trip_id,
        scheduled_stop_id,
        stop_sequence,
        arrival_time,
        departure_time,
        scheduled_arrival_seconds,
        scheduled_departure_seconds,
        scheduled_departure_seconds - scheduled_arrival_seconds as scheduled_dwell_time_sec,
        lead(scheduled_arrival_seconds) over (
            partition by trip_id
            order by stop_sequence
        ) - scheduled_departure_seconds as scheduled_travel_time_to_next_stop_sec
    from scheduled_stop_times
),

eligible_observations as (
    select
        observation_id,
        event_id,
        ingested_at,
        event_timestamp,
        entity_id,
        vehicle_id,
        vehicle_label,
        trip_id,
        route_id,
        direction_id,
        start_time,
        start_date,
        stop_id,
        current_stop_sequence,
        current_status,
        latitude,
        longitude,
        bearing,
        speed,
        occupancy_status,
        source,
        extract(hour from event_timestamp) as hour,
        extract(dow from event_timestamp) as day_of_week,
        case when extract(dow from event_timestamp) in (0, 6) then true else false end
            as is_weekend
    from {{ ref('stg_realtime_vehicle_positions') }}
    where
        vehicle_id is not null
        and trip_id is not null
        and route_id is not null
        and start_date is not null
        and current_stop_sequence is not null
        and event_timestamp is not null
),

observations_with_schedule as (
    select
        observations.*,
        schedule.scheduled_stop_id,
        schedule.arrival_time as scheduled_arrival_time,
        schedule.departure_time as scheduled_departure_time,
        schedule.scheduled_arrival_seconds,
        schedule.scheduled_departure_seconds,
        schedule.scheduled_dwell_time_sec,
        schedule.scheduled_travel_time_to_next_stop_sec,
        make_timestamptz(
            substring(observations.start_date from 1 for 4)::integer,
            substring(observations.start_date from 5 for 2)::integer,
            substring(observations.start_date from 7 for 2)::integer,
            0,
            0,
            0,
            'America/New_York'
        ) + make_interval(secs => schedule.scheduled_arrival_seconds)
            as scheduled_arrival_timestamp,
        observations.stop_id = schedule.scheduled_stop_id as observed_stop_matches_schedule
    from eligible_observations as observations
    inner join scheduled_features as schedule
        on observations.trip_id = schedule.trip_id
        and observations.current_stop_sequence = schedule.stop_sequence
),

current_features as (
    select
        *,
        round(extract(epoch from event_timestamp - scheduled_arrival_timestamp))::integer
            as current_delay_sec
    from observations_with_schedule
),

leakage_safe_features as (
    select
        *,
        lag(current_delay_sec) over (
            partition by vehicle_id, trip_id
            order by event_timestamp, observation_id
        ) as previous_observed_delay_sec,
        avg(current_delay_sec) over (
            partition by route_id
            order by event_timestamp, observation_id
            rows between unbounded preceding and 1 preceding
        ) as historical_route_delay_sec,
        avg(current_delay_sec) over (
            partition by scheduled_stop_id
            order by event_timestamp, observation_id
            rows between unbounded preceding and 1 preceding
        ) as historical_stop_delay_sec
    from current_features
),

future_candidates as (
    select
        current_observation.observation_id,
        future_observation.observation_id as target_observation_id,
        future_observation.event_timestamp as target_event_timestamp,
        round(
            extract(epoch from (
                future_observation.event_timestamp - future_schedule.scheduled_arrival_timestamp
            ))
        )::integer as target_delay_4_stops_sec,
        row_number() over (
            partition by current_observation.observation_id
            order by future_observation.event_timestamp, future_observation.observation_id
        ) as target_rank
    from current_features as current_observation
    inner join current_features as future_observation
        on current_observation.vehicle_id = future_observation.vehicle_id
        and current_observation.trip_id = future_observation.trip_id
        and future_observation.current_stop_sequence = current_observation.current_stop_sequence + 4
        and future_observation.event_timestamp > current_observation.event_timestamp
    inner join current_features as future_schedule
        on future_observation.observation_id = future_schedule.observation_id
),

targets as (
    select
        observation_id,
        target_observation_id,
        target_event_timestamp,
        target_delay_4_stops_sec
    from future_candidates
    where target_rank = 1
)

select
    features.observation_id,
    features.event_id,
    features.ingested_at,
    features.event_timestamp,
    features.entity_id,
    features.vehicle_id,
    features.vehicle_label,
    features.trip_id,
    features.route_id,
    features.direction_id,
    features.start_time,
    features.start_date,
    features.stop_id as observed_stop_id,
    features.scheduled_stop_id,
    features.observed_stop_matches_schedule,
    features.current_stop_sequence,
    features.current_status,
    features.latitude,
    features.longitude,
    features.bearing,
    features.speed,
    features.occupancy_status,
    features.scheduled_arrival_time,
    features.scheduled_departure_time,
    features.scheduled_arrival_timestamp,
    features.scheduled_arrival_seconds,
    features.scheduled_departure_seconds,
    features.scheduled_dwell_time_sec,
    features.scheduled_travel_time_to_next_stop_sec,
    features.hour,
    features.day_of_week,
    features.is_weekend,
    features.current_delay_sec,
    features.previous_observed_delay_sec,
    features.historical_route_delay_sec,
    features.historical_stop_delay_sec,
    targets.target_observation_id,
    targets.target_event_timestamp,
    targets.target_delay_4_stops_sec,
    targets.target_delay_4_stops_sec is not null as has_target_delay_4_stops,
    'vehicle_trip_stop_sequence_plus_4' as target_generation_method
from leakage_safe_features as features
left join targets
    on features.observation_id = targets.observation_id

