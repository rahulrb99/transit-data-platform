select 'training_rows' as metric, count(*)::text as value
from marts_marts.ml_delay_training

union all

select 'distinct_vehicles', count(distinct vehicle_id)::text
from marts_marts.ml_delay_training

union all

select 'distinct_trips', count(distinct trip_id)::text
from marts_marts.ml_delay_training

union all

select 'routes', count(distinct route_id)::text
from marts_marts.ml_delay_training

union all

select
    'time_coverage',
    min(prediction_timestamp)::text || ' to ' || max(prediction_timestamp)::text
from marts_marts.ml_delay_training

union all

select
    'eligible_feature_rows',
    count(*)::text
from marts_intermediate.int_delay_prediction_features

union all

select
    'valid_4_stop_target_pct',
    round(
        100.0 * avg(case when has_target_delay_4_stops then 1 else 0 end),
        2
    )::text
from marts_intermediate.int_delay_prediction_features

union all

select
    'target_delay_min_p50_avg_p95_max',
    concat_ws(
        ', ',
        min(target_delay_4_stops_sec)::text,
        percentile_cont(0.5) within group (order by target_delay_4_stops_sec)::integer::text,
        round(avg(target_delay_4_stops_sec), 2)::text,
        percentile_cont(0.95) within group (order by target_delay_4_stops_sec)::integer::text,
        max(target_delay_4_stops_sec)::text
    )
from marts_marts.ml_delay_training;

select
    'previous_observed_delay_sec' as feature,
    round(100.0 * avg(case when previous_observed_delay_sec is null then 1 else 0 end), 2)
        as missing_pct
from marts_marts.ml_delay_training

union all

select
    'historical_route_delay_sec',
    round(100.0 * avg(case when historical_route_delay_sec is null then 1 else 0 end), 2)
from marts_marts.ml_delay_training

union all

select
    'historical_stop_delay_sec',
    round(100.0 * avg(case when historical_stop_delay_sec is null then 1 else 0 end), 2)
from marts_marts.ml_delay_training

union all

select
    'scheduled_travel_time_to_next_stop_sec',
    round(100.0 * avg(
        case when scheduled_travel_time_to_next_stop_sec is null then 1 else 0 end
    ), 2)
from marts_marts.ml_delay_training

union all

select
    'speed',
    round(100.0 * avg(case when speed is null then 1 else 0 end), 2)
from marts_marts.ml_delay_training

union all

select
    'occupancy_status',
    round(100.0 * avg(case when occupancy_status is null then 1 else 0 end), 2)
from marts_marts.ml_delay_training;

