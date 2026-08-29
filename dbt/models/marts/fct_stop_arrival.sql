select
    trip_id,
    route_id,
    stop_id,
    stop_sequence,
    arrival_time as scheduled_arrival_time,
    departure_time as scheduled_departure_time
from {{ ref('int_trip_stop_times') }}
