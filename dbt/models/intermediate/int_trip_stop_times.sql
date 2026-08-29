select
    stop_times.trip_id,
    trips.route_id,
    stop_times.stop_id,
    stop_times.stop_sequence,
    stop_times.arrival_time,
    stop_times.departure_time
from {{ ref('stg_stop_times') }} as stop_times
inner join {{ ref('stg_trips') }} as trips
    on stop_times.trip_id = trips.trip_id

