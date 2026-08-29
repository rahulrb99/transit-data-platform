select
    trip_id,
    arrival_time,
    departure_time,
    stop_id,
    stop_sequence::integer as stop_sequence
from raw.stop_times

