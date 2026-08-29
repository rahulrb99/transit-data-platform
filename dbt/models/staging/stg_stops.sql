select
    stop_id,
    stop_code,
    stop_name,
    stop_lat::numeric as stop_latitude,
    stop_lon::numeric as stop_longitude,
    location_type
from raw.stops

