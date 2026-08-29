select
    stop_id,
    stop_code,
    stop_name,
    stop_latitude,
    stop_longitude,
    location_type
from {{ ref('stg_stops') }}

