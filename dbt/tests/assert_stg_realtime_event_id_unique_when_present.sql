select event_id
from {{ ref('stg_realtime_vehicle_positions') }}
where event_id is not null
group by event_id
having count(*) > 1
