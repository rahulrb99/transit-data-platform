select observation_id
from {{ ref('stg_realtime_vehicle_positions') }}
where event_timestamp is distinct from coalesce(
    vehicle_timestamp,
    feed_timestamp,
    ingested_at
)
