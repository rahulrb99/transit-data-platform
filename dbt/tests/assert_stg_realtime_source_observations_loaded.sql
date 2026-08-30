select
    source.id as observation_id
from public.realtime_vehicle_positions as source
left join {{ ref('stg_realtime_vehicle_positions') }} as staged
    on source.id = staged.observation_id
where staged.observation_id is null
