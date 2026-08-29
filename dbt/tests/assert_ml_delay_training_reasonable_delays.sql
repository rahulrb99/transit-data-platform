select *
from {{ ref('ml_delay_training') }}
where
    current_delay_sec not between -86400 and 86400
    or target_delay_4_stops_sec not between -86400 and 86400

