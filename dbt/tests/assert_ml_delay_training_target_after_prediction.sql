select *
from {{ ref('ml_delay_training') }}
where target_event_timestamp <= prediction_timestamp

