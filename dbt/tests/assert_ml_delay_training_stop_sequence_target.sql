select training.*
from {{ ref('ml_delay_training') }} as training
inner join {{ ref('int_delay_prediction_features') }} as target_features
    on training.target_observation_id = target_features.observation_id
where target_features.current_stop_sequence <> training.current_stop_sequence + 4

