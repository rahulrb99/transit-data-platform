# ML Training Dataset

## Prediction Problem

Given a vehicle's current realtime state, predict its delay approximately four
scheduled stops into the future.

The project does not train a model yet. This layer only creates and validates a
supervised-learning dataset that a future Python/XGBoost training script can
consume.

## Training Example

One training example is one realtime vehicle-position observation that can be
matched to a scheduled GTFS trip stop sequence.

The required join path is:

```text
realtime_vehicle_positions
  -> trip_id
  -> current_stop_sequence
  -> GTFS stop_times
  -> current scheduled stop
  -> current_stop_sequence + 4
  -> future realtime observation on the same vehicle and trip
```

Rows without a matching scheduled trip position stay in the intermediate feature
model for profiling, but only rows with a valid four-stop target are included in
`ml_delay_training`.

## Feature Definitions

The current feature set is intentionally simple and explainable:

- `vehicle_id`, `vehicle_label`: realtime vehicle identifiers.
- `ingested_at`, `feed_timestamp`, `vehicle_timestamp`, `prediction_timestamp`:
  source timestamps retained for debugging and model reproducibility.
- `trip_id`, `route_id`, `direction_id`: realtime trip context.
- `observed_stop_id`, `scheduled_stop_id`: current observed and scheduled stop.
- `observed_stop_matches_schedule`: whether the realtime stop matches static GTFS.
- `current_stop_sequence`: current GTFS stop sequence.
- `current_status`: GTFS-Realtime vehicle status.
- `latitude`, `longitude`, `bearing`, `speed`: realtime position fields when present.
- `scheduled_arrival_time`, `scheduled_departure_time`: static GTFS schedule fields.
- `scheduled_arrival_seconds`, `scheduled_departure_seconds`: parsed schedule seconds.
- `scheduled_dwell_time_sec`: scheduled departure minus scheduled arrival.
- `scheduled_travel_time_to_next_stop_sec`: next scheduled arrival minus current departure.
- `hour`, `day_of_week`, `is_weekend`: timestamp-derived temporal features.
- `current_delay_sec`: observed timestamp minus scheduled arrival timestamp.
- `previous_observed_delay_sec`: prior delay for the same vehicle and trip.
- `historical_route_delay_sec`: average prior delay on the same route.
- `historical_stop_delay_sec`: average prior delay at the same scheduled stop.

## Target Generation

The target is `target_delay_4_stops_sec`.

For each current observation, dbt looks for the first later realtime observation
with:

- same `vehicle_id`
- same `trip_id`
- `current_stop_sequence = current_stop_sequence + 4`
- `target_event_timestamp > prediction_timestamp`

The target delay is calculated as:

```text
target realtime event timestamp - scheduled arrival timestamp at the target stop
```

No target is fabricated. If the future observation is missing, the row is
excluded from `ml_delay_training`.

## Leakage Prevention

The target observation is never used as an input feature.

Historical route and stop delay features use window frames ending at the row
before the prediction event:

```text
rows between unbounded preceding and 1 preceding
```

This means each row only uses observations available before its prediction
timestamp.

## Known Limitations

- MBTA VehiclePositions does not include an explicit delay field. Delay is
  approximated from the vehicle timestamp, falling back to feed timestamp and
  ingestion timestamp when needed, minus the scheduled arrival time for the
  observed trip stop sequence.
- VehiclePositions snapshots may skip stops, repeat a stop, or emit statuses
  such as `IN_TRANSIT_TO`, which makes exact stop-arrival timing approximate.
- The four-stop target only exists after enough realtime history has been
  collected for the same vehicle and trip.
- Added/unscheduled trips may not map to static GTFS stop_times and are excluded.
- Static schedule times are interpreted in `America/New_York`.

## Why Weather Is Out Of Scope

Weather is intentionally excluded from the first training dataset. It introduces
another external data source, temporal joins, and backfill questions. Once the
baseline transit-only dataset is validated, weather can be added as an
experiment and kept only if it improves model performance.
