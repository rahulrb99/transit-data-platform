# ML target audit and TripUpdates design

The current mart is exploratory, not a validated supervised-learning dataset.
No TripUpdates producer, model training or prediction service was implemented here.

## What the SQL actually does

int_delay_prediction_features joins observations to static stop_times by trip_id and
current_stop_sequence, requiring vehicle, route, trip, start_date, sequence and event time.
It derives scheduled arrival from New York service-day midnight plus GTFS elapsed seconds.
Current delay is observation timestamp minus scheduled arrival, irrespective of vehicle status.

The target is the earliest later observation for the SAME vehicle_id/trip_id at numeric
current_stop_sequence + 4, minus the future stop's scheduled arrival. Missing matches remain
null and ml_delay_training excludes them. Numeric sequence + 4 is not necessarily four
scheduled stops when sequence numbers have gaps.

Short capture windows, trip ends, feed gaps, absent trip/sequence/date, static mismatches,
nonconsecutive sequences and the requirement to observe the same vehicle later all reduce
label coverage. Earlier small-label results cannot be attributed to one cause without
profiling the exact captured dataset; a populated fixture does not establish live coverage.

## Scientific limitations found

- IN_TRANSIT_TO refers to an upcoming stop; its timestamp is not that stop's arrival.
  STOPPED_AT polling timestamps bound an arrival interval, not an exact arrival instant.
- The future join omits service date/start time and can match a different daily trip instance.
- Historical windows use event time and row order but do not enforce ingestion availability.
  Late-arriving records and same-timestamp ties can leak unavailable information.
- Current static schedules are not selected by historical feed version. The new static ZIP
  archive/load ledger preserves future inputs, but does not repair old schedule joins.
- Hour/day-of-week extracts use the database session timezone, usually UTC, rather than
  consistently using MBTA local time. DST service-day handling needs dedicated tests.
- The model named leakage_safe_features is therefore NOT proof that leakage is prevented.

Do not publish delay accuracy or train XGBoost on these labels as observed ground truth.

## TripUpdates: stronger information, not automatically truth

MBTA documents a TripUpdates feed at
https://cdn.mbta.com/realtime/TripUpdates.pb in its
[GTFS-Realtime implementation](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs-realtime.md).
The [GTFS reference](https://gtfs.org/documentation/realtime/reference/) distinguishes
predicted stop-time events, uncertain measurements and past updates. Past stops may be
removed. A prediction that later falls in the past is not automatically a confirmed arrival.
Even uncertainty=0 needs provider-specific verification before treating it as measured truth.

## Smallest defensible next experiment

1. Sample and archive TripUpdates independently without changing VehiclePositions events.
2. Measure which arrival/departure fields, timestamps, uncertainty and schedule relationships
   MBTA actually supplies, and whether updates are revised after a stop is passed.
3. Key a trip instance by feed/schedule version, trip_id, start_date and start_time when
   needed (frequency/repeated services). Join stop occurrences by stop_sequence; stop_id
   alone is ambiguous on looping routes. Route and vehicle IDs are consistency checks.
4. Order scheduled stops and choose ordinal + 4, not arithmetic on arbitrary sequence values.
5. Separate agency forecast-at-prediction-time from confirmed outcome-after-stop-time.
   If no defensible realized outcome exists, call the task forecast imitation, not actual-delay prediction.
6. Permit features only when both event time and ingestion availability precede prediction time.
   Outcome information may occur later because it is the LABEL, never an input feature.
7. Handle cancellations, skipped/no-data stops, added/replacement trips, vehicle swaps,
   midnight, >24h GTFS times, DST and stale static versions explicitly. Unknowns stay null.
8. Use chronological train/validation/test splits by trip instance; evaluate against simple
   current-delay and agency-forecast baselines with label provenance and coverage reporting.

Later code changes: a new ingestion/realtime/trip_updates module, separate versioned
event contract/topic, consumer persistence and SQL migration, dbt staging/instance/label
models, replay tests and quality gates. Reuse existing retry, archival and reliability patterns.
This is P2 for publishing the map, but a prerequisite for any defensible ML launch.

Weather remains out of scope: first establish reliable labels, leakage-free features and
baseline performance; weather can then be evaluated through a held-out ablation.
