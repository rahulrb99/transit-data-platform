# dbt incremental processing

## Model decisions

`stg_realtime_vehicle_positions` is incremental because its PostgreSQL source is
append-only and grows continuously. Its unique key is `observation_id`, the source
table's `BIGSERIAL` primary key. `event_id` remains the business event identity and
is unique when present, but 498 legacy rows predate that field and contain nulls, so
it is not a safe dbt merge key for the complete historical relation.

The staging watermark is the maximum loaded `observation_id`, not a timestamp. A
newly inserted record always receives a higher source ID, so an event arriving with
an old `vehicle_timestamp`, `feed_timestamp`, or `ingested_at` is still selected.
Repeated runs with no higher source ID select no rows. The PostgreSQL
`delete+insert` strategy and unique key keep the model idempotent.

`ml_delay_training` is incremental because it is a growing supervised-learning fact
with one row per labeled observation and a stable `observation_id` unique key. It
recomputes the latest 24 hours of prediction timestamps on every run. This lookback
allows a recent observation to enter the mart when its four-stop-ahead target arrives
later, while `delete+insert` replaces any rows already present in that window. Set a
different window with:

```powershell
dbt run --project-dir dbt --profiles-dir dbt --select ml_delay_training --vars "{realtime_incremental_lookback_hours: 48}"
```

## Models intentionally left non-incremental

- `stg_routes`, `stg_stops`, `stg_trips`, and `stg_stop_times` remain views. They
  represent a replaceable static GTFS snapshot and have no reliable source change
  timestamp.
- `int_trip_stop_times` remains a view over the static snapshot.
- `int_delay_prediction_features` remains a view because previous-delay and
  historical aggregates use global windows, while a later observation can add a
  target to an older row. Incrementalizing it without recomputing impacted history
  would change feature meaning or introduce leakage.
- `dim_route`, `dim_stop`, and `fct_stop_arrival` remain tables rebuilt from the
  replaceable static feed. A static feed update can modify or remove existing rows,
  and there is no source update timestamp for a safe incremental filter.

Realtime observations are retained across feed changes, while the loaded static
GTFS tables represent the current snapshot. Historical realtime trip IDs can
therefore be absent from `stg_trips`. Relationship integrity is enforced on
`ml_delay_training`, after schedule matching, rather than across the entire realtime
staging history.

## Full refresh guidance

Run a full refresh when the realtime source is corrected with explicitly backfilled
IDs below the current watermark, when targets can arrive more than 24 hours late,
after changing model logic or unique keys, or when rebuilding all analytics from a
new baseline:

```powershell
dbt build --project-dir dbt --profiles-dir dbt --full-refresh
```

The staging strategy handles timestamp-late inserts, but not a manual source insert
that reuses an old `observation_id`. The training lookback is bounded; labels outside
the window require a wider override or full refresh. The global feature view still
performs full-history calculations, so training-mart incrementalization reduces
table replacement work but does not eliminate all upstream query cost.
