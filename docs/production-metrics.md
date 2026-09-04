# Production metrics

The platform extends its existing PostgreSQL-backed pipeline metrics and health command.
It does not run a separate monitoring service. The dashboard caches the engineering
snapshot for 60 seconds, so its refresh cadence does not create a query per vehicle or
per event.

## View metrics

Human-readable production snapshot:

```bash
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml \
  exec -T consumer python -m ingestion.realtime.health
```

Machine-readable JSON for a dated baseline artifact:

```bash
mkdir -p metrics-baselines
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml \
  exec -T consumer python -m ingestion.realtime.health --json \
  | tee "metrics-baselines/$(date -u +%Y%m%dT%H%M%SZ).json"
```

CPU, memory, disk, service health, service start times, restart counts, backup status,
and the application snapshot together:

```bash
.venv/bin/python -m scripts.ops_health_check --production \
  | tee "metrics-baselines/ops-$(date -u +%Y%m%dT%H%M%SZ).log"
```

The Streamlit dashboard also shows ingestion rate, peak rate, p95 latency, and consumer
success in **Engineering metrics**.

## Definitions

| Metric | Unit/type | Calculation and persistence |
|---|---|---|
| `realtime_events_received_total` | events | Sum of consumer batch inputs in retained `realtime_pipeline_metrics`; includes replay attempts. |
| `realtime_events_persisted_total` | events | Sum of newly inserted, idempotent realtime rows in retained metrics. |
| `realtime_batches_processed_total` | batches | Count of successfully persisted `valid_batch` metric rows. |
| `realtime_batches_failed_total` | attempts | PostgreSQL batch attempts that failed before a later successful retry. Persisted on that successful batch. |
| `realtime_events_dead_lettered_total` | events | Successfully persisted dead-letter events in retained metrics. |
| `duplicate_records_detected_total` | events | Received events rejected by the existing `event_id` uniqueness constraint. |
| `retry_count_total` | retries | PostgreSQL persistence retries recorded on later successful valid/dead-letter writes. |
| `consumer_processing_success_rate` | ratio | Successful valid batches divided by successful batches plus failed valid-batch attempts. |
| `current_ingestion_rate_events_per_minute` | events/minute | Unique inserts in the trailing 5 minutes divided by 5. |
| `peak_ingestion_rate_events_per_minute` | events/minute | Largest UTC calendar-minute insert total in retained metrics history. |
| `ingestion_latency_seconds` | distribution | `ingested_at - feed_timestamp` for valid observations ingested in the trailing 5 minutes. |
| `event_age_seconds` | distribution | Snapshot time minus `vehicle_timestamp`, falling back to `feed_timestamp`, over the same bounded observation set. |
| latency distribution fields | seconds | Count, average, p50, p95, p99, and maximum, calculated by PostgreSQL. |
| `invalid_realtime_records_total` | records | Current retained dead-letter row count. |
| `records_missing_required_identifiers_total` | records | Dead letters produced by the existing required-field validation. |
| `records_rejected_by_validation_total` | records | Retained dead letters classified as `ValueError`. |
| `schema_validation_failures_total` | records | Retained malformed JSON, Unicode, or non-object payload dead letters. |
| `realtime_table_row_estimate` | rows | PostgreSQL `pg_class.reltuples`; inexpensive and approximate. |
| `database_size_bytes` | bytes | `pg_database_size(current_database())`. |
| `realtime_table_size_bytes` | bytes | `pg_total_relation_size`, including indexes and TOAST data. |
| `latest_persisted_event_timestamp` | UTC timestamp | `ingested_at` from the highest realtime row ID. |
| `postgres_uptime_seconds` | seconds | Snapshot time minus `pg_postmaster_start_time()`. |
| static GTFS counts | rows | Exact routes, stops, trips, stop_times, calendar, and calendar_dates counts from the latest atomic static-load manifest; labeled PostgreSQL estimates are used only for installations predating that manifest. |
| `metrics_query_duration_seconds` | seconds | Wall-clock duration to assemble one application snapshot; process-local, not persisted. |
| backup duration/size/success/checksum | seconds/bytes/boolean | Persisted in the existing local upload-status JSON and S3 latest-success manifest after verified upload. |
| restore duration | seconds | Printed by the guarded restore-validation command after a successful restore into a new `_restore` database. |
| CPU, memory, disk, service health/restarts | point-in-time | Read from `docker stats`, `df`, Compose state, and Docker container state by the existing operations command. |

Totals backed by `realtime_pipeline_metrics` cover the configured metrics retention
period (30 days by default), not the lifetime of the deployment. Dead-letter quality
counts cover currently retained dead-letter rows. Static counts represent the latest
successfully activated GTFS snapshot.

## Latency validity

Latency queries inspect at most the newest 25,000 rows and retain only rows ingested in
the trailing five minutes. Feed-to-ingestion latency excludes missing feed timestamps,
timestamps after ingestion, and values over 24 hours. Event age uses vehicle time first,
falls back to feed time, and independently rejects missing, future, or over-24-hour
values. No zero value is substituted for an invalid timestamp.

`ingestion_latency_seconds` measures MBTA feed generation time to producer observation
time. `event_age_seconds` describes how old the underlying vehicle observation is at
snapshot time. Neither includes the final PostgreSQL transaction duration; that duration
remains available per batch as `processing_duration_seconds`.

## Reliability limitations

- A failed PostgreSQL attempt is durable only if a later retry succeeds and writes its
  metric transactionally. A terminal database outage remains visible in logs and
  unhealthy Compose state because PostgreSQL cannot persist its own outage metric.
- Kafka commit failures remain visible in consumer logs and lead to safe replay. They
  are not counted as PostgreSQL batch failures.
- Producer HTTP/protobuf failures are logged but are not persisted as consumer schema
  failures. This avoids adding a second metrics store or changing producer semantics.
- Consumer start/restart count is reported from Docker at observation time. It is not a
  durable lifetime counter and resets when a container is recreated.
- Restore duration is emitted by the restore command only when an operator runs a
  recovery drill; it is not persisted and automated restore validation is not currently
  scheduled.

## Baseline period

Collect the first baseline after 24 continuous hours to capture normal weekday variation.
Retain daily JSON and operations snapshots for at least 7 days before writing resume
claims. Use measured medians/p95 values and a clearly dated observation window; never
extrapolate a short smoke test into a production throughput claim.

The strongest resume evidence is sustained unique events persisted, peak events/minute,
p95 source-to-ingestion latency, consumer processing success rate, duplicate-safe replay
count, database/table size, and a verified backup duration and size.
