# Transit Data Platform

Local data engineering platform for MBTA transit data.

The project ingests MBTA GTFS static schedule data, preserves original raw files, records ingestion metadata, loads raw tables into PostgreSQL, transforms them with dbt into analytical models, and exposes summary metrics through a Streamlit dashboard.

## Architecture

```text
                    BATCH
MBTA GTFS Static -> Python ingestion -> raw storage -> PostgreSQL -> dbt

                    STREAM
MBTA GTFS-Realtime -> Python producer -> Redpanda -> Python consumer -> PostgreSQL
```

## Stack

| Layer | Technology |
| --- | --- |
| Language | Python |
| Warehouse | PostgreSQL |
| Transformation | dbt |
| Orchestration | Airflow |
| Streaming | Redpanda |
| Dashboard | Streamlit |
| Containers | Docker Compose |
| Testing | pytest + dbt tests |
| CI | GitHub Actions |

## Project Layout

```text
dags/                  Airflow DAGs
ingestion/             Python ingestion package
dbt/                   dbt project
dashboard/             Streamlit app
infrastructure/        SQL and local infrastructure assets
infrastructure/postgres/migrations/  Versioned database migrations
tests/                 Unit tests
data/                  Local raw and processed data folders
```

## Local Setup

1. Copy environment variables:

```bash
cp .env.example .env
```

Set `POSTGRES_PASSWORD` in `.env` before starting. Existing volumes require their
existing database password; changing `.env` alone does not rotate it.

2. Start the complete local runtime (no host producer/consumer needed):

```bash
docker compose up -d --build
```

Open <http://localhost:8501>. PostgreSQL, Redpanda, producer, consumer, and dashboard
are managed by Compose. See [Docker runtime](docs/docker-runtime.md) for restart,
health, storage migration, and EC2 preparation details. No AWS deployment is included.

3. For development/testing and dbt, install Python dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

4. Download the MBTA static GTFS feed:

```bash
python -m ingestion.static.download_mbta_gtfs
```

Or run the local smoke test:

```powershell
.\scripts\local_smoke_test.ps1
```

See [docs/development-checklist.md](docs/development-checklist.md) for the day-one validation checklist.

See [docs/continuous-integration.md](docs/continuous-integration.md) for the GitHub
Actions jobs, deterministic PostgreSQL/dbt validation, and equivalent local commands.

See [docs/ml-training-dataset.md](docs/ml-training-dataset.md) for the delay-prediction
training dataset design.

See [docs/production-readiness.md](docs/production-readiness.md) for the current
P0/P1/P2 production-readiness assessment, [docs/aws-deployment-runbook.md](docs/aws-deployment-runbook.md)
for future deployment commands, and [docs/backup-restore.md](docs/backup-restore.md)
for backup/restore procedures.
Production metric definitions and baseline commands are documented in
[docs/production-metrics.md](docs/production-metrics.md).
Production S3 permissions, lifecycle recommendations, IAM-role behavior, and manual
resource requirements are in [docs/s3-backup-archive.md](docs/s3-backup-archive.md).
Before any production Compose command, run
`python -m scripts.validate_production_environment --env-file .env.prod`; it rejects
mutable image tags, placeholders, missing S3 configuration, and stored AWS access keys.

## Realtime Streaming

Start the complete realtime pipeline:

```powershell
docker compose up -d
docker compose logs --tail 50 producer consumer
```

The console is optional and loopback-only. Start it with
`docker compose --profile tools up -d redpanda-console`, then open:

```text
http://localhost:8080
```

For an optional one-shot smoke test, use the running app image (the normal producer
already polls continuously; this extra snapshot may be deduplicated):

```powershell
docker compose exec producer python -m ingestion.realtime.producer --once
```

The consumer is already running under Compose. Inspect its logs instead of starting
a competing host consumer:

```powershell
docker compose logs --tail 50 consumer
```

The consumer group defaults to `VEHICLE_POSITIONS_CONSUMER_GROUP`, and
`MBTA_CONSUMER_BATCH_SIZE` controls the batch size (100 by default). Automatic offset
commits and offset storage are disabled. A batch is validated, bulk inserted in one
PostgreSQL transaction, and then committed synchronously using the next offset for
each partition represented in the batch. PostgreSQL failures are retried with capped
exponential backoff; replayed events remain idempotent through the unique `event_id`
index and `ON CONFLICT DO NOTHING` insertion. Valid events collected before a
malformed record are flushed first. The malformed record is then written to
`realtime_vehicle_position_dead_letters` and its partition offset is committed only
after that transaction succeeds, allowing later valid events to continue. The
15-minute maximum poll interval allows bounded database recovery; longer outages may
trigger a safe replay. SIGINT and SIGTERM flush pending valid or dead-letter work and
close the consumer cleanly.

Each dead-letter row represents one rejected Kafka record, uniquely identified by
`(topic, partition, kafka_offset)`. It preserves the raw payload, validation error,
processing timestamp, and any safely extractable event, vehicle, and trip IDs. A
replayed malformed record uses `ON CONFLICT DO NOTHING`. If dead-letter persistence
or its subsequent Kafka commit fails, the consumer stops without advancing past the
record so Redpanda can replay it. This remains at-least-once processing, not
exactly-once processing.

### Retention and migrations

Run versioned database migrations with:

```powershell
python scripts/migrate_database.py
```

The local Compose startup still initializes empty PostgreSQL volumes from
`infrastructure/postgres/init.sql`; migrations are the forward-change mechanism for
existing environments and CI fixtures.

Realtime retention is explicit. Preview eligible rows without deleting:

```powershell
docker compose exec producer python -m ingestion.retention
```

Apply local maintenance deliberately with:

```powershell
docker compose --profile maintenance run --rm retention --apply
```

The production overlay runs this same fail-closed worker hourly. Development does
not run retention automatically.

`REALTIME_RETENTION_DAYS`, `METRICS_RETENTION_DAYS`, and
`RAW_PAYLOAD_RETENTION_DAYS` control PostgreSQL and raw payload pruning. Raw
protobuf files and expired observations/dead letters are verified before deletion.
Development keeps them under `RAW_ARCHIVE_ROOT`; production uses the configured S3
archive backend described in [S3 backup and archive preparation](docs/s3-backup-archive.md).
Redpanda topic
retention is bounded by `REDPANDA_TOPIC_RETENTION_MS` and
`REDPANDA_TOPIC_RETENTION_BYTES`.

### Realtime pipeline health

Each row in `realtime_pipeline_metrics` represents one PostgreSQL processing
transaction: either a valid vehicle-position batch or one dead-letter record. It
stores received, inserted, duplicate, dead-letter, and error counts together with
processing duration and the latest source and ingestion timestamps in that unit.
Metrics are committed in the same transaction as the corresponding operational
data, before Kafka offsets advance.

Run the health command inside the existing application container:

```powershell
docker compose exec producer python -m ingestion.realtime.health
```

Freshness is `current UTC time - latest_source_event_timestamp`. The status is
`HEALTHY` when that age is at most `MBTA_REALTIME_STALE_THRESHOLD_SECONDS` (60 by
default), `STALE` above the threshold, and `UNKNOWN` before a source timestamp has
been recorded. Consumer lag is current state read directly from Redpanda for each
partition: `high-water offset - committed group offset`. It is not stored as a
historical metric. Historical metric totals count processing attempts since metrics
collection began; the dead-letter total is the authoritative current row count in
the idempotent dead-letter table.

Example output:

```text
Realtime Pipeline Health
------------------------
Latest event:        2026-08-30T12:12:37+00:00
Latest ingestion:    2026-08-30T12:12:40+00:00
Freshness:           3.0 seconds
Status:              HEALTHY

Events received:     100
Events persisted:    99
Duplicate events:    1
Dead-letter events:  2
Processing errors:   1
Recent batch size:   100
Recent duration:     0.154 seconds

Consumer lag:
  partition 0: lag=0 consumer=729 high_water=729
  total:       0
```

Verify the latest records:

```sql
SELECT *
FROM realtime_vehicle_positions
ORDER BY ingested_at DESC
LIMIT 20;
```

## Dashboard Overview

The dashboard starts with the full Compose stack. Start it separately with:

```powershell
docker compose up -d dashboard
```

The Overview page reads the existing static GTFS and realtime tables. It shows
pipeline freshness, the latest source event, cumulative realtime observations,
current vehicles, static route/stop/trip counts, dead-letter records, a vehicle
map, and the 25 newest observations. The route selector filters the current vehicle
table, map, and recent activity together. Route and stop names are enriched from
the stored static GTFS tables, and trip headsigns provide destinations when the
realtime trip ID matches the static snapshot. The dashboard never calls the MBTA
API directly.

"Vehicles currently observed" means the newest stored observation for each vehicle
within a bounded 5,000-observation window that is also newer than
`DASHBOARD_VEHICLE_MAX_AGE_SECONDS` (90 seconds by default). This keeps the query
bounded as history grows and hides old markers when ingestion is stale. Coordinates
that are missing or outside valid latitude and longitude ranges are excluded from
the map without removing the vehicle from the table. Missing labels, stops, trips,
headsigns, statuses, and coordinates display as `Unknown`. The dashboard does not
display delay or on-time claims because VehiclePositions does not provide an
authoritative delay value.

The Boston-centered PyDeck map uses open map tiles without a paid API key. Marker
colors group familiar MBTA route families. Hovering a marker shows a concise route,
destination, vehicle, status, and timestamp summary; selecting it opens a persistent
details panel. The page reports its last successful map refresh and automatically
refreshes through the existing Streamlit fragment interval.

Static catalog queries and exact aggregate counts are cached with bounded lifetimes.
Current vehicles and recent activity use the indexed observation identifier,
bounded result sets, parameterized route filters, and a short cache lifetime.

`DASHBOARD_REFRESH_INTERVAL_SECONDS` sets the default realtime refresh interval
(30 seconds by default). The sidebar can disable auto-refresh or select a different
interval without changing ingestion. "Last realtime event" is the source event
time from MBTA, while "Last dashboard refresh" is only the time Streamlit queried
the database.

Dashboard status has two independent parts. PostgreSQL connectivity is reported
separately from realtime freshness. Realtime data is `Receiving` when its latest
source event is no older than `MBTA_REALTIME_STALE_THRESHOLD_SECONDS`, `Stale` when
it exceeds that threshold, and `No data` when no source timestamp exists. A failed
section displays an unavailable message while the rest of the Overview remains
usable; database errors and credentials are not exposed in the page.

## Initial MVP

- Download and preserve the MBTA GTFS static ZIP.
- Record metadata: ingestion timestamp, source, file name, record count, checksum.
- Load key GTFS files into PostgreSQL raw tables.
- Build dbt staging models for routes, stops, trips, and stop times.

## Incremental dbt processing

The growing realtime staging relation and ML training fact use PostgreSQL dbt
incremental materializations. Realtime staging advances by the source
`observation_id`, which captures timestamp-late inserts without rescanning historical
rows. The training mart uses `observation_id` with a 24-hour lookback so recent rows
can gain four-stop-ahead targets safely. Static GTFS models and the global-window
feature model remain non-incremental by design. See
[docs/dbt-incremental-processing.md](docs/dbt-incremental-processing.md) for model
selection, late-arrival behavior, full-refresh guidance, and limitations.
- Add a dashboard with route and schedule overview metrics.
