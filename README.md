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
tests/                 Unit tests
data/                  Local raw and processed data folders
```

## Local Setup

1. Copy environment variables:

```bash
cp .env.example .env
```

2. Start PostgreSQL:

```bash
docker compose up -d postgres
```

3. Install Python dependencies:

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

## Realtime Streaming

Start PostgreSQL, Redpanda, and Redpanda Console:

```powershell
docker compose up -d postgres redpanda redpanda-init redpanda-console
```

Open Redpanda Console:

```text
http://localhost:8080
```

Publish one MBTA vehicle-position snapshot to the `vehicle_positions` topic:

```powershell
python -m ingestion.realtime.producer --once
```

Consume events into PostgreSQL:

```powershell
python -m ingestion.realtime.consumer --max-messages 500
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

### Realtime pipeline health

Each row in `realtime_pipeline_metrics` represents one PostgreSQL processing
transaction: either a valid vehicle-position batch or one dead-letter record. It
stores received, inserted, duplicate, dead-letter, and error counts together with
processing duration and the latest source and ingestion timestamps in that unit.
Metrics are committed in the same transaction as the corresponding operational
data, before Kafka offsets advance.

Run the local health command with:

```powershell
python -m ingestion.realtime.health
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
