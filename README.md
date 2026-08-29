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
- Add a dashboard with route and schedule overview metrics.
