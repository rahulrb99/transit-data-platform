# Transit Data Platform

Local data engineering platform for MBTA transit data.

The project ingests MBTA GTFS static schedule data, preserves original raw files, records ingestion metadata, loads raw tables into PostgreSQL, transforms them with dbt into analytical models, and exposes summary metrics through a Streamlit dashboard.

## Architecture

```text
MBTA GTFS Static / GTFS-Realtime
        |
        v
Python ingestion
        |
        +--> raw file storage with metadata
        |
        v
PostgreSQL raw schema
        |
        v
dbt staging / intermediate / marts
        |
        v
Streamlit dashboard
```

## Stack

| Layer | Technology |
| --- | --- |
| Language | Python |
| Warehouse | PostgreSQL |
| Transformation | dbt |
| Orchestration | Airflow |
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

## Initial MVP

- Download and preserve the MBTA GTFS static ZIP.
- Record metadata: ingestion timestamp, source, file name, record count, checksum.
- Load key GTFS files into PostgreSQL raw tables.
- Build dbt staging models for routes, stops, trips, and stop times.
- Add a dashboard with route and schedule overview metrics.

