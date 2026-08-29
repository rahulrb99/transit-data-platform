# Development Checklist

## Day 1 Target

- Confirm Docker Desktop is running and `docker --version` works in a new terminal.
- Run PostgreSQL with `docker compose up -d postgres`.
- Create `.env` from `.env.example`.
- Create a Python 3.11 virtual environment.
- Install dependencies with `pip install -e ".[dev]"`.
- Download the MBTA static GTFS ZIP.
- Load selected GTFS files into the `raw` schema.

## First Smoke Test

From the repository root:

```powershell
.\scripts\local_smoke_test.ps1 -PythonCommand python
```

If Python 3.11 is installed as `py -3.11`, run:

```powershell
.\scripts\local_smoke_test.ps1 -PythonCommand py -PythonArgs "-3.11"
```

## Success Criteria

- `raw.ingestion_metadata` has one row for the downloaded ZIP.
- `raw.routes`, `raw.stops`, `raw.trips`, and `raw.stop_times` exist in PostgreSQL.
- `dbt build --profiles-dir dbt` finishes successfully.
- `streamlit run dashboard/app.py` shows nonzero route, stop, trip, and scheduled-arrival counts.

## Next Implementation Slice

- Make raw table loads idempotent for each feed snapshot.
- Add row-count metadata for each extracted GTFS file.
- Add pytest coverage for checksum and ZIP discovery behavior.
- Add Airflow service definitions once the manual smoke test is stable.
