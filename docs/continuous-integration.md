# Continuous integration

GitHub Actions runs `.github/workflows/ci.yml` for pushes to `main` and pull
requests targeting `main`. The workflow has two jobs:

1. `lint` installs the pinned direct dependencies from `requirements-ci.txt` and
   runs `ruff check .`.
2. `database-and-dbt` starts a clean PostgreSQL 16 service, applies the production
   initialization SQL plus deterministic CI fixtures, runs the complete `pytest`
   suite, and executes `dbt parse` followed by `dbt build`. HTTP, Kafka, shutdown,
   retry, and consumer behavior use fakes and do not contact external services; two
   integration tests use the real PostgreSQL service.

The database job uses only ephemeral credentials defined in the workflow. It does
not read a developer `.env`, use GitHub secrets, contact MBTA, or retain data between
runs. Redpanda is intentionally absent because the current streaming tests are
unit-level and use Kafka fakes. The dashboard, live ingestion, Docker image build,
and deployment are also outside ordinary CI.

## Run the checks locally

Install the same direct dependency versions used by CI:

```bash
python -m pip install -r requirements-ci.txt
ruff check .
pytest -m "not integration"
```

For database and dbt checks, start a dedicated PostgreSQL database. This example
uses port `55433` so it does not conflict with the normal Compose database:

```bash
docker run --rm --name transit-ci-postgres \
  -e POSTGRES_DB=transit_ci \
  -e POSTGRES_USER=transit \
  -e POSTGRES_PASSWORD=transit_ci \
  -p 55433:5432 postgres:16
```

In another shell, configure the project to use that database:

```powershell
$env:POSTGRES_HOST = "localhost"
$env:POSTGRES_PORT = "55433"
$env:POSTGRES_DB = "transit_ci"
$env:POSTGRES_USER = "transit"
$env:POSTGRES_PASSWORD = "transit_ci"
python scripts/initialize_ci_database.py
pytest -m integration
dbt parse --project-dir dbt --profiles-dir dbt
dbt build --project-dir dbt --profiles-dir dbt
```

The initializer refuses to load fixtures unless `POSTGRES_DB` ends in `_ci`. Stop
the foreground container with Ctrl+C when validation is complete.
