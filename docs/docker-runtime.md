# Local Compose runtime

This is EC2 deployment preparation, not an AWS deployment. Caddy, HTTPS, and a
GitHub OIDC placeholder workflow are configured as repository files only. No S3,
AWS credentials, or live deployment automation is executed here.

## Start and stop

Copy `.env.example` to `.env` and set `POSTGRES_PASSWORD` before the first start.
Do not change the password in `.env` for an existing database unless its database
role password is changed too: PostgreSQL initializes credentials only on an empty
data directory. The `.env` file is ignored by Git and excluded from image builds.

Stop any previously host-run producer, consumer, and dashboard processes first.
The following commands require Docker and Compose, not a host Python environment:

```bash
docker compose config --quiet
docker compose up -d --build
docker compose ps
docker compose logs --tail 50 producer consumer
docker compose exec producer python -m ingestion.realtime.health
```

Open <http://localhost:8501>. If that port is occupied, set `DASHBOARD_PORT` in
`.env` and use that port instead. Subsequent starts can use `docker compose up -d`.
To stop and recreate the whole stack without deleting data:

```bash
docker compose --profile tools down
docker compose up -d
```

**Never use `down -v`, `volume prune`, or remove storage volumes to restart.**
Keep the Compose project name stable when moving/renaming the repository, or
explicitly migrate its volumes. Volumes are scoped to the Compose project.

## Services and networking

| Service | Runtime | Storage |
| --- | --- | --- |
| postgres | PostgreSQL 16 | `postgres_data` named volume |
| redpanda | Existing single-node Redpanda | `redpanda_data` named volume |
| producer | Existing continuous producer command | `raw_data` named volume |
| consumer | Existing batched consumer command | PostgreSQL and broker offsets |
| dashboard | Existing Streamlit app | Stateless |
| redpanda-init | One-shot topic creation, existing topics retained | None |
| migrate | One-shot ordered database migrations | PostgreSQL ledger |
| retention | Hourly in production; local `maintenance` profile | `raw_archive` volume |
| redpanda-console | Optional `tools` profile | None |

Producer, consumer, and dashboard reuse the `transit-app:local` image tag and the
existing `dashboard/Dockerfile`; BuildKit reuses their identical build layers.
Different commands select the service. The image
runs as non-root UID 10001, uses an init process for signal forwarding/reaping,
and never copies `.env`, host data, or the host virtual environment.

All application connections use `postgres:5432` and `redpanda:9092`, regardless
of host-side `POSTGRES_HOST`, `POSTGRES_PORT`, or `KAFKA_BOOTSTRAP_SERVERS` values.
The MBTA URL, cadence, timeout, consumer group/batch size, and dashboard settings
are passed explicitly from the existing environment configuration.

PostgreSQL's host port (default 55432) and Kafka's external listener (19092) remain
available **only on 127.0.0.1** for existing host-side dbt/tests/health commands.
The dashboard is also loopback-only. Redpanda admin port 9644 is not published.
The optional console binds only to 127.0.0.1:8080:

```bash
docker compose --profile tools up -d redpanda-console
```

No service is publicly reachable by this configuration. The later Caddy task
will publish HTTPS and reach `dashboard:8501` internally. Do not change these
database/broker bindings to `0.0.0.0` for EC2.

## Startup, restart, and health

PostgreSQL and Redpanda retain their existing readiness checks. Producer and
consumer wait for migrations, both stateful services, and topic initialization.
The initializer no longer masks every error as success. Dashboard waits for
PostgreSQL and checks Streamlit's HTTP health endpoint.

All long-running containers use `restart: unless-stopped`; the topic initializer
retries a failure up to three times. Docker restarts exited processes, including
the consumer's safe-replay exit after an offset-commit failure. Intentionally
stopped services remain stopped. Docker must itself be running/enabled at boot.

Producer and consumer health probes read existing successful-work timestamps:
producer ingestion metadata for the configured feed URL, and consumer transaction
metrics. `RUNTIME_HEALTH_MAX_AGE_SECONDS` defaults to 300. Missing/stale progress
or a database probe failure makes the container unhealthy. These indicate pipeline
progress, not just process liveness; an idle feed or another manually started
worker can affect the result. Do not run duplicate host workers during validation.

An unhealthy container is **not** automatically restarted by Compose. Dependency
readiness gates apply at startup, not continuously. Existing retries and process
restart policies handle recoverable failures; investigate stale health using logs
and `ingestion.realtime.health` (which still reports freshness and consumer lag).

SIGTERM reaches Python directly. The producer has a six-minute shutdown grace
period because the existing Kafka delivery timeout can take five minutes during
a broker outage. Consumer gets 90 seconds to flush pending work and close; PostgreSQL
and Redpanda get 60 seconds. Docker can force termination after these limits;
uncommitted Kafka records replay idempotently. No event format, transaction ordering,
batching, dead-letter behavior, or offset semantics changed.

Docker JSON logs rotate at 10 MB with three files per container. This bounds
container logs. Redpanda topic retention is bounded by `REDPANDA_TOPIC_RETENTION_MS`
and `REDPANDA_TOPIC_RETENTION_BYTES`; PostgreSQL and raw payload retention are
enforced by `python -m ingestion.retention`.

Run the local ops health script to inspect container health, application health,
database size, CPU, and memory:

```powershell
.\scripts\ops_health_check.ps1
```

## Existing Redpanda volume migration

Older Compose versions let the image create an anonymous volume. Before removing
that old container, capture its data volume name with `docker inspect` and stop all
writers and the broker cleanly. Copy that stopped volume's entire contents and
ownership into the new Compose `redpanda_data` volume using a temporary container:
old volume mounted read-only, new volume mounted read-write. Refuse to copy over a
nonempty destination. Start the new broker only after copying successfully, then
verify topic offsets and consumer group offsets. Retain the original volume until
verification is complete. Do not run two brokers against the same data directory.

New installations do not need this migration. Named volumes survive ordinary
`down`/`up`; this matters because anonymous volumes are not automatically reattached
after `down`. See [Docker volume documentation](https://docs.docker.com/engine/storage/volumes/).

## Static data and raw payloads

The producer saves original protobuf payloads in the persistent `raw_data` volume,
under `/app/data/raw/realtime/vehicle_positions/YYYY-MM-DD/`. Old host files under
`data/raw` are untouched, but are not automatically copied into this volume.
Both sets must be included in any later archive migration. Keep the volume on
persistent EBS when moving to EC2; a Docker volume is not itself a backup.
Retention copies expired raw payloads to the `raw_archive` volume before deleting
them from active raw storage.

Existing PostgreSQL static tables and dbt relations remain intact. For a new empty
database, load static GTFS using the same image (no host Python required):

```bash
docker compose exec producer python -m ingestion.static.download_mbta_gtfs
docker compose exec producer python -m ingestion.static.load_static_gtfs
```

dbt remains a batch/development command; it is not added as an always-on service.
The dashboard reads raw reference tables directly, so it does not require a dbt
build for live map startup. Before static loading, reference queries may show
unavailable warnings.

Static GTFS replacement is transactional. It stages and validates the complete feed,
archives the ZIP, and activates all tables together while preserving live table OIDs.
Dependent dbt views continue to resolve, and any failure leaves old rows committed.

## Production overlay

The production overlay removes development host ports from PostgreSQL, Redpanda,
and Streamlit, then exposes only Caddy on ports 80 and 443:

```bash
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml config --quiet
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

Copy `.env.prod.example` to `.env.prod` and fill `APP_IMAGE`, `POSTGRES_PASSWORD`,
`PUBLIC_HOSTNAME`, and `CADDY_ACME_EMAIL`. `APP_ENVIRONMENT=production` rejects
obvious development database settings at application startup.

## Validation

Run `ruff check .`, `pytest`, `dbt parse --project-dir dbt --profiles-dir dbt`, and
`dbt build --project-dir dbt --profiles-dir dbt` with the existing development
environment. The Compose contract tests use `docker compose config` only, not a
daemon or live MBTA. They skip if the Docker CLI is not installed. Health probe tests
use fakes and never contact external services. Existing CI remains database/dbt
validation, not deployment.

For a live runtime check, verify all five long-running services are healthy, inspect
producer/consumer logs for multiple cycles, check fresh database timestamps and
dashboard queries, then run `docker compose restart producer consumer` and confirm
SIGTERM shutdown logs followed by fresh events. An additional stop/start cycle
should retain broker offsets and PostgreSQL row counts. No fixture loader or
destructive test should run against the real database.

### Existing dbt source-completeness test

`assert_stg_realtime_source_observations_loaded` compares staging with all source
rows, including rows inserted after the staging model's snapshot. It can therefore
fail during continuous ingestion even when the consumer is correct. This deployment
task deliberately does not change that test or the models. For a stable-source
regression check, stop only the consumer, run dbt, and always start the consumer
again afterward. Producer polling and raw-file storage continue; Redpanda buffers
the backlog for replay. The live map becomes stale during this intentional pause.
Keep the pause shorter than the broker retention window.

The eventual scheduled dbt workflow needs an explicit ingestion watermark/snapshot
contract rather than pausing live ingestion. Treat that as follow-up analytical work,
not as a reason to weaken the existing test. Also ensure the host-side dbt command
receives the `.env` database settings (dbt does not load `.env` automatically).

## Verified locally on 2026-08-30

- `docker compose config --quiet` and the default `docker compose build` passed.
- The old stack was stopped and removed without deleting volumes, then started
  using Compose. All five long-running containers became healthy.
- Redpanda's pre-migration partition high watermarks (84037, 84118, 84492) and
  group offsets (83843, 83962, 84356) matched exactly after volume migration.
- All 252,657 original PostgreSQL observations remained; a subsequent check showed
  269,623 total rows as containerized ingestion continued.
- Multiple producer cycles published approximately 440 events per 15-second poll;
  the consumer persisted normal 100-record batches and partial batches.
- Worker restart logs showed SIGTERM handling, consumer flushing, and resumed work.
  Signaling each worker to exit also produced an automatic Docker restart
  (`RestartCount=1`), with both workers returning healthy.
- Container DNS resolved `postgres` and `redpanda`. The dashboard queried live
  vehicles and the existing static catalog as UID 10001. Browser validation showed
  fresh vehicle data and no browser errors. No `.env` file was present in the image.
- Raw protobuf files survived application-container recreation in the named volume.
- Ruff passed; pytest reported 73 passed and 2 skipped. The skips are existing
  tests requiring the isolated CI fixture, not tests run against real history.
- dbt parse passed. The first live-source build failed its existing completeness
  test on 800 newly arrived rows. A rerun with the consumer paused through staging
  and that test passed all 64 steps (11 models and 53 tests); the consumer resumed
  while downstream snapshot-based checks finished. No analytical code was changed.

These historical checks verify the earlier local runtime. Perform the current restore,
retention and soak drills in `production-readiness.md` before public launch.
