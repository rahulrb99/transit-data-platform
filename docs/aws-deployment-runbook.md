# Future AWS deployment runbook

These commands are for a future operator. Do not run them until AWS networking,
DNS, IAM, EBS, Docker, and secrets are prepared.

The operator reports that the EC2 instance, private S3 bucket, and EC2 role exist.
They have not been accessed or validated by this repository work.

## Local repository validation

```bash
ruff check .
pytest
dbt parse --project-dir dbt --profiles-dir dbt
dbt build --project-dir dbt --profiles-dir dbt
docker compose --env-file .env.example config --quiet
docker compose --env-file .env.prod.example -f docker-compose.yml -f docker-compose.prod.yml config --quiet
docker compose build
```

## Manual AWS and host checklist

Complete and record every item before the first Compose start:

1. **S3:** confirm the existing bucket is private, in `AWS_REGION`, has Block Public
   Access, default SSE-S3, versioning, lifecycle rules, and a TLS/encryption-enforcing
   bucket policy. Apply only the prefix-scoped permissions in
   [s3-backup-archive.md](s3-backup-archive.md); do not grant `s3:*`, bucket listing,
   deletion, or bucket administration to the application role.
2. **EC2 role:** confirm the existing role has an instance profile, attach it to this
   EC2 instance, require IMDSv2, and set the metadata response hop limit to 2. Verify
   the role from both the host and an application container. Never put AWS access keys,
   secret keys, or session tokens in `.env.prod`.
3. **Storage:** attach and mount the persistent EBS volume before Docker writes state.
   Put Docker's data root, including PostgreSQL, Redpanda, raw-data, and Caddy named
   volumes, on that mount. Verify with `docker info` and `findmnt`; configure `/etc/fstab`
   by filesystem UUID and test a reboot. Enable EBS encryption and low-space alarms.
4. **Security group:** allow inbound TCP 80 and 443 for the public dashboard. Do not open
   5432, 8501, 8080, 9092, 9644, or 19092. Prefer SSM Session Manager with no inbound SSH;
   if SSH is unavoidable, restrict TCP 22 to a named administrator IP range.
5. **Secrets:** create `/opt/transit-data-platform/.env.prod` with mode `0600`; set a long,
   unique PostgreSQL password, actual bucket/region, real hostname/operator email, and an
   ECR image URI pinned by `@sha256:`. Do not use example values unchanged.
6. **DNS/HTTPS:** point the public hostname at the instance's stable public address. Confirm
   TCP 80/443 reach Caddy, certificate issuance succeeds, HTTP redirects to HTTPS, and the
   Streamlit WebSocket remains connected through Caddy.
7. **Recovery:** run a real PostgreSQL backup to S3, confirm encryption/checksum/manifest,
   download that exact dump and sidecar, and restore into a new `*_restore` database.
   Compare row counts, timestamp coverage, static tables, indexes, and migration history.
8. **Soak:** run the complete stack, retention, backup, and operational checks for at least
   24 hours. Review restarts, lag, stale data, dead letters, S3 failures, disk growth,
   memory, and the successful daily backup before launch.
9. **Backup timer:** install and enable `transit-postgres-backup@.timer` using the exact
   commands in [backup-restore.md](backup-restore.md). Run the oneshot service once and
   verify the dump, checksum sidecar, encrypted S3 objects, and latest-success manifest.

## First host start

```bash
cp .env.prod.example .env.prod
chmod 600 .env.prod
# Fill APP_IMAGE, POSTGRES_PASSWORD, S3_BUCKET_NAME, AWS_REGION, prefixes,
# PUBLIC_HOSTNAME, CADDY_ACME_EMAIL, and any MBTA overrides. APP_IMAGE must use
# the repository@sha256:<digest> form, not latest or a mutable tag.
python -m scripts.validate_production_environment --env-file .env.prod
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml config --quiet
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml pull
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml up -d
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml ps
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml exec producer python -m ingestion.static.download_mbta_gtfs
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml exec producer python -m ingestion.static.load_static_gtfs
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml exec producer python -m ingestion.realtime.health
```

## Recurring operations

```bash
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml run --rm retention --apply
python -m scripts.postgres_backup --production backup --output backups
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml logs --tail 100 producer consumer dashboard
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml exec postgres pg_isready -U transit -d transit
```

The command above is the manual fallback. Normal daily execution uses the systemd
timer described in [backup-restore.md](backup-restore.md); it is intentionally not a
long-running Compose service.

## GitHub OIDC

`.github/workflows/deploy-production.yml` only validates inputs and prints a plan.
It intentionally does not assume a role or mutate AWS resources. Before enabling
deployment, configure a protected GitHub `production` environment and repository
variables `AWS_REGION`, `AWS_ROLE_TO_ASSUME`, `ECR_REPOSITORY_URI`, and
`EC2_INSTANCE_ID`. Implement the reviewed executor as these atomic stages:

```text
CI success for commit SHA
-> GitHub OIDC role
-> ECR image resolved to an immutable digest
-> SSM updates APP_IMAGE in protected .env.prod
-> Compose pull and up
-> migration/health/HTTP checks
-> restore the prior APP_IMAGE on failure
```

The host must already have Docker/Compose, the checked-out release configuration,
EBS-backed storage, `.env.prod` mode 0600, an SSM instance role, DNS and security
groups. Resolve the pushed ECR image to its digest and use the
`repository@sha256:<digest>` form, never `latest` or a mutable tag.

Exact host-side release commands once `APP_IMAGE` contains the pushed ECR URI:

```bash
cd /opt/transit-data-platform
python -m scripts.validate_production_environment --env-file .env.prod
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml config --quiet
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml pull
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml up -d
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml ps -a
python -m scripts.ops_health_check --production
```
