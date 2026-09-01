# Backup and restore

Local backups remain available without AWS. Production backups now require a verified,
encrypted S3 upload. No bucket, IAM role, lifecycle rule, or AWS resource is created.
See [S3 backup and archive preparation](s3-backup-archive.md).

## Local backup

Run from the repository using Python 3.11+ and Docker Compose:

```bash
python -m scripts.postgres_backup backup --output backups
```

PowerShell may use `.venv\Scripts\python.exe` instead of `python`. The PowerShell
wrapper delegates to this same binary-safe implementation. Credentials come from
the PostgreSQL container, not command-line arguments or host environment defaults.

The custom-format dump is written to a temporary file, checked using pg_restore,
then published with a SHA-256 sidecar. A failed dump is not published. This validates
format/integrity, not application recovery: perform the restore drill below.
Database roles/global grants are not in pg_dump; preserve production role definitions separately.

## Restore drill (non-destructive)

Use the actual generated filename:

```bash
python -m scripts.postgres_backup restore backups/FILE.dump --database transit_drill_restore
```

The command requires a NEW lower-case database name ending in `_restore`. It refuses
an existing database, verifies the checksum and performs a single-transaction restore.
The working application database is never overwritten. Failed restoration may leave
an empty new database for diagnosis. Delete that database only after checking its name
and verifying the working application's database is different.

Compare counts, min/max timestamps, static tables, migration ledger and uniqueness indexes.
Point an isolated dashboard/test process at the restored database before promoting it.
A row-count print is only a smoke test, not proof of complete recovery.

## Production backup schedule

Prefix the command with `--production` before `backup` or `restore` to select the
production Compose files and `.env.prod`. A production backup is successful only after
the dump, checksum sidecar, and latest-success manifest are verified in S3. The
repository provides a systemd oneshot service and persistent daily timer under
`infrastructure/systemd`. The timer starts at 03:15 UTC with up to 30 minutes of jitter.
If the instance was offline, `Persistent=true` runs the missed backup after startup.
A daily schedule implies up to 24 hours of database loss without replay.
Use a private encrypted S3 bucket with public access blocked, versioning,
least-privilege instance-role access, lifecycle limits and integrity manifests.
Agree RPO/RTO and budget before enabling it.

Back up raw_data, raw_archive, static ZIPs, migration scripts and deployment configuration.
Do not copy a running PostgreSQL data directory as a substitute for a consistent dump.
Same-EBS local copies protect against operator errors, not instance/volume loss.

### Install the EC2 systemd schedule

The units assume the documented deployment location `/opt/transit-data-platform`.
Run these commands as the same unprivileged deployment user that can already run
`docker compose`. The templated unit uses that account and adds the `docker`
supplementary group. It does not store database or AWS credentials in systemd.

```bash
cd /opt/transit-data-platform
DEPLOY_USER="$(id -un)"
DEPLOY_GROUP="$(id -gn)"

test -x .venv/bin/python || python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-ops.lock
sudo install -d -m 0700 -o "$DEPLOY_USER" -g "$DEPLOY_GROUP" \
  /opt/transit-data-platform/backups
sudo install -m 0644 infrastructure/systemd/transit-postgres-backup@.service \
  /etc/systemd/system/transit-postgres-backup@.service
sudo install -m 0644 infrastructure/systemd/transit-postgres-backup@.timer \
  /etc/systemd/system/transit-postgres-backup@.timer
sudo systemd-analyze verify \
  /etc/systemd/system/transit-postgres-backup@.service \
  /etc/systemd/system/transit-postgres-backup@.timer
sudo systemctl daemon-reload
sudo systemctl enable --now "transit-postgres-backup@${DEPLOY_USER}.timer"
sudo systemctl is-enabled "transit-postgres-backup@${DEPLOY_USER}.timer"
sudo systemctl is-active "transit-postgres-backup@${DEPLOY_USER}.timer"
sudo systemctl list-timers "transit-postgres-backup@${DEPLOY_USER}.timer"
```

Run one backup immediately through the same service context, then inspect its result:

```bash
DEPLOY_USER="$(id -un)"
sudo systemctl start "transit-postgres-backup@${DEPLOY_USER}.service"
sudo systemctl show "transit-postgres-backup@${DEPLOY_USER}.service" \
  --property=Result \
  --property=ExecMainStatus \
  --property=ExecMainStartTimestamp \
  --property=ExecMainExitTimestamp
sudo journalctl -u "transit-postgres-backup@${DEPLOY_USER}.service" -n 100 --no-pager
cat /opt/transit-data-platform/backups/.s3-upload-status.json
```

`systemctl start` returns nonzero when the dump, validation, or verified S3 upload
fails. Do not add `Environment=AWS_ACCESS_KEY_ID` or other long-lived credentials;
boto3 uses the EC2 role through its normal provider chain. Configure off-host alerting
for failed units separately because journal-only failures are not notifications.

## Promotion and Kafka offsets

Stop writers before switching the application to a restored database. Keep the old database
until recovery is accepted. Configure POSTGRES_DB to the restored target, run migrations,
then start the application. Never simply retain newer consumer offsets: the restored
database may be missing already-acknowledged records.

Use a NEW recovery consumer group reading the earliest retained broker offsets; unique
event IDs deduplicate rows still present in the database. Review the broker's actual low/high
offsets first. Data older than broker retention requires explicit archive replay/import
(which is not automated here); raw snapshots have collection gaps during producer outages.
Archive JSON preserves source rows and IDs, but restoring such exports needs sequence,
schema-version and duplicate handling. Do not claim a zero-loss restore until this is tested.

Realtime deduplication is bounded by retained database rows. Manually replaying events
older than the 14-day database window may insert them again; archived rows must be merged
by event_id in a future historical recovery pipeline.
