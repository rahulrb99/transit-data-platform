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

## Production backup policy (not provisioned)

Prefix the command with `--production` before `backup` or `restore` to select the
production Compose files and `.env.prod`. A production backup is successful only after
the dump, checksum sidecar, and latest-success manifest are verified in S3. Schedule
daily verified dumps and regular restore
drills. A daily schedule implies up to 24 hours of database loss without replay.
Use a private encrypted S3 bucket with public access blocked, versioning,
least-privilege instance-role access, lifecycle limits and integrity manifests.
Agree RPO/RTO and budget before enabling it.

Back up raw_data, raw_archive, static ZIPs, migration scripts and deployment configuration.
Do not copy a running PostgreSQL data directory as a substitute for a consistent dump.
Same-EBS local copies protect against operator errors, not instance/volume loss.

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
