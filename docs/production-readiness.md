# Production readiness assessment

Updated 2026-08-31. No AWS resources were created, contacted or deployed.

## Status

- LOCAL: Compose application, streaming, dashboard, tests and dbt exist.
- PREPARED: production override, Caddy configuration, migrations, verified local
  archival, retention worker and backup/restore tools.
- ACTUALLY DEPLOYED: no AWS deployment. Public-launch gates below remain mandatory.

## P0: before public launch

Fixed repository defects:
- Production overrides remove inherited host ports using Compose `!reset`.
  Only Caddy publishes 80/443. Production application validation cannot silently
  fall back to development mode. Secret environment files and backups are ignored.
- Startup runs serialized, ordered SQL migrations before application services.
  Migration checksums detect edits; failed upgrades roll back.
- Static imports stage the complete feed, then replace live rows in one transaction.
  Table identities and dependent views survive. A failed activation rolls back.
- Realtime history and dead letters are archived and verified before bounded deletion.
  Raw payload archival is atomic and collision-safe; insufficient disk fails closed.
- Backup/restore uses binary streams, checksum verification and new-database-only restore.

Remaining launch gates:
- Verify the reported existing private S3 bucket and EC2 role, then configure/confirm
  lifecycle rules, bucket policy, instance-profile attachment, daily backup schedule and
  failure alert. Application support exists, but no AWS resource has been tested here.
- Provide real production credentials, DNS, TLS validation, restricted security groups,
  EBS-backed Docker storage and an operator responsible for recovery. None is provisioned.
- Perform a backup restore and outage/soak drill on the actual target host.
- Size storage for raw data, dbt copies, archive, WAL, vacuum and static reload headroom.
  dbt historical copies and ingestion metadata are not automatically pruned.

## P1: high-value follow-up

- Least-privilege application/database roles; currently Compose uses the PostgreSQL
  bootstrap owner. Separate dashboard reads from ingestion and migration ownership.
- Add hash verification and a container vulnerability review to the version-locked
  runtime. Third-party containers and the application image are required by digest.
- Complete and test the OIDC/ECR/SSM deployment executor. Current workflow is deliberately
  validation/design only, not a functioning deployment pipeline.
- Off-host notifications for stale ingestion, sustained lag, backup failures and disk
  below 20% free. Existing logs/probes expose symptoms but do not notify an operator.
- A run-scoped dbt source boundary and gap-aware incremental capture. For now use the
  documented quiesced-source validation; do not disable the completeness test.
- Archive-aware analytical retention and recovery tooling before large-scale history collection.

## P2: later

TripUpdates collection and defensible ML labels, weather, model training, managed
services, partitioning after measurement, multi-host availability and rolling deployments.
The live vehicle map does not require any ML labels.

## Decisions

Keep one host and the current producer/broker/consumer/database architecture.
Retain realtime rows for 14 days, raw protobufs for 7 and operational metrics for 30.
At the inspection snapshot, 1,073,103 realtime rows occupied 585 MB including indexes.
A roughly 446-vehicle feed at 15 seconds is about 2.57 million observations/day:
roughly 1.4 GB/day of realtime relation storage at that measured density, before
WAL, dbt copies and bloat. This is a capacity estimate, not a measured sustained rate.

Use 1,000-row deletion batches with row locks and SKIP LOCKED. Repartitioning the existing
table would require changing global event-id uniqueness, backfilling history and handling
index dependencies; that risk is unjustified for this readiness patch.
Production runs at most 250 batches per hourly pass (up to 6 million rows/day).
Monitor backlog and increase the cap if capture exceeds pruning throughput.

Broker limits are seven days OR 1 GiB per partition, whichever is reached first.
Three partitions imply approximately 3 GiB of retained segments, NOT a hard disk quota:
segment granularity, compaction/deletion timing and internal topics add overhead.
A high-rate stream can exhaust replay history well before seven days.

## Verdict

Safer and locally verifiable, but NOT yet approved for public deployment. In particular,
repository support alone is not an off-host disaster-recovery plan until the required
AWS resources, schedule, alert, and restore drill exist.
See [operations](docker-runtime.md), [AWS runbook](aws-deployment-runbook.md),
[backup/restore](backup-restore.md) and [ML limitations](tripupdates-ml-design.md).

## Latest local validation

On 2026-08-31: Ruff passed; pytest passed 127 tests; dbt parse and all 64 dbt build
steps passed against the isolated fixture database; both Compose configurations and
Caddy validated; the application image built; migrations completed; all five
long-running services were healthy; producer/consumer freshness was healthy with zero
consumer lag; and broker retention resolved to seven days plus 1 GiB per partition.
S3 encryption, verification, failure handling, and freshness checks passed with fake
clients only; no AWS endpoint or resource was contacted.
The digest-pinned application image built from the runtime lock, passed runtime import
checks, and Docker Scout reported zero critical or high vulnerabilities.

A 279,540,398-byte custom PostgreSQL dump passed format/checksum verification and was
restored into a new database containing 1,073,103 observations. The working database
was not replaced. The current working table later contained 1,218,768 observations,
covered 2026-08-29 through 2026-08-31 and occupied 674 MB including indexes.
The retention dry run found zero rows older than 14 days, so destructive retention was
not exercised against live history; real transactional archive/delete behavior is
covered by the isolated PostgreSQL tests.
