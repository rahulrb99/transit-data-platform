from __future__ import annotations

import argparse
import gzip
import hashlib
import logging
import signal
import tempfile
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from psycopg import Error as DatabaseError
from psycopg import sql

from ingestion.config import get_settings
from ingestion.db import connect
from ingestion.raw_archive import ArchiveBackend, build_archive_backend

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetentionResult:
    realtime_rows_deleted: int
    metric_rows_deleted: int
    raw_files_archived: int
    raw_files_deleted: int


def enforce_postgres_retention(
    *,
    realtime_days: int | None = None,
    metrics_days: int | None = None,
    now: datetime | None = None,
    archive: ArchiveBackend | None = None,
) -> tuple[int, int]:
    settings = get_settings()
    resolved_now = now or datetime.now(UTC)
    realtime_cutoff = resolved_now - timedelta(
        days=realtime_days if realtime_days is not None else settings.realtime_retention_days
    )
    metrics_cutoff = resolved_now - timedelta(
        days=metrics_days if metrics_days is not None else settings.metrics_retention_days
    )

    resolved_archive = archive or build_archive_backend()
    realtime_deleted = metrics_deleted = 0
    for _ in range(settings.retention_max_batches):
        with connect() as connection:
            connection.execute("SET LOCAL lock_timeout = '2s'")
            connection.execute("SET LOCAL statement_timeout = '60s'")
            realtime_batch = archive_delete_batch(
                connection, resolved_archive, "realtime_vehicle_positions", "id", "ingested_at",
                realtime_cutoff, settings.retention_batch_size,
            )
            # Dead letters contain diagnostic payloads, so retain them through the same archive.
            dead_letter_batch = archive_delete_batch(
                connection, resolved_archive, "realtime_vehicle_position_dead_letters",
                "dead_letter_id", "processed_at", metrics_cutoff, settings.retention_batch_size,
            )
            metrics_batch = connection.execute("""
                DELETE FROM realtime_pipeline_metrics WHERE metric_id IN (
                    SELECT metric_id FROM realtime_pipeline_metrics
                    WHERE metric_timestamp < %s ORDER BY metric_timestamp
                    LIMIT %s FOR UPDATE SKIP LOCKED
                )
            """, (metrics_cutoff, settings.retention_batch_size)).rowcount
        realtime_deleted += realtime_batch
        metrics_deleted += metrics_batch
        if not realtime_batch and not dead_letter_batch and not metrics_batch:
            break
    return realtime_deleted, metrics_deleted


def archive_delete_batch(connection, archive, table, identity, timestamp, cutoff, batch_size) -> int:
    rows = connection.execute(sql.SQL("""
        SELECT {identity}, to_jsonb(t)::text FROM {table} t
        WHERE {timestamp} < %s ORDER BY {timestamp}, {identity}
        LIMIT %s FOR UPDATE SKIP LOCKED
    """).format(
        identity=sql.Identifier(identity), table=sql.Identifier(table),
        timestamp=sql.Identifier(timestamp),
    ), (cutoff, batch_size)).fetchall()
    if not rows:
        return 0
    content = ("\n".join(row[1] for row in rows) + "\n").encode()
    checksum = hashlib.sha256(content).hexdigest()
    # Archive is durable before DELETE. A crash may leave an extra archive, never lost history.
    with tempfile.TemporaryDirectory() as directory:
        payload = Path(directory) / "batch.jsonl.gz"
        payload.write_bytes(gzip.compress(content, mtime=0))
        archive.archive_file(payload, Path("postgres") / table / f"{checksum}.jsonl.gz")
    deleted = connection.execute(sql.SQL("DELETE FROM {} WHERE {} = ANY(%s)").format(
        sql.Identifier(table), sql.Identifier(identity)
    ), ([row[0] for row in rows],)).rowcount
    if deleted != len(rows):
        raise RuntimeError("Retention row count mismatch; transaction must roll back")
    return deleted


def enforce_raw_payload_retention(
    *,
    raw_root: Path | None = None,
    archive: ArchiveBackend | None = None,
    raw_days: int | None = None,
    now: datetime | None = None,
) -> tuple[int, int]:
    settings = get_settings()
    resolved_raw_root = raw_root or settings.raw_data_root / "realtime" / "vehicle_positions"
    resolved_archive = archive or build_archive_backend()
    cutoff = (now or datetime.now(UTC)) - timedelta(
        days=raw_days if raw_days is not None else settings.raw_payload_retention_days
    )
    archived = 0
    deleted = 0

    if not resolved_raw_root.exists():
        return archived, deleted

    for path in sorted(resolved_raw_root.rglob("*.pb")):
        modified_at = datetime.fromtimestamp(path.stat().st_mtime, UTC)
        if modified_at >= cutoff:
            continue
        relative_path = path.relative_to(resolved_raw_root.parent)
        resolved_archive.archive_file(path, relative_path)
        path.unlink()
        archived += 1
        deleted += 1
    return archived, deleted


def enforce_retention() -> RetentionResult:
    realtime_deleted, metric_deleted = enforce_postgres_retention()
    archived, raw_deleted = enforce_raw_payload_retention()
    return RetentionResult(
        realtime_rows_deleted=realtime_deleted,
        metric_rows_deleted=metric_deleted,
        raw_files_archived=archived,
        raw_files_deleted=raw_deleted,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Archive old raw payloads and prune realtime history.")
    parser.add_argument("--postgres-only", action="store_true")
    parser.add_argument("--raw-only", action="store_true")
    parser.add_argument("--apply", action="store_true", help="Archive and delete; default is read-only")
    parser.add_argument("--interval-seconds", type=int, help="Repeat maintenance until SIGTERM/SIGINT")
    args = parser.parse_args()
    if args.postgres_only and args.raw_only:
        raise SystemExit("--postgres-only and --raw-only cannot be combined")
    if args.interval_seconds is not None:
        if not args.apply or args.interval_seconds < 60 or args.postgres_only or args.raw_only:
            raise SystemExit("Repeated maintenance requires --apply and an interval >= 60 seconds")
        logging.basicConfig(level=logging.INFO)
        shutdown = threading.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda signum, frame: shutdown.set())
        while not shutdown.is_set():
            try:
                LOGGER.info("Retention complete: %s", enforce_retention())
            except (OSError, ValueError, DatabaseError):
                LOGGER.exception("Retention failed; unarchived data retained. Operator action required.")
            shutdown.wait(args.interval_seconds)
        return
    if not args.apply:
        settings = get_settings()
        with connect() as connection:
            count = connection.execute(
                "SELECT count(*) FROM realtime_vehicle_positions WHERE ingested_at < %s",
                (datetime.now(UTC) - timedelta(days=settings.realtime_retention_days),),
            ).fetchone()[0]
        print(f"Dry run: {count} realtime rows eligible; use --apply to archive and prune")
        return

    if args.postgres_only:
        realtime_deleted, metric_deleted = enforce_postgres_retention()
        print(f"Deleted realtime rows={realtime_deleted} metric rows={metric_deleted}")
        return
    if args.raw_only:
        archived, deleted = enforce_raw_payload_retention()
        print(f"Archived raw files={archived} deleted raw files={deleted}")
        return

    result = enforce_retention()
    print(
        "Retention complete: "
        f"realtime_rows_deleted={result.realtime_rows_deleted} "
        f"metric_rows_deleted={result.metric_rows_deleted} "
        f"raw_files_archived={result.raw_files_archived} "
        f"raw_files_deleted={result.raw_files_deleted}"
    )


if __name__ == "__main__":
    main()
