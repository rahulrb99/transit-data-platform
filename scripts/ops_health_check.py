"""Read-only operational snapshot; no Docker socket exposed to the application."""
import argparse
import json
import subprocess

from ingestion.archive_health import load_status
from ingestion.config import Settings

BASE_CONTINUOUS_SERVICES = {"postgres", "redpanda", "producer", "consumer", "dashboard"}
PRODUCTION_CONTINUOUS_SERVICES = BASE_CONTINUOUS_SERVICES | {"retention", "caddy"}
COMPLETED_SERVICES = {"migrate", "redpanda-init"}


def parse_compose_rows(output: str) -> list[dict[str, object]]:
    stripped = output.strip()
    if not stripped:
        return []
    if stripped.startswith("["):
        return json.loads(stripped)
    return [json.loads(line) for line in stripped.splitlines() if line.strip()]


def check_compose_services(compose: list[str], *, production: bool) -> None:
    result = subprocess.run(
        [*compose, "ps", "-a", "--format", "json"],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    print(result.stdout, end="")
    rows = {str(row.get("Service")): row for row in parse_compose_rows(result.stdout)}
    problems = []
    expected = PRODUCTION_CONTINUOUS_SERVICES if production else BASE_CONTINUOUS_SERVICES
    for service in sorted(expected):
        row = rows.get(service)
        if row is None:
            problems.append(f"{service}: missing")
            continue
        state = str(row.get("State", "")).lower()
        health = str(row.get("Health", "")).lower()
        if state != "running":
            problems.append(f"{service}: state={state or 'unknown'}")
        elif health and health != "healthy":
            problems.append(f"{service}: health={health}")
    for service in sorted(COMPLETED_SERVICES):
        row = rows.get(service)
        if row is None:
            problems.append(f"{service}: missing")
            continue
        if str(row.get("State", "")).lower() != "exited" or int(row.get("ExitCode", -1)) != 0:
            problems.append(
                f"{service}: state={row.get('State', 'unknown')} "
                f"exit={row.get('ExitCode', 'unknown')}"
            )
    if problems:
        raise RuntimeError("Compose service check failed: " + "; ".join(problems))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--production", action="store_true")
    args = parser.parse_args()
    compose = ["docker", "compose"]
    if args.production:
        compose += ["--env-file", ".env.prod", "-f", "docker-compose.yml",
                    "-f", "docker-compose.prod.yml"]
    commands = [
        [*compose, "exec", "-T", "consumer", "python", "-m", "ingestion.realtime.health"],
        [*compose, "exec", "-T", "postgres", "df", "-h", "/var/lib/postgresql/data"],
        [*compose, "exec", "-T", "producer", "df", "-h", "/app/data/raw", "/app/data/archive/raw"],
        ["docker", "stats", "--no-stream"],
        [*compose, "logs", "--tail", "20", "producer", "consumer"],
    ]
    if args.production:
        commands.append(
            [*compose, "run", "--rm", "--no-deps", "retention",
             "python", "-m", "ingestion.archive_health"]
        )
    failed = False
    try:
        check_compose_services(compose, production=args.production)
    except (RuntimeError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        failed = True
        print(error)
    for command in commands:
        try:
            subprocess.run(command, check=True, timeout=60)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            failed = True
            print("Operational probe failed:", " ".join(command))
    if args.production:
        settings = Settings(_env_file=".env.prod")
        backup_status = load_status(settings.backup_status_path)
        if backup_status is None:
            failed = True
            print("PostgreSQL backup upload: no local attempt status found")
        elif backup_status.get("success") is not True:
            failed = True
            print("PostgreSQL backup upload FAILED:", backup_status.get("error"))
        else:
            print("PostgreSQL backup upload succeeded:", backup_status.get("checked_at"))
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
