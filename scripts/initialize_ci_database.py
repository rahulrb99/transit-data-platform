from pathlib import Path

from ingestion.config import get_settings
from ingestion.db import connect
from scripts.migrate_database import run_migrations

FIXTURE_SQL = Path("infrastructure/postgres/ci-fixtures.sql")


def main() -> None:
    database_name = get_settings().postgres_db
    if not database_name.endswith("_ci"):
        raise RuntimeError(
            f"Refusing to load CI fixtures into database {database_name!r}; "
            "use a dedicated database whose name ends with '_ci'."
        )

    completed = run_migrations()
    print(f"Applied migrations: {', '.join(completed) if completed else 'none'}")
    with connect() as connection:
        connection.execute(FIXTURE_SQL.read_text(encoding="utf-8"))
        print(f"Applied {FIXTURE_SQL}")


if __name__ == "__main__":
    main()
