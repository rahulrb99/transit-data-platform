from pathlib import Path

from ingestion.config import get_settings
from ingestion.db import connect

SQL_FILES = (
    Path("infrastructure/postgres/init.sql"),
    Path("infrastructure/postgres/ci-fixtures.sql"),
)


def main() -> None:
    database_name = get_settings().postgres_db
    if not database_name.endswith("_ci"):
        raise RuntimeError(
            f"Refusing to load CI fixtures into database {database_name!r}; "
            "use a dedicated database whose name ends with '_ci'."
        )

    with connect() as connection:
        for sql_file in SQL_FILES:
            connection.execute(sql_file.read_text(encoding="utf-8"))
            print(f"Applied {sql_file}")


if __name__ == "__main__":
    main()
