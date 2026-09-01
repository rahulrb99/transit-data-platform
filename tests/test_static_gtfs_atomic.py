from __future__ import annotations

from typing import Self

from ingestion.static import load_static_gtfs


class FakeCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.rowcount = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: object, params=None) -> None:
        self.statements.append(str(query))

    def fetchall(self):
        return [("route_id",), ("route_type",)]

    def copy(self, query: object) -> FakeCopy:
        self.statements.append(str(query))
        return FakeCopy()

    def fetchone(self) -> tuple[int]:
        return (1,)


class FakeCopy:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def write(self, data: bytes) -> None:
        if b"fail" in data:
            raise RuntimeError("copy failed")


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()

    def cursor(self) -> FakeCursor:
        return self.cursor_instance


def test_static_gtfs_load_stages_tables_before_live_swap() -> None:
    connection = FakeConnection()

    row_counts = load_static_gtfs._load_static_gtfs_atomically(
        connection,
        {"routes": b"route_id,route_type\nR,3\n"},
    )

    statements = connection.cursor_instance.statements
    first_live_drop = next(
        index
        for index, statement in enumerate(statements)
        if "Identifier('routes')" in statement and "DELETE FROM" in statement
    )
    first_stage_create = next(
        index
        for index, statement in enumerate(statements)
        if "Identifier('__gtfs_load_routes')" in statement and "CREATE TABLE" in statement
    )
    assert row_counts == {"routes": 1}
    assert first_stage_create < first_live_drop


def test_static_gtfs_load_failure_happens_before_live_drop() -> None:
    connection = FakeConnection()

    try:
        load_static_gtfs._load_static_gtfs_atomically(
            connection,
            {"routes": b"route_id,route_type\nfail,3\n"},
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("copy failure should propagate")

    assert not any(
        "Identifier('routes')" in statement and "DELETE FROM" in statement
        for statement in connection.cursor_instance.statements
    )
