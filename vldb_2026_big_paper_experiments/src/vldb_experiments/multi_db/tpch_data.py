"""Shared TPC-H data export helpers for multi-db experiments."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pathlib

    import duckdb


TPCH_TABLES = [
    "lineitem",
    "orders",
    "customer",
    "part",
    "supplier",
    "partsupp",
    "nation",
    "region",
]


def map_duckdb_type(duckdb_type: str) -> str:
    type_upper = duckdb_type.upper()
    if type_upper.startswith("DECIMAL"):
        return type_upper
    if type_upper.startswith("DOUBLE"):
        return "DOUBLE PRECISION"
    if type_upper.startswith("VARCHAR"):
        return type_upper
    if type_upper.startswith("CHAR"):
        return type_upper
    if type_upper.startswith("HUGEINT"):
        return "BIGINT"
    return type_upper


def export_tpch_csvs(
    duckdb_conn: duckdb.DuckDBPyConnection,
    data_dir: pathlib.Path,
) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    for table in TPCH_TABLES:
        csv_path = data_dir / f"{table}.csv"
        if csv_path.exists():
            continue
        duckdb_conn.execute(
            f"COPY {table} TO '{csv_path.as_posix()}' "
            "(HEADER, DELIMITER ',')"
        )


def get_table_schema(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table: str,
) -> list[tuple[str, str]]:
    columns = duckdb_conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    schema: list[tuple[str, str]] = []
    for _cid, name, col_type, _notnull, _default, _pk in columns:
        schema.append((name, map_duckdb_type(col_type)))
    return schema
