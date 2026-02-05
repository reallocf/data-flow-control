"""SQLite database integration helpers."""

from __future__ import annotations

import csv
import datetime
import re
import sqlite3
from typing import TYPE_CHECKING, Any

from vldb_experiments.multi_db.tpch_data import TPCH_TABLES, export_tpch_csvs, get_table_schema

if TYPE_CHECKING:
    import pathlib

    import duckdb


def _map_duckdb_type(duckdb_type: str) -> str:
    type_upper = duckdb_type.upper()
    if type_upper.startswith(("DECIMAL", "DOUBLE")):
        return "REAL"
    if type_upper.startswith("DATE"):
        return "TEXT"
    if type_upper.startswith(("BIGINT", "HUGEINT", "INTEGER", "INT", "SMALLINT")):
        return "INTEGER"
    return "TEXT"


class SQLiteClient:
    """Helper for loading TPC-H data into a local SQLite database."""

    def __init__(self, data_dir: pathlib.Path) -> None:
        self.data_dir = data_dir
        self.conn: sqlite3.Connection | None = None

    def start(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def wait_ready(self, timeout_s: int = 60) -> None:
        _ = timeout_s

    def connect(self) -> sqlite3.Connection:
        self.conn = sqlite3.connect(self.data_dir / "tpch.sqlite")
        self.conn.execute("PRAGMA journal_mode=OFF")
        self.conn.execute("PRAGMA synchronous=OFF")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute("PRAGMA cache_size=500000")
        self.conn.execute("PRAGMA foreign_keys=OFF")
        return self.conn

    def ensure_tpch_data(self, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
        if self.conn is None:
            raise RuntimeError("SQLite connection not initialized.")

        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='lineitem'"
        )
        if cursor.fetchone():
            cursor.execute("SELECT COUNT(*) FROM lineitem")
            if cursor.fetchone()[0] > 0:
                return

        export_tpch_csvs(duckdb_conn, self.data_dir)

        cursor.execute("BEGIN")
        for table in TPCH_TABLES:
            schema = get_table_schema(duckdb_conn, table)
            col_defs = [f"{name} {_map_duckdb_type(col_type)}" for name, col_type in schema]
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            cursor.execute(f"CREATE TABLE {table} ({', '.join(col_defs)})")

            csv_path = self.data_dir / f"{table}.csv"
            with csv_path.open(newline="") as handle:
                reader = csv.reader(handle)
                headers = next(reader, None)
                if headers is None:
                    continue
                placeholders = ", ".join(["?"] * len(headers))
                insert_sql = f"INSERT INTO {table} VALUES ({placeholders})"
                batch: list[list[str | None]] = []
                for row in reader:
                    batch.append([val if val != "" else None for val in row])
                    if len(batch) >= 50000:
                        cursor.executemany(insert_sql, batch)
                        batch.clear()
                if batch:
                    cursor.executemany(insert_sql, batch)

        index_statements = [
            "CREATE INDEX IF NOT EXISTS idx_lineitem_orderkey ON lineitem(l_orderkey)",
            "CREATE INDEX IF NOT EXISTS idx_lineitem_partkey ON lineitem(l_partkey)",
            "CREATE INDEX IF NOT EXISTS idx_lineitem_suppkey ON lineitem(l_suppkey)",
            "CREATE INDEX IF NOT EXISTS idx_lineitem_shipdate ON lineitem(l_shipdate)",
            "CREATE INDEX IF NOT EXISTS idx_lineitem_commitdate ON lineitem(l_commitdate)",
            "CREATE INDEX IF NOT EXISTS idx_lineitem_receiptdate ON lineitem(l_receiptdate)",
            "CREATE INDEX IF NOT EXISTS idx_orders_orderkey ON orders(o_orderkey)",
            "CREATE INDEX IF NOT EXISTS idx_orders_custkey ON orders(o_custkey)",
            "CREATE INDEX IF NOT EXISTS idx_orders_orderdate ON orders(o_orderdate)",
            "CREATE INDEX IF NOT EXISTS idx_customer_custkey ON customer(c_custkey)",
            "CREATE INDEX IF NOT EXISTS idx_customer_nationkey ON customer(c_nationkey)",
            "CREATE INDEX IF NOT EXISTS idx_supplier_suppkey ON supplier(s_suppkey)",
            "CREATE INDEX IF NOT EXISTS idx_supplier_nationkey ON supplier(s_nationkey)",
            "CREATE INDEX IF NOT EXISTS idx_nation_nationkey ON nation(n_nationkey)",
            "CREATE INDEX IF NOT EXISTS idx_nation_regionkey ON nation(n_regionkey)",
            "CREATE INDEX IF NOT EXISTS idx_region_regionkey ON region(r_regionkey)",
            "CREATE INDEX IF NOT EXISTS idx_partsupp_partkey ON partsupp(ps_partkey)",
            "CREATE INDEX IF NOT EXISTS idx_partsupp_suppkey ON partsupp(ps_suppkey)",
            "CREATE INDEX IF NOT EXISTS idx_part_partkey ON part(p_partkey)",
            "CREATE INDEX IF NOT EXISTS idx_part_type ON part(p_type)",
        ]
        for stmt in index_statements:
            cursor.execute(stmt)
        self.conn.commit()

    def _normalize_query(self, query: str) -> str:
        query = re.sub(
            r"CAST\('([^']+)'\s+AS\s+DATE\)",
            r"'\1'",
            query,
            flags=re.IGNORECASE,
        )
        query = re.sub(
            r"DATE\s+'([^']+)'",
            r"'\1'",
            query,
            flags=re.IGNORECASE,
        )
        return re.sub(
            r"extract\(\s*year\s+from\s+([^)]+)\)",
            r"CAST(strftime('%Y', \1) AS INTEGER)",
            query,
            flags=re.IGNORECASE,
        )

    def _normalize_results(self, rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
        normalized: list[tuple[Any, ...]] = []
        for row in rows:
            converted = []
            for val in row:
                if isinstance(val, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", val):
                    try:
                        converted.append(datetime.date.fromisoformat(val))
                    except ValueError:
                        converted.append(val)
                else:
                    converted.append(val)
            normalized.append(tuple(converted))
        return normalized

    def fetchall(self, query: str) -> list[tuple[Any, ...]]:
        if self.conn is None:
            raise RuntimeError("SQLite connection not initialized.")
        cursor = self.conn.cursor()
        cursor.execute(self._normalize_query(query))
        return self._normalize_results(cursor.fetchall())

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None
