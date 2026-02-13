#!/usr/bin/env python3
"""Reproduce the SQL Server TPC-H SF=1 load + experiment + figures."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys

import contextlib

import duckdb
import pyodbc

from vldb_experiments.multi_db.sqlserver import SQLServerClient

TPCH_SF1_ROW_COUNTS = {
    "lineitem": 6_001_215,
    "orders": 1_500_000,
    "customer": 150_000,
    "part": 200_000,
    "supplier": 10_000,
    "partsupp": 800_000,
    "nation": 25,
    "region": 5,
}


def _sqlserver_conn(password: str) -> pyodbc.Connection:
    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=data-flow-control-sql-server.cu3qs4uisn3k.us-east-1.rds.amazonaws.com,1433;"
        "DATABASE=tpch;"
        "UID=tpch;"
        f"PWD={password};"
        "Encrypt=yes;TrustServerCertificate=yes;",
    )


def _reset_schema(schema: str, password: str) -> None:
    conn = _sqlserver_conn(password)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sys.schemas WHERE name = ?", (schema,))
    if cur.fetchone():
        cur.execute(
            "SELECT t.name FROM sys.tables t "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "WHERE s.name = ?",
            (schema,),
        )
        for (table,) in cur.fetchall():
            cur.execute(f"DROP TABLE [{schema}].[{table}]")
    cur.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = ?) "
        "EXEC('CREATE SCHEMA [' + ? + ']')",
        (schema, schema),
    )
    conn.close()


def _load_tpch(schema: str, source_db: Path, password: str) -> None:
    _reset_schema(schema, password)

    duck = duckdb.connect(str(source_db))
    with contextlib.suppress(Exception):
        duck.execute("LOAD tpch")

    client = SQLServerClient(Path("results") / "sqlserver", schema=schema)
    client.start()
    client.wait_ready(timeout_s=120)
    client.connect()
    client.ensure_tpch_data(duck)
    client.close()
    duck.close()


def _verify_counts(schema: str, password: str) -> None:
    conn = _sqlserver_conn(password)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        "SELECT t.name FROM sys.tables t "
        "JOIN sys.schemas s ON t.schema_id = s.schema_id "
        "WHERE s.name = ?",
        (schema,),
    )
    tables = [row[0] for row in cur.fetchall()]
    print(f"Tables in schema {schema}: {tables}")
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        count = cur.fetchone()[0]
        expected = TPCH_SF1_ROW_COUNTS.get(table)
        if expected is not None:
            status = "OK" if count == expected else "MISMATCH"
            print(f"  {table}: {count} (expected {expected}) [{status}]")
        else:
            print(f"  {table}: {count}")
    conn.close()


def _run(cmd: list[str]) -> None:
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load SQL Server TPC-H SF=1 data and run the SQL Server multi-db experiment.",
    )
    parser.add_argument("--sf", type=float, default=1.0, help="TPC-H scale factor (default: 1.0).")
    parser.add_argument("--schema", default="tpch_sf1", help="SQL Server schema name.")
    parser.add_argument(
        "--source-db",
        default="results/tpch_sf1.db",
        help="DuckDB database path with TPC-H data.",
    )
    parser.add_argument(
        "--suffix",
        default="_sqlserver_sf1_full3",
        help="Suffix for experiment CSV output.",
    )
    parser.add_argument("--output-dir", default="./results", help="Output directory for CSVs/figures.")
    parser.add_argument("--skip-load", action="store_true", help="Skip loading TPC-H data.")
    parser.add_argument("--skip-run", action="store_true", help="Skip the experiment run.")
    parser.add_argument("--skip-figures", action="store_true", help="Skip figure generation.")
    parser.add_argument("--skip-verify", action="store_true", help="Skip verifying row counts.")
    args = parser.parse_args()

    password = os.getenv("SQLSERVER_PASSWORD")
    if not password:
        raise SystemExit("SQLSERVER_PASSWORD must be set.")

    source_db = Path(args.source_db)
    if not source_db.exists():
        raise SystemExit(f"Missing DuckDB database at {source_db}")

    if not args.skip_load:
        _load_tpch(args.schema, source_db, password)

    if not args.skip_verify:
        _verify_counts(args.schema, password)

    if not args.skip_run:
        _run(
            [
                sys.executable,
                str(Path(__file__).parent / "run_tpch_multi_db.py"),
                "--sf",
                str(args.sf),
                "--suffix",
                args.suffix,
                "--engine",
                "sqlserver",
            ]
        )

    if not args.skip_figures:
        csv_name = f"tpch_multi_db_sf{args.sf}{args.suffix}.csv"
        csv_path = Path(args.output_dir) / csv_name
        _run(
            [
                sys.executable,
                str(Path(__file__).parent / "generate_multi_db_engine_figures.py"),
                str(csv_path),
                "--engine",
                "sqlserver",
                "--suffix",
                args.suffix.lstrip("_"),
                "--output-dir",
                args.output_dir,
                "--exclude-duckdb",
            ]
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
