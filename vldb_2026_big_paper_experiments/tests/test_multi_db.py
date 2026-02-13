"""Multi-DB smoke tests (requires Docker for server engines)."""

from __future__ import annotations

import contextlib
import csv
from pathlib import Path
import uuid

import pytest

from vldb_experiments.multi_db import (
    DataFusionClient,
    PostgresClient,
    SQLServerClient,
    UmbraClient,
    sqlserver_env_available,
)

DATA_DIR = Path("results") / "multi_db_test_data"


def _table_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _require_docker() -> None:
    import shutil

    if shutil.which("docker") is None:
        pytest.fail("docker not available")


def test_umbra_smoke() -> None:
    _require_docker()
    client = UmbraClient(DATA_DIR / "umbra")
    client.start()
    client.wait_ready(timeout_s=120)
    client.connect()
    table = _table_name("umbra_test")
    try:
        cursor = client.conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        cursor.execute(f"CREATE TABLE {table} (id INT, name VARCHAR)")
        cursor.execute(f"INSERT INTO {table} VALUES (1, 'alpha'), (2, 'beta')")
        results = client.fetchall(f"SELECT name FROM {table} WHERE id = 2")
        assert results == [("beta",)]
    finally:
        with contextlib.suppress(Exception):
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        client.close()


def test_postgres_smoke() -> None:
    _require_docker()
    client = PostgresClient(DATA_DIR / "postgres")
    client.start()
    client.wait_ready(timeout_s=120)
    client.connect()
    table = _table_name("pg_test")
    try:
        cursor = client.conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        cursor.execute(f"CREATE TABLE {table} (id INT, name VARCHAR)")
        cursor.execute(f"INSERT INTO {table} VALUES (1, 'alpha'), (2, 'beta')")
        results = client.fetchall(f"SELECT name FROM {table} WHERE id = 2")
        assert results == [("beta",)]
    finally:
        with contextlib.suppress(Exception):
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        client.close()



def test_datafusion_smoke() -> None:
    client = DataFusionClient(DATA_DIR / "datafusion")
    client.start()
    client.connect()

    table = _table_name("df_test")
    csv_path = DATA_DIR / "datafusion" / f"{table}.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with csv_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "name"])
        writer.writerow([1, "alpha"])
        writer.writerow([2, "beta"])

    client.ctx.register_csv(table, str(csv_path), has_header=True)
    results = client.fetchall(f"SELECT name FROM {table} WHERE id = 2")
    assert results == [("beta",)]
    client.close()


def test_sqlserver_smoke() -> None:
    if not sqlserver_env_available():
        pytest.skip("SQL Server env vars not set")

    db_name = f"sqlserver_test_{uuid.uuid4().hex[:8]}"
    client = SQLServerClient(
        DATA_DIR / "sqlserver",
        database=db_name,
        drop_database_on_close=True,
    )
    client.start()
    client.wait_ready(timeout_s=120)
    client.connect()
    table = _table_name("sqlserver_test")
    try:
        cursor = client.conn.cursor()
        cursor.execute(f"IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL DROP TABLE dbo.{table}")
        cursor.execute(f"CREATE TABLE dbo.{table} (id INT, name VARCHAR(50))")
        cursor.execute(f"INSERT INTO dbo.{table} (id, name) VALUES (?, ?)", (1, "alpha"))
        cursor.execute(f"INSERT INTO dbo.{table} (id, name) VALUES (?, ?)", (2, "beta"))
        cursor.execute(f"SELECT name FROM dbo.{table} WHERE id = 2")
        results = [tuple(row) for row in cursor.fetchall()]
        assert results == [("beta",)]
    finally:
        with contextlib.suppress(Exception):
            cursor.execute(f"IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL DROP TABLE dbo.{table}")
        client.close()
