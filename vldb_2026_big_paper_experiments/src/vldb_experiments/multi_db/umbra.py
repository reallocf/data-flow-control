"""Umbra database integration helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pg8000

from vldb_experiments.multi_db.common import normalize_results, run_docker, wait_for_pg_ready
from vldb_experiments.multi_db.tpch_data import TPCH_TABLES, export_tpch_csvs, get_table_schema

if TYPE_CHECKING:
    import pathlib

    import duckdb

UMBRA_CONTAINER_NAME = "umbra-tpch"
UMBRA_IMAGE = "umbradb/umbra:latest"
UMBRA_PORT = 5433
UMBRA_USER = "postgres"
UMBRA_PASSWORD = "postgres"
UMBRA_DB = "postgres"

class UmbraClient:
    """Helper for running Umbra in Docker and loading data from DuckDB."""

    def __init__(self, data_dir: pathlib.Path) -> None:
        self.data_dir = data_dir
        self.conn: pg8000.Connection | None = None

    def start(self) -> None:
        """Ensure Umbra container is running."""
        self.data_dir.mkdir(parents=True, exist_ok=True)

        existing = run_docker(
            ["ps", "-a", "--filter", f"name={UMBRA_CONTAINER_NAME}", "--format", "{{.ID}}"]
        )
        running = run_docker(
            ["ps", "--filter", f"name={UMBRA_CONTAINER_NAME}", "--format", "{{.ID}}"]
        )

        if existing:
            mounts_config = run_docker(
                ["inspect", "--format", "{{json .Mounts}}", UMBRA_CONTAINER_NAME]
            )
            has_expected_mount = str(self.data_dir.resolve()) in mounts_config
            if not has_expected_mount:
                run_docker(["rm", "-f", UMBRA_CONTAINER_NAME])
                existing = ""
                running = ""

        if not existing:
            run_docker(
                [
                    "run",
                    "-d",
                    "--name",
                    UMBRA_CONTAINER_NAME,
                    "-p",
                    f"{UMBRA_PORT}:5432",
                    "-v",
                    f"{self.data_dir.resolve()}:/data",
                    "--ulimit",
                    "nofile=1048576:1048576",
                    "--ulimit",
                    "memlock=-1:-1",
                    UMBRA_IMAGE,
                ]
            )
            return

        if not running:
            run_docker(["start", UMBRA_CONTAINER_NAME])

    def wait_ready(self, timeout_s: int = 60) -> None:
        """Wait for Umbra to accept connections."""
        wait_for_pg_ready(
            host="127.0.0.1",
            port=UMBRA_PORT,
            user=UMBRA_USER,
            password=UMBRA_PASSWORD,
            database=UMBRA_DB,
            timeout_s=timeout_s,
        )

    def connect(self) -> pg8000.Connection:
        """Open a persistent Umbra connection."""
        self.conn = pg8000.connect(
            host="127.0.0.1",
            port=UMBRA_PORT,
            user=UMBRA_USER,
            password=UMBRA_PASSWORD,
            database=UMBRA_DB,
            timeout=10,
        )
        self.conn.autocommit = True
        return self.conn

    def ensure_tpch_data(self, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
        """Load TPC-H tables into Umbra from DuckDB CSV exports."""
        if self.conn is None:
            raise RuntimeError("Umbra connection not initialized.")

        cursor = self.conn.cursor()
        try:
            cursor.execute("SELECT 1 FROM lineitem LIMIT 1")
            return
        except Exception:
            pass

        export_tpch_csvs(duckdb_conn, self.data_dir)

        for table in TPCH_TABLES:
            schema = get_table_schema(duckdb_conn, table)
            col_defs = [f"{name} {col_type}" for name, col_type in schema]
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            cursor.execute(f"CREATE TABLE {table} ({', '.join(col_defs)})")
            cursor.execute(
                f"COPY {table} FROM '/data/{table}.csv' WITH (FORMAT csv, HEADER true)"
            )

    def fetchall(self, query: str) -> list[tuple[Any, ...]]:
        """Execute a query and return normalized results."""
        if self.conn is None:
            raise RuntimeError("Umbra connection not initialized.")
        cursor = self.conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        return normalize_results(results)

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None
