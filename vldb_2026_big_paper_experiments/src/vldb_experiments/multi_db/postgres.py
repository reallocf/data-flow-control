"""Postgres database integration helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pg8000

from vldb_experiments.multi_db.common import normalize_results, run_docker, wait_for_pg_ready
from vldb_experiments.multi_db.tpch_data import TPCH_TABLES, export_tpch_csvs, get_table_schema

if TYPE_CHECKING:
    import pathlib

    import duckdb


POSTGRES_CONTAINER_NAME = "postgres-tpch"
POSTGRES_IMAGE = "postgres:16"
POSTGRES_PORT = 5434
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_DB = "tpch"
POSTGRES_SHM_SIZE_BYTES = 2 * 1024 * 1024 * 1024
POSTGRES_ARGS = [
    "postgres",
    "-c",
    "shared_buffers=1GB",
    "-c",
    "work_mem=64MB",
    "-c",
    "maintenance_work_mem=256MB",
    "-c",
    "max_parallel_workers=8",
    "-c",
    "max_parallel_workers_per_gather=4",
]


class PostgresClient:
    """Helper for running Postgres in Docker and loading TPC-H data."""

    def __init__(self, data_dir: pathlib.Path) -> None:
        self.data_dir = data_dir
        self.conn: pg8000.Connection | None = None

    def start(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        data_mount = self.data_dir.resolve()

        existing = run_docker(
            ["ps", "-a", "--filter", f"name={POSTGRES_CONTAINER_NAME}", "--format", "{{.ID}}"]
        )
        running = run_docker(
            ["ps", "--filter", f"name={POSTGRES_CONTAINER_NAME}", "--format", "{{.ID}}"]
        )

        if existing:
            mounts_config = run_docker(
                ["inspect", "--format", "{{json .Mounts}}", POSTGRES_CONTAINER_NAME]
            )
            shm_config = run_docker(
                ["inspect", "--format", "{{.HostConfig.ShmSize}}", POSTGRES_CONTAINER_NAME]
            )
            has_expected_mount = str(data_mount) in mounts_config
            shm_size_ok = shm_config.strip() == str(POSTGRES_SHM_SIZE_BYTES)
            if not has_expected_mount or not shm_size_ok:
                run_docker(["rm", "-f", POSTGRES_CONTAINER_NAME])
                existing = ""
                running = ""

        if existing and running and existing != "":
            try:
                conn = pg8000.connect(
                    host="127.0.0.1",
                    port=POSTGRES_PORT,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    database=POSTGRES_DB,
                    timeout=2,
                )
                conn.close()
                return
            except Exception:
                pass

        if not existing:
            run_docker(
                [
                    "run",
                    "-d",
                    "--name",
                    POSTGRES_CONTAINER_NAME,
                    "-p",
                    f"{POSTGRES_PORT}:5432",
                    "-v",
                    f"{data_mount}:/data",
                    "--shm-size",
                    f"{POSTGRES_SHM_SIZE_BYTES}",
                    "-e",
                    f"POSTGRES_PASSWORD={POSTGRES_PASSWORD}",
                    "-e",
                    f"POSTGRES_DB={POSTGRES_DB}",
                    POSTGRES_IMAGE,
                    *POSTGRES_ARGS,
                ]
            )
            return

        if not running:
            run_docker(["start", POSTGRES_CONTAINER_NAME])

    def wait_ready(self, timeout_s: int = 60) -> None:
        wait_for_pg_ready(
            host="127.0.0.1",
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB,
            timeout_s=timeout_s,
        )

    def connect(self) -> pg8000.Connection:
        self.conn = pg8000.connect(
            host="127.0.0.1",
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB,
            timeout=300,
        )
        self.conn.autocommit = True
        try:
            cursor = self.conn.cursor()
            cursor.execute("SET statement_timeout = 0")
        except Exception:
            pass
        return self.conn

    def ensure_tpch_data(self, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
        if self.conn is None:
            raise RuntimeError("Postgres connection not initialized.")

        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = 'lineitem'"
        )
        if cursor.fetchone():
            return

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
        if self.conn is None:
            raise RuntimeError("Postgres connection not initialized.")
        cursor = self.conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        return normalize_results(results)

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None
