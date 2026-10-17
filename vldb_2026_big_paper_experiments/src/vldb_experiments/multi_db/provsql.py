"""ProvSQL Docker integration for the TPC-H DFC baseline."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import pg8000

from vldb_experiments.multi_db.common import run_docker, wait_for_pg_ready
from vldb_experiments.multi_db.tpch_data import TPCH_TABLES, export_tpch_csvs, get_table_schema

if TYPE_CHECKING:
    import pathlib

    import duckdb


PROVSQL_CONTAINER_NAME = "provsql-tpch-dfc"
PROVSQL_IMAGE = "inriavalda/provsql:latest"
PROVSQL_PORT = 5435
PROVSQL_USER = "test"


def database_name_for_scale_factor(scale_factor: float) -> str:
    """Return a stable PostgreSQL database name for a TPC-H scale factor."""

    suffix = re.sub(r"[^a-zA-Z0-9]+", "_", f"{scale_factor:g}").strip("_")
    return f"tpch_dfc_sf_{suffix}"


class ProvSQLClient:
    """Start ProvSQL and load a provenance-tracked TPC-H instance."""

    def __init__(
        self,
        data_dir: pathlib.Path,
        *,
        scale_factor: float,
        port: int = PROVSQL_PORT,
    ) -> None:
        self.data_dir = data_dir
        self.scale_factor = scale_factor
        self.port = port
        self.database = database_name_for_scale_factor(scale_factor)
        self.conn: pg8000.Connection | None = None

    def start(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        data_mount = self.data_dir.resolve()

        existing = run_docker(
            ["ps", "-a", "--filter", f"name={PROVSQL_CONTAINER_NAME}", "--format", "{{.ID}}"]
        )
        running = run_docker(
            ["ps", "--filter", f"name={PROVSQL_CONTAINER_NAME}", "--format", "{{.ID}}"]
        )

        if existing:
            mounts = run_docker(["inspect", "--format", "{{json .Mounts}}", PROVSQL_CONTAINER_NAME])
            ports = run_docker(
                [
                    "inspect",
                    "--format",
                    "{{json .HostConfig.PortBindings}}",
                    PROVSQL_CONTAINER_NAME,
                ]
            )
            if str(data_mount) not in mounts or f'"HostPort":"{self.port}"' not in ports:
                run_docker(["rm", "-f", PROVSQL_CONTAINER_NAME])
                existing = ""
                running = ""

        if not existing:
            run_docker(
                [
                    "run",
                    "-d",
                    "--name",
                    PROVSQL_CONTAINER_NAME,
                    "-p",
                    f"{self.port}:5432",
                    "-v",
                    f"{data_mount}:/data",
                    PROVSQL_IMAGE,
                ]
            )
        elif not running:
            run_docker(["start", PROVSQL_CONTAINER_NAME])

    def wait_ready(self, timeout_s: int = 120) -> None:
        wait_for_pg_ready(
            host="127.0.0.1",
            port=self.port,
            user=PROVSQL_USER,
            password="",
            database="postgres",
            timeout_s=timeout_s,
        )

    def connect(self) -> pg8000.Connection:
        admin = pg8000.connect(
            host="127.0.0.1",
            port=self.port,
            user=PROVSQL_USER,
            password="",
            database="postgres",
            timeout=300,
        )
        admin.autocommit = True
        cursor = admin.cursor()
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.database,))
        if cursor.fetchone() is None:
            cursor.execute(f'CREATE DATABASE "{self.database}" OWNER {PROVSQL_USER}')
        cursor.close()
        admin.close()

        self.conn = pg8000.connect(
            host="127.0.0.1",
            port=self.port,
            user=PROVSQL_USER,
            password="",
            database=self.database,
            timeout=300,
        )
        self.conn.autocommit = True
        cursor = self.conn.cursor()
        cursor.execute("SET statement_timeout = 0")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS provsql CASCADE")
        cursor.close()
        return self.conn

    def ensure_tpch_data(self, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
        if self.conn is None:
            raise RuntimeError("ProvSQL connection not initialized")

        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = 'lineitem'"
        )
        tables_exist = cursor.fetchone() is not None
        if tables_exist:
            cursor.execute("SELECT EXISTS (SELECT 1 FROM lineitem)")
            if not cursor.fetchone()[0]:
                for table in TPCH_TABLES:
                    cursor.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
                tables_exist = False

        if not tables_exist:
            export_tpch_csvs(duckdb_conn, self.data_dir)
            for table in TPCH_TABLES:
                schema = get_table_schema(duckdb_conn, table)
                column_definitions = ", ".join(
                    f'"{name}" {data_type}' for name, data_type in schema
                )
                cursor.execute(f'CREATE TABLE "{table}" ({column_definitions})')
                cursor.execute(
                    f"COPY \"{table}\" FROM '/data/{table}.csv' WITH (FORMAT csv, HEADER true)"
                )
            self.conn.commit()

            cursor.execute("SELECT EXISTS (SELECT 1 FROM lineitem)")
            if not cursor.fetchone()[0]:
                raise RuntimeError("TPC-H lineitem COPY completed without loading any rows")

        cursor.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'lineitem' "
            "AND column_name = 'provsql'"
        )
        if cursor.fetchone() is None:
            cursor.execute("SELECT provsql.add_provenance(CAST('lineitem' AS regclass))")
        cursor.close()

    def fetchall(self, query: str) -> tuple[Any, list[tuple[Any, ...]]]:
        if self.conn is None:
            raise RuntimeError("ProvSQL connection not initialized")
        cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return cursor.description, rows

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None
