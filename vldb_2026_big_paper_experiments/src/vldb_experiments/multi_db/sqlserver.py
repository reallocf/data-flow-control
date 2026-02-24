"""SQL Server (AWS-hosted) integration helpers."""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any

from vldb_experiments.multi_db.common import normalize_results
from vldb_experiments.multi_db.tpch_data import TPCH_TABLES

if TYPE_CHECKING:
    import pathlib

    import duckdb
    import pyodbc


SQLSERVER_DRIVER = "ODBC Driver 18 for SQL Server"
SQLSERVER_ENCRYPT = "yes"
SQLSERVER_TRUST_SERVER_CERTIFICATE = "yes"
SQLSERVER_LOGIN_TIMEOUT = "10"
SQLSERVER_QUERY_TIMEOUT = "0"
SQLSERVER_DEFAULT_HOST = "data-flow-control-sql-server.cu3qs4uisn3k.us-east-1.rds.amazonaws.com"
SQLSERVER_DEFAULT_PORT = "1433"
SQLSERVER_DEFAULT_USER = "tpch"
SQLSERVER_DEFAULT_DATABASE = "tpch"
SQLSERVER_INSERT_BATCH_SIZE = 1000

_ENV_REQUIRED = ("SQLSERVER_PASSWORD",)

try:
    import pyodbc
except ModuleNotFoundError:  # pragma: no cover - depends on local driver installation
    pyodbc = None


def sqlserver_env_available() -> bool:
    return all(_get_env(name) for name in _ENV_REQUIRED)


def _get_env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    if value is None or value == "":
        return None
    return value


def _require_pyodbc() -> None:
    if pyodbc is None:
        raise RuntimeError("pyodbc is not installed. Install it to use SQL Server integration.")


def _load_config(require_env: bool = True) -> dict[str, str]:
    missing = [name for name in _ENV_REQUIRED if not _get_env(name)]
    if missing and require_env:
        missing_str = ", ".join(missing)
        raise RuntimeError(f"Missing SQL Server env vars: {missing_str}")

    return {
        "host": _get_env("SQLSERVER_HOST", SQLSERVER_DEFAULT_HOST) or "",
        "port": _get_env("SQLSERVER_PORT", SQLSERVER_DEFAULT_PORT) or "",
        "user": _get_env("SQLSERVER_USER", SQLSERVER_DEFAULT_USER) or "",
        "password": _get_env("SQLSERVER_PASSWORD") or "",
        "database": _get_env("SQLSERVER_DATABASE", SQLSERVER_DEFAULT_DATABASE) or "",
        "driver": SQLSERVER_DRIVER,
        "encrypt": SQLSERVER_ENCRYPT,
        "trust_server_certificate": SQLSERVER_TRUST_SERVER_CERTIFICATE,
        "login_timeout": SQLSERVER_LOGIN_TIMEOUT,
        "query_timeout": SQLSERVER_QUERY_TIMEOUT,
    }


def _ensure_database_exists(config: dict[str, str]) -> None:
    if not config["database"]:
        return
    master_config = dict(config)
    master_config["database"] = "master"
    conn = pyodbc.connect(
        _build_connection_string(master_config),
        timeout=int(config["login_timeout"]),
    )
    try:
        conn.autocommit = True
        cursor = conn.cursor()
        db_name = config["database"]
        cursor.execute(f"IF DB_ID(N'{db_name}') IS NULL CREATE DATABASE [{db_name}]")
    finally:
        conn.close()


def _sanitize_schema_name(schema: str) -> str:
    return schema.replace(".", "_")


def _ensure_schema_exists(conn: pyodbc.Connection, schema: str) -> None:
    cursor = conn.cursor()
    cursor.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = ?) "
        "EXEC('CREATE SCHEMA [' + ? + ']')",
        (schema, schema),
    )


def _qualify_query_tables(query: str, schema: str | None) -> str:
    import sqlglot
    from sqlglot import exp

    parsed = sqlglot.parse_one(query, read="duckdb")
    top_select = parsed
    if isinstance(parsed, exp.With):
        top_select = parsed.args.get("this")

    for select in parsed.find_all(exp.Select):
        if select is top_select:
            continue
        if select.args.get("order") is None:
            continue
        if any(select.args.get(key) is not None for key in ("limit", "offset", "top")):
            continue
        select.set("order", None)
    cte_names: set[str] = set()
    with_expr = parsed.args.get("with_")
    if with_expr is not None:
        for cte in with_expr.expressions:
            alias = cte.alias
            if alias is None:
                continue
            if isinstance(alias, str):
                cte_names.add(alias)
                continue
            alias_expr = getattr(alias, "this", None)
            if alias_expr is not None:
                cte_names.add(alias_expr.name)

    if schema:
        for table in parsed.find_all(exp.Table):
            table_name = table.name
            if table_name in cte_names:
                continue
            if table.args.get("db") is None:
                table.set("db", exp.Identifier(this=schema))

    return parsed.sql(dialect="tsql")


def _map_duckdb_type(duckdb_type: str) -> str:
    type_upper = duckdb_type.upper()
    if type_upper.startswith("DECIMAL"):
        return type_upper
    if type_upper.startswith("DOUBLE"):
        return "FLOAT"
    if type_upper.startswith("VARCHAR"):
        if "(" in type_upper:
            return type_upper
        return "VARCHAR(MAX)"
    if type_upper.startswith("CHAR"):
        if "(" in type_upper:
            return type_upper
        return "CHAR(1)"
    if type_upper.startswith("HUGEINT"):
        return "BIGINT"
    if type_upper.startswith("BIGINT"):
        return "BIGINT"
    if type_upper.startswith(("INTEGER", "INT")):
        return "INT"
    if type_upper.startswith("BOOLEAN"):
        return "BIT"
    if type_upper.startswith("TIMESTAMP"):
        return "DATETIME2"
    if type_upper.startswith("DATE"):
        return "DATE"
    return type_upper


def _get_sqlserver_schema(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table: str,
) -> list[tuple[str, str]]:
    columns = duckdb_conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    schema: list[tuple[str, str]] = []
    for _cid, name, col_type, _notnull, _default, _pk in columns:
        schema.append((name, _map_duckdb_type(col_type)))
    return schema


class SQLServerClient:
    """Helper for connecting to SQL Server on AWS and loading TPC-H data."""

    def __init__(
        self,
        data_dir: pathlib.Path,
        database: str | None = None,
        schema: str | None = None,
        drop_database_on_close: bool = False,
    ) -> None:
        self.data_dir = data_dir
        self.conn: pyodbc.Connection | None = None
        self.database = database
        self.schema = _sanitize_schema_name(schema) if schema else None
        self.drop_database_on_close = drop_database_on_close

    def start(self) -> None:
        _require_pyodbc()
        config = _load_config(require_env=True)
        if self.database:
            config["database"] = self.database
        _ensure_database_exists(config)

    def wait_ready(self, timeout_s: int = 60) -> None:
        _require_pyodbc()
        config = _load_config(require_env=True)
        if self.database:
            config["database"] = self.database
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                conn = pyodbc.connect(
                    _build_connection_string(config),
                    timeout=int(config["login_timeout"]),
                )
                conn.close()
                return
            except Exception:
                time.sleep(1)
        raise RuntimeError("SQL Server did not become ready in time.")

    def connect(self) -> pyodbc.Connection:
        _require_pyodbc()
        config = _load_config(require_env=True)
        if self.database:
            config["database"] = self.database
        self.conn = pyodbc.connect(
            _build_connection_string(config),
            timeout=int(config["login_timeout"]),
        )
        self.conn.autocommit = True
        cursor = self.conn.cursor()
        cursor.execute("SET NOCOUNT ON")
        if hasattr(cursor, "timeout"):
            cursor.timeout = int(config["query_timeout"])
        if self.schema:
            _ensure_schema_exists(self.conn, self.schema)
        return self.conn

    def ensure_tpch_data(self, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
        if self.conn is None:
            raise RuntimeError("SQL Server connection not initialized.")

        cursor = self.conn.cursor()
        schema_name = self.schema or "dbo"
        cursor.execute(
            "SELECT t.name "
            "FROM sys.tables t "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "WHERE s.name = ?",
            (schema_name,),
        )
        existing_tables = {row[0] for row in cursor.fetchall()}
        missing_tables = [table for table in TPCH_TABLES if table not in existing_tables]
        if not missing_tables:
            return

        _ensure_schema_exists(self.conn, schema_name)

        for table in TPCH_TABLES:
            schema = _get_sqlserver_schema(duckdb_conn, table)
            col_defs = ", ".join([f"[{name}] {col_type}" for name, col_type in schema])
            cursor.execute(
                f"IF OBJECT_ID(N'[{schema_name}].{table}', N'U') IS NOT NULL "
                f"DROP TABLE [{schema_name}].[{table}]"
            )
            cursor.execute(
                f"CREATE TABLE [{schema_name}].[{table}] ({col_defs})"
            )

            duck_cursor = duckdb_conn.execute(f"SELECT * FROM {table}")
            col_names = ", ".join([f"[{name}]" for name, _ in schema])
            placeholders = ", ".join(["?"] * len(schema))
            insert_sql = f"INSERT INTO [{schema_name}].[{table}] ({col_names}) VALUES ({placeholders})"

            cursor.fast_executemany = True
            while True:
                batch = duck_cursor.fetchmany(SQLSERVER_INSERT_BATCH_SIZE)
                if not batch:
                    break
                for attempt in range(3):
                    try:
                        cursor.executemany(insert_sql, batch)
                        break
                    except Exception:
                        if attempt >= 2:
                            raise
                        self.connect()
                        cursor = self.conn.cursor()
                        cursor.fast_executemany = True

    def fetchall(self, query: str) -> list[tuple[Any, ...]]:
        if self.conn is None:
            raise RuntimeError("SQL Server connection not initialized.")
        cursor = self.conn.cursor()
        query = _qualify_query_tables(query, self.schema)
        cursor.execute(query)
        results = cursor.fetchall()
        return normalize_results(results)

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        if self.drop_database_on_close and self.database:
            _require_pyodbc()
            config = _load_config(require_env=True)
            config["database"] = "master"
            conn = pyodbc.connect(
                _build_connection_string(config),
                timeout=int(config["login_timeout"]),
            )
            try:
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute(
                    f"IF DB_ID(N'{self.database}') IS NOT NULL "
                    f"ALTER DATABASE [{self.database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
                )
                cursor.execute(
                    f"IF DB_ID(N'{self.database}') IS NOT NULL DROP DATABASE [{self.database}]"
                )
            finally:
                conn.close()


def _build_connection_string(config: dict[str, str]) -> str:
    encrypt = "yes" if config["encrypt"] in ("1", "true", "yes") else "no"
    trust = "yes" if config["trust_server_certificate"] in ("1", "true", "yes") else "no"
    return (
        f"Driver={{{config['driver']}}};"
        f"Server={config['host']},{config['port']};"
        f"Database={config['database']};"
        f"UID={config['user']};"
        f"PWD={config['password']};"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust};"
    )
