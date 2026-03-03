"""DuckDB tool: executes SQL via SQLRewriter (DFC policies) when available, else raw DuckDB."""

from __future__ import annotations

import os
import re
from pathlib import Path

import duckdb
from langchain_core.tools import tool

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database" / "finance.db"

WRITE_SQL_RE = re.compile(r"^\s*(insert|update|delete|drop|alter|create|truncate)\b", re.IGNORECASE)

# Use sql_rewriter when available so DFC policies are applied before execution
try:
    from sql_rewriter import AggregateDFCPolicy, DFCPolicy, SQLRewriter
    _HAS_REWRITER = True
except ImportError:
    SQLRewriter = None
    _HAS_REWRITER = False


POLICIES_PATH = BASE_DIR / "policies.txt"


def _load_policy_strings() -> list[str]:
    """Load policy strings from env and/or policies.txt.

    - Env: DFC_POLICIES can contain newline-separated policy strings
    - File: langchain_hooks/policies.txt, one policy per line (supports comments with '#')
    """
    policy_strings: list[str] = []

    env_policies = os.environ.get("DFC_POLICIES", "")
    # Note: keep env as plain text; newline-separated policies
    if env_policies and env_policies.strip():
        for line in env_policies.splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            policy_strings.append(s)

    if POLICIES_PATH.exists():
        for line in POLICIES_PATH.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            policy_strings.append(s)

    return policy_strings


def _register_policies(rewriter: "SQLRewriter") -> None:
    """Register policies onto a SQLRewriter instance.

    We create fresh policy objects each time to avoid sqlglot mutability issues.
    """
    if not _HAS_REWRITER:
        return

    for policy_str in _load_policy_strings():
        # Aggregate policies must be parsed with AggregateDFCPolicy
        if policy_str.lstrip().upper().startswith("AGGREGATE"):
            policy = AggregateDFCPolicy.from_policy_str(policy_str)
        else:
            policy = DFCPolicy.from_policy_str(policy_str)
        rewriter.register_policy(policy)


def _execute_sql(sql: str):
    """Run SQL: through SQLRewriter (before execution) when available, else raw DuckDB."""
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        if _HAS_REWRITER:
            rewriter = SQLRewriter(conn=conn)
            _register_policies(rewriter)
            cursor = rewriter.execute(sql)
        else:
            cursor = conn.execute(sql)
        res = cursor.fetchall()
        cols = [c[0] for c in cursor.description] if cursor.description else []
        return cols, res
    finally:
        conn.close()


@tool
def query_duckdb(sql: str) -> str:
    """
    Execute a READ-ONLY SQL query against the local DuckDB database at `database/finance.db`.

    Queries are run through the SQL rewriter when available, so Data Flow Control (DFC)
    policies are applied before execution.

    Rules:
    - Only SELECT queries are allowed.
    - INSERT/UPDATE/DELETE/DROP/ALTER/CREATE/TRUNCATE are forbidden.
    - If you don't know the schema, use:
        - SHOW TABLES;
        - DESCRIBE <table>;
        - SELECT table_name FROM information_schema.tables;
    """
    # Enforce read-only here (do NOT raise)
    if WRITE_SQL_RE.match(sql or ""):
        return "FORBIDDEN_OPERATION: Write queries are not allowed. Use SELECT only."

    if not DB_PATH.exists():
        return f"SQL_ERROR: DuckDB file not found at {DB_PATH}. Create it or point DB_PATH to the correct location."

    try:
        cols, res = _execute_sql(sql)

        if not cols:
            return "OK (no result set)."

        lines = ["\t".join(cols)]
        for row in res:
            lines.append("\t".join(map(str, row)))
        return "\n".join(lines)

    except Exception as e:
        return f"SQL_ERROR: {e}"