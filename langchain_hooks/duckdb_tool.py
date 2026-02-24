import re
from pathlib import Path

import duckdb
from langchain_core.tools import tool

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database" / "finance.db"

WRITE_SQL_RE = re.compile(r"^\s*(insert|update|delete|drop|alter|create|truncate)\b", re.IGNORECASE)


@tool
def query_duckdb(sql: str) -> str:
    """
    Execute a READ-ONLY SQL query against the local DuckDB database at `database/finance.db`.

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
        with duckdb.connect(str(DB_PATH), read_only=True) as conn:
            # DuckDB supports SHOW TABLES; allow it
            res = conn.execute(sql).fetchall()
            cols = [c[0] for c in conn.description] if conn.description else []

        # Format result
        if not cols:
            return "OK (no result set)."

        lines = ["\t".join(cols)]
        for row in res:
            lines.append("\t".join(map(str, row)))
        return "\n".join(lines)

    except Exception as e:
        return f"SQL_ERROR: {e}"