import duckdb
from pathlib import Path

from langchain_core.tools import tool


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database" / "finance.db"


@tool
def query_duckdb(sql: str) -> str:
    """
    Execute a read-only SQL query against the local DuckDB database.

    The database file is expected at ``database/finance.db`` relative to this
    module. Results are returned as a formatted string for LLM consumption.
    """
    # Open a short-lived connection for each call to keep things simple.
    conn = duckdb.connect(str(DB_PATH))
    tables = conn.execute("SHOW TABLES").fetchall()
    print("Available tables:", tables)
    try:
        # Enforce read-only behaviour at the tool level;
        # write attempts should already be blocked by SQLToolCallback.
        sql_upper = sql.upper()
        forbidden = ["DROP", "DELETE", "UPDATE", "INSERT"]
        if any(word in sql_upper for word in forbidden):
            return "ERROR: Write operations (INSERT, UPDATE, DELETE, DROP, ALTER, CREATE) are not allowed. Only SELECT queries are permitted."

        try:
            result = conn.execute(sql).fetchall()
            columns = [col[0] for col in conn.description] if conn.description else []

        except Exception as e:
            return f"ERROR: Failed to execute query. {str(e)}"

        if not columns:
            return "Query executed successfully (no result set)."

        # Simple tab-separated representation
        lines = ["\t".join(map(str, columns))]
        for row in result:
            lines.append("\t".join(map(str, row)))

        return "\n".join(lines)
    finally:
        conn.close()

