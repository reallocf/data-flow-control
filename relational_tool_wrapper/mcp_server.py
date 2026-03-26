"""
MCP server that enforces data flow control between tool calls.

The agent cannot call Send Email directly. The agent must:
1. Use execute_sql to write a deterministic SQL program that builds a table
   with columns (to_address, subject, body).
2. Call send_emails_from_table(table_name) to send one email per row.

Send Email is implemented as an internal function only; it is not exposed as a tool.
"""

import sys
from contextlib import asynccontextmanager
from typing import AsyncIterator

import duckdb
from mcp.server.fastmcp import Context, FastMCP

REQUIRED_EMAIL_TABLE_COLUMNS = ("to_address", "subject", "body")


def _send_email(to_address: str, subject: str, body: str) -> None:
    """
    Dummy Send Email implementation. Does not send real email; prints inputs.
    This is the "existing tool" that we wrap - not exposed to the agent.
    """
    print(f"[Send Email] to={to_address!r} subject={subject!r} body={body!r}", file=sys.stderr)

#single poole connection to the duckdb. Both tools share the same session
@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[duckdb.DuckDBPyConnection]:
    con = duckdb.connect(":memory:")
    try:
        yield con
    finally:
        con.close()


mcp = FastMCP(
    "relational-tool-wrapper",
    lifespan=lifespan,
)


@mcp.tool()
def execute_sql(ctx: Context, sql: str) -> str:
    """
    Execute a SQL statement against the shared database. Use this to create
    tables and insert rows. Tables you create can be used as input to
    send_emails_from_table. Use deterministic SQL to construct tool inputs.
    """
    state = ctx.request_context.lifespan_context
    try:
        result = state.execute(sql)
        if result.description is None:
            return "OK (no result set)"
        rows = result.fetchall()
        if not rows:
            return "OK (0 rows)"
        col_names = [d[0] for d in result.description]
        lines = [" ".join(col_names)]
        for row in rows:
            lines.append(" ".join(str(v) for v in row))
        return "\n".join(lines)
    except duckdb.Error as e:
        return f"Error: {e}"


@mcp.tool()
def send_emails_from_table(ctx: Context, table_name: str) -> str:
    """
    Relational Tool Wrapper: read the given table and send one email per row.
    The table must have exactly the columns: to_address, subject, body.
    Use execute_sql to create and fill the table with deterministic SQL first.
    """
    state = ctx.request_context.lifespan_context
    try:
        result = state.execute(f'SELECT * FROM "{table_name}" LIMIT 0')
        columns = [d[0] for d in result.description]
    except duckdb.Error as e:
        return f"Error: Table {table_name!r} not found or not readable: {e}"

    if list(columns) != list(REQUIRED_EMAIL_TABLE_COLUMNS):
        return (
            f"Error: Table {table_name!r} must have columns "
            f"{REQUIRED_EMAIL_TABLE_COLUMNS}, got {columns}."
        )

    try:
        rows = state.execute(f'SELECT to_address, subject, body FROM "{table_name}"').fetchall()
    except duckdb.Error as e:
        return f"Error reading table: {e}"

    for to_address, subject, body in rows:
        _send_email(
            str(to_address) if to_address is not None else "",
            str(subject) if subject is not None else "",
            str(body) if body is not None else "",
        )

    return f"OK: sent {len(rows)} email(s) from table {table_name!r}."


if __name__ == "__main__":
    mcp.run(transport="stdio")
