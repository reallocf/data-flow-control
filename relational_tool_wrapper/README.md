# Relational Tool Wrapper (MCP)

Enforces **data flow control between tool calls** by requiring the agent to use a **deterministic program (SQL)** to construct tool inputs. The agent cannot call the underlying tool (Send Email) directly; it must populate a table and call the wrapper.

## MCP spec (what a tool needs)

Per the [MCP Tools spec](https://modelcontextprotocol.io/specification/2025-11-25/server/tools):

- **tools/list**: Server returns tools with `name`, `description`, `inputSchema` (JSON Schema: `type: object`, `properties`, `required`).
- **tools/call**: Client sends `name` + `arguments`; server returns `content` (e.g. `{ "type": "text", "text": "..." }`) and optional `isError`.

## Tools exposed to the agent

1. **execute_sql** (sql: string)  
   Runs SQL against the shared DuckDB. The agent uses this to create tables and insert rows (the deterministic program).

2. **send_emails_from_table** (table_name: string)  
   Relational Tool Wrapper: reads the table, checks that it has columns `to_address`, `subject`, `body`, then “sends” one email per row (via the internal dummy Send Email implementation).

**Send Email** is not exposed as a tool. It exists only as an internal function called by the wrapper for each row.

## Design

- One in-memory DuckDB connection (lifespan) shared by all tools.
- Schema check: `send_emails_from_table` returns an error if the table does not have exactly `(to_address, subject, body)`.
- Dummy Send Email: prints `[Send Email] to=... subject=... body=...` and does not send real email.

## Setup

```bash
cd relational_tool_wrapper
uv sync
```

## Run the MCP server (stdio)

```bash
uv run python mcp_server.py
```

## Test with dummy data

```bash
uv run python test_with_dummy_data.py
```

This will:

1. Call `execute_sql` to create table `emails` and insert 3 rows.
2. Call `send_emails_from_table("emails")` — you should see 3 `[Send Email]` lines.
3. Call `send_emails_from_table("bad")` on a table with wrong columns — you should see an error message.

## Report

After running the test, you should see:

- `execute_sql` creating the table and inserting rows.
- `send_emails_from_table` returning `OK: sent 3 email(s) from table 'emails'.`
- Three printed lines like `[Send Email] to='alice@example.com' subject='Hello' body='...'`.
- For table `bad`, an error like `Table 'bad' must have columns ('to_address', 'subject', 'body'), got ['a', 'b'].`
