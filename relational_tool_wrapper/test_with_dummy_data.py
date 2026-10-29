"""
Test the relational tool wrapper with dummy data.

Uses the MCP client to:
1. Call execute_sql to create a table and insert rows (deterministic program).
2. Call send_emails_from_table to send one "email" per row (wrapper invokes dummy Send Email).

Run from this directory:
  uv run python test_with_dummy_data.py

Or after uv sync:
  python test_with_dummy_data.py
"""

import asyncio
import sys
from pathlib import Path

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

_THIS_DIR = Path(__file__).resolve().parent


async def run_test() -> None:
    server = StdioServerParameters(
        command=sys.executable,
        args=[str(_THIS_DIR / "mcp_server.py")],
        cwd=str(_THIS_DIR),
    )
    async with stdio_client(server) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # 1. Create table and load dummy data (deterministic SQL program)
            create_sql = """
            CREATE TABLE emails (
                to_address VARCHAR,
                subject VARCHAR,
                body VARCHAR
            );
            INSERT INTO emails VALUES
                ('alice@example.com', 'Hello', 'Hi Alice, this is a test.'),
                ('bob@example.com', 'Meeting', 'Bob, meeting at 3pm.'),
                ('carol@example.com', 'Report', 'Please find the report attached.');
            """
            out = await session.call_tool("execute_sql", {"sql": create_sql})
            text = _text_content(out)
            print("execute_sql (create + insert):", text[:200] + ("..." if len(text) > 200 else ""))

            # 2. Call the relational tool wrapper - should "send" 3 emails
            out = await session.call_tool("send_emails_from_table", {"table_name": "emails"})
            text = _text_content(out)
            print("send_emails_from_table:", text)

            # 3. Test schema validation: wrong columns should error
            await session.call_tool("execute_sql", {"sql": "CREATE TABLE bad (a INT, b INT); INSERT INTO bad VALUES (1, 2);"})
            out = await session.call_tool("send_emails_from_table", {"table_name": "bad"})
            text = _text_content(out)
            print("send_emails_from_table(bad):", text)
            assert "must have columns" in text or "Error" in text, f"Expected schema error, got: {text}"

    print("\nDone. Check output above for [Send Email] lines (3 for 'emails', none for 'bad').")


def _text_content(call_result: object) -> str:
    if hasattr(call_result, "content") and call_result.content:
        for part in call_result.content:
            if hasattr(part, "text"):
                return part.text
    return str(call_result)


if __name__ == "__main__":
    asyncio.run(run_test())
