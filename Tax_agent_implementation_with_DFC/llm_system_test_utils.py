import asyncio
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import duckdb
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool as lc_tool
from langchain_mcp_adapters.client import MultiServerMCPClient

from tax_engine import compute_1040, expense_from_receipt, schedule_c_input_from_expenses


def _ensure_sql_rewriter_path():
    sys.path.insert(0, str(Path("sql_rewriter/src").resolve()))


def wait_for_port(host: str, port: int, timeout_s: float = 30.0):
    start = time.time()
    while time.time() - start < timeout_s:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            if sock.connect_ex((host, port)) == 0:
                return
        time.sleep(0.5)
    raise TimeoutError(f"Server on {host}:{port} did not become ready in {timeout_s}s")


def start_server_process() -> tuple[subprocess.Popen, str]:
    env = os.environ.copy()
    env.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("EASYOCR_MODULE_PATH", str(Path(".easyocr").resolve()))
    db_path = os.path.join(
        tempfile.gettempdir(),
        f"tax_agent_llm_e2e_{uuid.uuid4().hex[:8]}.duckdb",
    )
    env["EXPENSES_DB_PATH"] = db_path
    proc = subprocess.Popen(
        [sys.executable, "mcp_server_phase_18.py"],
        cwd=str(Path.cwd()),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
    )
    wait_for_port("127.0.0.1", 8000)
    return proc, db_path


def stop_server_process(proc: subprocess.Popen):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)


def read_process_output(proc: subprocess.Popen) -> str:
    if proc.stdout is None:
        return ""
    return proc.stdout.read() or ""


def _parse_receipt_id_from_note(note: str):
    match = re.search(r"receipt_id=(\d+)", note or "")
    return int(match.group(1)) if match else None


def compute_tax_from_db(
    db_path: str,
    *,
    gross_receipts: float,
    federal_withholding: float = 0.0,
) -> dict[str, Any]:
    con = duckdb.connect(db_path, read_only=True)
    rows = con.execute(
        """
        SELECT id, date, amount, category, business_use, note, valid
        FROM expenses
        WHERE valid = TRUE
        ORDER BY date ASC, id ASC
        """
    ).fetchall()

    expenses = []
    expense_details = []
    for row in rows:
        receipt_id = _parse_receipt_id_from_note(row[5])
        vendor = row[5] or f"Expense {row[0]}"
        if receipt_id is not None:
            receipt_row = con.execute(
                "SELECT vendor FROM receipts WHERE id = ?",
                [receipt_id],
            ).fetchone()
            if receipt_row and receipt_row[0]:
                vendor = receipt_row[0]

        receipt = {
            "receipt_id": receipt_id,
            "vendor": vendor,
            "amount": row[2],
            "category": row[3],
        }
        business_use = float(row[4] if row[4] is not None else 1.0)
        expense = expense_from_receipt(receipt, business_use=business_use)
        expenses.append(expense)
        expense_details.append(
            {
                "receipt_id": receipt_id,
                "description": expense.description,
                "category": expense.category,
                "original_amount": float(row[2]),
                "business_use": business_use,
                "tax_engine_amount": expense.amount,
                "expense_id": row[0],
                "date": str(row[1]),
                "source_note": row[5],
            }
        )

    schedule_c_input = schedule_c_input_from_expenses(gross_receipts, expenses)
    tax_return = compute_1040(
        schedule_c_input,
        federal_withholding=float(federal_withholding),
    )
    return {
        "expense_count": len(expenses),
        "expenses_used": expense_details,
        "schedule_c_input": {
            "gross_receipts": schedule_c_input.gross_receipts,
            "expenses": [
                {
                    "description": exp.description,
                    "amount": exp.amount,
                    "category": exp.category,
                    "receipt_present": exp.receipt_present,
                }
                for exp in schedule_c_input.expenses
            ],
        },
        "tax_return": tax_return,
    }


def fetch_db_rows(db_path: str, table: str) -> list[dict[str, Any]]:
    con = duckdb.connect(db_path, read_only=True)
    cursor = con.execute(f"SELECT * FROM {table} ORDER BY ALL")
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in rows]


async def run_llm_request(user_input: str, *, server_url: str = "http://127.0.0.1:8000/sse"):
    _ensure_sql_rewriter_path()
    import mcp_client_phase_18 as client

    traces: list[dict[str, Any]] = []
    mcp_client = MultiServerMCPClient(
        {
            "TaxAgent": {
                "transport": "sse",
                "url": server_url,
            }
        }
    )

    mcp_tools = await mcp_client.get_tools()
    mcp_tool_map = {t.name: t for t in mcp_tools}
    read_only_tools = {"list_expenses"}
    tool_schemas = client.build_tool_schemas([t for t in mcp_tools if t.name not in read_only_tools])
    local_db = client.init_local_db(tool_schemas)
    rewriter = client.create_rewriter(local_db)
    policies_registered = False
    policy_deps = client.parse_policy_deps(client.DFC_POLICIES)

    def _snapshot_table(table: str):
        cursor = local_db.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in rows]

    async def _execute_sql(sql: str) -> dict:
        nonlocal policies_registered
        trace: dict[str, Any] = {
            "sql_before_rewrite": sql,
            "policy_deps": policy_deps,
        }
        sink_table = client.get_sink_table(sql)
        if not sink_table:
            trace["result"] = {"status": "error", "error": "Not a valid INSERT statement."}
            traces.append(trace)
            return trace["result"]

        schema = tool_schemas.get(sink_table)
        if not schema:
            trace["result"] = {
                "status": "error",
                "error": f"Unknown table '{sink_table}'. Available: {list(tool_schemas.keys())}",
            }
            traces.append(trace)
            return trace["result"]

        trace["source_tables_before"] = {
            table: _snapshot_table(table)
            for table in client.get_source_tables(sql)
        }

        gate1_error = client.gate1_validate(sql, policy_deps)
        if gate1_error:
            trace["result"] = {"status": "error", "stage": "gate1", "error": gate1_error}
            traces.append(trace)
            return trace["result"]

        call_id = f"call_{uuid.uuid4().hex[:8]}"
        sql_final = client.inject_call_id(sql, call_id)
        trace["sql_after_call_id"] = sql_final

        if not policies_registered:
            policies_registered = client.register_policies(rewriter, client.DFC_POLICIES)

        for source_table in client.get_source_tables(sql_final):
            client.deduplicate_source_table(local_db, source_table)

        count_before = local_db.execute(f"SELECT COUNT(*) FROM {sink_table}").fetchone()[0]
        trace["sink_count_before"] = count_before

        try:
            if policies_registered:
                trace["sql_after_rewrite"] = rewriter.transform_query(sql_final)
                rewriter.execute(sql_final)
            else:
                trace["sql_after_rewrite"] = sql_final
                local_db.execute(sql_final)
        except Exception as exc:
            if "KILLing due to dfc policy violation" in str(exc):
                trace["result"] = {
                    "status": "blocked",
                    "reason": "BLOCKED: Meal receipts cannot be 100% business use. Use a lower percentage e.g. 0.5.",
                }
                trace["exception"] = str(exc)
                trace["sink_count_after"] = local_db.execute(
                    f"SELECT COUNT(*) FROM {sink_table}"
                ).fetchone()[0]
                traces.append(trace)
                return trace["result"]
            trace["result"] = {"status": "error", "error": str(exc)}
            trace["exception"] = str(exc)
            trace["sink_count_after"] = local_db.execute(
                f"SELECT COUNT(*) FROM {sink_table}"
            ).fetchone()[0]
            traces.append(trace)
            return trace["result"]

        count_after = local_db.execute(f"SELECT COUNT(*) FROM {sink_table}").fetchone()[0]
        trace["sink_count_after"] = count_after
        if count_after == count_before:
            trace["result"] = {
                "status": "blocked",
                "reason": "BLOCKED: DFC policy violation. Meal receipts cannot be 100% business use. Use a lower percentage e.g. 0.5.",
            }
            traces.append(trace)
            return trace["result"]

        input_row = client.fetch_row(local_db, sink_table, call_id)
        args = {col: input_row[col] for col in schema.input_columns if col in input_row}
        trace["tool_args"] = args
        mcp_tool = mcp_tool_map[schema.tool_name]
        raw_response = await mcp_tool.ainvoke(args)
        response = client.parse_mcp_response(raw_response)
        schema.store_output(local_db, call_id, response)
        trace["result"] = {"status": "ok", "tool": schema.tool_name, "result": response}
        trace["source_tables_after"] = {
            table: _snapshot_table(table)
            for table in [sink_table, schema.output_table]
        }
        traces.append(trace)
        return trace["result"]

    @lc_tool
    async def execute_sql(sql: str) -> str:
        """Execute INSERT SQL against the local DFC intent database."""
        result = await _execute_sql(sql)
        return json.dumps(result)

    pass_through_tools = [tool for tool in mcp_tools if f"{tool.name}_in" not in tool_schemas]
    llm_with_tools = client.llm.bind_tools([execute_sql] + pass_through_tools, parallel_tool_calls=False)

    messages: list[Any] = [
        SystemMessage(content=client.build_system_prompt(tool_schemas)),
        HumanMessage(content=user_input),
    ]

    for _ in range(8):
        ai_msg = await llm_with_tools.ainvoke(messages)
        messages.append(ai_msg)
        tool_calls = getattr(ai_msg, "tool_calls", None) or []
        if not tool_calls:
            break

        request_failed = False
        for tc in tool_calls:
            name = tc["name"]
            if name in ("execute_sql", "_execute_sql"):
                result = await _execute_sql(tc["args"].get("sql", ""))
                content = json.dumps(result)
                if result.get("status") == "blocked":
                    request_failed = True
            else:
                mcp_tool = mcp_tool_map.get(name)
                if mcp_tool:
                    raw = await mcp_tool.ainvoke(tc["args"])
                    content = json.dumps(client.parse_mcp_response(raw))
                else:
                    content = json.dumps({"status": "error", "error": f"Unknown tool '{name}'"})
                    request_failed = True
            messages.append(ToolMessage(content=content, tool_call_id=tc["id"]))
        if request_failed:
            break

    final_ai = next((m for m in reversed(messages) if getattr(m, "content", None) and m.__class__.__name__ == "AIMessage"), None)
    return {
        "final_message": final_ai.content if final_ai else "",
        "traces": traces,
        "messages": messages,
        "tool_tables": {
            table: _snapshot_table(table)
            for table in local_db.execute("SHOW TABLES").fetchall()
            for table in [table[0]]
        },
    }


def run_llm_request_sync(user_input: str, *, server_url: str = "http://127.0.0.1:8000/sse"):
    return asyncio.run(run_llm_request(user_input, server_url=server_url))
