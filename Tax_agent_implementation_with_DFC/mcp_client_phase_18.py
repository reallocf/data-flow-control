"""
Phase 7 — Tax Agent MCP Client

Architecture:
  - Tool schemas auto-derived from MCP server inputSchema at startup
  - Input/output tables created dynamically (schema-on-write for outputs)
  - Gate 1: validates SQL has required FROM clause (corrective, not hard block)
  - SQLRewriter: enforces cross-tool DFC policies (hard block on violation)
  - Client extracts params → JSON → vanilla MCP server
  - LLM learns from Gate 1 errors and rewrites SQL — no strict workflow in prompt

Gate 1 (new):
  Checks that INSERT INTO <tool>_in references the required source table.
  If missing → returns corrective error so LLM can fix its SQL.
  Driven by TOOL_DEPS — independent of DFC policies.

SQLRewriter (unchanged):
  Enforces data flow constraints (e.g. meal + 100% business use → KILL).
  Fires after Gate 1 passes.
"""

import asyncio
import json
import uuid
import tempfile
from typing import TypedDict, Annotated, Optional

import duckdb
import sqlglot
from sqlglot import exp
from dotenv import load_dotenv

from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.checkpoint.memory import MemorySaver

from langchain_core.messages import (
    BaseMessage, HumanMessage, SystemMessage, AIMessage, ToolMessage
)
from langchain_core.tools import tool as lc_tool
from langchain_openai import ChatOpenAI
from langchain_mcp_adapters.client import MultiServerMCPClient

load_dotenv()

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)


# =============================================================================
# TYPE MAPPING
# =============================================================================
JSON_TO_DUCKDB = {
    "integer": "INTEGER",
    "number":  "DOUBLE",
    "string":  "VARCHAR",
    "boolean": "BOOLEAN",
    "object":  "VARCHAR",
    "array":   "VARCHAR",
}

def json_type_to_duckdb(json_type: str) -> str:
    return JSON_TO_DUCKDB.get(json_type.lower(), "VARCHAR")


# =============================================================================
# TOOL SCHEMA
# =============================================================================
class ToolSchema:
    def __init__(self, tool_name: str, input_properties: dict):
        self.tool_name   = tool_name
        self.input_table = f"{tool_name}_in"
        self.output_table = f"{tool_name}_out"

        self.input_columns: dict[str, str] = {}
        for col, info in input_properties.items():
            if isinstance(info, dict):
                json_type = info.get("type", "string")
            elif isinstance(info, list) and info:
                first = info[0]
                json_type = first.get("type", "string") if isinstance(first, dict) else "string"
            else:
                json_type = "string"
            self.input_columns[col] = json_type_to_duckdb(json_type)

        self.output_columns: dict[str, str] = {}
        self.output_table_created = False

    def create_input_table(self, con: duckdb.DuckDBPyConnection):
        col_defs = ", ".join(
            f'"{col}" {dtype}'
            for col, dtype in self.input_columns.items()
        )
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.input_table} (
                call_id VARCHAR PRIMARY KEY,
                {col_defs}
            )
        """)
        print(f"[Schema] Created {self.input_table} {list(self.input_columns.keys())}")

    def ensure_output_table(self, con: duckdb.DuckDBPyConnection, response: dict):
        if self.output_table_created:
            return
        self.output_columns = {}
        for key, val in response.items():
            if isinstance(val, bool):     dtype = "BOOLEAN"
            elif isinstance(val, int):    dtype = "INTEGER"
            elif isinstance(val, float):  dtype = "DOUBLE"
            elif isinstance(val, (dict, list)): dtype = "VARCHAR"
            else:                         dtype = "VARCHAR"
            self.output_columns[key] = dtype

        col_defs = ", ".join(f"{col} {dtype}" for col, dtype in self.output_columns.items())
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.output_table} (
                call_id VARCHAR PRIMARY KEY,
                {col_defs}
            )
        """)
        self.output_table_created = True
        print(f"[Schema] Created {self.output_table} {list(self.output_columns.keys())}")

    def store_output(self, con: duckdb.DuckDBPyConnection, call_id: str, response: dict):
        self.ensure_output_table(con, response)
        cols = ["call_id"] + list(self.output_columns.keys())
        vals = [call_id]
        for col in self.output_columns:
            val = response.get(col)
            if isinstance(val, (dict, list)):
                val = json.dumps(val)
            vals.append(val)
        placeholders = ", ".join(["?"] * len(vals))
        col_list = ", ".join(cols)
        con.execute(
            f"INSERT OR REPLACE INTO {self.output_table} ({col_list}) VALUES ({placeholders})",
            vals
        )


def build_tool_schemas(mcp_tools: list) -> dict[str, ToolSchema]:
    schemas = {}
    for tool in mcp_tools:
        properties = {}

        raw_schema = getattr(tool, "args_schema", None)
        if isinstance(raw_schema, dict):
            properties = raw_schema.get("properties", raw_schema)
        elif raw_schema is not None:
            try:
                schema_dict = (
                    raw_schema.model_json_schema()
                    if hasattr(raw_schema, "model_json_schema")
                    else raw_schema.schema()
                )
                properties = schema_dict.get("properties", {})
            except Exception:
                pass

        if not properties:
            try:
                tool_args = tool.args
                if tool_args:
                    properties = {k: {"type": v.get("type", "string")} for k, v in tool_args.items()}
            except Exception:
                pass

        if not properties:
            print(f"[Schema] No inputSchema for {tool.name} — skipping")
            continue

        schema = ToolSchema(tool.name, properties)
        schemas[schema.input_table] = schema
        print(f"[Schema] Registered {tool.name} → {schema.input_table} columns: {list(properties.keys())}")

    return schemas


# =============================================================================
# LOCAL DB
# =============================================================================
def init_local_db(tool_schemas: dict[str, ToolSchema]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    for schema in tool_schemas.values():
        schema.create_input_table(con)
    return con


# =============================================================================
# SQL REWRITER
# =============================================================================
def create_rewriter(con: duckdb.DuckDBPyConnection):
    from sql_rewriter import SQLRewriter
    try:
        return SQLRewriter(conn=con)
    except Exception as e:
        if "kill" in str(e).lower() and "already exists" in str(e).lower():
            rewriter = object.__new__(SQLRewriter)
            rewriter.conn = con
            rewriter._policies = []
            rewriter._aggregate_policies = []
            rewriter._bedrock_client = None
            rewriter._bedrock_model_id = None
            rewriter._recorder = None
            rewriter._replay_manager = None
            with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
                rewriter._stream_file_path = f.name
            return rewriter
        raise


def register_policies(rewriter, policies: list[str]) -> bool:
    from sql_rewriter import DFCPolicy
    rewriter._policies = []
    rewriter._aggregate_policies = []
    success = True
    for policy_str in policies:
        try:
            rewriter.register_policy(DFCPolicy.from_policy_str(policy_str))
            print(f"[DFC] Registered policy: {policy_str[:60]}...")
        except Exception as e:
            print(f"[DFC] Policy registration failed: {e}")
            success = False
    return success


# =============================================================================
# POLICY PARSER — derives Gate 1 requirements from DFC policies
# SOURCES X SINK Y → Y requires FROM X
# =============================================================================
def parse_policy_deps(policies: list[str]) -> dict:
    """
    Derive Gate 1 source requirements from DFC policy strings.

    For each policy:
      SOURCES <source_table> SINK <sink_table> ...
      → sink_table requires SELECT FROM source_table

    Returns:
      {
        "set_expense_in": {
          "required_sources": {"get_receipt_out"},
          "via_key": "receipt_id"   # shared col between source and sink input tables
        }
      }
    """
    import re
    deps = {}

    for policy_str in policies:
        # extract SOURCES and SINK table names
        sources_match = re.search("SOURCES\\s+([\\w,\\s]+?)\\s+SINK", policy_str, re.IGNORECASE)
        sink_match    = re.search("SINK\\s+(\\w+)", policy_str, re.IGNORECASE)

        if not sources_match or not sink_match:
            continue

        source_tables = {s.strip() for s in sources_match.group(1).split(",")}
        sink_table    = sink_match.group(1).strip()

        if sink_table not in deps:
            deps[sink_table] = {"required_sources": set()}

        deps[sink_table]["required_sources"].update(source_tables)

    return deps


# =============================================================================
# SQL HELPERS
# =============================================================================
def get_sink_table(sql: str) -> Optional[str]:
    try:
        parsed = sqlglot.parse_one(sql, read="duckdb")
        if not isinstance(parsed, exp.Insert):
            return None
        if isinstance(parsed.this, exp.Schema):
            return parsed.this.this.name
        elif isinstance(parsed.this, exp.Table):
            return parsed.this.name
    except Exception:
        return None


def inject_call_id(sql: str, call_id: str) -> str:
    try:
        parsed = sqlglot.parse_one(sql, read="duckdb")
        if not isinstance(parsed, exp.Insert):
            return sql

        if isinstance(parsed.this, exp.Schema):
            cols = [c.name.lower() for c in parsed.this.expressions]
            if "call_id" not in cols:
                parsed.this.expressions.insert(0, exp.Identifier(this="call_id", quoted=False))

        values_expr = parsed.find(exp.Values)
        if values_expr and values_expr.expressions:
            tup = values_expr.expressions[0]
            if isinstance(tup, exp.Tuple):
                tup.expressions.insert(0, exp.Literal.string(call_id))
            return parsed.sql(dialect="duckdb")

        select_expr = parsed.find(exp.Select)
        if select_expr:
            select_expr.expressions.insert(
                0,
                exp.Alias(
                    this=exp.Literal.string(call_id),
                    alias=exp.Identifier(this="call_id", quoted=False)
                )
            )
            parsed.set("limit", exp.Limit(expression=exp.Literal.number(1)))
            return parsed.sql(dialect="duckdb")

        return sql
    except Exception as e:
        print(f"[inject_call_id] failed: {e}")
        return sql


def fetch_row(con: duckdb.DuckDBPyConnection, table: str, call_id: str) -> Optional[dict]:
    try:
        cursor = con.execute(f"SELECT * FROM {table} WHERE call_id = ?", [call_id])
        row = cursor.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cursor.description]
        return dict(zip(cols, row))
    except Exception:
        return None


def get_source_tables(sql: str) -> list[str]:
    try:
        parsed = sqlglot.parse_one(sql, read="duckdb")
        if not isinstance(parsed, exp.Insert):
            return []
        sink = get_sink_table(sql)
        return [t.name for t in parsed.find_all(exp.Table) if t.name and t.name != sink]
    except Exception:
        return []


def deduplicate_source_table(con: duckdb.DuckDBPyConnection, table: str):
    """Keep only latest row per natural key — prevents duplicate call_id errors."""
    if not table:
        return
    try:
        cols = [r[1].lower() for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
        skip = {"call_id", "status", "message", "vendor", "amount", "date", "expense_id", "valid", "note"}
        key_col = "receipt_id" if "receipt_id" in cols else next(
            (c for c in cols if c not in skip and c != "call_id"), None
        )
        if key_col:
            con.execute(f"""
                DELETE FROM {table}
                WHERE rowid NOT IN (
                    SELECT MAX(rowid) FROM {table} GROUP BY {key_col}
                )
            """)
    except Exception:
        pass


def parse_mcp_response(response) -> dict:
    if isinstance(response, dict):
        return response
    if isinstance(response, list):
        text = next((b["text"] for b in response if isinstance(b, dict) and b.get("type") == "text"), None)
        if text:
            try:
                return json.loads(text)
            except Exception:
                pass
    return {}


# =============================================================================
# GATE 1 — Source table validation
# Checks INSERT INTO <tool>_in references required source tables.
# Returns corrective error string if missing, None if passed.
# LLM reads the error and rewrites — not a hard block.
# Driven by TOOL_DEPS, independent of DFC policies.
# =============================================================================
def gate1_validate(sql: str, policy_deps: dict) -> Optional[str]:
    """
    Gate 1 — checks INSERT INTO <sink> references required source tables.
    policy_deps derived from DFC policies via parse_policy_deps().
    Returns corrective error string if check fails, None if passed.
    """
    sink_table = get_sink_table(sql)
    if not sink_table:
        return None

    dep_info = policy_deps.get(sink_table)
    if not dep_info:
        return None  # no policy requirement for this table

    required_sources = dep_info.get("required_sources", set())
    if not required_sources:
        return None

    actual_sources = set(get_source_tables(sql))
    missing = required_sources - actual_sources

    if missing:
        missing_table = list(missing)[0]
        source_tool   = missing_table.replace("_out", "")

        return (
            f"Gate 1 Error: INSERT INTO {sink_table} must SELECT FROM {missing_table}. "
            f"{missing_table} has not been referenced. "
            f"First call {source_tool} to populate {missing_table}, then rewrite as a SELECT:\n"
            f"INSERT INTO {sink_table} (...)\n"
            f"SELECT ... FROM {missing_table}\n"
            f"WHERE <shared_key> = <value>"
        )

    return None


# =============================================================================
# SYSTEM PROMPT — minimal, no hardcoded workflow
# LLM learns what tables exist. Gate 1 corrects wrong SQL.
# =============================================================================
def build_system_prompt(tool_schemas: dict[str, ToolSchema]) -> str:
    lines = [
        "You are a tax agent assistant that helps record business expenses.\n",
        "You have ONE tool: execute_sql(sql)",
        "Use it to declare tool intent by writing INSERT SQL.\n",
        "AVAILABLE INPUT TABLES:",
    ]
    for schema in tool_schemas.values():
        col_str = ", ".join(schema.input_columns.keys())
        lines.append(f"  {schema.input_table} ({col_str})")

    lines += [
        "",
        "OUTPUT TABLES (read from these in SELECT):",
    ]
    for schema in tool_schemas.values():
        lines.append(f"  {schema.output_table} — populated after {schema.tool_name} runs")

    lines += [
        "",
        "FOR READ-ONLY QUERIES:",
        "  Call list_expenses(limit=100) directly\n",
        "RULES:",
        "  - Decimals for percentages: 50% = 0.5, 100% = 1.0",
        "  - If execute_sql returns a Gate 1 Error → fix the SQL and retry",
        "  - status=blocked → explain the policy violation, stop",
        "  - status=error   → explain what went wrong, stop",
        "  - status=ok      → continue",
    ]
    return "\n".join(lines)


# =============================================================================
# DFC POLICIES — hardcoded for now, policy registry coming
# =============================================================================
DFC_POLICIES = [
    (
        "SOURCES get_receipt_out "
        "SINK set_expense_in "
        "CONSTRAINT NOT ("
        "MAX(get_receipt_out.category) = 'meal' "
        "AND set_expense_in.business_use = 1.0"
        ") "
        "ON FAIL KILL "
        "DESCRIPTION Meal receipts cannot be 100 percent business use"
    ),
]

# =============================================================================
# STATE
# =============================================================================
class AgentState(TypedDict):
    messages:       Annotated[list[BaseMessage], add_messages]
    request_failed: bool


# =============================================================================
# MAIN
# =============================================================================
async def main():
    mcp_client = MultiServerMCPClient({
        "TaxAgent": {
            "transport": "sse",
            "url": "http://127.0.0.1:8000/sse",
        }
    })

    mcp_tools = await mcp_client.get_tools()
    mcp_tool_map = {t.name: t for t in mcp_tools}
    print(f"MCP tools available: {list(mcp_tool_map.keys())}\n")

    READ_ONLY_TOOLS = {"list_expenses"}
    tool_schemas = build_tool_schemas(
        [t for t in mcp_tools if t.name not in READ_ONLY_TOOLS]
    )

    local_db = init_local_db(tool_schemas)
    rewriter = create_rewriter(local_db)
    policies_registered = False

    # derive Gate 1 requirements from DFC policies
    # SOURCES X SINK Y → Y requires FROM X
    policy_deps = parse_policy_deps(DFC_POLICIES)
    print(f"[Gate 1] Policy deps: {policy_deps}")

    # =========================================================================
    # execute_sql
    # =========================================================================
    async def _execute_sql(sql: str) -> dict:
        nonlocal policies_registered
        # policy_deps is read-only — no nonlocal needed

        print(f"\n[execute_sql] SQL:\n{sql}\n")

        sink_table = get_sink_table(sql)
        if not sink_table:
            return {"status": "error", "error": "Not a valid INSERT statement."}

        schema = tool_schemas.get(sink_table)
        if not schema:
            return {
                "status": "error",
                "error": f"Unknown table '{sink_table}'. Available: {list(tool_schemas.keys())}"
            }

        # ── Gate 1 — source table validation ─────────────────────────────────
        gate1_error = gate1_validate(sql, policy_deps)
        if gate1_error:
            print(f"[Gate 1] FAILED — {gate1_error}")
            return {"status": "error", "error": gate1_error}
        print(f"[Gate 1] PASSED — {sink_table}")

        # inject call_id
        call_id   = f"call_{uuid.uuid4().hex[:8]}"
        sql_final = inject_call_id(sql, call_id)
        print(f"[execute_sql] Injected call_id:\n{sql_final}\n")

        # ── DFC (SQLRewriter) ─────────────────────────────────────────────────
        if not policies_registered:
            policies_registered = register_policies(rewriter, DFC_POLICIES)

        for source_table in get_source_tables(sql_final):
            deduplicate_source_table(local_db, source_table)

        try:
            count_before = local_db.execute(f"SELECT COUNT(*) FROM {sink_table}").fetchone()[0]

            if policies_registered:
                rewritten = rewriter.transform_query(sql_final)
                print(f"[SQLRewriter] Rewritten SQL:\n{rewritten}\n")
                rewriter.execute(sql_final)
            else:
                local_db.execute(sql_final)

            count_after = local_db.execute(f"SELECT COUNT(*) FROM {sink_table}").fetchone()[0]

            if count_after == count_before:
                print(f"[SQLRewriter] BLOCKED — {sink_table} unchanged")
                return {
                    "status": "blocked",
                    "reason": (
                        "BLOCKED: DFC policy violation. "
                        "Meal receipts cannot be 100% business use. "
                        "Use a lower percentage e.g. 0.5."
                    )
                }

            print(f"[SQLRewriter] PASSED — {sink_table} {count_before} → {count_after}")

        except ValueError as e:
            if "KILLing due to dfc policy violation" in str(e):
                print(f"[SQLRewriter] KILL triggered")
                return {
                    "status": "blocked",
                    "reason": "BLOCKED: Meal receipts cannot be 100% business use. Use a lower percentage e.g. 0.5."
                }
            return {"status": "error", "error": str(e)}

        except Exception as e:
            return {"status": "error", "error": str(e)}

        # ── extract params → call MCP tool ────────────────────────────────────
        input_row = fetch_row(local_db, sink_table, call_id)
        if not input_row:
            return {"status": "error", "error": f"Could not read row from {sink_table}"}

        args = {col: input_row[col] for col in schema.input_columns if col in input_row}

        mcp_tool = mcp_tool_map.get(schema.tool_name)
        if not mcp_tool:
            return {"status": "error", "error": f"MCP tool '{schema.tool_name}' not found"}

        print(f"[execute_sql] → {schema.tool_name}({args})")
        raw_response = await mcp_tool.ainvoke(args)
        response     = parse_mcp_response(raw_response)
        print(f"[execute_sql] ← {schema.tool_name} response: {response}\n")

        # ── store response in output table ────────────────────────────────────
        schema.store_output(local_db, call_id, response)
        print(f"[execute_sql] Stored response in {schema.output_table}")

        if not policies_registered:
            policies_registered = register_policies(rewriter, DFC_POLICIES)
            if policies_registered:
                print(f"[DFC] Rewriter ready with {len(rewriter._policies)} policies")

        return {"status": "ok", "tool": schema.tool_name, "result": response}

    # =========================================================================
    # LANGGRAPH
    # =========================================================================
    @lc_tool
    async def execute_sql(sql: str) -> str:
        """Execute INSERT SQL to declare tool intent. Use available input tables."""
        result = await _execute_sql(sql)
        return json.dumps(result)

    pass_through_tools = [
        tool for tool in mcp_tools
        if f"{tool.name}_in" not in tool_schemas
    ]
    for t in pass_through_tools:
        print(f"[Schema] Pass-through tool: {t.name}")

    llm_with_tools = llm.bind_tools([execute_sql] + pass_through_tools, parallel_tool_calls=False)

    async def agent_node(state: AgentState):
        return {"messages": [await llm_with_tools.ainvoke(state["messages"])]}

    async def tools_node(state: AgentState):
        last     = state["messages"][-1]
        messages = []
        failed   = False

        for tc in getattr(last, "tool_calls", []):
            name = tc["name"]
            if name in ("execute_sql", "_execute_sql"):
                result  = await _execute_sql(tc["args"].get("sql", ""))
                content = json.dumps(result)
                # Gate 1 errors are corrective — don't fail, let LLM retry
                # Only fail on blocked (DFC violation)
                if result.get("status") == "blocked":
                    failed = True
            else:
                mcp_tool = mcp_tool_map.get(name)
                if mcp_tool:
                    raw     = await mcp_tool.ainvoke(tc["args"])
                    content = json.dumps(parse_mcp_response(raw))
                else:
                    content = json.dumps({"status": "error", "error": f"Unknown tool '{name}'"})
                    failed  = True

            messages.append(ToolMessage(content=content, tool_call_id=tc["id"]))

        return {"messages": messages, "request_failed": failed}

    def route_after_agent(state: AgentState) -> str:
        if state.get("request_failed"):
            return END
        last = state["messages"][-1]
        if not getattr(last, "tool_calls", None):
            return END
        return "tools"

    def route_after_tools(state: AgentState) -> str:
        if state.get("request_failed"):
            return "agent"
        return "agent"

    graph = StateGraph(AgentState)
    graph.add_node("agent", agent_node)
    graph.add_node("tools", tools_node)
    graph.add_edge(START, "agent")
    graph.add_conditional_edges("agent", route_after_agent, {"tools": "tools", END: END})
    graph.add_conditional_edges("tools", route_after_tools, {"agent": "agent"})

    app = graph.compile(checkpointer=MemorySaver())

    system_prompt = build_system_prompt(tool_schemas)
    print(f"\nSystem prompt:\n{system_prompt}\n")
    print("Tax Agent ready. Type 'quit' to exit.\n")

    while True:
        user_input = input("You: ").strip()
        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit"):
            print("Bye!")
            break

        try:
            result = await app.ainvoke(
                {
                    "messages": [SystemMessage(content=system_prompt), HumanMessage(content=user_input)],
                    "request_failed": False,
                },
                config={"configurable": {"thread_id": f"req_{uuid.uuid4().hex[:8]}"}},
            )

            final = next(
                (m for m in reversed(result["messages"]) if isinstance(m, AIMessage) and m.content),
                None,
            )
            print(f"Agent: {final.content}\n" if final else "Agent: Done.\n")

        except Exception as e:
            print(f"Error: {e}\n")


if __name__ == "__main__":
    asyncio.run(main())