"""Single SQL execution tool routed through SQLRewriter (1Phase)."""

from __future__ import annotations

import json
from pathlib import Path
import time
from typing import Any

import duckdb
from langchain.tools import tool
from sql_rewriter import AggregateDFCPolicy, DFCPolicy, SQLRewriter
import sqlglot
from sqlglot import exp


def _split_policy_messages(value: str) -> list[str]:
    return [part.strip() for part in value.split(" | ") if part.strip()]


class SQLExecutionHarness:
    """Encapsulates DB connection and SQLRewriter-backed execution."""

    def __init__(self, db_path: str, max_result_rows: int) -> None:
        self.conn = duckdb.connect(database=db_path)
        self.rewriter = SQLRewriter(conn=self.conn)
        self.max_result_rows = max_result_rows
        self.policy_mode = "observe"
        self.invalid_table_names = ["expenses"]

    def close(self) -> None:
        self.rewriter.close()

    def set_policy_mode(self, mode: str, invalid_table_names: list[str] | None = None) -> None:
        """Set policy handling mode for INVALIDATE_MESSAGE outputs.

        Modes:
        - observe: keep invalid rows and report violations only.
        - enforce: remove invalid rows and report violations to the model.
        """
        normalized = mode.strip().lower()
        if normalized not in {"observe", "enforce"}:
            raise ValueError("policy mode must be 'observe' or 'enforce'")
        self.policy_mode = normalized
        if invalid_table_names is not None:
            self.invalid_table_names = invalid_table_names

    def _enforce_expenses_insert_shape(self, sql: str) -> None:
        """Enforce INSERT INTO expenses to be INSERT ... SELECT ... FROM receipts."""
        try:
            parsed = sqlglot.parse_one(sql, read="duckdb")
        except Exception:
            # Let normal rewriter/sql execution surface parse errors.
            return

        if not isinstance(parsed, exp.Insert):
            return

        sink_table_name = None
        if isinstance(parsed.this, exp.Schema) and isinstance(parsed.this.this, exp.Table):
            sink_table_name = parsed.this.this.name.lower()
        elif isinstance(parsed.this, exp.Table):
            sink_table_name = parsed.this.name.lower()

        if sink_table_name != "expenses":
            return

        select_expr = parsed.find(exp.Select)
        if select_expr is None:
            raise ValueError(
                "INSERT INTO expenses must be an INSERT ... SELECT statement that reads from receipts. "
                "Example: INSERT INTO expenses (expense_id, receipt_id, expense_date, vendor_name, net_amount, "
                "tax_amount, gross_amount, currency_code, expense_category, deductible_pct, payment_channel, "
                "jurisdiction, billable_flag, project_tag, documentation_quality, compliance_notes, "
                "cannot_categorize_safely) "
                "SELECT receipt_id, receipt_id, tx_date, merchant, amount, 0, amount, currency, category, 100, "
                "payment_method, state || '-US', client_billable, project_code, 'HIGH', 'auto', FALSE "
                "FROM receipts;"
            )

        # invalid_string is internal to the harness and must never be set by the agent.
        if isinstance(parsed.this, exp.Schema):
            for col in parsed.this.expressions or []:
                if isinstance(col, exp.Identifier) and col.name.lower() == "invalid_string":
                    raise ValueError(
                        "Do not set invalid_string explicitly. It is internal to the harness."
                    )
                if isinstance(col, exp.Column) and str(col.name).lower() == "invalid_string":
                    raise ValueError(
                        "Do not set invalid_string explicitly. It is internal to the harness."
                    )

        for select_item in select_expr.expressions or []:
            if isinstance(select_item, exp.Alias) and select_item.alias and select_item.alias.lower() == "invalid_string":
                raise ValueError(
                    "Do not project invalid_string in INSERT INTO expenses. "
                    "invalid_string is internal to the harness."
                )
            if isinstance(select_item, exp.Column) and str(select_item.name).lower() == "invalid_string":
                raise ValueError(
                    "Do not project invalid_string in INSERT INTO expenses. "
                    "invalid_string is internal to the harness."
                )

        source_tables = {table.name.lower() for table in select_expr.find_all(exp.Table)}
        if "receipts" not in source_tables:
            raise ValueError(
                "INSERT INTO expenses must select from receipts. "
                "Use INSERT INTO ... SELECT ... FROM receipts ... ."
            )

    def register_policy(self, policy: DFCPolicy) -> None:
        """Register a pre-constructed standard DFC policy."""
        self.rewriter.register_policy(policy)

    def register_aggregate_policy(self, policy: AggregateDFCPolicy) -> None:
        """Register a pre-constructed aggregate DFC policy."""
        self.rewriter.register_policy(policy)

    def register_any_policy(self, policy: DFCPolicy | AggregateDFCPolicy) -> None:
        """Register a pre-constructed policy of either supported type."""
        self.rewriter.register_policy(policy)

    def register_policy_strings(self, policy_strings: list[str]) -> int:
        """Register DFC policies from policy strings."""
        registered = 0
        for policy_str in policy_strings:
            normalized = policy_str.strip()
            if not normalized:
                continue
            if normalized.upper().startswith("AGGREGATE "):
                policy = AggregateDFCPolicy.from_policy_str(normalized)
            else:
                policy = DFCPolicy.from_policy_str(normalized)
            self.rewriter.register_policy(policy)
            registered += 1
        return registered

    def register_policy_file(self, policy_file: str) -> int:
        """Register one policy per non-empty, non-comment line from a file."""
        lines = Path(policy_file).read_text(encoding="utf-8").splitlines()
        policies = []
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            policies.append(stripped)
        return self.register_policy_strings(policies)

    def _get_table_columns(self, table_name: str) -> list[str]:
        try:
            rows = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        except Exception:
            return []
        return [str(name) for _cid, name, *_rest in rows]

    def _collect_and_optionally_remove_invalid_rows(self) -> list[dict[str, Any]]:
        """Collect invalid rows from configured tables and remove them in enforce mode."""
        violations: list[dict[str, Any]] = []
        for table_name in self.invalid_table_names:
            table_columns = self._get_table_columns(table_name)
            lower_columns = [col.lower() for col in table_columns]
            if "invalid_string" not in lower_columns:
                continue
            invalid_idx = lower_columns.index("invalid_string")
            invalid_rows = self.conn.execute(
                f"SELECT * FROM {table_name} WHERE COALESCE(invalid_string, '') <> ''"
            ).fetchall()
            if not invalid_rows:
                continue
            violations.extend(
                [
                    {
                        "table": table_name,
                        "policy_messages": _split_policy_messages(str(row[invalid_idx])),
                    }
                    for row in invalid_rows
                ]
            )
            if self.policy_mode == "enforce":
                self.conn.execute(
                    f"DELETE FROM {table_name} WHERE COALESCE(invalid_string, '') <> ''"
                )
        return violations

    def execute_sql_1phase(self, sql: str) -> str:
        """Execute SQL by rewriting through SQLRewriter in 1Phase mode."""
        rewritten_sql = ""
        rewrite_time_ms = 0.0
        try:
            self._enforce_expenses_insert_shape(sql)

            rewrite_start = time.perf_counter()
            rewritten_sql = self.rewriter.transform_query(sql, use_two_phase=False)
            rewrite_time_ms = (time.perf_counter() - rewrite_start) * 1000.0

            exec_start = time.perf_counter()
            cursor = self.conn.execute(rewritten_sql)
            exec_time_ms = (time.perf_counter() - exec_start) * 1000.0
            rows = cursor.fetchall()
            columns = [desc[0] for desc in (cursor.description or [])]

            inline_violations: list[dict[str, Any]] = []
            lower_columns = [col.lower() for col in columns]
            if "invalid_string" in lower_columns:
                invalid_idx = lower_columns.index("invalid_string")
                kept_rows = []
                for row in rows:
                    invalid_message = row[invalid_idx]
                    if invalid_message not in (None, ""):
                        inline_violations.append(
                            {
                                "table": "<result_set>",
                                "policy_messages": _split_policy_messages(str(invalid_message)),
                            }
                        )
                        if self.policy_mode == "observe":
                            kept_rows.append(row)
                    else:
                        kept_rows.append(row)
                rows = kept_rows

                # Keep invalid_string internal to harness; never expose it to the model.
                columns = [col for idx, col in enumerate(columns) if idx != invalid_idx]
                rows = [
                    tuple(value for idx, value in enumerate(row) if idx != invalid_idx)
                    for row in rows
                ]

            table_violations = self._collect_and_optionally_remove_invalid_rows()
            all_violations = [*inline_violations, *table_violations]

            rows = rows[: self.max_result_rows]
            payload = {
                "rewritten_sql": rewritten_sql,
                "rewrite_time_ms": rewrite_time_ms,
                "exec_time_ms": exec_time_ms,
                "policy_mode": self.policy_mode,
                "policy_violation_count": len(all_violations),
                "policy_violations": all_violations[: self.max_result_rows],
                "row_count_returned": len(rows),
                "columns": columns,
                "rows": rows,
            }
            if self.policy_mode == "enforce" and all_violations:
                payload["policy_feedback"] = (
                    f"{len(all_violations)} row(s) violated policy and were removed. "
                    "Retry with corrected SQL."
                )
            return json.dumps(payload, default=str)
        except Exception as exc:
            payload = {
                "rewritten_sql": rewritten_sql,
                "rewrite_time_ms": rewrite_time_ms,
                "exec_time_ms": 0.0,
                "policy_mode": self.policy_mode,
                "policy_violation_count": 0,
                "policy_violations": [],
                "row_count_returned": 0,
                "columns": [],
                "rows": [],
                "error": str(exc),
            }
            return json.dumps(payload, default=str)


def make_execute_sql_tool(sql_harness: SQLExecutionHarness):
    """Create the single LangChain tool exposed to the model."""

    @tool
    def execute_sql(sql: str) -> str:
        """Execute SQL through SQLRewriter using 1Phase and return JSON results."""
        return sql_harness.execute_sql_1phase(sql)

    return execute_sql
