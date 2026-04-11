"""Shared helpers for LLM validation experiments."""

from __future__ import annotations

import json
import re
from typing import Any

from sql_rewriter import DFCPolicy, Resolution

DEFAULT_POLICY_COUNTS = [1, 2, 4, 8, 16, 32]
DEFAULT_RUNS_PER_SETTING = 5
DEFAULT_TPCH_SF = 1.0
DEFAULT_CLAUDE_MODEL = "claude-4.6-opus"
DEFAULT_GPT_MODEL = "gpt-5.2"

APPROACH_DFC_1PHASE = "dfc_1phase"
APPROACH_OPUS_QUERY_ONLY = "opus_query_only"
APPROACH_GPT_QUERY_ONLY = "gpt_query_only"
APPROACH_OPUS_QUERY_RESULTS = "opus_query_results"
APPROACH_GPT_QUERY_RESULTS = "gpt_query_results"


def policy_catalog() -> list[tuple[str, str]]:
    return [
        ("avg(lineitem.l_quantity) <= 26", "Average quantity should remain at or below 26."),
        ("avg(lineitem.l_quantity) >= 20", "Average quantity should be at least 20."),
        ("max(lineitem.l_quantity) <= 50", "No line item quantity should exceed 50 units."),
        ("min(lineitem.l_quantity) >= 1", "All line item quantities must be at least 1."),
        ("avg(lineitem.l_discount) <= 0.06", "Average discount should not exceed 6%."),
        ("max(lineitem.l_discount) <= 0.07", "No single discount should exceed 7%."),
        ("min(lineitem.l_discount) >= 0.00", "Discount should be non-negative."),
        ("avg(lineitem.l_tax) <= 0.06", "Average tax rate should stay under 6%."),
        ("max(lineitem.l_tax) <= 0.06", "No single tax rate should exceed 6%."),
        ("min(lineitem.l_tax) >= 0.00", "Tax should be non-negative."),
        ("min(lineitem.l_extendedprice) >= 0", "Extended prices should be non-negative."),
        ("avg(lineitem.l_extendedprice) <= 60000", "Average extended price should not exceed 60,000."),
        ("max(lineitem.l_extendedprice) <= 90000", "Single extended price should not exceed 90,000."),
        ("count(lineitem.l_orderkey) >= 1", "There should be at least one line item."),
        ("count(distinct lineitem.l_shipmode) <= 5", "Distinct ship modes should not exceed 5."),
        ("count(distinct lineitem.l_returnflag) <= 2", "Distinct return flags should not exceed 2."),
        ("count(distinct lineitem.l_linestatus) <= 2", "Distinct line statuses should not exceed 2."),
        ("max(lineitem.l_linenumber) <= 5", "Line numbers should stay within 1..5."),
        ("min(lineitem.l_linenumber) >= 1", "Line numbers should be at least 1."),
        ("min(lineitem.l_shipdate) >= DATE '1994-01-01'", "Ship dates should be on/after 1994-01-01."),
        ("max(lineitem.l_shipdate) <= DATE '1997-12-31'", "Ship dates should be on/before 1997-12-31."),
        ("min(lineitem.l_receiptdate) >= DATE '1994-01-01'", "Receipt dates should be on/after 1994-01-01."),
        ("max(lineitem.l_receiptdate) <= DATE '1997-12-31'", "Receipt dates should be on/before 1997-12-31."),
        ("min(lineitem.l_commitdate) >= DATE '1994-01-01'", "Commit dates should be on/after 1994-01-01."),
        ("max(lineitem.l_commitdate) <= DATE '1997-12-31'", "Commit dates should be on/before 1997-12-31."),
        ("sum(lineitem.l_discount) <= 180000", "Total discounts should stay below 180,000."),
        ("sum(lineitem.l_quantity) <= 80000000", "Total quantity should stay below 80M."),
        ("min(lineitem.l_orderkey) >= 1", "Order keys should be positive."),
        ("max(lineitem.l_orderkey) <= 3000000", "Order keys should remain within expected TPCH range."),
        ("min(lineitem.l_partkey) >= 1", "Part keys should be positive."),
        ("max(lineitem.l_partkey) <= 100000", "Part keys should remain within expected TPCH range."),
        ("max(lineitem.l_suppkey) <= 5000", "Supplier keys should remain within expected TPCH range."),
    ]


def build_policies(policy_count: int) -> list[DFCPolicy]:
    selected = policy_catalog()[:policy_count]
    return [
        DFCPolicy(
            sources=["lineitem"],
            constraint=constraint,
            on_fail=Resolution.INVALIDATE,
            description=description,
        )
        for constraint, description in selected
    ]


def message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and item.get("text"):
                parts.append(str(item["text"]))
        return "\n".join(parts).strip()
    return str(content)


def parse_violation_prediction(raw_text: str) -> bool | None:
    text = raw_text.strip()
    if not text:
        return None
    try:
        decoded = json.loads(text)
        if isinstance(decoded, dict) and isinstance(decoded.get("violates_policy"), bool):
            return bool(decoded["violates_policy"])
    except json.JSONDecodeError:
        pass

    lower = text.lower()
    match = re.search(r"\bviolates_policy\b[^a-zA-Z]*(true|false)", lower)
    if match:
        return match.group(1) == "true"
    if "true" in lower and "false" not in lower:
        return True
    if "false" in lower and "true" not in lower:
        return False
    return None


def format_result_rows(columns: list[str], rows: list[tuple[Any, ...]]) -> str:
    records = [{col: row[i] for i, col in enumerate(columns)} for row in rows]
    return json.dumps(records, default=str)


def build_run_nonce_preamble(run_nonce: str) -> str:
    return (
        "Run metadata:\n"
        f"- nonce: {run_nonce}\n"
        "- treat this evaluation as independent from any prior request\n\n"
    )


def build_query_only_prompt(
    query: str,
    policy_descriptions: list[str],
    run_nonce: str | None = None,
) -> str:
    policies_block = "\n".join([f"{idx + 1}. {desc}" for idx, desc in enumerate(policy_descriptions)])
    preamble = build_run_nonce_preamble(run_nonce) if run_nonce else ""
    return (
        f"{preamble}"
        "You are validating whether query output would violate active data policies.\n"
        "Decide if ANY policy is violated.\n"
        'Return JSON only: {"violates_policy": true|false}\n\n'
        "SQL Query:\n"
        f"{query}\n\n"
        "Policies:\n"
        f"{policies_block}\n"
    )


def build_query_results_prompt(
    query: str,
    policy_descriptions: list[str],
    sample_rows_json: str,
    run_nonce: str | None = None,
    results_label: str = "First 100 result rows:",
) -> str:
    policies_block = "\n".join([f"{idx + 1}. {desc}" for idx, desc in enumerate(policy_descriptions)])
    preamble = build_run_nonce_preamble(run_nonce) if run_nonce else ""
    return (
        f"{preamble}"
        "You are validating whether query output violates active data policies.\n"
        "Decide if ANY policy is violated based on the query and provided sample rows.\n"
        'Return JSON only: {"violates_policy": true|false}\n\n'
        "SQL Query:\n"
        f"{query}\n\n"
        "Policies:\n"
        f"{policies_block}\n\n"
        f"{results_label}\n"
        f"{sample_rows_json}\n"
    )


def default_database_specs(
    database_sfs: list[float] | None,
    base_filename_prefix: str,
) -> list[dict[str, Any]]:
    selected_sfs = database_sfs or [0.1, 1.0, 10.0]
    specs: list[dict[str, Any]] = []
    for sf in selected_sfs:
        label = f"sf{sf:g}"
        specs.append(
            {
                "label": label,
                "tpch_sf": float(sf),
                "db_path": f"./results/{base_filename_prefix}_{label}.db",
            }
        )
    return specs


def normalize_database_specs(
    database_specs: list[dict[str, Any]] | None,
    database_sfs: list[float] | None,
    base_filename_prefix: str,
) -> list[dict[str, Any]]:
    if not database_specs:
        return default_database_specs(database_sfs, base_filename_prefix)

    normalized: list[dict[str, Any]] = []
    for spec in database_specs:
        tpch_sf = float(spec["tpch_sf"])
        label = str(spec.get("label", f"sf{tpch_sf:g}"))
        db_path = str(spec.get("db_path", f"./results/{base_filename_prefix}_{label}.db"))
        normalized.append(
            {
                "label": label,
                "tpch_sf": tpch_sf,
                "db_path": db_path,
            }
        )
    return normalized
