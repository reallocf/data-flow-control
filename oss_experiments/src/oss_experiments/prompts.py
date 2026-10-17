"""Prompt builders and JSON parsing for LLM policy validation."""

from __future__ import annotations

import json
import re
from typing import Any

INCLUDED_CONTEXT_QUERY_AND_RESULTS = "query_and_results"
INCLUDED_CONTEXT_QUERY_RESULTS_AND_INPUT = "query_and_results_and_input"
INCLUDED_CONTEXTS = (
    INCLUDED_CONTEXT_QUERY_AND_RESULTS,
    INCLUDED_CONTEXT_QUERY_RESULTS_AND_INPUT,
)


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


def build_nonce_block(
    nonce: str,
    *,
    query_set: str,
    query_id: str,
    batch_id: int,
    rows_selected: int,
) -> str:
    return (
        f"Run nonce: {nonce}\n"
        f"Query set: {query_set}\n"
        f"Query id: {query_id}\n"
        f"Batch id: {batch_id}\n"
        f"Rows selected: {rows_selected}\n"
        "Treat this evaluation as independent from any prior request.\n\n"
    )


def build_llm_prompt(
    *,
    nonce: str,
    query_set: str,
    query_id: str,
    batch_id: int,
    rows_selected: int,
    sql: str,
    policy_descriptions: list[str],
    included_context: str,
    expense_columns: list[str],
    expense_rows: list[tuple[Any, ...]],
    receipt_columns: list[str] | None = None,
    receipt_rows: list[tuple[Any, ...]] | None = None,
    input_relations: list[tuple[str, list[str], list[tuple[Any, ...]]]] | None = None,
) -> str:
    policies_block = "\n".join(f"{idx + 1}. {desc}" for idx, desc in enumerate(policy_descriptions))
    nonce_block = build_nonce_block(
        nonce,
        query_set=query_set,
        query_id=query_id,
        batch_id=batch_id,
        rows_selected=rows_selected,
    )
    expense_json = format_result_rows(expense_columns, expense_rows)
    parts = [
        nonce_block,
        "You are validating whether query output would violate active data policies.\n",
        "Decide if ANY policy is violated.\n",
        'Return JSON only: {"violates_policy": true|false}\n\n',
        "SQL Query:\n",
        sql,
        "\n\nPolicies:\n",
        policies_block,
        "\n\nCandidate Expenses rows:\n",
        expense_json,
    ]
    if included_context == INCLUDED_CONTEXT_QUERY_RESULTS_AND_INPUT:
        relations = input_relations
        if relations is None:
            relations = [("Receipts", receipt_columns or [], receipt_rows or [])]
        for relation_name, relation_columns, relation_rows in relations:
            relation_json = format_result_rows(relation_columns, relation_rows)
            parts.extend(["\n\n", relation_name, " input relation:\n", relation_json])
    return "".join(parts)
