"""Mode-aware facade for tax receipt dataset and query catalog."""

from __future__ import annotations

import time
from typing import Any

from data_flow_control import dfc
from data_flow_control.adapters.kill import KILL_MESSAGE
import duckdb  # noqa: TC002

from oss_experiments import data_setup_agg, data_setup_select
from oss_experiments.data_setup_agg import MERCHANT_NAMES
from oss_experiments.data_setup_common import (
    DEFAULT_BATCH_ROW_COUNT,
    DEFAULT_INPUT_ROW_COUNT,
    DEFAULT_NUM_BATCHES,
    EXPERIMENT_MODE_AGG,
    EXPERIMENT_MODE_SELECT,
    EXPERIMENT_MODES,
    FAIL_BIZ_USE,
    PASS_BIZ_USE,
    QUERY_SET_HUNDRED_ROW,
    QUERY_SET_ONE_ROW,
    QUERY_SET_ROW_COUNTS,
    QUERY_SET_TEN_ROW,
    QUERY_SETS,
    QueryRecord,
    default_dataset_label,
    normalize_experiment_mode,
)
from oss_experiments.policies import register_policy_set

__all__ = [
    "DEFAULT_BATCH_ROW_COUNT",
    "DEFAULT_INPUT_ROW_COUNT",
    "DEFAULT_NUM_BATCHES",
    "EXPERIMENT_MODES",
    "EXPERIMENT_MODE_AGG",
    "EXPERIMENT_MODE_SELECT",
    "FAIL_BIZ_USE",
    "MERCHANT_NAMES",
    "PASS_BIZ_USE",
    "QUERY_SETS",
    "QUERY_SET_HUNDRED_ROW",
    "QUERY_SET_ONE_ROW",
    "QUERY_SET_ROW_COUNTS",
    "QUERY_SET_TEN_ROW",
    "QueryRecord",
    "build_query_catalog",
    "default_dataset_label",
    "evaluate_dfc_truth",
    "execute_raw_candidate_rows",
    "fetch_all_input_relations",
    "fetch_all_receipt_rows",
    "fetch_input_receipt_rows",
    "normalize_experiment_mode",
    "reset_dataset",
]

_MODE_MODULES = {
    EXPERIMENT_MODE_AGG: data_setup_agg,
    EXPERIMENT_MODE_SELECT: data_setup_select,
}


def _module_for_mode(mode: str):
    return _MODE_MODULES[normalize_experiment_mode(mode)]


def build_query_catalog(
    *,
    mode: str = EXPERIMENT_MODE_AGG,
    batch_row_count: int = DEFAULT_BATCH_ROW_COUNT,
    num_batches: int = DEFAULT_NUM_BATCHES,
) -> list[QueryRecord]:
    return _module_for_mode(mode).build_query_catalog(
        batch_row_count=batch_row_count,
        num_batches=num_batches,
    )


def reset_dataset(
    conn: duckdb.DuckDBPyConnection,
    *,
    mode: str = EXPERIMENT_MODE_AGG,
    input_row_count: int = DEFAULT_INPUT_ROW_COUNT,
    batch_row_count: int = DEFAULT_BATCH_ROW_COUNT,
) -> None:
    _module_for_mode(mode).reset_dataset(
        conn,
        input_row_count=input_row_count,
        batch_row_count=batch_row_count,
    )


def execute_raw_candidate_rows(
    conn: duckdb.DuckDBPyConnection,
    query: QueryRecord,
    *,
    mode: str | None = None,
    input_row_count: int = DEFAULT_INPUT_ROW_COUNT,
    batch_row_count: int = DEFAULT_BATCH_ROW_COUNT,
) -> tuple[list[str], list[tuple[Any, ...]]]:
    resolved_mode = mode or query.experiment_mode
    return _module_for_mode(resolved_mode).execute_raw_candidate_rows(
        conn,
        query,
        input_row_count=input_row_count,
        batch_row_count=batch_row_count,
    )


def fetch_all_receipt_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    mode: str = EXPERIMENT_MODE_AGG,
) -> tuple[list[str], list[tuple[Any, ...]]]:
    return _module_for_mode(mode).fetch_all_receipt_rows(conn)


def fetch_all_input_relations(
    conn: duckdb.DuckDBPyConnection,
    *,
    mode: str = EXPERIMENT_MODE_AGG,
) -> list[tuple[str, list[str], list[tuple[Any, ...]]]]:
    return _module_for_mode(mode).fetch_all_input_relations(conn)


def fetch_input_receipt_rows(
    conn: duckdb.DuckDBPyConnection,
    query: QueryRecord,
) -> tuple[list[str], list[tuple[Any, ...]]]:
    return _module_for_mode(query.experiment_mode).fetch_input_receipt_rows(conn, query)


def evaluate_dfc_truth(
    raw_conn: duckdb.DuckDBPyConnection,
    query: QueryRecord,
    *,
    mode: str | None = None,
    policy_ids: list[str] | None = None,
    input_row_count: int = DEFAULT_INPUT_ROW_COUNT,
    batch_row_count: int = DEFAULT_BATCH_ROW_COUNT,
) -> dict[str, Any]:
    resolved_mode = mode or query.experiment_mode
    _module_for_mode(resolved_mode).reset_dataset(
        raw_conn,
        input_row_count=input_row_count,
        batch_row_count=batch_row_count,
    )
    dfc_conn = dfc(raw_conn)
    register_policy_set(dfc_conn, policy_ids, mode=resolved_mode)

    rewrite_start = time.perf_counter()
    rewritten_sql = dfc_conn.transform_query(query.sql)
    rewrite_ms = (time.perf_counter() - rewrite_start) * 1000.0

    dfc_killed = False
    exec_start = time.perf_counter()
    try:
        dfc_conn.execute(query.sql)
    except Exception as exc:
        if KILL_MESSAGE not in str(exc):
            raise
        dfc_killed = True
    exec_ms = (time.perf_counter() - exec_start) * 1000.0

    ground_truth_violation = dfc_killed
    matches = dfc_killed == query.expected_dfc_kill
    if not matches:
        raise AssertionError(
            f"DFC outcome mismatch for {query.key}: "
            f"dfc_killed={dfc_killed}, expected={query.expected_dfc_kill}"
        )

    return {
        "ground_truth_violation": ground_truth_violation,
        "dfc_killed": dfc_killed,
        "expected_dfc_kill": query.expected_dfc_kill,
        "expected_meal_limit_kill": query.expected_meal_limit_kill,
        "dfc_truth_matches_expected": matches,
        "rewritten_sql": rewritten_sql,
        "dfc_rewrite_time_ms": rewrite_ms,
        "dfc_exec_time_ms": exec_ms,
    }
