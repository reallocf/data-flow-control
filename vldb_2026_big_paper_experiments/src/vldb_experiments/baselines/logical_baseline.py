"""Logical baseline implementation using CTE-based rewriting."""

from typing import Any

import duckdb
from shared_sql_utils import combine_constraints_balanced
from sql_rewriter import DFCPolicy

from .logical_rewriter import rewrite_query_with_cte


def rewrite_query_logical(query: str, policy: DFCPolicy) -> str:
    """Rewrite query using logical baseline approach (CTE-based).

    This approach:
    1. Creates a CTE that includes the base query plus policy columns
    2. Filters the CTE results based on the policy constraint

    Args:
        query: Original SQL query
        policy: DFCPolicy instance (must have source specified)

    Returns:
        Rewritten query string
    """
    # Determine if query is aggregation
    import sqlglot
    parsed = sqlglot.parse_one(query, read="duckdb")
    has_aggregations = False
    if isinstance(parsed, sqlglot.exp.Select):
        # Check for aggregation functions
        for expr in parsed.expressions:
            if isinstance(expr, sqlglot.exp.Alias):
                expr = expr.this
            # Check if expression is an aggregation function
            if hasattr(expr, "is_aggregation") and expr.is_aggregation:
                has_aggregations = True
                break
            # Also check for common aggregation function names
            if hasattr(expr, "this") and hasattr(expr.this, "sql_name"):
                agg_names = ["COUNT", "SUM", "AVG", "MAX", "MIN", "STDDEV", "VARIANCE"]
                if expr.this.sql_name().upper() in agg_names:
                    has_aggregations = True
                    break
        # Check for GROUP BY
        if parsed.args.get("group"):
            has_aggregations = True

    return rewrite_query_with_cte(
        query=query,
        policy=policy,
        is_aggregation=has_aggregations
    )


def execute_query_logical(conn: duckdb.DuckDBPyConnection, query: str, policy: DFCPolicy) -> tuple[list[Any], float]:
    """Execute query using logical baseline approach.

    Args:
        conn: DuckDB connection
        query: Original SQL query
        policy: DFCPolicy instance (must have source specified)

    Returns:
        Tuple of (results, execution_time_ms)
    """
    import time

    # Rewrite query
    rewritten_query = rewrite_query_logical(query, policy)

    # Execute and time
    start = time.perf_counter()
    cursor = conn.execute(rewritten_query)
    results = cursor.fetchall()
    execution_time = (time.perf_counter() - start) * 1000.0

    return results, execution_time


def rewrite_query_logical_multi(query: str, policies: list[DFCPolicy]) -> str:
    """Rewrite query using logical baseline with multiple policies.

    Applies policies sequentially using CTE-based rewriting.

    Args:
        query: Original SQL query
        policies: List of DFCPolicy instances

    Returns:
        Rewritten query string after applying all policies
    """
    if not policies:
        return query

    sources = {tuple(policy.sources) for policy in policies}
    sinks = {policy.sink for policy in policies}
    resolutions = {policy.on_fail for policy in policies}

    if len(sources) == 1 and len(sinks) == 1 and len(resolutions) == 1:
        combined_constraint = combine_constraints_balanced(
            [policy.constraint for policy in policies]
        )
        combined_policy = DFCPolicy(
            sources=policies[0].sources,
            sink=policies[0].sink,
            constraint=combined_constraint,
            on_fail=policies[0].on_fail,
            description="combined_policy_constraints",
        )
        return rewrite_query_logical(query, combined_policy)

    rewritten_query = query
    for policy in policies:
        rewritten_query = rewrite_query_logical(rewritten_query, policy)
    return rewritten_query


def execute_query_logical_multi(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    policies: list[DFCPolicy],
) -> tuple[list[Any], float, float]:
    """Execute query using logical baseline approach with multiple policies.

    Args:
        conn: DuckDB connection
        query: Original SQL query
        policies: List of DFCPolicy instances

    Returns:
        Tuple of (results, rewrite_time_ms, exec_time_ms)
    """
    import time

    rewrite_start = time.perf_counter()
    rewritten_query = rewrite_query_logical_multi(query, policies)
    rewrite_time = (time.perf_counter() - rewrite_start) * 1000.0
    exec_start = time.perf_counter()
    cursor = conn.execute(rewritten_query)
    results = cursor.fetchall()
    exec_time = (time.perf_counter() - exec_start) * 1000.0

    return results, rewrite_time, exec_time
