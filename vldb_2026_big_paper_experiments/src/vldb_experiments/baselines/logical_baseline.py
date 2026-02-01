"""Logical baseline implementation using CTE-based rewriting."""

from typing import Any

import duckdb
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
