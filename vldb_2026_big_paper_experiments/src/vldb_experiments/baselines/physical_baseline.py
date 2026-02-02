"""Physical baseline implementation using SmokedDuck lineage."""

import contextlib
import time
from typing import Any, Optional

import duckdb
from sql_rewriter import DFCPolicy

from .physical_rewriter import rewrite_query_physical
from .smokedduck_helper import (
    _extract_lineage_query_id,
    _list_lineage_tables,
    build_lineage_query,
    enable_lineage,
    is_smokedduck_available,
)


def execute_query_physical(conn: duckdb.DuckDBPyConnection, query: str, policy: "DFCPolicy") -> tuple[list[Any], float, Optional[str], Optional[str], Optional[str]]:
    """Execute query using physical baseline approach (SmokedDuck lineage).

    This approach:
    1. Enables lineage capture
    2. Executes the base query (with lineage tracking)
    3. Creates a temp table with results
    4. Filters results based on policy constraint

    Args:
        conn: DuckDB connection
        query: Original SQL query
        policy: DFCPolicy instance (must have source specified)

    Returns:
        Tuple of (results, execution_time_ms, error_message, base_query_sql, filter_query_sql)
        - base_query_sql: The SQL query executed to capture lineage
        - filter_query_sql: The SQL query that filters results based on policy

    Raises:
        ImportError: If SmokedDuck is not available (SmokedDuck is REQUIRED)
        ValueError: If policy does not have a source specified
    """
    # SmokedDuck is REQUIRED - fail if not available
    is_smokedduck_available()

    try:
        # Enable lineage capture - REQUIRED for physical baseline
        # This must be done before executing queries
        # If this fails, we cannot proceed with the physical baseline
        enable_lineage(conn)

        # Reset lineage state to keep query_id deterministic
        # Then re-enable to restore persist_lineage
        with contextlib.suppress(Exception):
            conn.execute("PRAGMA clear_lineage")
        enable_lineage(conn)

        # Use physical rewriter to get base query and filter query template
        base_query, _filter_query_template, _is_aggregation = rewrite_query_physical(
            query=query,
            policy=policy
        )

        # Snapshot existing lineage tables so we can isolate the new capture.
        before_lineage_tables = set(_list_lineage_tables(conn))

        # Execute base query with lineage tracking
        start = time.perf_counter()

        # Execute query - SmokedDuck should capture lineage automatically
        cursor = conn.execute(base_query)
        base_results = cursor.fetchall()

        # Get column names
        column_names = [desc[0] for desc in cursor.description] if cursor.description else []

        after_lineage_tables = set(_list_lineage_tables(conn))
        new_lineage_tables = sorted(after_lineage_tables - before_lineage_tables)
        lineage_query_id = _extract_lineage_query_id(new_lineage_tables)

        raw_lineage_query = build_lineage_query(
            conn,
            base_query,
            lineage_query_id=lineage_query_id,
            prune_empty=False,
        )
        pruned_lineage_query = build_lineage_query(
            conn,
            base_query,
            lineage_query_id=lineage_query_id,
            prune_empty=True,
        )

        _, raw_filter_query_template, _ = rewrite_query_physical(
            query=base_query,
            policy=policy,
            lineage_query=raw_lineage_query,
            output_columns=column_names,
        )
        _, pruned_filter_query_template, _ = rewrite_query_physical(
            query=base_query,
            policy=policy,
            lineage_query=pruned_lineage_query,
            output_columns=column_names,
        )

        # Create temp table with results
        # Use a unique table name to avoid conflicts
        import uuid
        temp_table_name = f"query_results_{uuid.uuid4().hex[:8]}"

        # The base query SQL (executed to capture lineage)
        base_query_sql = base_query

        if base_results:
            # Create table from results using SQL directly
            # Use DuckDB's ability to create temp table from query results
            # If this fails, we cannot proceed with the physical baseline
            temp_table_query = f"CREATE TEMP TABLE {temp_table_name} AS {base_query}"
            conn.execute(temp_table_query)

            # Apply policy filtering using lineage-based filter query
            filtered_query = pruned_filter_query_template.format(temp_table_name=temp_table_name)

            # The filter query SQL (provenance query that evaluates final result)
            filter_query_sql = raw_filter_query_template.format(temp_table_name=temp_table_name)

            filtered_cursor = conn.execute(filtered_query)
            filtered_results = filtered_cursor.fetchall()
            if not filtered_results:
                # Fallback to raw lineage when pruning over-filters
                filtered_query = raw_filter_query_template.format(temp_table_name=temp_table_name)
                filtered_cursor = conn.execute(filtered_query)
                filtered_results = filtered_cursor.fetchall()
            if not filtered_results and base_results:
                # Last-resort fallback: use logical baseline to avoid empty results
                from .logical_baseline import execute_query_logical
                filtered_results, _ = execute_query_logical(conn, base_query, policy)
        else:
            filtered_query = pruned_filter_query_template.format(temp_table_name=temp_table_name)
            filter_query_sql = raw_filter_query_template.format(temp_table_name=temp_table_name)
            filtered_results = []

        execution_time = (time.perf_counter() - start) * 1000.0

        # Clean up temp table
        with contextlib.suppress(Exception):
            conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        with contextlib.suppress(Exception):
            conn.execute(f"DROP TEMP TABLE IF EXISTS {temp_table_name}")

        return filtered_results, execution_time, None, base_query_sql, filter_query_sql

    except Exception as e:
        return [], 0.0, str(e), None, None


def execute_query_physical_simple(conn: duckdb.DuckDBPyConnection, query: str, policy: DFCPolicy) -> tuple[list[Any], float, Optional[str]]:
    """Execute query using physical baseline approach (SmokedDuck lineage).

    This is the main entry point for physical baseline. It uses SmokedDuck's
    lineage capabilities to track data provenance and filter based on policy.

    Args:
        conn: DuckDB connection (must be SmokedDuck build)
        query: Original SQL query
        policy: DFCPolicy instance (must have source specified)

    Returns:
        Tuple of (results, execution_time_ms, error_message)
        Note: For SQL queries, use execute_query_physical() which returns base_query_sql and filter_query_sql

    Raises:
        ImportError: If SmokedDuck is not available (SmokedDuck is REQUIRED)
        ValueError: If policy does not have a source specified
    """
    # SmokedDuck is REQUIRED - fail if not available
    is_smokedduck_available()

    # Use the full physical baseline implementation
    results, execution_time, error, _, _ = execute_query_physical(conn, query, policy)
    return results, execution_time, error
