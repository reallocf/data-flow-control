"""Physical baseline implementation using SmokedDuck lineage."""

import time
from typing import Any, Optional

import duckdb
from sql_rewriter import DFCPolicy

from .physical_rewriter import rewrite_query_physical
from .smokedduck_helper import enable_lineage, is_smokedduck_available


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

        # Use physical rewriter to get base query and filter query template
        base_query, _filter_query_template, is_aggregation = rewrite_query_physical(
            query=query,
            policy=policy
        )

        # Execute base query with lineage tracking
        start = time.perf_counter()

        # Execute query - SmokedDuck should capture lineage automatically
        cursor = conn.execute(base_query)
        base_results = cursor.fetchall()

        # Get column names
        column_names = [desc[0] for desc in cursor.description] if cursor.description else []

        # Rebuild filter query with actual column names if we couldn't determine them from parsing
        # This handles SELECT * and complex expressions
        from .physical_rewriter import build_filter_query
        actual_filter_query_template = build_filter_query(
            temp_table_name="{temp_table_name}",
            constraint=policy.constraint,
            source_table=policy.source,
            column_names=column_names,
            is_aggregation=is_aggregation
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

            # Apply policy filtering using the rewriter's filter query with actual column names
            filtered_query = actual_filter_query_template.format(temp_table_name=temp_table_name)

            # The filter query SQL (provenance query that evaluates final result)
            filter_query_sql = filtered_query

            filtered_cursor = conn.execute(filtered_query)
            filtered_results = filtered_cursor.fetchall()
        else:
            filtered_query = actual_filter_query_template.format(temp_table_name=temp_table_name)
            filter_query_sql = filtered_query
            filtered_results = []

        execution_time = (time.perf_counter() - start) * 1000.0

        # Clean up temp table
        try:
            conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        except:
            pass
        try:
            # Also try dropping as temp table
            conn.execute(f"DROP TEMP TABLE IF EXISTS {temp_table_name}")
        except:
            pass

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
