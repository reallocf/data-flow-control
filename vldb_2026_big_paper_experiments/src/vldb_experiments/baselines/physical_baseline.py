"""Physical baseline implementation using the DuckDB lineage extension."""

import contextlib
import time
from typing import Any, Optional

import duckdb
from sql_rewriter import DFCPolicy
from sql_rewriter.sqlglot_utils import get_column_name, get_table_name_from_column
import sqlglot
from sqlglot import exp

from .physical_rewriter import rewrite_query_physical
from .smokedduck_helper import (
    build_lineage_query,
    disable_lineage,
    enable_lineage,
    is_smokedduck_available,
)

_TPCH_COLUMN_PREFIX = {
    "lineitem": "l_",
    "orders": "o_",
    "customer": "c_",
    "part": "p_",
    "supplier": "s_",
    "partsupp": "ps_",
    "nation": "n_",
    "region": "r_",
}


def _remove_condition(expr: exp.Expression, target_sqls: set[str]) -> exp.Expression | None:
    if isinstance(expr, exp.And):
        left = _remove_condition(expr.this, target_sqls)
        right = _remove_condition(expr.expression, target_sqls)
        if left and right:
            return exp.And(this=left, expression=right)
        return left or right
    expr_sql = expr.sql(dialect="duckdb")
    if expr_sql in target_sqls:
        return None
    return expr


def _rewrite_exists_to_join_base(parsed: exp.Select, policy_source: str) -> str | None:
    where_expr = parsed.args.get("where")
    if not where_expr:
        return None

    exists_nodes = list(where_expr.find_all(exp.Exists))
    if not exists_nodes:
        return None

    exists_node = exists_nodes[0]
    subquery = exists_node.this
    if isinstance(subquery, exp.Subquery):
        subquery_select = subquery.this
    elif isinstance(subquery, exp.Select):
        subquery_select = subquery
    else:
        return None

    subquery_from = subquery_select.args.get("from_")
    if not subquery_from:
        return None

    has_policy_source = any(
        hasattr(table, "name")
        and table.name
        and table.name.lower() == policy_source.lower()
        for table in subquery_from.find_all(exp.Table)
    )
    if not has_policy_source:
        return None

    subquery_where = subquery_select.args.get("where")
    if not subquery_where:
        return None

    correlation_expr = None
    policy_col = None
    outer_col = None
    prefix = _TPCH_COLUMN_PREFIX.get(policy_source.lower())

    def _is_policy_column(col: exp.Column) -> bool:
        table_name = get_table_name_from_column(col)
        if table_name:
            return table_name == policy_source.lower()
        if prefix:
            return get_column_name(col).lower().startswith(prefix)
        return False

    for eq in subquery_where.find_all(exp.EQ):
        left = eq.this
        right = eq.expression
        if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
            continue
        left_is_policy = _is_policy_column(left)
        right_is_policy = _is_policy_column(right)
        if left_is_policy and not right_is_policy:
            correlation_expr = eq
            policy_col = left
            outer_col = right
            break
        if right_is_policy and not left_is_policy:
            correlation_expr = eq
            policy_col = right
            outer_col = left
            break

    if not correlation_expr or not policy_col or not outer_col:
        return None

    remaining_subquery_where = _remove_condition(
        subquery_where.this,
        {correlation_expr.sql(dialect="duckdb")},
    )

    parsed_copy = sqlglot.parse_one(parsed.sql(dialect="duckdb"), read="duckdb")
    if not isinstance(parsed_copy, exp.Select):
        return None

    join_on = sqlglot.parse_one(correlation_expr.sql(dialect="duckdb"), read="duckdb")
    if remaining_subquery_where is not None:
        join_on = exp.and_(
            join_on,
            sqlglot.parse_one(remaining_subquery_where.sql(dialect="duckdb"), read="duckdb"),
        )

    join = exp.Join(
        this=exp.to_table(policy_source),
        on=join_on,
        join_type="INNER",
    )

    joins = list(parsed_copy.args.get("joins", []))
    joins.append(join)
    parsed_copy.set("joins", joins)

    new_where_expr = _remove_condition(
        parsed_copy.args.get("where").this,
        {exists_node.sql(dialect="duckdb")},
    )
    if new_where_expr is None:
        parsed_copy.set("where", None)
    else:
        parsed_copy.set("where", exp.Where(this=new_where_expr))

    outer_col_expr = sqlglot.parse_one(outer_col.sql(dialect="duckdb"), read="duckdb")
    distinct_outer = exp.Distinct(expressions=[outer_col_expr.copy()])
    for expr in parsed_copy.expressions:
        if isinstance(expr, exp.Alias):
            inner = expr.this
            if isinstance(inner, exp.Count) and not inner.args.get("distinct"):
                expr.set(
                    "this",
                    exp.Count(this=distinct_outer.copy()),
                )
        elif isinstance(expr, exp.Count) and not expr.args.get("distinct"):
            expr.replace(exp.Count(this=distinct_outer.copy()))

    return parsed_copy.sql(dialect="duckdb")


def _rewrite_in_to_join_base(parsed: exp.Select, policy_source: str) -> str | None:
    where_expr = parsed.args.get("where")
    if not where_expr:
        return None

    in_nodes = list(where_expr.find_all(exp.In))
    if not in_nodes:
        return None

    for in_node in in_nodes:
        query_expr = in_node.args.get("query")
        if isinstance(query_expr, exp.Subquery):
            subquery_select = query_expr.this
        elif isinstance(query_expr, exp.Select):
            subquery_select = query_expr
        else:
            continue

        subquery_from = subquery_select.args.get("from_")
        if not subquery_from:
            continue
        has_policy_source = any(
            hasattr(table, "name")
            and table.name
            and table.name.lower() == policy_source.lower()
            for table in subquery_from.find_all(exp.Table)
        )
        if not has_policy_source:
            continue

        if not subquery_select.expressions:
            continue

        subquery_col_expr = subquery_select.expressions[0]
        if isinstance(subquery_col_expr, exp.Alias):
            subquery_col_name = subquery_col_expr.alias_or_name
        elif isinstance(subquery_col_expr, exp.Column):
            subquery_col_name = get_column_name(subquery_col_expr)
        else:
            subquery_col_name = subquery_col_expr.sql(dialect="duckdb")

        parsed_copy = sqlglot.parse_one(parsed.sql(dialect="duckdb"), read="duckdb")
        if not isinstance(parsed_copy, exp.Select):
            return None

        subquery_select_copy = sqlglot.parse_one(subquery_select.sql(dialect="duckdb"), read="duckdb")
        if not isinstance(subquery_select_copy, exp.Select):
            return None

        subquery_alias = exp.TableAlias(this=exp.to_identifier("in_subquery"))
        subquery_node = exp.Subquery(this=subquery_select_copy, alias=subquery_alias)

        join_left = sqlglot.parse_one(in_node.this.sql(dialect="duckdb"), read="duckdb")
        join_right = exp.Column(
            this=exp.Identifier(this=subquery_col_name),
            table=exp.Identifier(this="in_subquery"),
        )
        join_on = exp.EQ(this=join_left, expression=join_right)
        join = exp.Join(this=subquery_node, on=join_on, join_type="INNER")

        joins = list(parsed_copy.args.get("joins", []))
        joins.append(join)
        parsed_copy.set("joins", joins)

        new_where_expr = _remove_condition(
            parsed_copy.args.get("where").this,
            {in_node.sql(dialect="duckdb")},
        )
        if new_where_expr is None:
            parsed_copy.set("where", None)
        else:
            parsed_copy.set("where", exp.Where(this=new_where_expr))

        for col in parsed_copy.find_all(exp.Column):
            if col.table:
                continue
            if get_column_name(col).lower() != subquery_col_name.lower():
                continue
            current = col
            in_subquery = False
            while hasattr(current, "parent"):
                if isinstance(current.parent, exp.Subquery):
                    in_subquery = True
                    break
                current = current.parent
            if not in_subquery:
                col.set("table", exp.Identifier(this=policy_source))

        return parsed_copy.sql(dialect="duckdb")

    return None


def _rewrite_base_query_for_lineage(query: str, policy_source: str) -> str:
    try:
        parsed = sqlglot.parse_one(query, read="duckdb")
    except Exception:
        return query
    if not isinstance(parsed, exp.Select):
        return query

    exists_rewrite = _rewrite_exists_to_join_base(parsed, policy_source)
    if exists_rewrite:
        print(
            "[physical_baseline] Rewrote base query for lineage:\n"
            f"{exists_rewrite}",
            flush=True,
        )
        return exists_rewrite

    in_rewrite = _rewrite_in_to_join_base(parsed, policy_source)
    if in_rewrite:
        print(
            "[physical_baseline] Rewrote base query for lineage:\n"
            f"{in_rewrite}",
            flush=True,
        )
        return in_rewrite

    return query


def _execute_query_physical_impl(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    policy: "DFCPolicy | list[DFCPolicy]",
) -> tuple[list[Any], dict[str, float], Optional[str], Optional[str], Optional[str]]:
    """Execute query using physical baseline approach (DuckDB lineage extension).

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
        ImportError: If lineage extension is not available (lineage is REQUIRED)
        ValueError: If policy does not have a source specified
    """
    # Lineage extension is REQUIRED - fail if not available
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

        policies = policy if isinstance(policy, list) else [policy]
        if not policies:
            raise ValueError("policies must contain at least one DFCPolicy instance")
        base_policy = policies[0]
        for pol in policies:
            if not pol.sources:
                raise ValueError("policy must have sources specified")
            if len(pol.sources) != 1:
                raise ValueError("physical baseline supports a single source table per policy")
            if pol.sources[0].lower() != base_policy.sources[0].lower():
                raise ValueError("physical baseline requires all policies to share the same source table")

        base_query = _rewrite_base_query_for_lineage(query, base_policy.sources[0]).rstrip().rstrip(";")

        # Execute base query with lineage tracking
        total_start = time.perf_counter()
        base_capture_start = time.perf_counter()

        # Execute query - lineage should capture the exact result semantics (including ORDER/LIMIT)
        cursor = conn.execute(base_query)
        base_results = cursor.fetchall()
        base_capture_time = (time.perf_counter() - base_capture_start) * 1000.0

        # Get column names
        column_names = [desc[0] for desc in cursor.description] if cursor.description else []

        # Follow lineage API flow: disable capture before reading lineage metadata.
        disable_lineage(conn)

        query_id_row = conn.execute(
            "SELECT query_id FROM lineage_meta() ORDER BY query_id DESC LIMIT 1"
        ).fetchone()
        query_id = query_id_row[0] if query_id_row else None
        if query_id is None:
            raise RuntimeError("Failed to resolve query_id from lineage_meta()")
        conn.execute(f"PRAGMA PrepareLineage({query_id})")

        # Create temp table with results
        # Use a unique table name to avoid conflicts
        import uuid
        temp_table_name = f"query_results_{uuid.uuid4().hex[:8]}"

        # The base query SQL (executed to capture lineage)
        base_query_sql = base_query

        # Lineage block queries must run with lineage disabled.

        if base_results:
            # Create table schema without re-running the full query, then insert rows in order.
            conn.execute(f"CREATE TEMP TABLE {temp_table_name} AS SELECT * FROM ({base_query}) LIMIT 0")
            placeholders = ", ".join(["?"] * len(column_names))
            conn.executemany(f"INSERT INTO {temp_table_name} VALUES ({placeholders})", base_results)
        else:
            filtered_results = []

        rewrite_start = time.perf_counter()
        lineage_query = build_lineage_query(conn, base_policy.sources[0], query_id)

        _, filter_query_template, _ = rewrite_query_physical(
            query=base_query,
            policy=policies,
            lineage_query=lineage_query,
            output_columns=column_names,
        )
        rewrite_time = (time.perf_counter() - rewrite_start) * 1000.0

        lineage_query_time = 0.0
        filtered_query = filter_query_template.format(temp_table_name=temp_table_name)
        filter_query_sql = filtered_query
        if base_results:
            lineage_query_start = time.perf_counter()
            filtered_cursor = conn.execute(filtered_query)
            filtered_results = filtered_cursor.fetchall()
            lineage_query_time = (time.perf_counter() - lineage_query_start) * 1000.0

        total_time = (time.perf_counter() - total_start) * 1000.0

        # Clean up temp table
        with contextlib.suppress(Exception):
            conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        with contextlib.suppress(Exception):
            conn.execute(f"DROP TEMP TABLE IF EXISTS {temp_table_name}")

        timing = {
            "rewrite_time_ms": rewrite_time,
            "base_capture_time_ms": base_capture_time,
            "lineage_query_time_ms": lineage_query_time,
            "runtime_time_ms": base_capture_time + lineage_query_time,
            "total_time_ms": total_time,
        }
        return filtered_results, timing, None, base_query_sql, filter_query_sql

    except Exception as e:
        return [], {}, str(e), None, None


def execute_query_physical(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    policy: "DFCPolicy | list[DFCPolicy]",
) -> tuple[list[Any], float, Optional[str], Optional[str], Optional[str]]:
    results, timing, error, base_query_sql, filter_query_sql = _execute_query_physical_impl(
        conn,
        query,
        policy,
    )
    execution_time = timing.get("total_time_ms", 0.0)
    return results, execution_time, error, base_query_sql, filter_query_sql


def execute_query_physical_detailed(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    policy: "DFCPolicy | list[DFCPolicy]",
) -> tuple[list[Any], dict[str, float], Optional[str], Optional[str], Optional[str]]:
    return _execute_query_physical_impl(conn, query, policy)


def execute_precomputed_query_physical_detailed(
    conn: duckdb.DuckDBPyConnection,
    base_query: str,
    filter_query_template: str,
    policy_source: str,
) -> tuple[list[Any], dict[str, float], Optional[str], Optional[str], Optional[str]]:
    """Execute a physical-baseline query with precomputed SQL templates.

    This bypasses runtime query rewriting. `filter_query_template` must accept
    `{temp_table_name}` and `{lineage_query}` placeholders.
    """
    is_smokedduck_available()

    try:
        enable_lineage(conn)
        with contextlib.suppress(Exception):
            conn.execute("PRAGMA clear_lineage")
        enable_lineage(conn)

        base_query_sql = base_query.rstrip().rstrip(";")

        total_start = time.perf_counter()
        base_capture_start = time.perf_counter()
        cursor = conn.execute(base_query_sql)
        base_results = cursor.fetchall()
        base_capture_time = (time.perf_counter() - base_capture_start) * 1000.0

        column_names = [desc[0] for desc in cursor.description] if cursor.description else []

        disable_lineage(conn)

        query_id_row = conn.execute(
            "SELECT query_id FROM lineage_meta() ORDER BY query_id DESC LIMIT 1"
        ).fetchone()
        query_id = query_id_row[0] if query_id_row else None
        if query_id is None:
            raise RuntimeError("Failed to resolve query_id from lineage_meta()")
        conn.execute(f"PRAGMA PrepareLineage({query_id})")

        import uuid

        temp_table_name = f"query_results_{uuid.uuid4().hex[:8]}"

        if base_results:
            conn.execute(f"CREATE TEMP TABLE {temp_table_name} AS SELECT * FROM ({base_query_sql}) LIMIT 0")
            placeholders = ", ".join(["?"] * len(column_names))
            conn.executemany(f"INSERT INTO {temp_table_name} VALUES ({placeholders})", base_results)

        lineage_query = build_lineage_query(conn, policy_source, query_id)
        filtered_results = []
        lineage_query_time = 0.0
        filter_query_sql = filter_query_template.format(
            temp_table_name=temp_table_name,
            lineage_query=lineage_query,
        )
        if base_results:
            lineage_query_start = time.perf_counter()
            filtered_cursor = conn.execute(filter_query_sql)
            filtered_results = filtered_cursor.fetchall()
            lineage_query_time = (time.perf_counter() - lineage_query_start) * 1000.0

        total_time = (time.perf_counter() - total_start) * 1000.0

        with contextlib.suppress(Exception):
            conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        with contextlib.suppress(Exception):
            conn.execute(f"DROP TEMP TABLE IF EXISTS {temp_table_name}")

        timing = {
            "rewrite_time_ms": 0.0,
            "base_capture_time_ms": base_capture_time,
            "lineage_query_time_ms": lineage_query_time,
            "runtime_time_ms": base_capture_time + lineage_query_time,
            "total_time_ms": total_time,
        }
        return filtered_results, timing, None, base_query_sql, filter_query_sql
    except Exception as e:
        return [], {}, str(e), None, None


def execute_query_physical_simple(conn: duckdb.DuckDBPyConnection, query: str, policy: DFCPolicy) -> tuple[list[Any], float, Optional[str]]:
    """Execute query using physical baseline approach (DuckDB lineage extension).

    This is the main entry point for physical baseline. It uses the lineage
    extension to track data provenance and filter based on policy.

    Args:
        conn: DuckDB connection
        query: Original SQL query
        policy: DFCPolicy instance (must have source specified)

    Returns:
        Tuple of (results, execution_time_ms, error_message)
        Note: For SQL queries, use execute_query_physical() which returns base_query_sql and filter_query_sql

    Raises:
        ImportError: If lineage extension is not available (lineage is REQUIRED)
        ValueError: If policy does not have a source specified
    """
    # Lineage extension is REQUIRED - fail if not available
    is_smokedduck_available()

    # Use the full physical baseline implementation
    results, execution_time, error, _, _ = execute_query_physical(conn, query, policy)
    return results, execution_time, error
