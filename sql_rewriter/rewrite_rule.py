"""Rewrite rules for applying DFC policies to SQL queries."""

import sqlglot
from sqlglot import exp
from typing import Set

from .policy import DFCPolicy


def apply_policy_constraints_to_aggregation(
    parsed: exp.Select,
    policies: list[DFCPolicy],
    source_tables: Set[str]
) -> None:
    """Apply policy constraints to an aggregation query.
    
    Adds HAVING clauses for each policy constraint and ensures all referenced
    columns are accessible.
    
    Args:
        parsed: The parsed SELECT statement to modify.
        policies: List of policies to apply.
        source_tables: Set of source table names in the query.
    """
    for policy in policies:
        # Copy the constraint expression (sqlglot expressions are mutable)
        # Parse it fresh to avoid mutating the cached version
        constraint_expr = sqlglot.parse_one(policy.constraint, read="duckdb")
        
        # Ensure all columns referenced in the constraint are accessible
        # This ensures columns in the constraint are either in SELECT or GROUP BY
        ensure_columns_accessible(parsed, constraint_expr, source_tables)
        
        # Add HAVING clause with the constraint
        # If HAVING already exists, combine with AND
        # Access HAVING clause directly via args, not via having() method which returns SQL
        existing_having_expr = None
        if hasattr(parsed, 'args') and 'having' in parsed.args:
            existing_having_expr = parsed.args['having']
        
        if existing_having_expr:
            # Combine existing HAVING with new constraint using AND
            existing_expr = existing_having_expr.this if isinstance(existing_having_expr, exp.Having) else existing_having_expr
            combined = exp.And(this=existing_expr, expression=constraint_expr)
            parsed.set("having", exp.Having(this=combined))
        else:
            # Create new HAVING clause
            parsed.set("having", exp.Having(this=constraint_expr))


def ensure_columns_accessible(
    parsed: exp.Select,
    constraint_expr: exp.Expression,
    source_tables: Set[str]
) -> None:
    """Ensure all columns referenced in the constraint are accessible in the query.
    
    For aggregation queries with HAVING clauses, columns in aggregations are
    automatically accessible since aggregations can reference source table columns.
    Non-aggregated columns must be in SELECT or GROUP BY.
    
    For now, we assume that policy constraints use aggregated source columns,
    which are always accessible in HAVING clauses. This is a no-op for now.
    
    Args:
        parsed: The parsed SELECT statement.
        constraint_expr: The constraint expression to check.
        source_tables: Set of source table names in the query.
    """
    # Policy constraints require aggregated source columns, which are always
    # accessible in HAVING clauses. No action needed for now.
    # Future enhancement: check non-aggregated columns and ensure they're in SELECT/GROUP BY
    pass


def apply_policy_constraints_to_scan(
    parsed: exp.Select,
    policies: list[DFCPolicy],
    source_tables: Set[str]
) -> None:
    """Apply policy constraints to a non-aggregation query (table scan).
    
    Transforms aggregation functions in constraints to their underlying columns
    and adds WHERE clauses. For example:
    - max(foo.id) > 10 becomes id > 10
    - COUNT(*) > 0 becomes 1 > 0 (which is always true, so we can simplify)
    
    Args:
        parsed: The parsed SELECT statement to modify.
        policies: List of policies to apply.
        source_tables: Set of source table names in the query.
    """
    for policy in policies:
        # Copy the constraint expression and transform aggregations to columns
        constraint_expr = transform_aggregations_to_columns(
            policy._constraint_parsed, source_tables
        )
        
        # Add WHERE clause with the transformed constraint
        # If WHERE already exists, combine with AND
        existing_where_expr = None
        if hasattr(parsed, 'args') and 'where' in parsed.args:
            existing_where_expr = parsed.args['where']
        
        if existing_where_expr:
            # Combine existing WHERE with new constraint using AND
            existing_expr = existing_where_expr.this if isinstance(existing_where_expr, exp.Where) else existing_where_expr
            combined = exp.And(this=existing_expr, expression=constraint_expr)
            parsed.set("where", exp.Where(this=combined))
        else:
            # Create new WHERE clause
            parsed.set("where", exp.Where(this=constraint_expr))


def transform_aggregations_to_columns(
    constraint_expr: exp.Expression,
    source_tables: Set[str]
) -> exp.Expression:
    """Transform aggregation functions in a constraint to their underlying columns.
    
    For non-aggregation queries, we treat aggregations as if they're over a single row:
    - COUNT_IF(condition) → CASE WHEN condition THEN 1 ELSE 0 END
      * For a single row, COUNT_IF returns 1 if condition is true, 0 if false
    - ARRAY_AGG(column) → ARRAY[column] or [column] (DuckDB syntax)
      * For a single row, array_agg returns an array with just that value
    - Count-like functions → 1:
      * COUNT, COUNT(DISTINCT ...), COUNT_STAR
      * APPROX_COUNT_DISTINCT (parsed as APPROX_DISTINCT)
      * REGR_COUNT
    - All other aggregations → underlying column:
      * Statistical: max, min, sum, avg, stddev, variance, etc.
      * Percentile: quantile, percentile_cont, percentile_disc, etc.
      * String: string_agg, listagg, group_concat, etc.
      * Other: first, last, any_value, mode, median, etc.
    
    The logic is: COUNT_IF evaluates the condition per row, ARRAY_AGG creates a single-element
    array, other count functions return 1 for a single row, and other aggregations return the column value.
    
    Args:
        constraint_expr: The constraint expression to transform.
        source_tables: Set of source table names in the query.
        
    Returns:
        A new expression with aggregations replaced by columns.
    """
    # Create a copy of the expression to avoid mutating the original
    # Parse it from SQL to get a fresh copy
    constraint_sql = constraint_expr.sql()
    transformed = sqlglot.parse_one(constraint_sql, read="duckdb")
    
    # Use sqlglot's transform to replace aggregations
    def replace_agg(node):
        if isinstance(node, exp.AggFunc):
            # Get the aggregation function name and class name
            agg_name = node.sql_name().upper() if hasattr(node, 'sql_name') else str(node).upper()
            agg_class = node.__class__.__name__.upper()
            
            # Find the column(s) inside the aggregation
            columns = list(node.find_all(exp.Column))
            
            # COUNT_IF(condition) should be transformed to CASE WHEN condition THEN 1 ELSE 0 END
            # For a single row, COUNT_IF returns 1 if condition is true, 0 if false
            if agg_name in ('COUNT_IF', 'COUNTIF'):
                # Extract the condition from COUNT_IF
                condition = node.this if hasattr(node, 'this') and node.this else None
                if condition:
                    # Create CASE WHEN condition THEN 1 ELSE 0 END
                    # Copy the condition to avoid mutating the original
                    condition_sql = condition.sql()
                    condition_copy = sqlglot.parse_one(condition_sql, read="duckdb")
                    return exp.Case(
                        ifs=[exp.If(
                            this=condition_copy,
                            true=exp.Literal(this="1", is_string=False)
                        )],
                        default=exp.Literal(this="0", is_string=False)
                    )
                else:
                    # Fallback if no condition found
                    return exp.Literal(this="1", is_string=False)
            
            # Other count-like functions should return 1
            # (for a single row, count = 1)
            count_like_sql_names = {
                'COUNT', 'COUNT_STAR',
                'APPROX_DISTINCT',  # APPROX_COUNT_DISTINCT is parsed as APPROX_DISTINCT
                'REGR_COUNT',  # Regression count
            }
            
            # Also check if it's a Count function with distinct=True
            is_count_with_distinct = (
                agg_name == 'COUNT' and 
                hasattr(node, 'distinct') and 
                node.distinct
            )
            
            if agg_name in count_like_sql_names or is_count_with_distinct:
                # Count-like functions → 1
                return exp.Literal(this="1", is_string=False)
            
            # Array aggregation functions should return an array with a single element
            # For a single row, array_agg returns an array with just that value
            # Note: 'list' in DuckDB is not parsed as AggFunc by sqlglot, so we only handle array_agg
            if agg_name == 'ARRAY_AGG' or agg_class == 'ARRAYAGG':
                if columns:
                    # Create ARRAY[column] for a single element array
                    col = columns[0]
                    # Copy the column to avoid mutating the original
                    col_sql = col.sql()
                    col_copy = sqlglot.parse_one(col_sql, read="duckdb")
                    return exp.Array(expressions=[col_copy])
                else:
                    # Fallback: empty array or array with NULL
                    return exp.Array(expressions=[exp.Literal(this="NULL", is_string=False)])
            
            elif columns:
                # For other aggregations (max, min, sum, avg, percentile, etc.),
                # replace with the underlying column
                # For a single row, most aggregations just return the column value
                # Use the first column (most aggregations have one column)
                col = columns[0]
                return exp.Column(
                    this=col.this,
                    table=col.table
                )
            else:
                # No column found (e.g., COUNT(*)), replace with 1
                return exp.Literal(this="1", is_string=False)
        return node
    
    # Transform the expression
    transformed = transformed.transform(replace_agg, copy=True)
    return transformed

