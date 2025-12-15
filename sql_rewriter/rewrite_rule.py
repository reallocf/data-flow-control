"""Rewrite rules for applying DFC policies to SQL queries."""

import sqlglot
from sqlglot import exp
from typing import Set

from .policy import DFCPolicy, Resolution


def _wrap_kill_constraint(constraint_expr: exp.Expression) -> exp.Expression:
    """Wrap a constraint expression in CASE WHEN for KILL resolution policies.
    
    Args:
        constraint_expr: The constraint expression to wrap.
        
    Returns:
        A CASE WHEN expression that returns true if constraint passes,
        or calls kill() if constraint fails.
    """
    kill_call = exp.Anonymous(this="kill", expressions=[])
    return exp.Case(
        ifs=[exp.If(
            this=constraint_expr,
            true=exp.Literal(this="true", is_string=False)
        )],
        default=kill_call
    )


def _add_clause_to_select(
    parsed: exp.Select,
    clause_name: str,
    clause_expr: exp.Expression,
    clause_class: type
) -> None:
    """Add a clause (HAVING or WHERE) to a SELECT statement, combining with existing if needed.
    
    Args:
        parsed: The parsed SELECT statement to modify.
        clause_name: The name of the clause ('having' or 'where').
        clause_expr: The expression to add to the clause.
        clause_class: The clause class (exp.Having or exp.Where).
    """
    existing_clause_expr = None
    if hasattr(parsed, 'args') and clause_name in parsed.args:
        existing_clause_expr = parsed.args[clause_name]
    
    if existing_clause_expr:
        existing_expr = existing_clause_expr.this if isinstance(existing_clause_expr, clause_class) else existing_clause_expr
        combined = exp.And(this=existing_expr, expression=clause_expr)
        parsed.set(clause_name, clause_class(this=combined))
    else:
        parsed.set(clause_name, clause_class(this=clause_expr))


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
        constraint_expr = sqlglot.parse_one(policy.constraint, read="duckdb")
        
        ensure_columns_accessible(parsed, constraint_expr, source_tables)
        
        if policy.on_fail == Resolution.KILL:
            constraint_expr = _wrap_kill_constraint(constraint_expr)
        
        _add_clause_to_select(parsed, "having", constraint_expr, exp.Having)


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
        constraint_expr = transform_aggregations_to_columns(
            policy._constraint_parsed, source_tables
        )
        
        if policy.on_fail == Resolution.KILL:
            constraint_expr = _wrap_kill_constraint(constraint_expr)
        
        _add_clause_to_select(parsed, "where", constraint_expr, exp.Where)


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
    constraint_sql = constraint_expr.sql()
    transformed = sqlglot.parse_one(constraint_sql, read="duckdb")
    
    def replace_agg(node):
        if isinstance(node, exp.AggFunc):
            agg_name = node.sql_name().upper() if hasattr(node, 'sql_name') else str(node).upper()
            agg_class = node.__class__.__name__.upper()
            
            columns = list(node.find_all(exp.Column))
            
            if agg_name in ('COUNT_IF', 'COUNTIF'):
                condition = node.this if hasattr(node, 'this') and node.this else None
                if condition:
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
                    return exp.Literal(this="1", is_string=False)
            
            count_like_sql_names = {
                'COUNT', 'COUNT_STAR',
                'APPROX_DISTINCT',  # APPROX_COUNT_DISTINCT is parsed as APPROX_DISTINCT
                'REGR_COUNT',
            }
            
            is_count_with_distinct = (
                agg_name == 'COUNT' and 
                hasattr(node, 'distinct') and 
                node.distinct
            )
            
            if agg_name in count_like_sql_names or is_count_with_distinct:
                return exp.Literal(this="1", is_string=False)
            
            # Note: 'list' in DuckDB is not parsed as AggFunc by sqlglot, so we only handle array_agg
            if agg_name == 'ARRAY_AGG' or agg_class == 'ARRAYAGG':
                if columns:
                    col = columns[0]
                    col_sql = col.sql()
                    col_copy = sqlglot.parse_one(col_sql, read="duckdb")
                    return exp.Array(expressions=[col_copy])
                else:
                    return exp.Array(expressions=[exp.Literal(this="NULL", is_string=False)])
            
            else:
                # This preserves complex expressions like CASE WHEN, function calls, etc.
                if hasattr(node, 'this') and node.this:
                    expr_sql = node.this.sql()
                    expr_copy = sqlglot.parse_one(expr_sql, read="duckdb")
                    return expr_copy
                else:
                    return exp.Literal(this="1", is_string=False)
        return node
    
    transformed = transformed.transform(replace_agg, copy=True)
    return transformed

