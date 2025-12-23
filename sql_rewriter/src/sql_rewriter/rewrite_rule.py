"""Rewrite rules for applying DFC policies to SQL queries."""

import sqlglot
from sqlglot import exp
from typing import Set, List, Optional

from .policy import DFCPolicy, Resolution
from .sqlglot_utils import get_column_name, get_table_name_from_column


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


def _extract_columns_from_constraint(
    constraint_expr: exp.Expression,
    source_tables: Set[str]
) -> List[exp.Column]:
    """Extract all columns from a constraint expression that belong to source tables.
    
    Args:
        constraint_expr: The constraint expression to extract columns from.
        source_tables: Set of source table names.
        
    Returns:
        List of column expressions from source tables, in order of appearance.
    """
    columns = []
    seen = set()
    
    for column in constraint_expr.find_all(exp.Column):
        table_name = get_table_name_from_column(column)
        if table_name and table_name in source_tables:
            # Create a unique key for the column
            col_key = (table_name, get_column_name(column))
            if col_key not in seen:
                seen.add(col_key)
                # Copy the column expression to avoid mutability issues
                # Serialize and deserialize to create a fresh copy
                col_sql = column.sql()
                # Parse as a column expression by wrapping in a SELECT
                select_sql = f"SELECT {col_sql} AS col"
                parsed = sqlglot.parse_one(select_sql, read="duckdb")
                if isinstance(parsed, exp.Select) and parsed.expressions:
                    expr = parsed.expressions[0]
                    if isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Column):
                        columns.append(expr.this)
                    elif isinstance(expr, exp.Column):
                        columns.append(expr)
    
    return columns


def _wrap_human_constraint(
    constraint_expr: exp.Expression,
    policy: DFCPolicy,
    source_tables: Set[str],
    stream_file_path: Optional[str] = None
) -> exp.Expression:
    """Wrap a constraint expression in CASE WHEN for HUMAN resolution policies.
    
    When the constraint fails, calls address_violating_rows with columns from the
    constraint. The function returns False to filter out the row, allowing it to
    be handled by the external operator.
    
    Args:
        constraint_expr: The constraint expression to wrap.
        policy: The policy being applied.
        source_tables: Set of source table names in the query.
        stream_file_path: Optional path to stream file for approved rows.
        
    Returns:
        A CASE WHEN expression that returns true if constraint passes,
        or calls address_violating_rows() if constraint fails.
    """
    # Extract columns from the constraint that belong to source tables
    columns = _extract_columns_from_constraint(constraint_expr, source_tables)
    
    # Build the address_violating_rows function call
    # Pass stream_file_path as the last argument
    if stream_file_path:
        # Escape single quotes in the path
        escaped_path = stream_file_path.replace("'", "''")
        stream_endpoint = exp.Literal(this=f"'{escaped_path}'", is_string=True)
    else:
        stream_endpoint = exp.Literal(this="''", is_string=True)
    
    # Create function call with columns and stream_endpoint
    # address_violating_rows(col1, col2, ..., stream_endpoint)
    expressions = columns + [stream_endpoint]
    address_call = exp.Anonymous(this="address_violating_rows", expressions=expressions)
    
    return exp.Case(
        ifs=[exp.If(
            this=constraint_expr,
            true=exp.Literal(this="true", is_string=False)
        )],
        default=address_call
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
    source_tables: Set[str],
    stream_file_path: Optional[str] = None
) -> None:
    """Apply policy constraints to an aggregation query.
    
    Adds HAVING clauses for each policy constraint and ensures all referenced
    columns are accessible.
    
    Args:
        parsed: The parsed SELECT statement to modify.
        policies: List of policies to apply.
        source_tables: Set of source table names in the query.
    """
    # Build mapping from source tables to subquery/CTE aliases
    table_mapping = _get_source_table_to_alias_mapping(parsed, source_tables)
    
    for policy in policies:
        constraint_expr = sqlglot.parse_one(policy.constraint, read="duckdb")
        
        # Replace table references with subquery/CTE aliases if needed
        constraint_expr = _replace_table_references_in_constraint(
            constraint_expr, table_mapping
        )
        
        ensure_columns_accessible(parsed, constraint_expr, source_tables)
        
        if policy.on_fail == Resolution.KILL:
            constraint_expr = _wrap_kill_constraint(constraint_expr)
        elif policy.on_fail == Resolution.HUMAN:
            constraint_expr = _wrap_human_constraint(constraint_expr, policy, source_tables, stream_file_path)
        
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


def _get_subqueries_in_from(parsed: exp.Select) -> List[tuple[exp.Subquery, str]]:
    """Find all subqueries in FROM clauses with their aliases.
    
    Args:
        parsed: The parsed SELECT statement.
        
    Returns:
        List of tuples (subquery, alias) where alias is the subquery alias (lowercase).
    """
    subqueries = []
    
    # Find all Subquery nodes and check if they're in FROM/JOIN clauses
    all_subqueries = list(parsed.find_all(exp.Subquery))
    for subquery in all_subqueries:
        # Check if this subquery is in a FROM or JOIN clause
        from_ancestor = subquery.find_ancestor(exp.From)
        join_ancestor = subquery.find_ancestor(exp.Join)
        if from_ancestor or join_ancestor:
            alias = None
            
            # Try to get alias from the From.this if it's a Subquery
            if from_ancestor and hasattr(from_ancestor, 'this'):
                from_table = from_ancestor.this
                if isinstance(from_table, exp.Subquery):
                    # The From.this is directly the Subquery, get alias from Subquery
                    if hasattr(from_table, 'alias') and from_table.alias:
                        if isinstance(from_table.alias, exp.Identifier):
                            alias = from_table.alias.name.lower()
                        elif isinstance(from_table.alias, str):
                            alias = from_table.alias.lower()
                        else:
                            alias = str(from_table.alias).lower()
                elif isinstance(from_table, exp.Table):
                    # The From.this is a Table containing the subquery
                    if hasattr(from_table, 'alias') and from_table.alias:
                        if isinstance(from_table.alias, exp.Identifier):
                            alias = from_table.alias.name.lower()
                        elif isinstance(from_table.alias, str):
                            alias = from_table.alias.lower()
                        else:
                            alias = str(from_table.alias).lower()
                    if not alias and hasattr(from_table, 'name') and from_table.name:
                        alias = from_table.name.lower()
            
            # Fallback: try finding via Table ancestor
            if not alias:
                table_ancestor = subquery.find_ancestor(exp.Table)
                if table_ancestor:
                    if hasattr(table_ancestor, 'alias') and table_ancestor.alias:
                        if isinstance(table_ancestor.alias, exp.Identifier):
                            alias = table_ancestor.alias.name.lower()
                        elif isinstance(table_ancestor.alias, str):
                            alias = table_ancestor.alias.lower()
                        else:
                            alias = str(table_ancestor.alias).lower()
                    if not alias and hasattr(table_ancestor, 'name') and table_ancestor.name:
                        alias = table_ancestor.name.lower()
            
            if alias:
                subqueries.append((subquery, alias))
    
    return subqueries


def _get_selected_columns(subquery: exp.Subquery) -> Set[str]:
    """Get the set of column names selected in a subquery.
    
    Args:
        subquery: The subquery expression.
        
    Returns:
        Set of column names (lowercase) in the SELECT list.
    """
    if not isinstance(subquery.this, exp.Select):
        return set()
    return _get_selected_columns_from_select(subquery.this)


def _get_table_alias_in_subquery(subquery: exp.Subquery, table_name: str) -> str:
    """Get the table alias used in a subquery for a given table name.
    
    Args:
        subquery: The subquery expression.
        table_name: The table name to find the alias for.
        
    Returns:
        The table alias if found, otherwise the table name.
    """
    if not isinstance(subquery.this, exp.Select):
        return table_name
    return _get_table_alias_in_select(subquery.this, table_name)


def _add_column_to_subquery(subquery: exp.Subquery, table_name: str, column_name: str) -> None:
    """Add a column to a subquery's SELECT list if it's not already there.
    
    Args:
        subquery: The subquery expression to modify.
        table_name: The table name for the column (for qualification).
        column_name: The column name to add.
    """
    if not isinstance(subquery.this, exp.Select):
        return
    
    select_expr = subquery.this
    selected = _get_selected_columns(subquery)
    
    # Check if column is already selected (by name or alias)
    if column_name.lower() in selected:
        return
    
    # Check if SELECT * is used - in that case, we can't safely add columns
    # because we don't know what columns are actually selected
    has_star = any(isinstance(expr, exp.Star) for expr in select_expr.expressions)
    if has_star:
        # Can't safely add columns when SELECT * is used
        return
    
    # Get the table alias if one exists
    table_ref = _get_table_alias_in_subquery(subquery, table_name)
    
    # Create a column expression with table qualification
    col_expr = exp.Column(
        this=exp.Identifier(this=column_name, quoted=False),
        table=exp.Identifier(this=table_ref, quoted=False)
    )
    
    # Add the column to the SELECT list
    select_expr.expressions.append(col_expr)


def _get_ctes(parsed: exp.Select) -> List[tuple[exp.CTE, str]]:
    """Find all CTEs (Common Table Expressions) in a SELECT statement with their aliases.
    
    Args:
        parsed: The parsed SELECT statement.
        
    Returns:
        List of tuples (cte, alias) where alias is the CTE alias (lowercase).
    """
    ctes = []
    # Find WITH clause - access via args dictionary
    with_clause = parsed.args.get('with_') if hasattr(parsed, 'args') else None
    if not with_clause:
        # Fallback: try accessing as attribute
        with_clause = getattr(parsed, 'with_', None)
        # If it's a method, try calling it (though it shouldn't be)
        if callable(with_clause):
            with_clause = None
    
    if with_clause and hasattr(with_clause, 'expressions'):
        for cte in with_clause.expressions:
            if isinstance(cte, exp.CTE):
                # Get the CTE alias - in sqlglot, CTE.this is the SELECT, and alias is in cte.alias
                alias = None
                # Check the alias attribute (TableAlias)
                if hasattr(cte, 'alias') and cte.alias:
                    # The alias is a TableAlias, and its 'this' is the Identifier
                    if hasattr(cte.alias, 'this'):
                        alias_obj = cte.alias.this
                        if isinstance(alias_obj, exp.Identifier):
                            alias = alias_obj.name.lower()
                        elif isinstance(alias_obj, str):
                            alias = alias_obj.lower()
                        else:
                            alias = str(alias_obj).lower()
                    elif isinstance(cte.alias, exp.Identifier):
                        alias = cte.alias.name.lower()
                    elif isinstance(cte.alias, str):
                        alias = cte.alias.lower()
                    else:
                        alias = str(cte.alias).lower()
                # Fallback: check CTE.this if it's an Identifier (older sqlglot versions)
                if not alias and hasattr(cte, 'this') and cte.this:
                    if isinstance(cte.this, exp.Identifier):
                        alias = cte.this.name.lower()
                    elif isinstance(cte.this, str):
                        alias = cte.this.lower()
                    else:
                        alias = str(cte.this).lower()
                if alias:
                    ctes.append((cte, alias))
    return ctes


def _get_selected_columns_from_select(select_expr: exp.Select) -> Set[str]:
    """Get the set of column names selected in a SELECT statement.
    
    Args:
        select_expr: The SELECT expression.
        
    Returns:
        Set of column names (lowercase) in the SELECT list.
    """
    selected = set()
    for expr in select_expr.expressions:
        if isinstance(expr, exp.Column):
            selected.add(get_column_name(expr).lower())
        elif isinstance(expr, exp.Alias):
            # Handle aliased columns
            if isinstance(expr.this, exp.Column):
                selected.add(get_column_name(expr.this).lower())
            # Also add the alias name
            if expr.alias:
                selected.add(expr.alias.lower())
        elif isinstance(expr, exp.Star):
            # SELECT * means all columns are selected
            # We can't determine which columns, so return empty set
            # and let the caller handle it
            pass
    return selected


def _get_table_alias_in_select(select_expr: exp.Select, table_name: str) -> str:
    """Get the table alias used in a SELECT statement for a given table name.
    
    Args:
        select_expr: The SELECT expression.
        table_name: The table name to find the alias for.
        
    Returns:
        The table alias if found, otherwise the table name.
    """
    # Find the table in FROM/JOIN clauses
    for table in select_expr.find_all(exp.Table):
        if table.find_ancestor(exp.From) or table.find_ancestor(exp.Join):
            if table.name.lower() == table_name.lower():
                # Check if there's an alias
                if table.alias:
                    return table.alias
                return table_name
    return table_name


def _add_column_to_cte(cte: exp.CTE, table_name: str, column_name: str) -> None:
    """Add a column to a CTE's SELECT list if it's not already there.
    
    Args:
        cte: The CTE expression to modify.
        table_name: The table name for the column (for qualification).
        column_name: The column name to add.
    """
    # In sqlglot, CTE.this is the SELECT expression
    cte_select = cte.this if hasattr(cte, 'this') and isinstance(cte.this, exp.Select) else None
    if not cte_select:
        # Fallback: check expression attribute
        cte_select = cte.expression if hasattr(cte, 'expression') else None
    if not isinstance(cte_select, exp.Select):
        return
    
    select_expr = cte_select
    selected = _get_selected_columns_from_select(select_expr)
    
    # Check if column is already selected (by name or alias)
    if column_name.lower() in selected:
        return
    
    # Check if SELECT * is used - in that case, we can't safely add columns
    # because we don't know what columns are actually selected
    has_star = any(isinstance(expr, exp.Star) for expr in select_expr.expressions)
    if has_star:
        # Can't safely add columns when SELECT * is used
        return
    
    # Get the table alias if one exists
    table_ref = _get_table_alias_in_select(select_expr, table_name)
    
    # Create a column expression with table qualification
    col_expr = exp.Column(
        this=exp.Identifier(this=column_name, quoted=False),
        table=exp.Identifier(this=table_ref, quoted=False)
    )
    
    # Add the column to the SELECT list
    select_expr.expressions.append(col_expr)


def _get_source_table_to_alias_mapping(
    parsed: exp.Select,
    source_tables: Set[str]
) -> dict[str, str]:
    """Build a mapping from source table names to their subquery/CTE aliases.
    
    Args:
        parsed: The parsed SELECT statement.
        source_tables: Set of source table names in the query.
        
    Returns:
        Dictionary mapping source table name (lowercase) to subquery/CTE alias (lowercase).
        Only includes mappings for source tables that are in subqueries/CTEs, not in the main query.
    """
    mapping = {}
    
    # Get CTE aliases to exclude them from main_query_tables
    cte_aliases = {alias for _, alias in _get_ctes(parsed)}
    
    # Check which source tables are in the main query's FROM/JOIN (not in subqueries/CTEs)
    # We need to find actual table names, not subquery/CTE aliases
    main_query_tables = set()
    # Get the main query's FROM clause
    if hasattr(parsed, 'from') and parsed.from_:
        # Find all tables directly in the main FROM clause
        for table in parsed.from_.find_all(exp.Table):
            # Exclude tables that are subqueries (they have Subquery as 'this')
            if not (hasattr(table, 'this') and isinstance(table.this, exp.Subquery)):
                # Also exclude if this table is inside a subquery or CTE
                if (not table.find_ancestor(exp.Subquery) and 
                    not table.find_ancestor(exp.CTE)):
                    # Exclude CTE aliases (they're not source tables)
                    if table.name.lower() not in cte_aliases:
                        main_query_tables.add(table.name.lower())
        # Also check JOINs directly in the main query
        for join in parsed.find_all(exp.Join):
            if (not join.find_ancestor(exp.Subquery) and 
                not join.find_ancestor(exp.CTE)):
                for table in join.find_all(exp.Table):
                    # Exclude subquery tables
                    if not (hasattr(table, 'this') and isinstance(table.this, exp.Subquery)):
                        # Exclude CTE aliases (they're not source tables)
                        if table.name.lower() not in cte_aliases:
                            main_query_tables.add(table.name.lower())
    
    # Find all subqueries in FROM clauses
    subqueries = _get_subqueries_in_from(parsed)
    for subquery, subquery_alias in subqueries:
        if not isinstance(subquery.this, exp.Select):
            continue
        
        # Find which source tables are referenced in this subquery
        subquery_tables = set()
        for table in subquery.this.find_all(exp.Table):
            if table.find_ancestor(exp.From) or table.find_ancestor(exp.Join):
                subquery_tables.add(table.name.lower())
        
        # Map source tables that are in this subquery but not in main query
        for source_table in source_tables:
            if source_table in subquery_tables and source_table not in main_query_tables:
                mapping[source_table] = subquery_alias
    
    # Find all CTEs
    ctes = _get_ctes(parsed)
    for cte, cte_alias in ctes:
        # In sqlglot, CTE.this is the SELECT expression
        cte_select = cte.this if hasattr(cte, 'this') and isinstance(cte.this, exp.Select) else None
        if not cte_select:
            # Fallback: check expression attribute
            cte_select = cte.expression if hasattr(cte, 'expression') else None
        if not isinstance(cte_select, exp.Select):
            continue
        
        # Find which source tables are referenced in this CTE
        cte_tables = set()
        for table in cte_select.find_all(exp.Table):
            if table.find_ancestor(exp.From) or table.find_ancestor(exp.Join):
                cte_tables.add(table.name.lower())
        
        # Map source tables that are in this CTE but not in main query
        for source_table in source_tables:
            if source_table in cte_tables and source_table not in main_query_tables:
                mapping[source_table] = cte_alias
    
    return mapping


def _replace_table_references_in_constraint(
    constraint_expr: exp.Expression,
    table_mapping: dict[str, str]
) -> exp.Expression:
    """Replace table references in a constraint expression with subquery/CTE aliases.
    
    This function replaces table references in columns, including columns inside
    aggregation functions. For example, max(foo.id) > 1 becomes max(sub.id) > 1
    when foo is mapped to sub.
    
    Args:
        constraint_expr: The constraint expression to modify.
        table_mapping: Dictionary mapping source table names to subquery/CTE aliases.
        
    Returns:
        A new constraint expression with table references replaced.
    """
    if not table_mapping:
        return constraint_expr
    
    def replace_table(node):
        if isinstance(node, exp.Column):
            table_name = get_table_name_from_column(node)
            if table_name and table_name in table_mapping:
                # Replace the table reference with the subquery/CTE alias
                new_table = exp.Identifier(this=table_mapping[table_name], quoted=False)
                new_column = exp.Column(
                    this=node.this,
                    table=new_table
                )
                return new_column
        return node
    
    # Transform the expression, replacing all column table references
    # This works for columns both inside and outside aggregation functions
    transformed = constraint_expr.transform(replace_table, copy=True)
    return transformed


def ensure_subqueries_have_constraint_columns(
    parsed: exp.Select,
    policies: List[DFCPolicy],
    source_tables: Set[str]
) -> None:
    """Ensure subqueries and CTEs that reference source tables include columns needed for constraints.
    
    For each subquery or CTE that references a source table, this function ensures that
    all columns needed to evaluate policy constraints are included in the SELECT list.
    This allows the outer query to evaluate the constraints correctly.
    
    Args:
        parsed: The parsed SELECT statement.
        policies: List of policies that will be applied.
        source_tables: Set of source table names in the query.
    """
    # Find all subqueries in FROM clauses
    subqueries = _get_subqueries_in_from(parsed)
    
    for subquery, subquery_alias in subqueries:
        if not isinstance(subquery.this, exp.Select):
            continue
        
        # Find which source tables are referenced in this subquery
        subquery_tables = set()
        for table in subquery.this.find_all(exp.Table):
            if table.find_ancestor(exp.From) or table.find_ancestor(exp.Join):
                subquery_tables.add(table.name.lower())
        
        # Check if this subquery references any source tables
        referenced_source_tables = subquery_tables & source_tables
        if not referenced_source_tables:
            continue
        
        # For each source table referenced in the subquery, check each policy
        for source_table in referenced_source_tables:
            for policy in policies:
                if policy.source and policy.source.lower() == source_table:
                    # Use pre-calculated columns needed from the policy
                    needed_columns = policy._source_columns_needed
                    
                    # Add missing columns to the subquery's SELECT list
                    for col_name in needed_columns:
                        _add_column_to_subquery(subquery, source_table, col_name)
    
    # Find all CTEs
    ctes = _get_ctes(parsed)
    
    for cte, cte_alias in ctes:
        # Check if CTE has a SELECT expression
        cte_select = cte.this if hasattr(cte, 'this') and isinstance(cte.this, exp.Select) else None
        if not cte_select:
            cte_select = cte.expression if hasattr(cte, 'expression') else None
        if not isinstance(cte_select, exp.Select):
            continue
        
        # Find which source tables are referenced in this CTE
        cte_tables = set()
        for table in cte_select.find_all(exp.Table):
            if table.find_ancestor(exp.From) or table.find_ancestor(exp.Join):
                cte_tables.add(table.name.lower())
        
        # Check if this CTE references any source tables
        referenced_source_tables = cte_tables & source_tables
        if not referenced_source_tables:
            continue
        
        # For each source table referenced in the CTE, check each policy
        for source_table in referenced_source_tables:
            for policy in policies:
                if policy.source and policy.source.lower() == source_table:
                    # Use pre-calculated columns needed from the policy
                    needed_columns = policy._source_columns_needed
                    
                    # Add missing columns to the CTE's SELECT list
                    for col_name in needed_columns:
                        _add_column_to_cte(cte, source_table, col_name)


def apply_policy_constraints_to_scan(
    parsed: exp.Select,
    policies: list[DFCPolicy],
    source_tables: Set[str],
    stream_file_path: Optional[str] = None
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
    # Build mapping from source tables to subquery/CTE aliases
    table_mapping = _get_source_table_to_alias_mapping(parsed, source_tables)
    
    for policy in policies:
        constraint_expr = transform_aggregations_to_columns(
            policy._constraint_parsed, source_tables
        )
        
        # Replace table references with subquery/CTE aliases if needed
        constraint_expr = _replace_table_references_in_constraint(
            constraint_expr, table_mapping
        )
        
        if policy.on_fail == Resolution.KILL:
            constraint_expr = _wrap_kill_constraint(constraint_expr)
        elif policy.on_fail == Resolution.HUMAN:
            constraint_expr = _wrap_human_constraint(constraint_expr, policy, source_tables, stream_file_path)
        
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

