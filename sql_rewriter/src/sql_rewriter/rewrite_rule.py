"""Rewrite rules for applying DFC policies to SQL queries."""

import json
from typing import List, Optional, Set

import sqlglot
from sqlglot import exp

from .policy import AggregateDFCPolicy, DFCPolicy, Resolution
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
    source_tables: Set[str],
    sink_table: Optional[str] = None,
    sink_to_output_mapping: Optional[dict[str, str]] = None
) -> List[exp.Column]:
    """Extract all columns from a constraint expression that belong to source or sink tables.
    
    For sink table columns, returns the corresponding SELECT output column references.
    
    Args:
        constraint_expr: The constraint expression to extract columns from.
        source_tables: Set of source table names.
        sink_table: Optional sink table name.
        sink_to_output_mapping: Optional mapping from sink column names to SELECT output column names.
        
    Returns:
        List of column expressions from source and sink tables, in order of appearance.
        Sink columns are returned as unqualified references to SELECT output columns.
    """
    columns = []
    seen = set()

    for column in constraint_expr.find_all(exp.Column):
        table_name = get_table_name_from_column(column)

        # Handle source table columns
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

        # Handle sink table columns - map to SELECT output columns
        elif table_name and sink_table and table_name == sink_table and sink_to_output_mapping:
            col_name = get_column_name(column).lower()
            if col_name in sink_to_output_mapping:
                output_col_name = sink_to_output_mapping[col_name]
                # Create unqualified column reference to SELECT output
                col_key = ("sink", output_col_name)
                if col_key not in seen:
                    seen.add(col_key)
                    output_col = exp.Column(
                        this=exp.Identifier(this=output_col_name, quoted=False)
                    )
                    columns.append(output_col)

    return columns


def _wrap_llm_constraint(
    constraint_expr: exp.Expression,
    policy: DFCPolicy,
    source_tables: Set[str],
    stream_file_path: Optional[str] = None,
    sink_table: Optional[str] = None,
    sink_to_output_mapping: Optional[dict[str, str]] = None,
    parsed: Optional[exp.Select] = None,
    insert_columns: Optional[List[str]] = None
) -> exp.Expression:
    """Wrap a constraint expression in CASE WHEN for LLM resolution policies.
    
    When the constraint fails, calls address_violating_rows with columns from the
    constraint. The function returns False to filter out the row, allowing it to
    be handled by the external operator.
    
    Args:
        constraint_expr: The constraint expression to wrap.
        policy: The policy being applied.
        source_tables: Set of source table names in the query.
        stream_file_path: Optional path to stream file for approved rows.
        sink_table: Optional sink table name (for INSERT statements).
        sink_to_output_mapping: Optional mapping from sink column names to SELECT output column names.
        parsed: Optional parsed SELECT statement to extract all output columns from.
        insert_columns: Optional list of column names in INSERT column list (for INSERT statements).
        
    Returns:
        A CASE WHEN expression that returns true if constraint passes,
        or calls address_violating_rows() if constraint fails.
    """
    columns = []
    column_names = []

    # First, extract source table columns from the constraint and add them to the UDF call
    # These columns are needed for the UDF but should NOT be added to the SELECT output
    # (they'll be available in the filter child for constraint evaluation, but not in SELECT)
    if source_tables:
        # Find which source tables are actually present in the query (FROM/JOIN/subqueries/CTEs)
        actual_source_tables = set()
        if parsed:
            for table in parsed.find_all(exp.Table):
                # Only consider tables in FROM/JOIN clauses, not in column references
                if table.find_ancestor(exp.From) or table.find_ancestor(exp.Join):
                    actual_source_tables.add(table.name.lower())

            # Also check subqueries and CTEs for source tables
            for subquery in parsed.find_all(exp.Subquery):
                if isinstance(subquery.this, exp.Select):
                    for table in subquery.this.find_all(exp.Table):
                        if table.find_ancestor(exp.From) or table.find_ancestor(exp.Join):
                            actual_source_tables.add(table.name.lower())

        # Extract source table columns from constraint and add to UDF call
        # These will be passed to the UDF and written to the stream, but NOT added to SELECT
        # NOTE: Even if the source table is not in the query's FROM clause (e.g., no FROM clause),
        # we still need to include source columns if they're referenced in the constraint,
        # because the constraint might reference them for evaluation purposes
        for column in constraint_expr.find_all(exp.Column):
            table_name = get_table_name_from_column(column)
            # Include if table is in source_tables (from policy)
            # We check actual_source_tables, but if it's empty (no FROM clause), we still include
            # source columns if they're in the policy's source_tables
            if table_name and table_name in source_tables:
                # Only require actual_source_tables check if we have a FROM clause
                # If no FROM clause (actual_source_tables is empty), still include source columns
                if actual_source_tables and table_name.lower() not in actual_source_tables:
                    continue  # Skip if FROM clause exists but table is not in it

                column_name = get_column_name(column)
                # Check if we already have this column (don't duplicate)
                already_included = any(
                    isinstance(c, exp.Column) and
                    get_table_name_from_column(c) and
                    get_table_name_from_column(c).lower() == table_name.lower() and
                    get_column_name(c).lower() == column_name.lower()
                    for c in columns
                )
                if not already_included:
                    # Add source column to UDF call (qualified: bank_txn.category)
                    # NOTE: We do NOT add it to the SELECT output because:
                    # 1. The filter is applied to the table scan, so category is available from the table scan
                    # 2. Adding it to SELECT would make the SELECT have 5 columns, but INSERT expects 4
                    # 3. The UDF can access category from the filter child (table scan) via the column reference

                    # Add to UDF call arguments (this makes it available to the UDF)
                    col_expr = exp.Column(
                        this=exp.Identifier(this=column_name, quoted=False),
                        table=exp.Identifier(this=table_name, quoted=False)
                    )
                    columns.append(col_expr)
                    column_names.append(f"{table_name}.{column_name}")

    # Now extract all columns from the SELECT output
    # These are the columns that will be in the final SELECT output (for INSERT)
    if parsed:
        for expr in parsed.expressions:
            # Skip aggregate policy temp columns (they start with _policy_ and contain _tmp)
            # These are internal tracking columns and shouldn't be passed to the LLM UDF
            if isinstance(expr, exp.Alias):
                alias_name = get_column_name(expr.alias).lower()
                if alias_name.startswith("_policy_") and "_tmp" in alias_name:
                    continue  # Skip aggregate policy temp columns

            col_name = None
            table_name = None
            if isinstance(expr, exp.Alias):
                # Expression with alias (e.g., ABS(amount) AS amount)
                # Use the alias name to reference the column
                if isinstance(expr.alias, exp.Identifier):
                    col_name = expr.alias.name
                elif isinstance(expr.alias, str):
                    col_name = expr.alias
                else:
                    col_name = str(expr.alias)
                # Check if the expression itself is a column (e.g., bank_txn.category AS category)
                if isinstance(expr.this, exp.Column):
                    table_name = get_table_name_from_column(expr.this)
            elif isinstance(expr, exp.Column):
                # Column without alias - use column name
                col_name = get_column_name(expr)
                table_name = get_table_name_from_column(expr)
            elif isinstance(expr, exp.Star):
                # SELECT * - can't extract individual columns
                # Fall back to extracting from constraint
                break
            else:
                # Other expression types (e.g., literals, function calls without alias)
                # Try to get a name from the expression if possible
                # For now, skip these - they're not typically needed for LLM fixing
                # If needed, we could generate a position-based name like "col0", "col1", etc.
                pass

            # Include all columns from SELECT for the UDF call
            # This includes source table columns (like "category") needed for constraints
            if col_name:
                # Check if we already have this column (don't duplicate)
                already_included = any(
                    isinstance(c, exp.Column) and
                    get_column_name(c).lower() == col_name.lower()
                    for c in columns
                )
                if not already_included:
                    # Create column reference using the alias or column name
                    output_col = exp.Column(
                        this=exp.Identifier(this=col_name, quoted=False)
                    )
                    columns.append(output_col)
                    # Use qualified name if it's a source table column, otherwise just the column name
                    if table_name and table_name in source_tables:
                        column_names.append(f"{table_name}.{col_name}")
                    else:
                        column_names.append(col_name.lower())
    else:
        # Fallback: Extract only sink table columns from the constraint
        # This is the old behavior for backwards compatibility
        if sink_table and sink_to_output_mapping:
            for column in constraint_expr.find_all(exp.Column):
                table_name = get_table_name_from_column(column)
                # Only include sink table columns
                if table_name and table_name == sink_table:
                    col_name = get_column_name(column).lower()
                    if col_name in sink_to_output_mapping:
                        output_col_name = sink_to_output_mapping[col_name]
                        # Check if we already have this column (don't duplicate)
                        already_included = any(
                            get_column_name(col).lower() == output_col_name.lower()
                            for col in columns
                        )
                        if not already_included:
                            # Create unqualified column reference to SELECT output
                            output_col = exp.Column(
                                this=exp.Identifier(this=output_col_name, quoted=False)
                            )
                            columns.append(output_col)
                            column_names.append(output_col_name)

        # For INSERT statements, also include specific columns: txn_id, amount, kind, business_use_pct
        if sink_table and sink_to_output_mapping:
            # HACK -- hard coded
            additional_columns = ["txn_id", "amount", "kind", "business_use_pct"]
            for col_name in additional_columns:
                if col_name in sink_to_output_mapping:
                    output_col_name = sink_to_output_mapping[col_name]
                    # Check if we already have this column (don't duplicate)
                    already_included = any(
                        isinstance(col, exp.Column) and
                        get_column_name(col).lower() == output_col_name.lower()
                        for col in columns
                    )
                    if not already_included:
                        # Create unqualified column reference to SELECT output
                        output_col = exp.Column(
                            this=exp.Identifier(this=output_col_name, quoted=False)
                        )
                        columns.append(output_col)
                        # Add column name in same order as column is added
                        column_names.append(output_col_name)

    # Ensure column_names matches columns exactly (same order and count)
    # This is critical for the UDF to receive values in the correct order
    assert len(column_names) == len(columns), (
        f"Column names count ({len(column_names)}) must match columns count ({len(columns)})"
    )

    # Build the address_violating_rows function call
    # Order: columns, constraint, description, column_names_json, stream_endpoint (stream_endpoint is last for async_rewrite)
    # Escape policy constraint and description for SQL string literals
    constraint_str = policy.constraint.replace("'", "''")
    description_str = (policy.description or "").replace("'", "''")

    # Create JSON string of column names
    column_names_json = json.dumps(column_names)
    # Escape single quotes in JSON string for SQL
    column_names_json_escaped = column_names_json.replace("'", "''")

    constraint_literal = exp.Literal(this=f"'{constraint_str}'", is_string=True)
    description_literal = exp.Literal(this=f"'{description_str}'", is_string=True)
    column_names_literal = exp.Literal(this=f"'{column_names_json_escaped}'", is_string=True)

    # stream_endpoint is last (for async_rewrite to find it easily)
    # Pass path directly without escaping (file paths don't contain quotes)
    # This matches db_example.py which uses f-string interpolation: '{stream_path}'
    if stream_file_path:
        stream_endpoint = exp.Literal(this=stream_file_path, is_string=True)
    else:
        stream_endpoint = exp.Literal(this="", is_string=True)

    # Create function call with columns, constraint, description, column_names_json, stream_endpoint
    # address_violating_rows(col1, col2, ..., constraint, description, column_names_json, stream_endpoint)
    expressions = columns + [constraint_literal, description_literal, column_names_literal, stream_endpoint]
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
    
    Wraps both the existing expression and the new constraint in parentheses when combining
    them to ensure proper operator precedence, especially when OR clauses are involved.
    
    Args:
        parsed: The parsed SELECT statement to modify.
        clause_name: The name of the clause ('having' or 'where').
        clause_expr: The expression to add to the clause.
        clause_class: The clause class (exp.Having or exp.Where).
    """
    existing_clause_expr = None
    if hasattr(parsed, "args") and clause_name in parsed.args:
        existing_clause_expr = parsed.args[clause_name]

    if existing_clause_expr:
        existing_expr = existing_clause_expr.this if isinstance(existing_clause_expr, clause_class) else existing_clause_expr
        # Wrap both expressions in parentheses to ensure proper operator precedence
        # This is especially important when OR clauses are involved
        # If existing_expr is already a Paren (from a previous policy), use it as-is
        wrapped_existing = existing_expr if isinstance(existing_expr, exp.Paren) else exp.Paren(this=existing_expr)
        wrapped_new = exp.Paren(this=clause_expr)
        combined = exp.And(this=wrapped_existing, expression=wrapped_new)
        parsed.set(clause_name, clause_class(this=combined))
    else:
        # Wrap each policy addition in its own parentheses for consistency
        wrapped_new = exp.Paren(this=clause_expr)
        parsed.set(clause_name, clause_class(this=wrapped_new))


def _add_invalidate_column_to_select(
    parsed: exp.Select,
    constraint_expr: exp.Expression,
    replace_existing: bool = False
) -> None:
    """Add a 'valid' column to a SELECT statement, combining with existing if needed.
    
    If a 'valid' column already exists, combines the new constraint with the existing
    one using AND (both must pass for valid=true), unless replace_existing is True.
    
    The valid column is true when the constraint passes, false when it fails.
    
    Args:
        parsed: The parsed SELECT statement to modify.
        constraint_expr: The constraint expression to add as the 'valid' column.
        replace_existing: If True and 'valid' already exists, replace it instead of combining.
    """
    # Create a fresh copy of the constraint expression to avoid mutability issues
    constraint_sql = constraint_expr.sql()
    constraint_copy = sqlglot.parse_one(constraint_sql, read="duckdb")

    # Check if 'valid' column already exists
    existing_valid_expr = None
    for expr in parsed.expressions:
        if isinstance(expr, exp.Alias) and expr.alias and expr.alias.lower() == "valid":
            existing_valid_expr = expr.this
            break
        if isinstance(expr, exp.Column) and get_column_name(expr).lower() == "valid":
            # If there's an unaliased 'valid' column, we'll replace it
            existing_valid_expr = expr
            break

    if existing_valid_expr and not replace_existing:
        # Create a fresh copy of existing expression to avoid mutability issues
        existing_sql = existing_valid_expr.sql()
        existing_copy = sqlglot.parse_one(existing_sql, read="duckdb")

        # Combine existing and new constraint with AND
        # Both must pass for valid=true
        wrapped_existing = existing_copy if isinstance(existing_copy, exp.Paren) else exp.Paren(this=existing_copy)
        wrapped_new = exp.Paren(this=constraint_copy)
        combined = exp.And(this=wrapped_existing, expression=wrapped_new)
        valid_expr = combined
    else:
        # Wrap the constraint in parentheses for consistency with REMOVE
        wrapped_new = exp.Paren(this=constraint_copy)
        valid_expr = wrapped_new

    # Create the aliased column
    valid_alias = exp.Alias(
        this=valid_expr,
        alias=exp.Identifier(this="valid", quoted=False)
    )

    # Remove existing 'valid' column if it exists (modify list in place)
    expressions_to_remove = []
    for i, expr in enumerate(parsed.expressions):
        if (isinstance(expr, exp.Alias) and expr.alias and expr.alias.lower() == "valid") or \
           (isinstance(expr, exp.Column) and get_column_name(expr).lower() == "valid"):
            expressions_to_remove.append(i)

    # Remove in reverse order to maintain indices
    for i in reversed(expressions_to_remove):
        parsed.expressions.pop(i)

    # Add the new 'valid' column to the SELECT list
    parsed.expressions.append(valid_alias)


def apply_policy_constraints_to_aggregation(
    parsed: exp.Select,
    policies: list[DFCPolicy],
    source_tables: Set[str],
    stream_file_path: Optional[str] = None,
    sink_table: Optional[str] = None,
    sink_to_output_mapping: Optional[dict[str, str]] = None,
    replace_existing_valid: bool = False,
    insert_columns: Optional[List[str]] = None
) -> None:
    """Apply policy constraints to an aggregation query.
    
    Adds HAVING clauses for each policy constraint and ensures all referenced
    columns are accessible.
    
    Args:
        parsed: The parsed SELECT statement to modify.
        policies: List of policies to apply.
        source_tables: Set of source table names in the query.
        stream_file_path: Optional path to stream file for LLM resolution.
        sink_table: Optional sink table name (for INSERT statements).
        sink_to_output_mapping: Optional mapping from sink column names to SELECT output column names.
    """
    # Build mapping from source tables to subquery/CTE aliases
    table_mapping = _get_source_table_to_alias_mapping(parsed, source_tables)

    for policy in policies:
        # Check if policy requires source but source is not present
        # If policy has both source and sink, source must be present in the query
        policy_source = policy.source.lower() if policy.source else None
        if policy_source and sink_table and policy_source not in source_tables:
            # Policy requires source but source is not present - constraint fails
            constraint_expr = exp.Literal(this="false", is_string=False)
        else:
            constraint_expr = sqlglot.parse_one(policy.constraint, read="duckdb")

            # Replace sink table references with SELECT output column references if needed
            if sink_table and sink_to_output_mapping:
                constraint_expr = _replace_sink_table_references_in_constraint(
                    constraint_expr, sink_table, sink_to_output_mapping
                )

            # Replace table references with subquery/CTE aliases if needed
            constraint_expr = _replace_table_references_in_constraint(
                constraint_expr, table_mapping
            )

            ensure_columns_accessible(parsed, constraint_expr, source_tables)

        if policy.on_fail == Resolution.KILL:
            constraint_expr = _wrap_kill_constraint(constraint_expr)
            _add_clause_to_select(parsed, "having", constraint_expr, exp.Having)
        elif policy.on_fail == Resolution.LLM:
            constraint_expr = _wrap_llm_constraint(
                constraint_expr, policy, source_tables, stream_file_path,
                sink_table, sink_to_output_mapping, parsed=parsed,
                insert_columns=insert_columns
            )
            _add_clause_to_select(parsed, "having", constraint_expr, exp.Having)
        elif policy.on_fail == Resolution.INVALIDATE:
            _add_invalidate_column_to_select(parsed, constraint_expr, replace_existing=replace_existing_valid)
        else:
            # REMOVE resolution - add HAVING clause
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
            if from_ancestor and hasattr(from_ancestor, "this"):
                from_table = from_ancestor.this
                if isinstance(from_table, exp.Subquery):
                    # The From.this is directly the Subquery, get alias from Subquery
                    if hasattr(from_table, "alias") and from_table.alias:
                        if isinstance(from_table.alias, exp.Identifier):
                            alias = from_table.alias.name.lower()
                        elif isinstance(from_table.alias, str):
                            alias = from_table.alias.lower()
                        else:
                            alias = str(from_table.alias).lower()
                elif isinstance(from_table, exp.Table):
                    # The From.this is a Table containing the subquery
                    if hasattr(from_table, "alias") and from_table.alias:
                        if isinstance(from_table.alias, exp.Identifier):
                            alias = from_table.alias.name.lower()
                        elif isinstance(from_table.alias, str):
                            alias = from_table.alias.lower()
                        else:
                            alias = str(from_table.alias).lower()
                    if not alias and hasattr(from_table, "name") and from_table.name:
                        alias = from_table.name.lower()

            # Fallback: try finding via Table ancestor
            if not alias:
                table_ancestor = subquery.find_ancestor(exp.Table)
                if table_ancestor:
                    if hasattr(table_ancestor, "alias") and table_ancestor.alias:
                        if isinstance(table_ancestor.alias, exp.Identifier):
                            alias = table_ancestor.alias.name.lower()
                        elif isinstance(table_ancestor.alias, str):
                            alias = table_ancestor.alias.lower()
                        else:
                            alias = str(table_ancestor.alias).lower()
                    if not alias and hasattr(table_ancestor, "name") and table_ancestor.name:
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
    with_clause = parsed.args.get("with_") if hasattr(parsed, "args") else None
    if not with_clause:
        # Fallback: try accessing as attribute
        with_clause = getattr(parsed, "with_", None)
        # If it's a method, try calling it (though it shouldn't be)
        if callable(with_clause):
            with_clause = None

    if with_clause and hasattr(with_clause, "expressions"):
        for cte in with_clause.expressions:
            if isinstance(cte, exp.CTE):
                # Get the CTE alias - in sqlglot, CTE.this is the SELECT, and alias is in cte.alias
                alias = None
                # Check the alias attribute (TableAlias)
                if hasattr(cte, "alias") and cte.alias:
                    # The alias is a TableAlias, and its 'this' is the Identifier
                    if hasattr(cte.alias, "this"):
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
                if not alias and hasattr(cte, "this") and cte.this:
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
    cte_select = cte.this if hasattr(cte, "this") and isinstance(cte.this, exp.Select) else None
    if not cte_select:
        # Fallback: check expression attribute
        cte_select = cte.expression if hasattr(cte, "expression") else None
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


def _replace_sink_table_references_in_constraint(
    constraint_expr: exp.Expression,
    sink_table: str,
    sink_to_output_mapping: dict[str, str]
) -> exp.Expression:
    """Replace sink table column references in a constraint with SELECT output column references.
    
    For example, if constraint has `sink.col1` and mapping is `{'col1': 'x'}`,
    it becomes just `x` (unqualified column reference to the SELECT output).
    
    Args:
        constraint_expr: The constraint expression to modify.
        sink_table: The sink table name (lowercase).
        sink_to_output_mapping: Dictionary mapping sink column names to SELECT output column names.
        
    Returns:
        A new constraint expression with sink table references replaced.
    """
    if not sink_to_output_mapping:
        return constraint_expr

    def replace_sink_column(node):
        if isinstance(node, exp.Column):
            table_name = get_table_name_from_column(node)
            col_name = get_column_name(node).lower()

            # Check if this is a qualified sink table column (e.g., irs_form.amount)
            if table_name and table_name == sink_table:
                if col_name in sink_to_output_mapping:
                    # Replace with unqualified column reference to SELECT output
                    output_col_name = sink_to_output_mapping[col_name]
                    return exp.Column(
                        this=exp.Identifier(this=output_col_name, quoted=False)
                    )
            # Check if this is an unqualified column that matches the sink table name
            # This handles cases like sum(irs_form) where irs_form is shorthand
            elif not table_name and col_name == sink_table.lower():
                # For unqualified sink table name, we need to determine which column to use
                # In aggregate functions, this typically means we should use a specific column
                # For now, if there's only one column in the mapping, use it; otherwise use the first one
                # This is a heuristic - ideally the policy should specify the column explicitly
                if len(sink_to_output_mapping) == 1:
                    output_col_name = list(sink_to_output_mapping.values())[0]
                    return exp.Column(
                        this=exp.Identifier(this=output_col_name, quoted=False)
                    )
                # If multiple columns, try to use a common one like 'amount' or the first one
                if "amount" in sink_to_output_mapping:
                    return exp.Column(
                        this=exp.Identifier(this=sink_to_output_mapping["amount"], quoted=False)
                    )
                # Use the first column in the mapping
                output_col_name = list(sink_to_output_mapping.values())[0]
                return exp.Column(
                    this=exp.Identifier(this=output_col_name, quoted=False)
                )
        return node

    # Transform the expression, replacing all sink column references
    transformed = constraint_expr.transform(replace_sink_column, copy=True)
    return transformed


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
    if hasattr(parsed, "from") and parsed.from_:
        # Find all tables directly in the main FROM clause
        for table in parsed.from_.find_all(exp.Table):
            # Exclude tables that are subqueries (they have Subquery as 'this')
            if not (hasattr(table, "this") and isinstance(table.this, exp.Subquery)):
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
                    if not (hasattr(table, "this") and isinstance(table.this, exp.Subquery)):
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
        cte_select = cte.this if hasattr(cte, "this") and isinstance(cte.this, exp.Select) else None
        if not cte_select:
            # Fallback: check expression attribute
            cte_select = cte.expression if hasattr(cte, "expression") else None
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
        cte_select = cte.this if hasattr(cte, "this") and isinstance(cte.this, exp.Select) else None
        if not cte_select:
            cte_select = cte.expression if hasattr(cte, "expression") else None
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
    stream_file_path: Optional[str] = None,
    sink_table: Optional[str] = None,
    sink_to_output_mapping: Optional[dict[str, str]] = None,
    replace_existing_valid: bool = False,
    insert_columns: Optional[List[str]] = None
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
        stream_file_path: Optional path to stream file for LLM resolution.
        sink_table: Optional sink table name (for INSERT statements).
        sink_to_output_mapping: Optional mapping from sink column names to SELECT output column names.
    """
    # Build mapping from source tables to subquery/CTE aliases
    table_mapping = _get_source_table_to_alias_mapping(parsed, source_tables)

    for policy in policies:
        # Check if policy requires source but source is not present
        # If policy has both source and sink, source must be present in the query
        policy_source = policy.source.lower() if policy.source else None
        if policy_source and sink_table and policy_source not in source_tables:
            # Policy requires source but source is not present - constraint fails
            constraint_expr = exp.Literal(this="false", is_string=False)
        else:
            constraint_expr = transform_aggregations_to_columns(
                policy._constraint_parsed, source_tables
            )

            # Replace sink table references with SELECT output column references if needed
            if sink_table and sink_to_output_mapping:
                constraint_expr = _replace_sink_table_references_in_constraint(
                    constraint_expr, sink_table, sink_to_output_mapping
                )

            # Replace table references with subquery/CTE aliases if needed
            constraint_expr = _replace_table_references_in_constraint(
                constraint_expr, table_mapping
            )

        if policy.on_fail == Resolution.KILL:
            constraint_expr = _wrap_kill_constraint(constraint_expr)
            _add_clause_to_select(parsed, "where", constraint_expr, exp.Where)
        elif policy.on_fail == Resolution.LLM:
            constraint_expr = _wrap_llm_constraint(
                constraint_expr, policy, source_tables, stream_file_path,
                sink_table, sink_to_output_mapping, parsed=parsed,
                insert_columns=insert_columns
            )
            _add_clause_to_select(parsed, "where", constraint_expr, exp.Where)
        elif policy.on_fail == Resolution.INVALIDATE:
            _add_invalidate_column_to_select(parsed, constraint_expr, replace_existing=replace_existing_valid)
        else:
            # REMOVE resolution - add WHERE clause
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
            agg_name = node.sql_name().upper() if hasattr(node, "sql_name") else str(node).upper()
            agg_class = node.__class__.__name__.upper()

            columns = list(node.find_all(exp.Column))

            if agg_name in ("COUNT_IF", "COUNTIF"):
                condition = node.this if hasattr(node, "this") and node.this else None
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
                return exp.Literal(this="1", is_string=False)

            count_like_sql_names = {
                "COUNT", "COUNT_STAR",
                "APPROX_DISTINCT",  # APPROX_COUNT_DISTINCT is parsed as APPROX_DISTINCT
                "REGR_COUNT",
            }

            is_count_with_distinct = (
                agg_name == "COUNT" and
                hasattr(node, "distinct") and
                node.distinct
            )

            if agg_name in count_like_sql_names or is_count_with_distinct:
                return exp.Literal(this="1", is_string=False)

            # Note: 'list' in DuckDB is not parsed as AggFunc by sqlglot, so we only handle array_agg
            if agg_name == "ARRAY_AGG" or agg_class == "ARRAYAGG":
                if columns:
                    col = columns[0]
                    col_sql = col.sql()
                    col_copy = sqlglot.parse_one(col_sql, read="duckdb")
                    return exp.Array(expressions=[col_copy])
                return exp.Array(expressions=[exp.Literal(this="NULL", is_string=False)])

            # This preserves complex expressions like CASE WHEN, function calls, etc.
            if hasattr(node, "this") and node.this:
                expr_sql = node.this.sql()
                expr_copy = sqlglot.parse_one(expr_sql, read="duckdb")
                return expr_copy
            return exp.Literal(this="1", is_string=False)
        return node

    transformed = transformed.transform(replace_agg, copy=True)
    return transformed


def get_policy_identifier(policy: AggregateDFCPolicy) -> str:
    """Generate a unique identifier for a policy for temp column naming.
    
    Args:
        policy: The AggregateDFCPolicy instance.
        
    Returns:
        A string identifier derived from the policy.
    """
    # Use a hash of the constraint and source/sink to create a unique identifier
    import hashlib
    policy_str = f"{policy.source or ''}_{policy.sink or ''}_{policy.constraint}"
    hash_obj = hashlib.md5(policy_str.encode())
    return f"policy_{hash_obj.hexdigest()[:8]}"


def _extract_source_aggregates_from_constraint(
    constraint_expr: exp.Expression,
    source_table: str
) -> List[exp.AggFunc]:
    """Extract innermost source aggregate expressions from a constraint.
    
    For nested aggregates like max(sum(foo.amount)), extracts the innermost
    aggregate (sum(foo.amount)) that directly contains the source column.
    The outer aggregate (max) will be applied during finalize.
    
    Args:
        constraint_expr: The parsed constraint expression.
        source_table: The source table name.
        
    Returns:
        List of innermost aggregate function expressions that reference the source table.
    """
    aggregates = []
    seen = set()

    for agg_func in constraint_expr.find_all(exp.AggFunc):
        # Check if this aggregate references the source table directly
        columns = list(agg_func.find_all(exp.Column))
        has_source_column = False
        for column in columns:
            table_name = get_table_name_from_column(column)
            if table_name == source_table.lower():
                has_source_column = True
                break

        if not has_source_column:
            continue

        # Check if this aggregate is nested inside another aggregate
        # by checking if its parent is an AggFunc
        parent = agg_func.parent
        is_nested = isinstance(parent, exp.AggFunc)

        # Check if this aggregate wraps another aggregate (it's an outer aggregate)
        # In sqlglot, nested aggregates have the inner aggregate as the 'this' attribute
        has_nested_agg = hasattr(agg_func, "this") and isinstance(agg_func.this, exp.AggFunc)

        if is_nested and not has_nested_agg:
            # This is nested inside another aggregate and doesn't wrap another aggregate
            # So it's the innermost one we want
            agg_sql = agg_func.sql()
            if agg_sql not in seen:
                seen.add(agg_sql)
                # Parse fresh to avoid mutability issues
                agg_copy = sqlglot.parse_one(agg_sql, read="duckdb")
                aggregates.append(agg_copy)
        elif not is_nested and not has_nested_agg:
            # Not nested and doesn't wrap another aggregate - it's a simple aggregate
            agg_sql = agg_func.sql()
            if agg_sql not in seen:
                seen.add(agg_sql)
                agg_copy = sqlglot.parse_one(agg_sql, read="duckdb")
                aggregates.append(agg_copy)

    return aggregates


def _find_outer_aggregate_for_inner(
    constraint_expr: exp.Expression,
    inner_agg_sql: str
) -> Optional[str]:
    """Find the outer aggregate function name that wraps an inner aggregate.
    
    For nested aggregates like max(sum(foo.amount)), if inner_agg_sql is
    "sum(foo.amount)", returns "MAX".
    
    Args:
        constraint_expr: The parsed constraint expression.
        inner_agg_sql: The SQL of the inner aggregate expression.
        
    Returns:
        The outer aggregate function name (e.g., "MAX", "MIN") or None if not nested.
    """
    # Find all aggregates in the constraint
    for agg_func in constraint_expr.find_all(exp.AggFunc):
        # Check if this aggregate wraps the inner aggregate
        # In sqlglot, nested aggregates have the inner aggregate as the 'this' attribute
        if hasattr(agg_func, "this") and isinstance(agg_func.this, exp.AggFunc):
            # Check if the inner aggregate matches
            inner_agg_in_expr = agg_func.this
            if inner_agg_in_expr.sql().upper() == inner_agg_sql.upper():
                # This aggregate wraps the inner one - return its function name
                # The function name is the class name (Max, Min, etc.) or sql_name()
                if hasattr(agg_func, "sql_name"):
                    return agg_func.sql_name().upper()
                # Use class name (Max -> MAX, Min -> MIN, etc.)
                class_name = type(agg_func).__name__
                return class_name.upper()

    return None


def _extract_sink_expressions_from_constraint(
    constraint_expr: exp.Expression,
    sink_table: str,
    sink_to_output_mapping: Optional[dict[str, str]] = None
) -> List[exp.Expression]:
    """Extract all sink column/expression references from a constraint.
    
    Args:
        constraint_expr: The parsed constraint expression.
        sink_table: The sink table name.
        sink_to_output_mapping: Optional mapping from sink column names to SELECT output column names.
        
    Returns:
        List of expressions that reference the sink table (columns or aggregates).
    """
    expressions = []
    seen = set()

    # First, find all aggregate functions that reference the sink table
    # This handles cases like sum(irs_form) where irs_form is unqualified
    for agg_func in constraint_expr.find_all(exp.AggFunc):
        # Check if this aggregate references the sink table
        references_sink = False

        # Check the direct argument to the aggregate (agg_func.this)
        if hasattr(agg_func, "this"):
            this_expr = agg_func.this
            if isinstance(this_expr, exp.Column):
                table_name = get_table_name_from_column(this_expr)
                col_name = get_column_name(this_expr).lower()

                # Check if column references sink table (qualified or unqualified match)
                if table_name == sink_table.lower():
                    references_sink = True
                # Check if unqualified column matches sink table name (shorthand like sum(irs_form))
                elif not table_name and col_name == sink_table.lower():
                    # The direct argument to an aggregate is never inside a FILTER clause
                    # (FILTER is a separate attribute of the aggregate, not a parent of 'this')
                    references_sink = True

        # Also check other columns in the aggregate (for cases where sink table is referenced elsewhere)
        if not references_sink:
            columns = list(agg_func.find_all(exp.Column))
            for column in columns:
                table_name = get_table_name_from_column(column)
                if table_name == sink_table.lower():
                    # Skip columns inside FILTER clauses (they're part of the filter, not the aggregate expression)
                    if column.find_ancestor(exp.Filter) is None:
                        references_sink = True
                        break

        if references_sink:
            # Extract the whole aggregate (including FILTER clause if present)
            # If the aggregate is wrapped in a FILTER, extract the Filter node instead
            if isinstance(agg_func.parent, exp.Filter):
                # The FILTER wraps the aggregate, so extract the Filter node
                filter_node = agg_func.parent
                filter_sql = filter_node.sql()
                if filter_sql not in seen:
                    seen.add(filter_sql)
                    filter_copy = sqlglot.parse_one(filter_sql, read="duckdb")
                    expressions.append(filter_copy)
            else:
                # No FILTER, just extract the aggregate
                agg_sql = agg_func.sql()
                if agg_sql not in seen:
                    seen.add(agg_sql)
                    agg_copy = sqlglot.parse_one(agg_sql, read="duckdb")
                    expressions.append(agg_copy)

    # Also find non-aggregate columns that reference the sink table
    # Skip columns that are already part of aggregates we extracted (including those in FILTER clauses)
    for column in constraint_expr.find_all(exp.Column):
        table_name = get_table_name_from_column(column)
        if table_name == sink_table.lower():
            # Skip if this column is already part of an aggregate we extracted
            agg_ancestor = column.find_ancestor(exp.AggFunc)
            if agg_ancestor:
                # Check if we already extracted this aggregate (or its Filter wrapper)
                agg_sql = agg_ancestor.sql()
                # Check if the aggregate is wrapped in a Filter
                if isinstance(agg_ancestor.parent, exp.Filter):
                    filter_sql = agg_ancestor.parent.sql()
                    if filter_sql in seen:
                        continue
                if agg_sql in seen:
                    continue

            # Skip columns inside FILTER clauses - they're part of the filter condition, not standalone expressions
            if column.find_ancestor(exp.Filter) is not None:
                continue

            # This is a regular sink column (not in an aggregate)
            col_sql = column.sql()
            if sink_to_output_mapping:
                # Map to output column name
                col_name = get_column_name(column).lower()
                if col_name in sink_to_output_mapping:
                    output_col_name = sink_to_output_mapping[col_name]
                    col_expr = exp.Column(
                        this=exp.Identifier(this=output_col_name, quoted=False)
                    )
                    col_sql = col_expr.sql()

            if col_sql not in seen:
                seen.add(col_sql)
                col_copy = sqlglot.parse_one(col_sql, read="duckdb")
                expressions.append(col_copy)

    return expressions


def _add_temp_column_to_select(
    parsed: exp.Select,
    expr: exp.Expression,
    column_name: str,
    source_tables: Optional[Set[str]] = None
) -> None:
    """Add a temp column to a SELECT statement.
    
    Args:
        parsed: The parsed SELECT statement to modify.
        expr: The expression to add as a column.
        column_name: The name for the temp column.
        source_tables: Optional set of source table names to help ensure columns are accessible.
    """
    # Create a fresh copy of the expression to avoid mutability issues
    expr_sql = expr.sql()
    expr_copy = sqlglot.parse_one(expr_sql, read="duckdb")

    # Check if this is a scan query (no GROUP BY, no aggregations in main SELECT)
    # For scan queries, FILTER clauses in aggregates can't reference SELECT output columns
    # We need to convert FILTER to CASE expression
    has_group_by = parsed.args.get("group") is not None
    is_scan_query = not has_group_by and not any(
        isinstance(e, exp.AggFunc) or
        (isinstance(e, exp.Alias) and isinstance(e.this, exp.AggFunc))
        for e in parsed.expressions
        if not isinstance(e, exp.Alias) or not isinstance(e.this, exp.Subquery)
    )

    # If it's a scan query and we have a Filter-wrapped aggregate, handle it specially
    # For scan queries, we can't use aggregates in SELECT without GROUP BY
    # We also can't reference SELECT output columns in the condition
    # So we'll just store the scalar value (amount or 0) and let finalize aggregate it
    if is_scan_query and isinstance(expr_copy, exp.Filter):
        agg_func = expr_copy.this
        if isinstance(agg_func, exp.AggFunc):
            where_expr = expr_copy.expression  # This is a Where expression
            condition = where_expr.this if hasattr(where_expr, "this") else where_expr

            # Get the aggregate argument (e.g., 'amount' from SUM(amount))
            agg_arg = agg_func.this if hasattr(agg_func, "this") else exp.Literal(this="1", is_string=False)

            # Check if condition references SELECT output columns (unqualified columns that are in SELECT)
            # Get all SELECT output column names (aliases or column names)
            select_output_cols = set()
            for expr in parsed.expressions:
                if isinstance(expr, exp.Alias):
                    alias_name = get_column_name(expr.alias).lower()
                    select_output_cols.add(alias_name)
                    # Also check if the expression itself is a column
                    if isinstance(expr.this, exp.Column):
                        col_name = get_column_name(expr.this).lower()
                        select_output_cols.add(col_name)
                elif isinstance(expr, exp.Column):
                    col_name = get_column_name(expr).lower()
                    select_output_cols.add(col_name)

            # Check if condition references any SELECT output columns
            condition_cols = [get_column_name(col).lower() for col in condition.find_all(exp.Column)
                             if not get_table_name_from_column(col)]  # Unqualified columns
            references_select_output = any(col in select_output_cols for col in condition_cols)

            if references_select_output:
                # Replace SELECT output column references with their actual values/expressions
                # Build mapping from output column names to their expressions
                output_col_to_expr = {}
                for expr in parsed.expressions:
                    output_col_name = None
                    if isinstance(expr, exp.Alias):
                        alias_name = get_column_name(expr.alias).lower()
                        output_col_to_expr[alias_name] = expr.this
                        # Also map the underlying column name if it's a column
                        if isinstance(expr.this, exp.Column):
                            col_name = get_column_name(expr.this).lower()
                            output_col_to_expr[col_name] = expr.this
                    elif isinstance(expr, exp.Column):
                        col_name = get_column_name(expr).lower()
                        output_col_to_expr[col_name] = expr

                # Replace column references in condition with their actual expressions
                def replace_output_col_refs(node):
                    if isinstance(node, exp.Column):
                        col_name = get_column_name(node).lower()
                        table_name = get_table_name_from_column(node)
                        # If it's an unqualified column that matches a SELECT output
                        if not table_name and col_name in output_col_to_expr:
                            # Replace with the actual expression/value
                            replacement = output_col_to_expr[col_name]

                            # If replacement is a Literal, use it directly
                            if isinstance(replacement, exp.Literal):
                                # Create a fresh copy
                                return exp.Literal(this=replacement.this, is_string=replacement.is_string)

                            # If replacement is a Column that's actually a quoted string literal,
                            # extract the value and create a proper Literal
                            if isinstance(replacement, exp.Column):
                                # Check if it's a quoted identifier that should be a literal
                                col_identifier = replacement.this
                                if isinstance(col_identifier, exp.Identifier):
                                    # Extract the name (which might be the string value)
                                    str_value = col_identifier.name if hasattr(col_identifier, "name") else str(col_identifier)
                                    # Create a proper string literal
                                    return exp.Literal(this=str_value, is_string=True)

                            # For other expression types, create a fresh copy
                            replacement_sql = replacement.sql()
                            # Parse as an expression (not a full statement)
                            # Wrap in parentheses to ensure proper parsing
                            parsed_replacement = sqlglot.parse_one(f"({replacement_sql})", read="duckdb")
                            # Extract the expression from the parentheses
                            if isinstance(parsed_replacement, exp.Paren):
                                return parsed_replacement.this
                            return parsed_replacement
                    return node

                # Transform the condition to replace output column references
                condition_replaced = condition.transform(replace_output_col_refs, copy=True)

                # Now use the replaced condition in CASE expression
                case_expr = exp.Case(
                    ifs=[exp.If(this=condition_replaced, true=agg_arg)],
                    default=exp.Literal(this="0", is_string=False)
                )
                expr_copy = case_expr
            else:
                # Condition can be evaluated - use CASE expression (without aggregate for scan query)
                case_expr = exp.Case(
                    ifs=[exp.If(this=condition, true=agg_arg)],
                    default=exp.Literal(this="0", is_string=False)
                )
                expr_copy = case_expr

    # Check if expression references columns that need to be accessible
    # If it's a Filter-wrapped aggregate (for aggregation queries), ensure filter columns are accessible
    if isinstance(expr_copy, exp.Filter):
        # Extract columns from the filter condition
        filter_columns = list(expr_copy.find_all(exp.Column))
        # Ensure these columns are accessible in the SELECT context
        if source_tables:
            ensure_columns_accessible(parsed, expr_copy, source_tables)

    # Create alias
    alias = exp.Alias(
        this=expr_copy,
        alias=exp.Identifier(this=column_name, quoted=False)
    )

    # Add to SELECT list
    parsed.expressions.append(alias)


def apply_aggregate_policy_constraints_to_aggregation(
    parsed: exp.Select,
    policies: list[AggregateDFCPolicy],
    source_tables: Set[str],
    sink_table: Optional[str] = None,
    sink_to_output_mapping: Optional[dict[str, str]] = None
) -> None:
    """Apply aggregate policy constraints to an aggregation query.
    
    Adds temp columns for source aggregates (inner aggregates) and sink expressions.
    These temp columns will be used during finalize to compute outer aggregates.
    
    Args:
        parsed: The parsed SELECT statement to modify.
        policies: List of aggregate policies to apply.
        source_tables: Set of source table names in the query.
        sink_table: Optional sink table name (for INSERT statements).
        sink_to_output_mapping: Optional mapping from sink column names to SELECT output column names.
    """
    for policy in policies:
        policy_id = get_policy_identifier(policy)
        temp_col_counter = 1

        # Extract and add source aggregates
        if policy.source and policy.source.lower() in source_tables:
            source_aggregates = _extract_source_aggregates_from_constraint(
                policy._constraint_parsed, policy.source
            )

            for agg_expr in source_aggregates:
                temp_col_name = f"_{policy_id}_tmp{temp_col_counter}"
                _add_temp_column_to_select(parsed, agg_expr, temp_col_name, source_tables)
                temp_col_counter += 1

        # Extract and add sink expressions
        if policy.sink and sink_table and policy.sink.lower() == sink_table.lower():
            # Extract sink expressions BEFORE replacement (they need to reference the sink table)
            constraint_expr_orig = sqlglot.parse_one(policy.constraint, read="duckdb")
            sink_expressions = _extract_sink_expressions_from_constraint(
                constraint_expr_orig, sink_table, sink_to_output_mapping
            )

            for sink_expr in sink_expressions:
                # Replace sink table references in the expression (including FILTER clauses)
                # This is needed because sink table columns need to reference SELECT output columns
                if sink_to_output_mapping:
                    sink_expr = _replace_sink_table_references_in_constraint(
                        sink_expr, sink_table, sink_to_output_mapping
                    )

                temp_col_name = f"_{policy_id}_tmp{temp_col_counter}"
                _add_temp_column_to_select(parsed, sink_expr, temp_col_name, source_tables)
                temp_col_counter += 1


def apply_aggregate_policy_constraints_to_scan(
    parsed: exp.Select,
    policies: list[AggregateDFCPolicy],
    source_tables: Set[str],
    sink_table: Optional[str] = None,
    sink_to_output_mapping: Optional[dict[str, str]] = None
) -> None:
    """Apply aggregate policy constraints to a scan (non-aggregation) query.
    
    Adds temp columns for source aggregates (inner aggregates computed per row/group)
    and sink expressions. Source columns still need inner aggregation even in scan queries.
    
    Args:
        parsed: The parsed SELECT statement to modify.
        policies: List of aggregate policies to apply.
        source_tables: Set of source table names in the query.
        sink_table: Optional sink table name (for INSERT statements).
        sink_to_output_mapping: Optional mapping from sink column names to SELECT output column names.
    """
    for policy in policies:
        policy_id = get_policy_identifier(policy)
        temp_col_counter = 1

        # Extract and add source aggregates (still need inner aggregation in scan queries)
        if policy.source and policy.source.lower() in source_tables:
            source_aggregates = _extract_source_aggregates_from_constraint(
                policy._constraint_parsed, policy.source
            )

            for agg_expr in source_aggregates:
                temp_col_name = f"_{policy_id}_tmp{temp_col_counter}"
                _add_temp_column_to_select(parsed, agg_expr, temp_col_name, source_tables)
                temp_col_counter += 1

        # Extract and add sink expressions
        if policy.sink and sink_table and policy.sink.lower() == sink_table.lower():
            # Extract sink expressions BEFORE replacement (they need to reference the sink table)
            constraint_expr_orig = sqlglot.parse_one(policy.constraint, read="duckdb")
            sink_expressions = _extract_sink_expressions_from_constraint(
                constraint_expr_orig, sink_table, sink_to_output_mapping
            )

            for sink_expr in sink_expressions:
                # Replace sink table references in the expression (including FILTER clauses)
                # This is needed because sink table columns need to reference SELECT output columns
                if sink_to_output_mapping:
                    sink_expr = _replace_sink_table_references_in_constraint(
                        sink_expr, sink_table, sink_to_output_mapping
                    )

                temp_col_name = f"_{policy_id}_tmp{temp_col_counter}"
                _add_temp_column_to_select(parsed, sink_expr, temp_col_name, source_tables)
                temp_col_counter += 1

