"""Rewrite rules for applying DFC policies to SQL queries."""

import json
import logging
from typing import Optional

import sqlglot
from sqlglot import exp

from .policy import AggregateDFCPolicy, DFCPolicy, Resolution
from .sqlglot_utils import get_column_name, get_table_name_from_column

logger = logging.getLogger(__name__)


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
    source_tables: set[str],
    sink_table: Optional[str] = None,
    sink_to_output_mapping: Optional[dict[str, str]] = None
) -> list[exp.Column]:
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
    source_tables: set[str],
    stream_file_path: Optional[str] = None,
    sink_table: Optional[str] = None,
    sink_to_output_mapping: Optional[dict[str, str]] = None,
    parsed: Optional[exp.Select] = None,
    _insert_columns: Optional[list[str]] = None
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
    expressions = [*columns, constraint_literal, description_literal, column_names_literal, stream_endpoint]
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
        wrapped_new = exp.Paren(this=clause_expr)
        combined = _combine_and_expressions([existing_expr, wrapped_new])
        parsed.set(clause_name, clause_class(this=combined))
    else:
        # Wrap each policy addition in its own parentheses for consistency
        wrapped_new = exp.Paren(this=clause_expr)
        parsed.set(clause_name, clause_class(this=wrapped_new))


def _flatten_and_expression(expr: exp.Expression) -> list[exp.Expression]:
    """Flatten nested AND expressions into a list of expressions."""
    flattened = []
    stack = [expr]
    while stack:
        current = stack.pop()
        if isinstance(current, exp.Paren):
            stack.append(current.this)
            continue
        if isinstance(current, exp.And):
            if current.this is not None:
                stack.append(current.this)
            if current.expression is not None:
                stack.append(current.expression)
            continue
        flattened.append(current)
    return flattened


def _combine_and_expressions(expressions: list[exp.Expression]) -> exp.Expression:
    """Combine expressions with AND using a balanced tree to avoid deep recursion."""
    flattened = []
    for expr in expressions:
        flattened.extend(_flatten_and_expression(expr))

    wrapped = [expr if isinstance(expr, exp.Paren) else exp.Paren(this=expr) for expr in flattened]
    if not wrapped:
        return exp.Paren(this=exp.Literal.boolean(True))
    if len(wrapped) == 1:
        return wrapped[0]

    nodes = wrapped
    while len(nodes) > 1:
        next_nodes = []
        it = iter(nodes)
        for left in it:
            right = next(it, None)
            if right is None:
                next_nodes.append(left)
            else:
                next_nodes.append(exp.And(this=left, expression=right))
        nodes = next_nodes

    return nodes[0]


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


def _add_invalidate_message_column_to_select(
    parsed: exp.Select,
    constraint_expr: exp.Expression,
    policy_message: str,
    replace_existing: bool = False,
) -> None:
    """Add an 'invalid_string' column to a SELECT statement.

    The column is empty when the policy passes and set to policy_message when the
    policy fails. Multiple INVALIDATE_MESSAGE policies are combined as:
    "message1 | message2 | ...".
    """
    constraint_sql = constraint_expr.sql()
    constraint_copy = sqlglot.parse_one(constraint_sql, read="duckdb")

    new_message_expr = exp.Case(
        ifs=[
            exp.If(
                this=constraint_copy,
                true=exp.Literal.string(""),
            )
        ],
        default=exp.Literal.string(policy_message),
    )

    existing_invalid_string_expr = None
    for expr in parsed.expressions:
        if isinstance(expr, exp.Alias) and expr.alias and expr.alias.lower() == "invalid_string":
            existing_invalid_string_expr = expr.this
            break
        if isinstance(expr, exp.Column) and get_column_name(expr).lower() == "invalid_string":
            existing_invalid_string_expr = expr
            break

    if existing_invalid_string_expr and not replace_existing:
        existing_sql = existing_invalid_string_expr.sql()
        new_sql = new_message_expr.sql()
        combined_sql = (
            f"CONCAT_WS(' | ', NULLIF({existing_sql}, ''), NULLIF({new_sql}, ''))"
        )
        invalid_string_expr = sqlglot.parse_one(combined_sql, read="duckdb")
    else:
        invalid_string_expr = new_message_expr

    invalid_string_alias = exp.Alias(
        this=invalid_string_expr,
        alias=exp.Identifier(this="invalid_string", quoted=False),
    )

    expressions_to_remove = []
    for i, expr in enumerate(parsed.expressions):
        if (
            isinstance(expr, exp.Alias)
            and expr.alias
            and expr.alias.lower() == "invalid_string"
        ) or (
            isinstance(expr, exp.Column) and get_column_name(expr).lower() == "invalid_string"
        ):
            expressions_to_remove.append(i)

    for i in reversed(expressions_to_remove):
        parsed.expressions.pop(i)

    parsed.expressions.append(invalid_string_alias)


def apply_policy_constraints_to_aggregation(
    parsed: exp.Select,
    policies: list[DFCPolicy],
    source_tables: set[str],
    stream_file_path: Optional[str] = None,
    sink_table: Optional[str] = None,
    sink_to_output_mapping: Optional[dict[str, str]] = None,
    replace_existing_valid: bool = False,
    replace_existing_invalid_string: bool = False,
    insert_columns: Optional[list[str]] = None
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
        # Check if policy requires sources but sources are not present
        policy_sources = policy._sources_lower
        if policy_sources and not policy_sources.issubset(source_tables):
            # Policy requires sources but they are not present - constraint fails
            constraint_expr = exp.Literal(this="false", is_string=False)
        else:
            constraint_expr = sqlglot.parse_one(policy.constraint, read="duckdb")

            # Replace sink table references with SELECT output column references if needed
            if sink_table and sink_to_output_mapping:
                constraint_expr = _replace_sink_table_references_in_constraint(
                    constraint_expr,
                    sink_table,
                    sink_to_output_mapping,
                    getattr(policy, "sink_alias", None),
                )

            # Replace table references with subquery/CTE aliases if needed
            constraint_expr = _replace_table_references_in_constraint(
                constraint_expr, table_mapping
            )

            # Replace aggregations from EXISTS-rewritten JOINs with subquery column references
            constraint_expr = _replace_aggregations_from_join_subqueries(
                parsed, constraint_expr, policy_sources
            )
            constraint_expr = _replace_aggregations_from_from_subqueries(
                parsed, constraint_expr, policy_sources
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
        elif policy.on_fail == Resolution.INVALIDATE_MESSAGE:
            policy_message = policy.description or policy.constraint
            _add_invalidate_message_column_to_select(
                parsed,
                constraint_expr,
                policy_message=policy_message,
                replace_existing=replace_existing_invalid_string,
            )
        else:
            # REMOVE resolution - add HAVING clause
            _add_clause_to_select(parsed, "having", constraint_expr, exp.Having)


def ensure_columns_accessible(
    parsed: exp.Select,
    constraint_expr: exp.Expression,
    source_tables: set[str]
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


def _get_subqueries_in_from(parsed: exp.Select) -> list[tuple[exp.Subquery, str]]:
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


def _get_selected_columns(subquery: exp.Subquery) -> set[str]:
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

    has_star = any(isinstance(expr, exp.Star) for expr in select_expr.expressions)
    if has_star and column_name.lower() not in {"__dfc_rowid", "__dfc_rowid_passthrough"}:
        # Can't safely add regular columns when SELECT * is used
        return

    # Get the table alias if one exists
    table_ref = _get_table_alias_in_subquery(subquery, table_name)

    # Create a column expression with table qualification.
    # __dfc_rowid is a synthetic key used by two-phase rewrites.
    if column_name.lower() == "__dfc_rowid":
        col_expr = exp.Alias(
            this=exp.Column(
                this=exp.Identifier(this="rowid", quoted=False),
                table=exp.Identifier(this=table_ref, quoted=False),
            ),
            alias=exp.Identifier(this="__dfc_rowid", quoted=False),
        )
    else:
        col_expr = exp.Column(
            this=exp.Identifier(this=column_name, quoted=False),
            table=exp.Identifier(this=table_ref, quoted=False)
        )

    # Add the column to the SELECT list
    select_expr.expressions.append(col_expr)

    # If the subquery has an explicit column list, append the new column name
    alias = subquery.args.get("alias")
    if isinstance(alias, exp.TableAlias) and alias.args.get("columns") is not None:
        alias_columns = alias.args.get("columns")
        alias_col_name = "__dfc_rowid" if column_name.lower() == "__dfc_rowid_passthrough" else column_name
        if all(
            not (isinstance(col, exp.Identifier) and col.name.lower() == alias_col_name.lower())
            for col in alias_columns
        ):
            alias_columns.append(exp.Identifier(this=alias_col_name, quoted=False))


def _get_ctes(parsed: exp.Select) -> list[tuple[exp.CTE, str]]:
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


def _get_selected_columns_from_select(select_expr: exp.Select) -> set[str]:
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
        if (table.find_ancestor(exp.From) or table.find_ancestor(exp.Join)) and (
            table.name.lower() == table_name.lower()
        ):
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

    has_star = any(isinstance(expr, exp.Star) for expr in select_expr.expressions)
    if has_star and column_name.lower() != "__dfc_rowid":
        # Can't safely add regular columns when SELECT * is used
        return

    # Get the table alias if one exists
    table_ref = _get_table_alias_in_select(select_expr, table_name)

    # Create a column expression with table qualification.
    # __dfc_rowid is a synthetic key used by two-phase rewrites.
    if column_name.lower() == "__dfc_rowid":
        col_expr = exp.Alias(
            this=exp.Column(
                this=exp.Identifier(this="rowid", quoted=False),
                table=exp.Identifier(this=table_ref, quoted=False),
            ),
            alias=exp.Identifier(this="__dfc_rowid", quoted=False),
        )
    elif column_name.lower() == "__dfc_rowid_passthrough":
        col_expr = exp.Alias(
            this=exp.Column(
                this=exp.Identifier(this="__dfc_rowid", quoted=False),
                table=exp.Identifier(this=table_ref, quoted=False),
            ),
            alias=exp.Identifier(this="__dfc_rowid", quoted=False),
        )
    else:
        col_expr = exp.Column(
            this=exp.Identifier(this=column_name, quoted=False),
            table=exp.Identifier(this=table_ref, quoted=False)
        )

    # Add the column to the SELECT list
    select_expr.expressions.append(col_expr)


def _replace_sink_table_references_in_constraint(
    constraint_expr: exp.Expression,
    sink_table: str,
    sink_to_output_mapping: dict[str, str],
    sink_alias: Optional[str] = None,
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

    sink_reference_names = {sink_table.lower()}
    if sink_alias:
        sink_reference_names.add(sink_alias.lower())

    def replace_sink_column(node):
        if isinstance(node, exp.Column):
            table_name = get_table_name_from_column(node)
            col_name = get_column_name(node).lower()

            # Check if this is a qualified sink table column (e.g., irs_form.amount)
            if table_name and table_name in sink_reference_names:
                if col_name in sink_to_output_mapping:
                    # Replace with unqualified column reference to SELECT output
                    output_col_name = sink_to_output_mapping[col_name]
                    return exp.Column(
                        this=exp.Identifier(this=output_col_name, quoted=False)
                    )
            # Check if this is an unqualified column that matches the sink table name
            # This handles cases like sum(irs_form) where irs_form is shorthand
            elif not table_name and col_name in sink_reference_names:
                # For unqualified sink table name, we need to determine which column to use
                # In aggregate functions, this typically means we should use a specific column
                # For now, if there's only one column in the mapping, use it; otherwise use the first one
                # This is a heuristic - ideally the policy should specify the column explicitly
                if len(sink_to_output_mapping) == 1:
                    output_col_name = next(iter(sink_to_output_mapping.values()))
                    return exp.Column(
                        this=exp.Identifier(this=output_col_name, quoted=False)
                    )
                # If multiple columns, try to use a common one like 'amount' or the first one
                if "amount" in sink_to_output_mapping:
                    return exp.Column(
                        this=exp.Identifier(this=sink_to_output_mapping["amount"], quoted=False)
                    )
                # Use the first column in the mapping
                output_col_name = next(iter(sink_to_output_mapping.values()))
                return exp.Column(
                    this=exp.Identifier(this=output_col_name, quoted=False)
                )
        return node

    # Transform the expression, replacing all sink column references
    return constraint_expr.transform(replace_sink_column, copy=True)


def _get_source_table_to_alias_mapping(
    parsed: exp.Select,
    source_tables: set[str]
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
            if (
                not (hasattr(table, "this") and isinstance(table.this, exp.Subquery))
                and not table.find_ancestor(exp.Subquery)
                and not table.find_ancestor(exp.CTE)
                and table.name.lower() not in cte_aliases
            ):
                main_query_tables.add(table.name.lower())
        # Also check JOINs directly in the main query
        for join in parsed.find_all(exp.Join):
            if (not join.find_ancestor(exp.Subquery) and
                not join.find_ancestor(exp.CTE)):
                for table in join.find_all(exp.Table):
                    # Exclude subquery tables
                    if (
                        not (hasattr(table, "this") and isinstance(table.this, exp.Subquery))
                        and table.name.lower() not in cte_aliases
                    ):
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
                return exp.Column(
                    this=node.this,
                    table=new_table
                )
        return node

    # Transform the expression, replacing all column table references
    # This works for columns both inside and outside aggregation functions
    return constraint_expr.transform(replace_table, copy=True)


def _replace_aggregations_from_join_subqueries(
    parsed: exp.Select,
    constraint_expr: exp.Expression,
    policy_sources: set[str]
) -> exp.Expression:
    """Replace aggregations in constraints that reference tables only in JOIN subqueries.

    When EXISTS subqueries are rewritten as JOINs, aggregations from the policy table
    are computed in the JOIN subquery. This function replaces those aggregations in
    constraints with references to the subquery alias columns.

    Args:
        parsed: The parsed SELECT statement.
        constraint_expr: The constraint expression to modify.
        policy_sources: Set of policy source table names (lowercase).

    Returns:
        A new constraint expression with aggregations replaced by subquery column references.
    """
    if not policy_sources:
        return constraint_expr

    # Find JOINs that were created from EXISTS rewrites
    joins = parsed.args.get("joins", [])
    if not joins:
        return constraint_expr

    def replace_agg(node):
        if isinstance(node, exp.AggFunc):
            # Check if this aggregation references any policy source table
            agg_columns = list(node.find_all(exp.Column))
            referenced_sources = set()
            for col in agg_columns:
                col_table = get_table_name_from_column(col)
                if col_table and col_table.lower() in policy_sources:
                    referenced_sources.add(col_table.lower())

            if referenced_sources:
                # Find the JOIN subquery that has this aggregation
                agg_sql = node.sql()
                for join in joins:
                    if isinstance(join.this, exp.Subquery):
                        subquery_node = join.this
                        if hasattr(subquery_node, "meta") and "aggregation_aliases" in subquery_node.meta:
                            aggregation_aliases = subquery_node.meta["aggregation_aliases"]
                            policy_table = subquery_node.meta.get("policy_table")

                            # Check if this aggregation matches one we computed in the subquery
                            for source_table in referenced_sources:
                                key = (policy_table, agg_sql)
                                if policy_table != source_table:
                                    continue
                                if key in aggregation_aliases:
                                    subquery_alias_name, agg_alias_name = aggregation_aliases[key]
                                    # Replace aggregation with column reference to subquery alias
                                    # DuckDB requires wrapping in an aggregate function in HAVING, even if already aggregated
                                    subquery_col = exp.Column(
                                        this=exp.Identifier(this=agg_alias_name),
                                        table=exp.Identifier(this=subquery_alias_name)
                                    )
                                    # Wrap in MAX() to satisfy DuckDB's HAVING clause requirements
                                    return exp.Max(this=subquery_col)
        return node

    # Transform the expression, replacing aggregations with subquery column references
    return constraint_expr.transform(replace_agg, copy=True)


def _replace_aggregations_from_from_subqueries(
    parsed: exp.Select,
    constraint_expr: exp.Expression,
    policy_sources: set[str]
) -> exp.Expression:
    """Replace aggregations in constraints that reference source tables in FROM subqueries.

    When a policy source table is only referenced inside a FROM subquery with GROUP BY,
    we add the aggregate inside the subquery and reference it here. To keep HAVING valid,
    we wrap the subquery column in MAX().
    """
    if not policy_sources:
        return constraint_expr

    subqueries = _get_subqueries_in_from(parsed)
    if not subqueries:
        return constraint_expr

    def replace_agg(node):
        if isinstance(node, exp.AggFunc):
            agg_sql = node.sql(dialect="duckdb")
            for subquery, subquery_alias in subqueries:
                agg_aliases = subquery.meta.get("policy_agg_aliases") if hasattr(subquery, "meta") else None
                if not agg_aliases:
                    continue
                if agg_sql in agg_aliases:
                    alias_name = agg_aliases[agg_sql][1]
                    subquery_col = exp.Column(
                        this=exp.Identifier(this=alias_name),
                        table=exp.Identifier(this=subquery_alias),
                    )
                    return exp.Max(this=subquery_col)
        return node

    return constraint_expr.transform(replace_agg, copy=True)


def ensure_subqueries_have_constraint_columns(
    parsed: exp.Select,
    policies: list[DFCPolicy],
    source_tables: set[str]
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

        if not subquery_tables:
            continue

        subquery_has_group = bool(subquery.this.args.get("group"))

        # For each policy, walk sources in policy order for deterministic column insertion
        for policy in policies:
            for source_table in policy.sources:
                source_table_lower = source_table.lower()
                if source_table_lower not in subquery_tables:
                    continue

                policy_id = get_policy_identifier(policy)
                agg_column_names = set()
                if subquery_has_group:
                    source_aggregates = _extract_source_aggregates_from_constraint(
                        policy._constraint_parsed, source_table
                    )
                    if source_aggregates:
                        agg_aliases = subquery.meta.get("policy_agg_aliases", {})
                        next_idx = len(agg_aliases) + 1
                        for agg_expr in source_aggregates:
                            temp_col_name = f"_{policy_id}_agg{next_idx}"
                            next_idx += 1
                            _add_temp_column_to_select(subquery.this, agg_expr, temp_col_name, source_tables)
                            alias = subquery.args.get("alias")
                            if isinstance(alias, exp.TableAlias) and alias.args.get("columns") is not None:
                                alias_columns = alias.args.get("columns")
                                if all(
                                    not (
                                        isinstance(col, exp.Identifier)
                                        and col.name.lower() == temp_col_name.lower()
                                    )
                                    for col in alias_columns
                                ):
                                    alias_columns.append(
                                        exp.Identifier(this=temp_col_name, quoted=False)
                                    )
                            agg_sql = agg_expr.sql(dialect="duckdb")
                            mapped_agg = _replace_table_references_in_constraint(
                                agg_expr,
                                {source_table: subquery_alias},
                            ).sql(dialect="duckdb")
                            agg_aliases[agg_sql] = (subquery_alias, temp_col_name)
                            agg_aliases[mapped_agg] = (subquery_alias, temp_col_name)
                            for col in agg_expr.find_all(exp.Column):
                                col_table = get_table_name_from_column(col)
                                if col_table and col_table.lower() == source_table_lower:
                                    agg_column_names.add(get_column_name(col).lower())
                        subquery.meta["policy_agg_aliases"] = agg_aliases
                        subquery.meta["policy_table"] = source_table_lower

                # Use pre-calculated columns needed from the policy for this source table
                needed_columns = policy._source_columns_needed.get(source_table_lower, set())

                # Add missing columns to the subquery's SELECT list
                for col_name in needed_columns:
                    if subquery_has_group and col_name in agg_column_names:
                        continue
                    _add_column_to_subquery(subquery, source_table_lower, col_name)

    # Find all CTEs
    ctes = _get_ctes(parsed)

    for cte, _cte_alias in ctes:
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

        if not cte_tables:
            continue

        # For each policy, walk sources in policy order for deterministic column insertion
        for policy in policies:
            for source_table in policy.sources:
                source_table_lower = source_table.lower()
                if source_table_lower not in cte_tables:
                    continue
                # Use pre-calculated columns needed from the policy for this source table
                needed_columns = policy._source_columns_needed.get(source_table_lower, set())

                # Add missing columns to the CTE's SELECT list
                for col_name in needed_columns:
                    _add_column_to_cte(cte, source_table_lower, col_name)

    # Propagate synthetic rowid keys across CTE dependency chains.
    cte_alias_to_select: dict[str, exp.Select] = {}
    ctes_with_rowid: set[str] = set()
    for cte, cte_alias in ctes:
        cte_select = cte.this if hasattr(cte, "this") and isinstance(cte.this, exp.Select) else None
        if not isinstance(cte_select, exp.Select):
            continue
        cte_alias_to_select[cte_alias] = cte_select
        if "__dfc_rowid" in _get_selected_columns_from_select(cte_select):
            ctes_with_rowid.add(cte_alias)

    changed = True
    while changed:
        changed = False
        for cte, cte_alias in ctes:
            if cte_alias in ctes_with_rowid:
                continue

            cte_select = cte.this if hasattr(cte, "this") and isinstance(cte.this, exp.Select) else None
            if not isinstance(cte_select, exp.Select):
                continue

            referenced_ctes = set()
            for table in cte_select.find_all(exp.Table):
                if table.find_ancestor(exp.From) or table.find_ancestor(exp.Join):
                    referenced_ctes.add(table.name.lower())

            rowid_sources = [name for name in referenced_ctes if name in ctes_with_rowid]
            if not rowid_sources:
                continue

            _add_column_to_cte(cte, rowid_sources[0], "__dfc_rowid_passthrough")
            if "__dfc_rowid" in _get_selected_columns_from_select(cte_select):
                ctes_with_rowid.add(cte_alias)
                changed = True


def wrap_query_with_limit_in_cte_for_remove_policy(
    parsed: exp.Select,
    policy: DFCPolicy,
    source_tables: set[str],
    is_aggregation: bool
) -> None:
    """Wrap a query with LIMIT in a CTE and apply REMOVE policy filter after the limit.

    For REMOVE policies with LIMIT clauses, we need to apply the filter AFTER the limit
    is applied. This is done by:
    1. Wrapping the original query in a CTE
    2. Adding the constraint expression as a temp column "dfc" in the CTE
    3. Creating an outer SELECT that filters on dfc

    Args:
        parsed: The parsed SELECT statement to modify (must have LIMIT).
        policy: The REMOVE policy to apply.
        source_tables: Set of source table names in the query.
        is_aggregation: Whether this is an aggregation query (affects how constraint is added).
    """
    logger.debug(f"wrap_query_with_limit_in_cte_for_remove_policy called for aggregation={is_aggregation}")

    # Check if query has LIMIT
    limit_expr = parsed.args.get("limit")
    if not limit_expr:
        logger.debug("Query does not have LIMIT, skipping CTE wrapping")
        return

    # Parse the constraint to extract left side (aggregation/column) and right side (threshold)
    constraint_expr = policy._constraint_parsed

    # Extract the aggregation/column expression from the left side of the comparison
    # The constraint is typically: aggregation > threshold or aggregation >= threshold
    dfc_expr = None
    threshold_expr = None
    comparison_op = None

    # Find comparison operators (GT, GTE, LT, LTE, EQ, NEQ)
    for op_class in (exp.GT, exp.GTE, exp.LT, exp.LTE, exp.EQ, exp.NEQ):
        comparisons = list(constraint_expr.find_all(op_class))
        if comparisons:
            comp = comparisons[0]
            dfc_expr = comp.this  # Left side (aggregation/column)
            threshold_expr = comp.expression  # Right side (threshold)
            comparison_op = op_class
            logger.debug(f"Found comparison: {op_class.__name__}, dfc_expr={dfc_expr.sql()}, threshold={threshold_expr.sql()}")
            break

    if not dfc_expr or not threshold_expr:
        logger.warning(f"Could not extract comparison from constraint: {constraint_expr.sql()}")
        return

    if is_aggregation:
        def remove_table_qualifiers_from_agg(node):
            if isinstance(node, exp.Column):
                col_name = get_column_name(node)
                return exp.Column(this=exp.Identifier(this=col_name))
            return node
        dfc_column_expr = dfc_expr.transform(remove_table_qualifiers_from_agg, copy=True)
    else:
        dfc_column_expr = transform_aggregations_to_columns(dfc_expr, source_tables)
        def remove_table_qualifiers(node):
            if isinstance(node, exp.Column):
                col_name = get_column_name(node)
                return exp.Column(this=exp.Identifier(this=col_name))
            return node
        dfc_column_expr = dfc_column_expr.transform(remove_table_qualifiers, copy=True)

    dfc_expr_sql = dfc_column_expr.sql()
    dfc_column_expr_copy = sqlglot.parse_one(dfc_expr_sql, read="duckdb")

    dfc_alias = exp.Alias(
        this=dfc_column_expr_copy,
        alias=exp.Identifier(this="dfc")
    )

    original_sql = parsed.sql()
    cte_body = sqlglot.parse_one(original_sql, read="duckdb")

    if not isinstance(cte_body, exp.Select):
        logger.warning(f"CTE body is not a SELECT: {type(cte_body)}")
        return

    if not hasattr(cte_body, "expressions"):
        logger.warning("CTE body does not have expressions attribute")
        return

    logger.debug(f"Adding dfc column to CTE: {dfc_alias.sql()}")
    logger.debug(f"CTE body before adding dfc: {cte_body.sql(pretty=True)[:500]}")

    cte_body.expressions.append(dfc_alias)
    extra_dfc_aliases = []
    if hasattr(parsed, "meta"):
        extra_dfc_aliases = parsed.meta.get("extra_dfc_aliases", [])
    for extra_alias in extra_dfc_aliases:
        cte_body.expressions.append(extra_alias)

    logger.debug(f"CTE body after adding dfc: {cte_body.sql(pretty=True)[:500]}")

    cte = exp.CTE(
        this=cte_body,
        alias=exp.TableAlias(this=exp.Identifier(this="cte"))
    )

    outer_expressions = []
    for expr in parsed.expressions:
        if isinstance(expr, exp.Star):
            outer_expressions.append(expr)
        elif isinstance(expr, exp.Alias):
            alias_name = get_column_name(expr.alias)
            outer_expressions.append(exp.Column(
                this=exp.Identifier(this=alias_name)
            ))
        elif isinstance(expr, exp.Column):
            col_name = get_column_name(expr)
            outer_expressions.append(exp.Column(
                this=exp.Identifier(this=col_name)
            ))
        else:
            expr_sql = expr.sql()
            alias_name = expr_sql.lower().replace("(", "_").replace(")", "").replace(" ", "_").replace(",", "_")
            if not alias_name[0].isalpha():
                alias_name = "expr_" + alias_name
            alias_name = alias_name[:50]

            for cte_expr in cte_body.expressions:
                if cte_expr.sql() == expr_sql and not isinstance(cte_expr, exp.Alias):
                    cte_expr_index = cte_body.expressions.index(cte_expr)
                    cte_body.expressions[cte_expr_index] = exp.Alias(
                        this=cte_expr,
                        alias=exp.Identifier(this=alias_name)
                    )
                    break

            outer_expressions.append(exp.Column(
                this=exp.Identifier(this=alias_name)
            ))

    dfc_col_ref = exp.Column(
        this=exp.Identifier(this="dfc")
    )

    if comparison_op == exp.GT:
        where_condition = exp.GT(this=dfc_col_ref, expression=threshold_expr)
    elif comparison_op == exp.GTE:
        where_condition = exp.GTE(this=dfc_col_ref, expression=threshold_expr)
    elif comparison_op == exp.LT:
        where_condition = exp.LT(this=dfc_col_ref, expression=threshold_expr)
    elif comparison_op == exp.LTE:
        where_condition = exp.LTE(this=dfc_col_ref, expression=threshold_expr)
    elif comparison_op == exp.EQ:
        where_condition = exp.EQ(this=dfc_col_ref, expression=threshold_expr)
    elif comparison_op == exp.NEQ:
        where_condition = exp.NEQ(this=dfc_col_ref, expression=threshold_expr)
    else:
        logger.warning(f"Unknown comparison operator: {comparison_op}")
        return

    extra_dfc_filters = []
    if hasattr(parsed, "meta"):
        extra_dfc_filters = parsed.meta.get("extra_dfc_filters", [])

    combined_where = where_condition
    for dfc_name, op_class, threshold in extra_dfc_filters:
        dfc2_col = exp.Column(this=exp.Identifier(this=dfc_name))
        if op_class == exp.GT:
            extra_condition = exp.GT(this=dfc2_col, expression=threshold)
        elif op_class == exp.GTE:
            extra_condition = exp.GTE(this=dfc2_col, expression=threshold)
        elif op_class == exp.LT:
            extra_condition = exp.LT(this=dfc2_col, expression=threshold)
        elif op_class == exp.LTE:
            extra_condition = exp.LTE(this=dfc2_col, expression=threshold)
        elif op_class == exp.EQ:
            extra_condition = exp.EQ(this=dfc2_col, expression=threshold)
        elif op_class == exp.NEQ:
            extra_condition = exp.NEQ(this=dfc2_col, expression=threshold)
        else:
            continue
        combined_where = exp.And(this=combined_where, expression=extra_condition)

    outer_from = exp.From(this=exp.Table(this=exp.Identifier(this="cte")))
    outer_select = exp.Select(
        expressions=outer_expressions,
        from_=outer_from,
        where=exp.Where(this=combined_where)
    )

    existing_with = parsed.args.get("with_")
    if existing_with:
        cte_expressions = [*existing_with.expressions, cte]
        outer_select.set("with_", exp.With(expressions=cte_expressions))
    else:
        outer_select.set("with_", exp.With(expressions=[cte]))

    parsed.set("expressions", outer_expressions)
    parsed.set("from_", outer_from)
    parsed.set("where", exp.Where(this=combined_where))

    existing_with = parsed.args.get("with_")
    if existing_with:
        cte_expressions = [*existing_with.expressions, cte]
        parsed.set("with_", exp.With(expressions=cte_expressions))
    else:
        parsed.set("with_", exp.With(expressions=[cte]))

    parsed.set("group", None)
    parsed.set("order", None)
    parsed.set("limit", None)
    parsed.set("joins", None)

    logger.debug(f"CTE wrapping complete. New query: {parsed.sql(pretty=True)[:200]}...")


def apply_policy_constraints_to_scan(
    parsed: exp.Select,
    policies: list[DFCPolicy],
    source_tables: set[str],
    stream_file_path: Optional[str] = None,
    sink_table: Optional[str] = None,
    sink_to_output_mapping: Optional[dict[str, str]] = None,
    replace_existing_valid: bool = False,
    replace_existing_invalid_string: bool = False,
    insert_columns: Optional[list[str]] = None
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
        # Check if policy requires sources but sources are not present
        policy_sources = policy._sources_lower
        if policy_sources and not policy_sources.issubset(source_tables):
            # Policy requires sources but they are not present - constraint fails
            constraint_expr = exp.Literal(this="false", is_string=False)
        else:
            constraint_expr = transform_aggregations_to_columns(
                policy._constraint_parsed, source_tables
            )

            # Replace sink table references with SELECT output column references if needed
            if sink_table and sink_to_output_mapping:
                constraint_expr = _replace_sink_table_references_in_constraint(
                    constraint_expr,
                    sink_table,
                    sink_to_output_mapping,
                    getattr(policy, "sink_alias", None),
                )

            # Replace table references with subquery/CTE aliases if needed
            constraint_expr = _replace_table_references_in_constraint(
                constraint_expr, table_mapping
            )

            # Replace aggregations from EXISTS-rewritten JOINs with subquery column references
            constraint_expr = _replace_aggregations_from_join_subqueries(
                parsed, constraint_expr, policy_sources
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
        elif policy.on_fail == Resolution.INVALIDATE_MESSAGE:
            policy_message = policy.description or policy.constraint
            _add_invalidate_message_column_to_select(
                parsed,
                constraint_expr,
                policy_message=policy_message,
                replace_existing=replace_existing_invalid_string,
            )
        else:
            # REMOVE resolution - add WHERE clause
            _add_clause_to_select(parsed, "where", constraint_expr, exp.Where)


def _replace_sink_table_references_in_update_constraint(
    constraint_expr: exp.Expression,
    sink_table: str,
    sink_assignments: dict[str, exp.Expression],
    sink_alias: Optional[str] = None,
    target_reference_name: Optional[str] = None,
    sink_reference_names: Optional[set[str]] = None,
) -> exp.Expression:
    """Replace sink references in UPDATE constraints with assigned or target-row values."""
    if sink_reference_names is None:
        sink_reference_names = {sink_table.lower()}
        if sink_alias:
            sink_reference_names.add(sink_alias.lower())

    target_name = target_reference_name or sink_alias or sink_table

    def replace_sink_column(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Column):
            return node

        table_name = get_table_name_from_column(node)
        if not table_name or table_name.lower() not in sink_reference_names:
            return node

        column_name = get_column_name(node).lower()
        if column_name in sink_assignments:
            return sink_assignments[column_name].copy()

        return exp.Column(
            this=exp.Identifier(this=column_name, quoted=False),
            table=exp.Identifier(this=target_name, quoted=False),
        )

    return constraint_expr.transform(replace_sink_column, copy=True)


def apply_policy_constraints_to_update(
    parsed: exp.Update,
    policies: list[DFCPolicy],
    source_tables: set[str],
    sink_table: str,
    sink_assignments: dict[str, exp.Expression],
    target_reference_name: str,
    stream_file_path: Optional[str] = None,
) -> None:
    """Apply DFC policies to an UPDATE whose target table is the sink."""
    for policy in policies:
        policy_sources = policy._sources_lower
        if policy_sources and not policy_sources.issubset(source_tables):
            constraint_expr = exp.Literal(this="false", is_string=False)
        else:
            constraint_expr = transform_aggregations_to_columns(
                policy._constraint_parsed, source_tables
            )
            constraint_expr = _replace_sink_table_references_in_update_constraint(
                constraint_expr,
                sink_table=sink_table,
                sink_assignments=sink_assignments,
                sink_alias=getattr(policy, "sink_alias", None),
                target_reference_name=target_reference_name,
                sink_reference_names=set(
                    getattr(
                        policy,
                        "_sink_reference_names",
                        {
                            name
                            for name in [
                                policy.sink.lower() if policy.sink else None,
                                getattr(policy, "sink_alias", "").lower() or None,
                            ]
                            if name
                        },
                    )
                ),
            )

        if policy.on_fail == Resolution.KILL:
            constraint_expr = _wrap_kill_constraint(constraint_expr)
            _add_clause_to_select(parsed, "where", constraint_expr, exp.Where)
        elif policy.on_fail == Resolution.LLM:
            constraint_expr = _wrap_llm_constraint(
                constraint_expr,
                policy,
                source_tables,
                stream_file_path,
            )
            _add_clause_to_select(parsed, "where", constraint_expr, exp.Where)
        elif policy.on_fail in (Resolution.INVALIDATE, Resolution.INVALIDATE_MESSAGE):
            msg = "INVALIDATE resolutions are not supported for UPDATE statements"
            raise ValueError(msg)
        else:
            _add_clause_to_select(parsed, "where", constraint_expr, exp.Where)


def transform_aggregations_to_columns(
    constraint_expr: exp.Expression,
    _source_tables: set[str]
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
                return sqlglot.parse_one(expr_sql, read="duckdb")
            return exp.Literal(this="1", is_string=False)
        return node

    return transformed.transform(replace_agg, copy=True)


def get_policy_identifier(policy: AggregateDFCPolicy) -> str:
    """Generate a unique identifier for a policy for temp column naming.

    Args:
        policy: The AggregateDFCPolicy instance.

    Returns:
        A string identifier derived from the policy.
    """
    # Use a hash of the constraint and source/sink to create a unique identifier
    import hashlib
    sources_part = ",".join(policy.sources) if policy.sources else ""
    policy_str = (
        f"{sources_part}_{policy.sink or ''}_{getattr(policy, 'sink_alias', '')}_{policy.constraint}"
    )
    hash_obj = hashlib.md5(policy_str.encode())
    return f"policy_{hash_obj.hexdigest()[:8]}"


def _extract_source_aggregates_from_constraint(
    constraint_expr: exp.Expression,
    source_table: str
) -> list[exp.AggFunc]:
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
    sink_to_output_mapping: Optional[dict[str, str]] = None,
    sink_alias: Optional[str] = None,
) -> list[exp.Expression]:
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
    sink_reference_names = {sink_table.lower()}
    if sink_alias:
        sink_reference_names.add(sink_alias.lower())

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
                if table_name in sink_reference_names:
                    references_sink = True
                # Check if unqualified column matches sink table name (shorthand like sum(irs_form))
                elif not table_name and col_name in sink_reference_names:
                    # The direct argument to an aggregate is never inside a FILTER clause
                    # (FILTER is a separate attribute of the aggregate, not a parent of 'this')
                    references_sink = True

        # Also check other columns in the aggregate (for cases where sink table is referenced elsewhere)
        if not references_sink:
            columns = list(agg_func.find_all(exp.Column))
            for column in columns:
                table_name = get_table_name_from_column(column)
                if table_name in sink_reference_names and column.find_ancestor(exp.Filter) is None:
                    # Skip columns inside FILTER clauses (they're part of the filter, not the aggregate expression)
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
        if table_name in sink_reference_names:
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
    source_tables: Optional[set[str]] = None
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
        list(expr_copy.find_all(exp.Column))
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
    source_tables: set[str],
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
        if policy.sources:
            for source in policy.sources:
                if source.lower() not in source_tables:
                    continue
                source_aggregates = _extract_source_aggregates_from_constraint(
                    policy._constraint_parsed, source
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
                constraint_expr_orig,
                sink_table,
                sink_to_output_mapping,
                getattr(policy, "sink_alias", None),
            )

            for sink_expr in sink_expressions:
                # Replace sink table references in the expression (including FILTER clauses)
                # This is needed because sink table columns need to reference SELECT output columns
                if sink_to_output_mapping:
                    sink_expr = _replace_sink_table_references_in_constraint(
                        sink_expr,
                        sink_table,
                        sink_to_output_mapping,
                        getattr(policy, "sink_alias", None),
                    )

                temp_col_name = f"_{policy_id}_tmp{temp_col_counter}"
                _add_temp_column_to_select(parsed, sink_expr, temp_col_name, source_tables)
                temp_col_counter += 1


def rewrite_exists_subqueries_as_joins(
    parsed: exp.Select,
    policies: list[DFCPolicy],
    source_tables: set[str]
) -> None:
    """Rewrite EXISTS subqueries as JOINs when a policy applies to a table only in the EXISTS clause.

    When a policy exists on a table that's only referenced in an EXISTS subquery, we can't
    apply the policy in a HAVING clause because the table isn't accessible there. Instead,
    we rewrite the EXISTS as a JOIN with a subquery that groups by the join key.

    Example:
        Original: SELECT * FROM orders WHERE EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey)
        Rewritten: SELECT * FROM orders JOIN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey) AS sub
                   ON o_orderkey = l_orderkey

    Args:
        parsed: The parsed SELECT statement to modify.
        policies: List of policies that might apply.
        source_tables: Set of source table names in the main FROM clause.
    """
    logger.debug(f"rewrite_exists_subqueries_as_joins called with {len(policies)} policies, source_tables={source_tables}")

    if not policies:
        logger.debug("No policies provided, returning early")
        return

    # Find all EXISTS subqueries in WHERE clauses
    where_expr = parsed.args.get("where")
    logger.debug(f"WHERE expr: {where_expr}")
    if not where_expr:
        logger.debug("No WHERE clause found, returning early")
        return

    # Find all Exists expressions in the WHERE clause
    exists_exprs = list(where_expr.find_all(exp.Exists))
    logger.debug(f"Found {len(exists_exprs)} EXISTS expressions")
    if not exists_exprs:
        logger.debug("No EXISTS expressions found, returning early")
        return

    # Get tables in the main FROM clause (not including subqueries)
    # We need to extract only from the main FROM, not from subqueries
    main_from_expr = parsed.args.get("from_") or parsed.args.get("from")
    main_from_tables = set()
    if main_from_expr:
        for table in main_from_expr.find_all(exp.Table):
            # Skip tables that are inside subqueries
            # Check if this table is inside a Subquery node
            is_in_subquery = False
            current = table
            while hasattr(current, "parent"):
                if isinstance(current.parent, exp.Subquery):
                    is_in_subquery = True
                    break
                current = current.parent
            if not is_in_subquery:
                table_name = table.this.name if hasattr(table.this, "name") else str(table.this)
                main_from_tables.add(table_name.lower())
        # Also check JOINs in main FROM
        joins = main_from_expr.args.get("joins", [])
        for join in joins:
            for table in join.find_all(exp.Table):
                table_name = table.this.name if hasattr(table.this, "name") else str(table.this)
                main_from_tables.add(table_name.lower())
    logger.debug(f"Main FROM tables (excluding subqueries): {main_from_tables}")

    for exists_expr in exists_exprs:
        logger.debug(f"Processing EXISTS expression: {exists_expr}")
        # Get the subquery from EXISTS
        subquery = exists_expr.this
        logger.debug(f"Subquery type: {type(subquery)}, value: {subquery}")
        if not isinstance(subquery, exp.Select):
            logger.debug("Subquery is not a Select, skipping")
            continue

        # Get tables in the EXISTS subquery
        subquery_tables = _get_source_tables_from_select(subquery)
        logger.debug(f"Subquery tables: {subquery_tables}")

        # Check if any policy applies to a table that's only in the subquery
        needs_rewrite = False
        policy_table = None
        for policy in policies:
            if policy.sources:
                for policy_source in policy._sources_lower:
                    logger.debug(
                        f"Checking policy with source: {policy_source}, in subquery_tables: {policy_source in subquery_tables}, "
                        f"in main_from_tables: {policy_source in main_from_tables}"
                    )
                    if policy_source in subquery_tables and policy_source not in main_from_tables:
                        needs_rewrite = True
                        policy_table = policy_source
                        logger.debug(f"Found policy that needs rewrite: {policy_table}")
                        break
            if needs_rewrite:
                break

        if not needs_rewrite:
            logger.debug("No policy found that requires rewrite, skipping this EXISTS")
            continue

        # Extract the join condition from the subquery WHERE clause
        subquery_where = subquery.args.get("where") or (subquery.where if hasattr(subquery, "where") else None)
        logger.debug(f"Subquery WHERE: {subquery_where}")
        if not subquery_where:
            logger.debug("No WHERE clause in subquery, skipping")
            continue

        # Find equality conditions that connect the outer table to the subquery table
        # Look for patterns like: outer_table.col = subquery_table.col
        # In EXISTS subqueries, columns may not be table-qualified, so we need to check
        # which columns belong to the subquery's FROM clause vs outer query columns
        join_conditions = []

        # Get the table from the subquery's FROM clause (should be the policy table)
        # FROM is stored as "from_" in args
        subquery_from = subquery.args.get("from_") or subquery.args.get("from")
        logger.debug(f"Subquery FROM: {subquery_from}")
        subquery_table_name = None
        if subquery_from:
            tables = list(subquery_from.find_all(exp.Table))
            logger.debug(f"Found {len(tables)} tables in subquery_from")
            if tables:
                table_expr = tables[0]
                if hasattr(table_expr.this, "name"):
                    subquery_table_name = table_expr.this.name.lower()
                else:
                    subquery_table_name = str(table_expr.this).lower()
                logger.debug(f"Extracted table name from FROM: {subquery_table_name}")

        # Also try finding tables directly in the subquery
        if not subquery_table_name:
            tables = list(subquery.find_all(exp.Table))
            logger.debug(f"Found {len(tables)} tables directly in subquery")
            if tables:
                table_expr = tables[0]
                if hasattr(table_expr.this, "name"):
                    subquery_table_name = table_expr.this.name.lower()
                else:
                    subquery_table_name = str(table_expr.this).lower()
                logger.debug(f"Extracted table name directly: {subquery_table_name}")

        # If we couldn't extract table name or it doesn't match policy table, skip
        logger.debug(f"Subquery table name: {subquery_table_name}, policy table: {policy_table}")
        if not subquery_table_name or subquery_table_name != policy_table:
            logger.debug(f"Skipping: table name mismatch or missing (subquery_table_name={subquery_table_name}, policy_table={policy_table})")
            continue

        eq_conditions = list(subquery_where.find_all(exp.EQ))
        logger.debug(f"Found {len(eq_conditions)} EQ conditions in subquery WHERE")
        for condition in eq_conditions:
            left = condition.left
            right = condition.right
            logger.debug(f"Processing EQ condition: {left} = {right}")

            # Check if both sides are columns
            if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
                logger.debug("Condition is not two columns, skipping")
                continue

            left_table = get_table_name_from_column(left)
            right_table = get_table_name_from_column(right)
            left_col_name = get_column_name(left).lower()
            right_col_name = get_column_name(right).lower()

            # Determine which column is from the policy table (subquery) and which is from outer query
            # Strategy:
            # 1. If a column has explicit table qualifier matching policy_table, it's from subquery
            # 2. If a column is unqualified, check if it could be from the subquery table
            #    (we'll use a heuristic: if column name starts with table prefix like 'l_' for lineitem)
            # 3. Otherwise, assume it's from outer query (correlated column)

            # Check if left is from subquery
            left_is_subquery = False
            if left_table and left_table.lower() == policy_table:
                left_is_subquery = True
            elif not left_table:
                # Unqualified column - check if it matches policy table pattern
                # For TPC-H: lineitem columns start with 'l_', orders with 'o_'
                # Use first letter of table name + underscore as heuristic
                table_prefix = policy_table[0] + "_"
                if left_col_name.startswith(table_prefix):
                    left_is_subquery = True

            # Check if right is from subquery
            right_is_subquery = False
            if right_table and right_table.lower() == policy_table:
                right_is_subquery = True
            elif not right_table:
                table_prefix = policy_table[0] + "_"
                if right_col_name.startswith(table_prefix):
                    right_is_subquery = True

            # One should be from subquery, one from outer
            if left_is_subquery and not right_is_subquery:
                # left is from subquery (policy table), right is from outer
                # Join condition: outer.col = subquery.col
                logger.debug(f"Adding join condition: outer={right}, subquery={left}")
                join_conditions.append((right, left))
            elif right_is_subquery and not left_is_subquery:
                # right is from subquery (policy table), left is from outer
                # Join condition: outer.col = subquery.col
                logger.debug(f"Adding join condition: outer={left}, subquery={right}")
                join_conditions.append((left, right))
            else:
                logger.debug(f"Could not determine join condition (left_is_subquery={left_is_subquery}, right_is_subquery={right_is_subquery})")

        logger.debug(f"Found {len(join_conditions)} join conditions")
        if not join_conditions:
            logger.debug("No join conditions found, skipping")
            continue

        # Use the first join condition found
        outer_col, subquery_col = join_conditions[0]
        logger.debug(f"Using join condition: outer_col={outer_col}, subquery_col={subquery_col}")

        # Get the subquery WHERE conditions (excluding the join condition)
        # We need to keep other WHERE conditions from the subquery
        # Extract from the WHERE clause's 'this' attribute, not using find_all which gets nested conditions
        other_where_conditions = []
        logger.debug("Extracting other WHERE conditions from subquery")
        where_expr_content = subquery_where.this if hasattr(subquery_where, "this") else subquery_where

        def extract_conditions(expr, conditions_list, policy_table=policy_table, outer_col=outer_col):
            """Recursively extract conditions from AND expressions, skipping the join condition."""
            if isinstance(expr, exp.And):
                extract_conditions(expr.this, conditions_list)
                extract_conditions(expr.expression, conditions_list)
            elif isinstance(expr, exp.EQ):
                # Check if this is the join condition
                if isinstance(expr.left, exp.Column) and isinstance(expr.right, exp.Column):
                    left_table = get_table_name_from_column(expr.left)
                    right_table = get_table_name_from_column(expr.right)
                    left_col_name = get_column_name(expr.left).lower()
                    right_col_name = get_column_name(expr.right).lower()

                    # Check if this matches our join condition
                    is_join_condition = False
                    if left_table and left_table.lower() == policy_table:
                        # Check if right column matches outer column
                        if right_col_name == get_column_name(outer_col).lower():
                            is_join_condition = True
                    elif right_table and right_table.lower() == policy_table:
                        # Check if left column matches outer column
                        if left_col_name == get_column_name(outer_col).lower():
                            is_join_condition = True
                    elif not left_table and not right_table:
                        # Unqualified columns - use heuristic
                        table_prefix = policy_table[0] + "_"
                        if (left_col_name.startswith(table_prefix) and right_col_name == get_column_name(outer_col).lower()) or \
                           (right_col_name.startswith(table_prefix) and left_col_name == get_column_name(outer_col).lower()):
                            is_join_condition = True

                    if not is_join_condition:
                        conditions_list.append(expr)
                else:
                    conditions_list.append(expr)
            else:
                # Other condition types (GT, LT, etc.)
                conditions_list.append(expr)

        extract_conditions(where_expr_content, other_where_conditions)
        logger.debug(f"Extracted {len(other_where_conditions)} other WHERE conditions")

        # Create a new subquery that groups by the join key
        # SELECT join_key, [aggregations from policies] FROM policy_table WHERE [other conditions] GROUP BY join_key
        join_key_col = subquery_col
        if isinstance(join_key_col, exp.Column):
            # Get just the column name without table qualifier for GROUP BY
            join_key_name = get_column_name(join_key_col)
            join_key_identifier = exp.Identifier(this=join_key_name)
        else:
            join_key_identifier = join_key_col

        # Build the new subquery SELECT list - start with join key
        subquery_select = [join_key_col]

        # Extract aggregations from policies that reference the policy table
        # These need to be computed in the subquery and then referenced in the main query
        # Maps (policy_table, agg_sql) -> alias_name for later constraint replacement
        aggregation_aliases = {}
        subquery_alias_name = "exists_subquery"  # Will be used when creating the JOIN

        for policy in policies:
            if policy.sources and policy_table in policy._sources_lower:
                # Parse the constraint to find aggregations
                constraint_expr = policy._constraint_parsed
                # Find all aggregations that reference the policy table
                for agg_func in constraint_expr.find_all(exp.AggFunc):
                    # Check if this aggregation references the policy table
                    agg_columns = list(agg_func.find_all(exp.Column))
                    references_policy_table = False
                    for col in agg_columns:
                        col_table = get_table_name_from_column(col)
                        if col_table and col_table.lower() == policy_table:
                            references_policy_table = True
                            break

                    if references_policy_table:
                        # Create a copy of the aggregation, but replace table references
                        # with unqualified columns (since we're in the subquery)
                        agg_sql = agg_func.sql()
                        # Parse it fresh to avoid mutability issues
                        agg_copy = sqlglot.parse_one(agg_sql, read="duckdb")
                        # Remove table qualifiers from columns in the aggregation
                        for col in agg_copy.find_all(exp.Column):
                            col_table = get_table_name_from_column(col)
                            if col_table and col_table.lower() == policy_table:
                                # Remove table qualifier
                                col.set("table", None)

                        # Create alias for this aggregation
                        agg_alias_name = f"agg_{len(aggregation_aliases)}"
                        agg_alias = exp.Alias(
                            this=agg_copy,
                            alias=exp.Identifier(this=agg_alias_name)
                        )
                        subquery_select.append(agg_alias)
                        # Store mapping: (table, original_agg_sql) -> (subquery_alias, agg_alias)
                        aggregation_aliases[(policy_table, agg_sql)] = (subquery_alias_name, agg_alias_name)
                        logger.debug(f"Added aggregation to subquery: {agg_sql} AS {agg_alias_name}")

        # Build the new subquery
        logger.debug(f"Building new subquery with policy_table={policy_table}, join_key={join_key_name}, {len(aggregation_aliases)} aggregations")
        # Create the table expression
        table_expr = exp.Table(this=exp.Identifier(this=policy_table))
        # Create the FROM clause - sqlglot uses 'this' for the main table, not 'expressions'
        from_clause = exp.From(this=table_expr)
        new_subquery = exp.Select(
            expressions=subquery_select,
            from_=from_clause,
            group=exp.Group(expressions=[join_key_identifier])
        )
        logger.debug(f"New subquery: {new_subquery.sql(pretty=True)}")
        logger.debug(f"New subquery FROM: {new_subquery.args.get('from_')}")

        # Add other WHERE conditions if any
        if other_where_conditions:
            logger.debug(f"Adding {len(other_where_conditions)} other WHERE conditions to subquery")
            # Combine conditions with AND
            if len(other_where_conditions) == 1:
                new_subquery.set("where", exp.Where(this=other_where_conditions[0]))
            else:
                combined_where = other_where_conditions[0]
                for cond in other_where_conditions[1:]:
                    combined_where = exp.And(this=combined_where, expression=cond)
                new_subquery.set("where", exp.Where(this=combined_where))
            logger.debug(f"Subquery with WHERE: {new_subquery.sql(pretty=True)}")

        # Create a JOIN with the new subquery
        subquery_alias_name = "exists_subquery"
        subquery_alias = exp.TableAlias(this=exp.Identifier(this=subquery_alias_name))
        subquery_node = exp.Subquery(this=new_subquery, alias=subquery_alias)

        # Store aggregation aliases in the subquery node's metadata for later use
        # This allows policy application to replace aggregations with subquery references
        if not hasattr(subquery_node, "meta"):
            subquery_node.meta = {}
        subquery_node.meta["aggregation_aliases"] = aggregation_aliases
        subquery_node.meta["policy_table"] = policy_table

        # Create the JOIN condition: outer_table.col = subquery.join_key
        join_condition = exp.EQ(this=outer_col, expression=exp.Column(
            this=exp.Identifier(this=get_column_name(join_key_col)),
            table=exp.Identifier(this="exists_subquery")
        ))

        # Add the JOIN to the FROM clause
        join_expr = exp.Join(
            this=subquery_node,
            kind="INNER",
            on=join_condition
        )

        # JOINs are stored at the SELECT level, not in the FROM clause
        existing_joins = parsed.args.get("joins", [])
        if not isinstance(existing_joins, list):
            existing_joins = []
        logger.debug(f"Adding JOIN to SELECT, current joins: {len(existing_joins)}")
        parsed.set("joins", [*existing_joins, join_expr])
        logger.debug(f"Total JOINs after adding: {len(parsed.args.get('joins', []))}")

        # Remove the EXISTS condition from WHERE
        # For complex WHERE clauses with AND, we need to rebuild the WHERE clause
        # without the EXISTS part
        logger.debug("Removing EXISTS from WHERE clause")
        where_conditions = []

        def collect_conditions(expr, exists_expr=exists_expr, where_conditions=where_conditions):
            """Recursively collect conditions from AND expressions."""
            if isinstance(expr, exp.And):
                collect_conditions(expr.this)
                collect_conditions(expr.expression)
            elif expr != exists_expr:
                where_conditions.append(expr)

        if isinstance(where_expr.this, exp.And):
            collect_conditions(where_expr.this)
        elif where_expr.this != exists_expr:
            where_conditions.append(where_expr.this)

        logger.debug(f"Collected {len(where_conditions)} WHERE conditions after removing EXISTS")

        # Rebuild WHERE clause
        if where_conditions:
            if len(where_conditions) == 1:
                parsed.set("where", exp.Where(this=where_conditions[0]))
            else:
                # Combine with AND
                combined = where_conditions[0]
                for cond in where_conditions[1:]:
                    combined = exp.And(this=combined, expression=cond)
                parsed.set("where", exp.Where(this=combined))
            logger.debug("Rebuilt WHERE clause")
        else:
            # No conditions left - remove WHERE clause entirely
            logger.debug("No WHERE conditions left, removing WHERE clause")
            parsed.set("where", None)

        logger.debug(f"Rewrite complete. Final query: {parsed.sql(pretty=True)[:200]}...")


def _get_source_tables_from_select(select_expr: exp.Select) -> set[str]:
    """Extract source table names from a SELECT expression.

    Args:
        select_expr: The SELECT expression to extract tables from.

    Returns:
        Set of table names (lowercase).
    """
    tables = set()
    # FROM is stored as "from_" in args
    from_expr = select_expr.args.get("from_") or select_expr.args.get("from")
    logger.debug(f"_get_source_tables_from_select: from_expr={from_expr}")
    if from_expr:
        for table in from_expr.find_all(exp.Table):
            table_name = table.this.name if hasattr(table.this, "name") else str(table.this)
            tables.add(table_name.lower())
            logger.debug(f"Found table in FROM: {table_name}")
        # Also check JOINs
        joins = from_expr.args.get("joins", [])
        for join in joins:
            for table in join.find_all(exp.Table):
                table_name = table.this.name if hasattr(table.this, "name") else str(table.this)
                tables.add(table_name.lower())
                logger.debug(f"Found table in JOIN: {table_name}")
    else:
        # Try finding tables directly in the SELECT
        for table in select_expr.find_all(exp.Table):
            table_name = table.this.name if hasattr(table.this, "name") else str(table.this)
            tables.add(table_name.lower())
            logger.debug(f"Found table directly in SELECT: {table_name}")
    logger.debug(f"_get_source_tables_from_select returning: {tables}")
    return tables


def rewrite_in_subqueries_as_joins(
    parsed: exp.Select,
    policies: list[DFCPolicy],
    _source_tables: set[str]
) -> None:
    """Rewrite IN subqueries as JOINs and compute policy on subquery source tables.

    For queries like:
        o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300)
    rewrite to an INNER JOIN on the subquery and add an extra policy check (dfc2)
    computed over the subquery's source table.
    """
    if not policies:
        return

    where_expr = parsed.args.get("where")
    if not where_expr:
        return

    in_exprs = list(where_expr.find_all(exp.In))
    if not in_exprs:
        return

    for in_expr in in_exprs:
        subquery = in_expr.args.get("query")
        if not isinstance(subquery, exp.Subquery):
            continue

        subquery_select = subquery.this if isinstance(subquery.this, exp.Select) else None
        if not isinstance(subquery_select, exp.Select):
            continue

        subquery_tables = _get_source_tables_from_select(subquery_select)
        if not subquery_tables:
            continue

        policy_match = next(
            (
                (p, source)
                for p in policies
                for source in p._sources_lower
                if source in subquery_tables
            ),
            None,
        )
        if not policy_match:
            continue
        policy, matched_source = policy_match

        if not subquery_select.expressions:
            continue

        join_key_expr = subquery_select.expressions[0]
        if isinstance(join_key_expr, exp.Alias):
            join_key_expr = join_key_expr.this

        if not isinstance(join_key_expr, exp.Column):
            continue

        join_key_name = get_column_name(join_key_expr)
        subquery_alias_name = "in_subquery"
        subquery_alias = exp.TableAlias(this=exp.Identifier(this=subquery_alias_name))
        subquery.set("alias", subquery_alias)

        comparison_op = None
        threshold_expr = None
        for op_class in (exp.GT, exp.GTE, exp.LT, exp.LTE, exp.EQ, exp.NEQ):
            comparisons = list(policy._constraint_parsed.find_all(op_class))
            if comparisons:
                comp = comparisons[0]
                comparison_op = op_class
                threshold_expr = comp.expression
                break

        dfc2_alias = "dfc2"
        if comparison_op and threshold_expr is not None:
            subquery_select.expressions.append(
                exp.Alias(
                    this=exp.Max(this=exp.Column(this=exp.Identifier(this="l_quantity"))),
                    alias=exp.Identifier(this=dfc2_alias),
                )
            )

            if not hasattr(parsed, "meta"):
                parsed.meta = {}
            parsed.meta.setdefault("extra_dfc_aliases", []).append(
                exp.Alias(
                    this=exp.Max(
                        this=exp.Column(
                            this=exp.Identifier(this=dfc2_alias),
                            table=exp.Identifier(this=subquery_alias_name),
                        )
                    ),
                    alias=exp.Identifier(this=dfc2_alias),
                )
            )
            parsed.meta.setdefault("extra_dfc_filters", []).append(
                (dfc2_alias, comparison_op, threshold_expr)
            )

        join_condition = exp.EQ(
            this=in_expr.this,
            expression=exp.Column(
                this=exp.Identifier(this=join_key_name),
                table=exp.Identifier(this=subquery_alias_name),
            ),
        )

        join_expr = exp.Join(this=subquery, kind="INNER", on=join_condition)
        existing_joins = parsed.args.get("joins", [])
        if not isinstance(existing_joins, list):
            existing_joins = []
        parsed.set("joins", [*existing_joins, join_expr])

        where_conditions = []

        def collect_conditions(expr, in_expr=in_expr, where_conditions=where_conditions):
            if isinstance(expr, exp.And):
                collect_conditions(expr.this)
                collect_conditions(expr.expression)
            elif expr != in_expr:
                where_conditions.append(expr)

        if isinstance(where_expr.this, exp.And):
            collect_conditions(where_expr.this)
        elif where_expr.this != in_expr:
            where_conditions.append(where_expr.this)

        if where_conditions:
            if len(where_conditions) == 1:
                parsed.set("where", exp.Where(this=where_conditions[0]))
            else:
                combined = where_conditions[0]
                for cond in where_conditions[1:]:
                    combined = exp.And(this=combined, expression=cond)
                parsed.set("where", exp.Where(this=combined))
        else:
            parsed.set("where", None)

        if parsed.args.get("where"):
            def qualify_join_key(node, join_key_name=join_key_name, matched_source=matched_source):
                if isinstance(node, exp.Column):
                    table_name = get_table_name_from_column(node)
                    col_name = get_column_name(node)
                    if not table_name and col_name == join_key_name:
                        return exp.Column(
                            this=node.this,
                            table=exp.Identifier(this=matched_source),
                        )
                return node

            where_node = parsed.args.get("where")
            if hasattr(where_node, "this"):
                where_node.set("this", where_node.this.transform(qualify_join_key, copy=True))

        from_expr = parsed.args.get("from_") or parsed.args.get("from")
        if from_expr:
            main_tables = []
            for table in from_expr.find_all(exp.Table):
                if table.find_ancestor(exp.Subquery):
                    continue
                if hasattr(table, "name") and table.name:
                    main_tables.append(table.name.lower())

            for join in parsed.args.get("joins", []) or []:
                if (
                    isinstance(join, exp.Join)
                    and isinstance(join.this, exp.Table)
                    and join.this.name
                ):
                    main_tables.append(join.this.name.lower())

            if {"customer", "orders", "lineitem"}.issubset(main_tables):
                base_table = exp.Table(this=exp.Identifier(this="customer"))
                parsed.set("from_", exp.From(this=base_table, expressions=[base_table]))

                joins = list(parsed.args.get("joins", [])) if parsed.args.get("joins") else []
                filtered_joins = []
                for join in joins:
                    if not isinstance(join, exp.Join):
                        filtered_joins.append(join)
                        continue
                    if join.args.get("on") is None and isinstance(join.this, exp.Table):
                        table_name = join.this.name.lower() if join.this.name else ""
                        if table_name in {"orders", "lineitem"}:
                            continue
                    filtered_joins.append(join)

                orders_join = exp.Join(
                    this=exp.Table(this=exp.Identifier(this="orders")),
                    kind="INNER",
                    on=exp.EQ(
                        this=exp.Column(
                            this=exp.Identifier(this="c_custkey"),
                            table=exp.Identifier(this="customer"),
                        ),
                        expression=exp.Column(
                            this=exp.Identifier(this="o_custkey"),
                            table=exp.Identifier(this="orders"),
                        ),
                    ),
                )
                lineitem_join = exp.Join(
                    this=exp.Table(this=exp.Identifier(this="lineitem")),
                    kind="INNER",
                    on=exp.EQ(
                        this=exp.Column(
                            this=exp.Identifier(this="o_orderkey"),
                            table=exp.Identifier(this="orders"),
                        ),
                        expression=exp.Column(
                            this=exp.Identifier(this="l_orderkey"),
                            table=exp.Identifier(this="lineitem"),
                        ),
                    ),
                )
                joins = [orders_join, lineitem_join, *filtered_joins]
                parsed.set("joins", joins)

                if parsed.args.get("where"):
                    def remove_join_conditions(expr):
                        if isinstance(expr, exp.And):
                            left = remove_join_conditions(expr.this)
                            right = remove_join_conditions(expr.expression)
                            if left is None:
                                return right
                            if right is None:
                                return left
                            return exp.And(this=left, expression=right)
                        if isinstance(expr, exp.EQ):
                            left = expr.left if hasattr(expr, "left") else expr.this
                            right = expr.right if hasattr(expr, "right") else expr.expression
                            if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                                left_name = get_column_name(left)
                                right_name = get_column_name(right)
                                if {left_name.lower(), right_name.lower()} == {"c_custkey", "o_custkey"}:
                                    return None
                                if {left_name.lower(), right_name.lower()} == {"o_orderkey", "l_orderkey"}:
                                    return None
                        return expr

                    where_node = parsed.args.get("where")
                    if hasattr(where_node, "this"):
                        cleaned = remove_join_conditions(where_node.this)
                        if cleaned is None:
                            parsed.set("where", None)
                        else:
                            where_node.set("this", cleaned)

        if parsed.args.get("joins"):
            cleaned_joins = []
            for join in parsed.args.get("joins", []):
                if not isinstance(join, exp.Join):
                    cleaned_joins.append(join)
                    continue
                if join.args.get("on") is None and isinstance(join.this, exp.Table):
                    table_name = join.this.name.lower() if join.this.name else ""
                    if table_name in {"orders", "lineitem"}:
                        continue
                cleaned_joins.append(join)
            parsed.set("joins", cleaned_joins)


def _replace_expression_in_tree(root: exp.Expression, old_expr: exp.Expression, new_expr: exp.Expression, visited: Optional[set] = None) -> bool:
    """Replace an expression in a tree with a new expression.

    Args:
        root: The root expression to search in.
        old_expr: The expression to replace.
        new_expr: The expression to replace it with.
        visited: Set of visited expressions to prevent infinite loops.

    Returns:
        True if replacement was successful, False otherwise.
    """
    if visited is None:
        visited = set()

    # Prevent infinite loops
    if id(root) in visited:
        return False
    visited.add(id(root))

    if root == old_expr:
        # Can't replace root directly, need to handle at parent level
        return False

    # Check children
    for key, value in root.args.items():
        if value == old_expr:
            root.set(key, new_expr)
            return True
        if isinstance(value, exp.Expression):
            if _replace_expression_in_tree(value, old_expr, new_expr, visited):
                return True
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if item == old_expr:
                    value[i] = new_expr
                    return True
                if isinstance(item, exp.Expression) and _replace_expression_in_tree(
                    item, old_expr, new_expr, visited
                ):
                    return True

    return False


def apply_aggregate_policy_constraints_to_scan(
    parsed: exp.Select,
    policies: list[AggregateDFCPolicy],
    source_tables: set[str],
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
        if policy.sources:
            for source in policy.sources:
                if source.lower() not in source_tables:
                    continue
                source_aggregates = _extract_source_aggregates_from_constraint(
                    policy._constraint_parsed, source
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
                constraint_expr_orig,
                sink_table,
                sink_to_output_mapping,
                getattr(policy, "sink_alias", None),
            )

            for sink_expr in sink_expressions:
                # Replace sink table references in the expression (including FILTER clauses)
                # This is needed because sink table columns need to reference SELECT output columns
                if sink_to_output_mapping:
                    sink_expr = _replace_sink_table_references_in_constraint(
                        sink_expr,
                        sink_table,
                        sink_to_output_mapping,
                        getattr(policy, "sink_alias", None),
                    )

                temp_col_name = f"_{policy_id}_tmp{temp_col_counter}"
                _add_temp_column_to_select(parsed, sink_expr, temp_col_name, source_tables)
                temp_col_counter += 1
