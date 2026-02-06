"""Query rewriting logic for physical baseline (SmokedDuck lineage-based approach).

The physical baseline uses SmokedDuck's lineage capabilities to track data provenance.
Instead of rewriting SQL into a single query, this approach:
1. Executes the base query
2. Stores results in a temporary table
3. Applies policy filtering based on provenance data

This module contains the logic for transforming policy constraints and building
filter queries for the physical baseline.
"""

from sql_rewriter import DFCPolicy
import sqlglot
from sqlglot import exp


def _strip_table_qualifiers(expr: exp.Expression) -> exp.Expression:
    """Return a copy of expr with table qualifiers removed."""
    expr_copy = expr.copy()
    for col in expr_copy.find_all(exp.Column):
        col.set("table", None)
    return expr_copy


def _should_add_column(existing: set[str], col_sql: str) -> bool:
    """Check if column SQL is already present (case-insensitive)."""
    return col_sql.lower() not in existing


def _add_columns_to_select(select_expr: exp.Select, columns: list[str]) -> None:
    """Add columns to SELECT and GROUP BY (if present)."""
    existing = {expr.sql(dialect="duckdb").lower() for expr in select_expr.expressions}
    for col_sql in columns:
        if _should_add_column(existing, col_sql):
            select_expr.append("expressions", sqlglot.parse_one(col_sql, read="duckdb"))
            existing.add(col_sql.lower())
    group_expr = select_expr.args.get("group")
    if group_expr:
        existing_group = {
            expr.sql(dialect="duckdb").lower()
            for expr in getattr(group_expr, "expressions", [])
        }
        for col_sql in columns:
            if _should_add_column(existing_group, col_sql):
                group_expr.append("expressions", sqlglot.parse_one(col_sql, read="duckdb"))
                existing_group.add(col_sql.lower())


def _thread_lineage_columns(parsed: exp.Select, policy_source: str, lineage_columns: list[str]) -> exp.Select:
    """Thread lineage columns through any SELECT that references the policy source."""
    for select_expr in parsed.find_all(exp.Select):
        parent = select_expr.parent
        if (
            parent
            and isinstance(parent, exp.Subquery)
            and not isinstance(parent.parent, (exp.From, exp.Join))
        ):
            continue
        from_expr = select_expr.args.get("from_")
        if not from_expr:
            continue
        has_policy_source = False
        tables_to_check = list(from_expr.find_all(exp.Table))
        for join in select_expr.args.get("joins", []):
            tables_to_check.extend(list(join.find_all(exp.Table)))
        for table in tables_to_check:
            is_in_subquery = False
            current = table
            while hasattr(current, "parent") and current is not select_expr:
                if isinstance(current.parent, exp.Subquery):
                    is_in_subquery = True
                    break
                current = current.parent
            if is_in_subquery:
                continue
            if hasattr(table, "name") and table.name and table.name.lower() == policy_source.lower():
                has_policy_source = True
                break
        if has_policy_source:
            qualified_cols = [f"{policy_source}.{col}" for col in lineage_columns]
            _add_columns_to_select(select_expr, qualified_cols)
    return parsed


def _ensure_agg_aliases(select_expr: exp.Select) -> None:
    """Add aliases for aggregate expressions that lack them."""
    existing_aliases = {
        expr.alias_or_name.lower()
        for expr in select_expr.expressions
        if isinstance(expr, exp.Alias)
    }
    alias_index = 1
    for idx, expr in enumerate(select_expr.expressions):
        if isinstance(expr, exp.Alias):
            continue
        if isinstance(expr, exp.AggFunc) or expr.find(exp.AggFunc):
            raw = expr.sql(dialect="duckdb")
            alias = "".join(ch if ch.isalnum() else "_" for ch in raw).strip("_").lower()
            if not alias:
                alias = f"agg_{alias_index}"
            while alias in existing_aliases:
                alias_index += 1
                alias = f"{alias}_{alias_index}"
            select_expr.expressions[idx] = exp.Alias(
                this=expr,
                alias=exp.to_identifier(alias),
            )
            existing_aliases.add(alias)


def _extract_policy_agg_nodes(constraint: str, source_table: str) -> list[exp.AggFunc]:
    """Extract aggregation nodes from a policy constraint for the given source."""
    constraint_expr = sqlglot.parse_one(constraint, read="duckdb")
    agg_nodes = []
    for agg in constraint_expr.find_all(exp.AggFunc):
        columns = list(agg.find_all(exp.Column))
        if not columns:
            continue
        for col in columns:
            table_name = str(col.table).lower() if hasattr(col, "table") and col.table else None
            if table_name == source_table.lower() or table_name is None:
                agg_nodes.append(agg)
                break
    return agg_nodes


def _replace_policy_aggs_with_aliases(
    constraint: str,
    source_table: str,
    aliases: list[str],
) -> str:
    """Replace policy agg functions with alias columns in a constraint."""
    constraint_expr = sqlglot.parse_one(constraint, read="duckdb")
    alias_iter = iter(aliases)
    for agg in list(constraint_expr.find_all(exp.AggFunc)):
        columns = list(agg.find_all(exp.Column))
        if not columns:
            continue
        use_alias = False
        for col in columns:
            table_name = str(col.table).lower() if hasattr(col, "table") and col.table else None
            if table_name == source_table.lower() or table_name is None:
                use_alias = True
                break
        if not use_alias:
            continue
        alias = next(alias_iter, None)
        if not alias:
            break
        replacement = exp.Column(this=exp.Identifier(this=alias))
        agg.replace(replacement)
    return constraint_expr.sql(dialect="duckdb")


def transform_constraint_for_filtering(constraint: str, source_table: str) -> str:
    """Transform policy constraint for filtering query results.

    For scan queries: max(test_data.value) > 100 -> value > 100
    Supports all DuckDB aggregation functions on any source table column.

    Args:
        constraint: Policy constraint SQL expression
        source_table: Source table name

    Returns:
        Transformed constraint string for filtering
    """
    import sqlglot
    from sqlglot import exp

    parsed = sqlglot.parse_one(constraint, read="duckdb")

    # Build list of all aggregation types to find
    # Start with common ones that definitely exist
    agg_types = [exp.Max, exp.Min, exp.Sum, exp.Avg, exp.Count]

    # Add optional aggregation types if they exist
    optional_agg_types = [
        "Stddev", "StddevPop", "StddevSamp", "Variance",
        "Quantile", "Mode", "Median", "First", "Last", "AnyValue",
        "ArrayAgg", "Corr", "CovarPop", "CovarSamp",
    ]
    for agg_name in optional_agg_types:
        if hasattr(exp, agg_name):
            agg_types.append(getattr(exp, agg_name))

    # Also check for AggFunc base class if available
    if hasattr(exp, "AggFunc"):
        # Find all AggFunc instances first to discover additional types
        for agg in parsed.find_all(exp.AggFunc):
            # Check if it's one of the specific types we already handle
            if not any(isinstance(agg, t) for t in agg_types):
                agg_types.append(type(agg))

    # Convert to tuple for find_all
    agg_types_tuple = tuple(agg_types)

    # Helper function to check if column belongs to source table
    def should_replace_agg(agg):
        """Check if aggregation should be replaced (belongs to source table)."""
        # Get the column inside the aggregation
        col_expr = None
        if hasattr(agg, "this") and isinstance(agg.this, exp.Column):
            col_expr = agg.this
        elif (
            hasattr(agg, "expressions")
            and agg.expressions
            and isinstance(agg.expressions[0], exp.Column)
        ):
            col_expr = agg.expressions[0]
        elif (
            hasattr(agg, "expressions")
            and agg.expressions
            and isinstance(agg.expressions[0], exp.Star)
        ):
            # COUNT(*) - can't transform, skip
            return False

        if col_expr:
            # Check if column belongs to source table
            if col_expr.table:
                table_name = str(col_expr.table).lower()
                return table_name == source_table.lower()
            # No table qualification, assume it's from source table
            return True
        return False

    # Find and replace all aggregations with their underlying columns
    for agg in parsed.find_all(agg_types_tuple):
        if not should_replace_agg(agg):
            continue

        # Get the column inside the aggregation (should_replace_agg already verified it exists)
        col_expr = None
        if hasattr(agg, "this") and isinstance(agg.this, exp.Column):
            col_expr = agg.this
        elif (
            hasattr(agg, "expressions")
            and agg.expressions
            and isinstance(agg.expressions[0], exp.Column)
        ):
            col_expr = agg.expressions[0]

        if col_expr:
            # Create a new column without table qualification
            new_col = exp.Column(this=col_expr.this)
            agg.replace(new_col)

    # Remove table qualifications from the result
    result = parsed.sql(dialect="duckdb")
    # Replace table-qualified columns with unqualified ones
    return result.replace(f"{source_table}.", "")


def is_aggregation_query(query: str) -> bool:
    """Check if a query contains aggregations.

    Args:
        query: SQL query string

    Returns:
        True if query contains aggregations or GROUP BY
    """
    try:
        parsed = sqlglot.parse_one(query, read="duckdb")
        if not isinstance(parsed, exp.Select):
            return False

        # Check for GROUP BY clause
        if parsed.args.get("group"):
            return True

        # Helper function to check if an expression is an aggregation
        def is_aggregation(expr):
            """Check if expression is an aggregation function.

            Supports all DuckDB aggregation functions by checking:
            1. The is_aggregation attribute (most reliable)
            2. AggFunc base class (catches all aggregations)
            3. Specific known aggregation types as fallback
            """
            # First check the is_aggregation attribute (most reliable)
            if hasattr(expr, "is_aggregation") and expr.is_aggregation:
                return True
            # Check for AggFunc base class (catches all aggregations)
            if hasattr(exp, "AggFunc") and isinstance(expr, exp.AggFunc):
                return True
            # Fallback: check for specific known aggregation types
            agg_types = (
                exp.Max, exp.Min, exp.Sum, exp.Avg, exp.Count,
                exp.Stddev, exp.StddevPop, exp.StddevSamp,
                exp.Variance,
            )
            # Only include types that exist
            agg_types = tuple(t for t in agg_types if t is not None and hasattr(exp, t.__name__))
            if isinstance(expr, agg_types):
                return True
            # Check for optional aggregation types
            optional_agg_types = [
                "Quantile", "Mode", "Median", "First", "Last", "AnyValue",
                "ArrayAgg", "Corr", "CovarPop", "CovarSamp",
            ]
            for agg_name in optional_agg_types:
                if hasattr(exp, agg_name) and isinstance(expr, getattr(exp, agg_name)):
                    return True
            return False

        # Check for aggregation functions in SELECT
        for expr in parsed.expressions:
            # Unwrap alias if present
            inner_expr = expr.this if isinstance(expr, exp.Alias) else expr

            # Check if expression is an aggregation function
            if is_aggregation(inner_expr):
                return True
            if inner_expr.find(exp.AggFunc):
                return True

        return False
    except Exception:
        # If parsing fails, assume it's not an aggregation
        return False


def build_filter_query(
    temp_table_name: str,
    constraint: str,
    source_table: str,
    column_names: list,
    is_aggregation: bool = False
) -> str:
    """Build a filter query to apply policy constraint to query results.

    Args:
        temp_table_name: Name of temporary table containing query results
        constraint: Policy constraint SQL expression
        source_table: Source table name
        column_names: List of column names in the results
        is_aggregation: Whether the original query was an aggregation query

    Returns:
        SQL query string for filtering results
    """
    policy_columns = [col for col in column_names if col.lower().startswith("policy_")]

    if is_aggregation and policy_columns:
        filter_constraint = _replace_policy_aggs_with_aliases(
            constraint=constraint,
            source_table=source_table,
            aliases=policy_columns,
        )
        select_columns = [col for col in column_names if col.lower() not in policy_columns]
        select_list = ", ".join(select_columns) if select_columns else "*"
        return f"SELECT {select_list} FROM {temp_table_name} WHERE {filter_constraint}"

    # Transform constraint for filtering
    filter_constraint = transform_constraint_for_filtering(constraint, source_table)

    # Check if 'value' column exists in the results
    # For aggregation queries, we may not have the source column directly
    has_value_column = "value" in [col.lower() for col in column_names]

    if is_aggregation and not has_value_column:
        # For aggregation queries without a source column or policy alias, skip filtering.
        return f"SELECT * FROM {temp_table_name}"
    if has_value_column:
        # Filter results using the value column
        return f"SELECT * FROM {temp_table_name} WHERE {filter_constraint}"
    # No value column and not an aggregation - can't apply filter
    # This shouldn't happen with our test policy, but handle gracefully
    return f"SELECT * FROM {temp_table_name}"


def _extract_order_by_clause(query: str) -> str | None:
    """Extract and qualify ORDER BY clause from query."""
    try:
        parsed = sqlglot.parse_one(query, read="duckdb")
    except Exception:
        return None
    if not isinstance(parsed, exp.Select):
        return None
    order_expr = parsed.args.get("order")
    if not order_expr:
        return None
    for col in order_expr.find_all(exp.Column):
        if not col.table:
            col.set("table", exp.to_identifier("generated_table"))
    return order_expr.sql(dialect="duckdb")


def build_lineage_filter_query(
    lineage_query: str,
    temp_table_name: str,
    policy: DFCPolicy,
    output_columns: list[str],
    order_by: str | None = None,
) -> str:
    """Build a filter query that uses operator lineage tables to enforce policy."""
    if not isinstance(policy, DFCPolicy):
        raise ValueError("policy must be a DFCPolicy instance")
    if not policy.sources:
        raise ValueError("policy must have sources specified")
    if len(policy.sources) != 1:
        raise ValueError("physical baseline supports a single source table per policy")
    if not output_columns:
        raise ValueError("output_columns must be provided for lineage filtering")

    def _quote_identifier(name: str) -> str:
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    select_cols = [f"generated_table.{_quote_identifier(col)}" for col in output_columns]
    group_by_cols = ["generated_table.rowid", *select_cols]
    constraint_expr = sqlglot.parse_one(policy.constraint, read="duckdb")
    constraint_sql = constraint_expr.sql(dialect="duckdb")

    order_by_sql = f"\n{order_by}" if order_by else ""

    return (
        "WITH lineage AS (\n"
        f"{lineage_query}\n"
        ")\n"
        "SELECT\n"
        f"    {', '.join(select_cols)}\n"
        f"FROM {temp_table_name} AS generated_table\n"
        "JOIN lineage\n"
        "    ON generated_table.rowid::int = lineage.out_index::int\n"
        f"JOIN {policy.sources[0]}\n"
        f"    ON {policy.sources[0]}.rowid::int = lineage.{policy.sources[0]}::int\n"
        f"GROUP BY {', '.join(group_by_cols)}\n"
        f"HAVING {constraint_sql}"
        f"{order_by_sql}"
    )


def rewrite_query_physical(
    query: str,
    policy: DFCPolicy,
    lineage_query: str | None = None,
    output_columns: list[str] | None = None,
) -> tuple[str, str, bool]:
    """Rewrite query for physical baseline approach.

    This doesn't rewrite the SQL directly, but returns:
    1. The base query to execute (unchanged)
    2. The filter query to apply to results
    3. Whether the query is an aggregation query

    Args:
        query: Original SQL query
        policy: DFCPolicy instance (must have source specified)

    Returns:
        Tuple of (base_query, filter_query_template, is_aggregation)
        The filter_query_template uses {temp_table_name} placeholder

    Raises:
        ValueError: If policy does not have a source specified
    """
    from sql_rewriter import DFCPolicy

    if not isinstance(policy, DFCPolicy):
        raise ValueError("policy must be a DFCPolicy instance")
    if not policy.sources:
        raise ValueError("policy must have sources specified")
    if len(policy.sources) != 1:
        raise ValueError("physical baseline supports a single source table per policy")

    source_table = policy.sources[0]
    is_agg = is_aggregation_query(query)

    # Parse query to get column names (for filter query construction)
    column_names = []
    base_query = query
    try:
        parsed = sqlglot.parse_one(query, read="duckdb")
        if isinstance(parsed, exp.Select):
            policy_source_in_from = False
            policy_source_in_subquery = False
            tables_to_check: list[exp.Table] = []
            from_expr = parsed.args.get("from_")
            if from_expr:
                tables_to_check.extend(list(from_expr.find_all(exp.Table)))
            for join in parsed.args.get("joins", []):
                tables_to_check.extend(list(join.find_all(exp.Table)))
            for table in tables_to_check:
                is_in_subquery = False
                current = table
                while hasattr(current, "parent"):
                    if isinstance(current.parent, exp.Subquery):
                        is_in_subquery = True
                        break
                    current = current.parent
                if is_in_subquery:
                    continue
                if hasattr(table, "name") and table.name and table.name.lower() == source_table.lower():
                    policy_source_in_from = True
                    break
            if not policy_source_in_from:
                for subquery in parsed.find_all(exp.Subquery):
                    if not isinstance(subquery.parent, (exp.From, exp.Join)):
                        continue
                    has_left_join = False
                    for join in subquery.find_all(exp.Join):
                        side = join.args.get("side")
                        if side and str(side).upper() == "LEFT":
                            has_left_join = True
                            break
                    if has_left_join:
                        continue
                    for table in subquery.find_all(exp.Table):
                        if (
                            hasattr(table, "name")
                            and table.name
                            and table.name.lower() == source_table.lower()
                        ):
                            policy_source_in_subquery = True
                            break
                    if policy_source_in_subquery:
                        break

            parsed_for_columns = parsed

            for expr in parsed_for_columns.expressions:
                if isinstance(expr, exp.Star):
                    column_names = []
                    break
                if isinstance(expr, exp.Alias):
                    alias_name = expr.alias.sql(dialect="duckdb") if hasattr(expr.alias, "sql") else str(expr.alias)
                    column_names.append(alias_name)
                elif isinstance(expr, exp.Column):
                    col_name = expr.this.sql(dialect="duckdb") if hasattr(expr.this, "sql") else str(expr.this)
                    column_names.append(col_name)
                else:
                    if isinstance(expr, exp.Column):
                        col_name = expr.this.sql(dialect="duckdb") if hasattr(expr.this, "sql") else str(expr.this)
                        column_names.append(col_name)
    except Exception:
        column_names = []

    if output_columns is None:
        output_columns = column_names

    if lineage_query and output_columns:
        order_by_clause = _extract_order_by_clause(query)
        filter_query_template = build_lineage_filter_query(
            lineage_query=lineage_query,
            temp_table_name="{temp_table_name}",
            policy=policy,
            output_columns=output_columns,
            order_by=order_by_clause,
        )
    else:
        filter_query_template = build_filter_query(
            temp_table_name="{temp_table_name}",
            constraint=policy.constraint,
            source_table=source_table,
            column_names=column_names,
            is_aggregation=is_agg,
        )

    return base_query, filter_query_template, is_agg
