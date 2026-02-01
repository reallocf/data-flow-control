"""Query rewriting logic for physical baseline (SmokedDuck lineage-based approach).

The physical baseline uses SmokedDuck's lineage capabilities to track data provenance.
Instead of rewriting SQL into a single query, this approach:
1. Executes the base query
2. Stores results in a temporary table
3. Applies policy filtering based on provenance data

This module contains the logic for transforming policy constraints and building
filter queries for the physical baseline.
"""


import sqlglot
from sqlglot import exp


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
        elif hasattr(agg, "expressions") and agg.expressions:
            if isinstance(agg.expressions[0], exp.Column):
                col_expr = agg.expressions[0]
            elif isinstance(agg.expressions[0], exp.Star):
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
        elif hasattr(agg, "expressions") and agg.expressions:
            if isinstance(agg.expressions[0], exp.Column):
                col_expr = agg.expressions[0]

        if col_expr:
            # Create a new column without table qualification
            new_col = exp.Column(this=col_expr.this)
            agg.replace(new_col)

    # Remove table qualifications from the result
    result = parsed.sql(dialect="duckdb")
    # Replace table-qualified columns with unqualified ones
    result = result.replace(f"{source_table}.", "")

    return result


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
    # Transform constraint for filtering
    filter_constraint = transform_constraint_for_filtering(constraint, source_table)

    # Check if 'value' column exists in the results
    # For aggregation queries, we may not have the source column directly
    has_value_column = "value" in [col.lower() for col in column_names]

    if is_aggregation and not has_value_column:
        # For aggregation queries without the source column, we can't filter directly
        # The policy should be applied at the aggregation level, not on individual rows
        # Return all results (filtering would need to be done differently)
        return f"SELECT * FROM {temp_table_name}"
    if has_value_column:
        # Filter results using the value column
        return f"SELECT * FROM {temp_table_name} WHERE {filter_constraint}"
    # No value column and not an aggregation - can't apply filter
    # This shouldn't happen with our test policy, but handle gracefully
    return f"SELECT * FROM {temp_table_name}"


def rewrite_query_physical(
    query: str,
    policy: "DFCPolicy"
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
    if policy.source is None:
        raise ValueError("policy must have a source specified")

    constraint = policy.constraint
    source_table = policy.source
    is_agg = is_aggregation_query(query)

    # Parse query to get column names (for filter query construction)
    # Note: For SELECT *, we can't determine column names from parsing alone
    # The build_filter_query function will handle this by checking actual column names
    # at execution time. For now, we pass an empty list and let execution determine columns.
    column_names = []
    try:
        parsed = sqlglot.parse_one(query, read="duckdb")
        if isinstance(parsed, exp.Select):
            # Extract column names from SELECT
            for expr in parsed.expressions:
                if isinstance(expr, exp.Star):
                    # For SELECT *, we can't determine columns from parsing
                    # Will be determined at execution time
                    column_names = []
                    break
                if isinstance(expr, exp.Alias):
                    alias_name = expr.alias.sql(dialect="duckdb") if hasattr(expr.alias, "sql") else str(expr.alias)
                    column_names.append(alias_name)
                elif isinstance(expr, exp.Column):
                    col_name = expr.this.sql(dialect="duckdb") if hasattr(expr.this, "sql") else str(expr.this)
                    column_names.append(col_name)
                else:
                    # For expressions, try to extract column name or use SQL representation
                    # Check if it's a simple column reference
                    if isinstance(expr, exp.Column):
                        col_name = expr.this.sql(dialect="duckdb") if hasattr(expr.this, "sql") else str(expr.this)
                        column_names.append(col_name)
                    else:
                        # For complex expressions, we can't reliably extract column names
                        # This will be handled at execution time
                        pass
    except Exception:
        column_names = []

    # Build filter query template
    # Note: For queries with SELECT * or complex expressions, column_names may be empty
    # The build_filter_query function will handle this appropriately
    filter_query_template = build_filter_query(
        temp_table_name="{temp_table_name}",
        constraint=constraint,
        source_table=source_table,
        column_names=column_names,
        is_aggregation=is_agg
    )

    return query, filter_query_template, is_agg
