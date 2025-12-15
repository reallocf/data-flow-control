"""SQL rewriter that intercepts queries, transforms them, and executes against DuckDB."""

import duckdb
import sqlglot
from sqlglot import exp
from typing import Any, Optional, Set

from .policy import DFCPolicy
from .sqlglot_utils import get_column_name, get_table_name_from_column


class SQLRewriter:
    """SQL rewriter that intercepts queries, transforms them, and executes against DuckDB."""

    def __init__(self, database: Optional[str] = None) -> None:
        """Initialize the SQL rewriter with a DuckDB connection.

        Args:
            database: Optional path to DuckDB database file. If None, uses in-memory database.
        """
        if database:
            self.conn = duckdb.connect(database)
        else:
            self.conn = duckdb.connect()
        self._policies: list[DFCPolicy] = []

    def transform_query(self, query: str) -> str:
        """Transform a SQL query according to the rewriter's rules.

        Currently adds column "bar" to SELECT statements that query table "foo".
        Also applies DFC policies to aggregation queries over source tables.

        Args:
            query: The original SQL query string.

        Returns:
            The transformed SQL query string.
        """
        try:
            # Parse the SQL query
            parsed = sqlglot.parse_one(query, read="duckdb")

            # Check if this is a SELECT statement
            if isinstance(parsed, exp.Select):
                # Get source tables from the query
                from_tables = self._get_source_tables(parsed)
                
                if from_tables:
                    # Find matching policies for source tables
                    matching_policies = self._find_matching_policies(from_tables)
                    
                    if matching_policies:
                        # Check if this is an aggregation query
                        if self._has_aggregations(parsed):
                            # Apply policy constraints as HAVING clauses
                            self._apply_policy_constraints_to_aggregation(parsed, matching_policies, from_tables)
                        else:
                            # Apply policy constraints as WHERE clauses (transform aggregations to columns)
                            self._apply_policy_constraints_to_scan(parsed, matching_policies, from_tables)
                
                # Legacy transformation: Check if table "foo" is in the FROM clause
                if "foo" in from_tables:
                    # Skip transformation for aggregate queries (e.g., COUNT(*), SUM(*))
                    has_aggregate = any(
                        isinstance(expr, exp.AggFunc) or 
                        (isinstance(expr, exp.Column) and expr.find_ancestor(exp.AggFunc))
                        for expr in parsed.expressions
                    )
                    
                    if has_aggregate:
                        # Don't modify aggregate queries (policies are handled above)
                        pass
                    else:
                        # Check if "bar" column is already selected
                        has_bar = False
                        has_wildcard = False

                        for expr in parsed.expressions:
                            # Check for SELECT *
                            if isinstance(expr, exp.Star):
                                has_wildcard = True
                                break
                            # Check for explicit "bar" column
                            elif isinstance(expr, exp.Column):
                                col_name = expr.alias_or_name.lower()
                                if col_name == "bar":
                                    has_bar = True
                                    break

                        # Only add bar if it's not already there and we don't have SELECT *
                        # (SELECT * already includes all columns, so bar is already included)
                        if not has_bar and not has_wildcard:
                            # Add "bar" column to the SELECT by appending to expressions
                            parsed.expressions.append(
                                exp.Column(this=exp.Identifier(this="bar"))
                            )

            # Return the transformed SQL
            return parsed.sql(pretty=True, dialect="duckdb")
        except Exception as e:
            # If transformation fails, return original query
            # In production, you might want to log this error
            return query

    def _execute_transformed(self, query: str):
        """Execute a transformed query and return the cursor.
        
        Args:
            query: The SQL query string to execute.
            
        Returns:
            The DuckDB cursor from executing the transformed query.
        """
        transformed_query = self.transform_query(query)
        return self.conn.execute(transformed_query)

    def execute(self, query: str) -> Any:
        """Execute a SQL query after transforming it.

        Args:
            query: The SQL query string to execute.

        Returns:
            The result of executing the query.
        """
        return self._execute_transformed(query)

    def fetchall(self, query: str) -> list[tuple]:
        """Execute a query and fetch all results.

        Args:
            query: The SQL query string to execute.

        Returns:
            List of tuples containing the query results.
        """
        return self._execute_transformed(query).fetchall()

    def fetchone(self, query: str) -> Optional[tuple]:
        """Execute a query and fetch one result.

        Args:
            query: The SQL query string to execute.

        Returns:
            A single tuple containing one row of results, or None if no results.
        """
        return self._execute_transformed(query).fetchone()

    def _table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database.

        Args:
            table_name: The name of the table to check.

        Returns:
            True if the table exists, False otherwise.
        """
        try:
            result = self.conn.execute(
                """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'main' AND table_name = ?
                """,
                [table_name.lower()]
            ).fetchone()
            return result is not None
        except Exception:
            return False

    def _get_table_columns(self, table_name: str) -> Set[str]:
        """Get all column names for a table.

        Args:
            table_name: The name of the table.

        Returns:
            A set of column names (lowercase).

        Raises:
            ValueError: If the table does not exist.
        """
        if not self._table_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist in the database")

        try:
            result = self.conn.execute(
                """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'main' AND table_name = ?
                """,
                [table_name.lower()]
            ).fetchall()
            return {row[0].lower() for row in result}
        except Exception as e:
            raise ValueError(f"Failed to get columns for table '{table_name}': {e}")


    def _validate_table_exists(self, table_name: str, table_type: str) -> None:
        """Validate that a table exists in the database.
        
        Args:
            table_name: The table name to validate.
            table_type: The type of table ("Source" or "Sink") for error messages.
            
        Raises:
            ValueError: If the table does not exist.
        """
        if not self._table_exists(table_name):
            raise ValueError(f"{table_type} table '{table_name}' does not exist in the database")

    def _get_column_table_type(
        self, column: exp.Column, policy: DFCPolicy
    ) -> Optional[str]:
        """Determine which table type (source/sink) a column belongs to.
        
        Args:
            column: The column expression to check.
            policy: The policy to check against.
            
        Returns:
            "source" if column belongs to source table, "sink" if sink table,
            or None if it doesn't belong to either.
        """
        table_name = get_table_name_from_column(column)
        if not table_name:
            return None
        
        if policy.source and table_name == policy.source.lower():
            return "source"
        elif policy.sink and table_name == policy.sink.lower():
            return "sink"
        return None

    def _validate_column_in_table(
        self,
        column: exp.Column,
        table_name: str,
        table_columns: Set[str],
        table_type: str,
    ) -> None:
        """Validate that a column exists in a specific table.
        
        Args:
            column: The column expression to validate.
            table_name: The table name the column should belong to.
            table_columns: Set of column names in the table.
            table_type: The type of table ("source" or "sink") for error messages.
            
        Raises:
            ValueError: If the column doesn't exist in the table.
        """
        col_name = get_column_name(column).lower()
        if col_name not in table_columns:
            raise ValueError(
                f"Column '{table_name}.{col_name}' referenced in constraint "
                f"does not exist in {table_type} table '{table_name}'"
            )

    def register_policy(self, policy: DFCPolicy) -> None:
        """Register a DFC policy with the rewriter.

        This validates that:
        - The source table exists (if provided)
        - The sink table exists (if provided)
        - All columns referenced in the constraint exist in their respective tables

        Args:
            policy: The DFCPolicy to register.

        Raises:
            ValueError: If validation fails (table doesn't exist, column doesn't exist, etc.).
        """
        # Validate source and sink tables exist
        if policy.source:
            self._validate_table_exists(policy.source, "Source")
        if policy.sink:
            self._validate_table_exists(policy.sink, "Sink")

        # Get column names for each table
        source_columns: Optional[Set[str]] = None
        sink_columns: Optional[Set[str]] = None

        if policy.source:
            source_columns = self._get_table_columns(policy.source)
        if policy.sink:
            sink_columns = self._get_table_columns(policy.sink)

        # Validate each column exists in the appropriate table
        columns = list(policy._constraint_parsed.find_all(exp.Column))
        for column in columns:
            table_name = get_table_name_from_column(column)
            if not table_name:
                col_name = get_column_name(column)
                raise ValueError(
                    f"Column '{col_name}' in constraint is not qualified with a table name. "
                    "This should have been caught during policy creation."
                )

            table_type = self._get_column_table_type(column, policy)
            col_name = get_column_name(column).lower()

            if table_type == "source":
                if source_columns is None:
                    raise ValueError(f"Source table '{policy.source}' has no columns")
                self._validate_column_in_table(column, table_name, source_columns, "source")
            elif table_type == "sink":
                if sink_columns is None:
                    raise ValueError(f"Sink table '{policy.sink}' has no columns")
                self._validate_column_in_table(column, table_name, sink_columns, "sink")
            else:
                raise ValueError(
                    f"Column '{table_name}.{col_name}' referenced in constraint "
                    f"references table '{table_name}', which is not the source "
                    f"('{policy.source}') or sink ('{policy.sink}')"
                )

        # All validations passed, register the policy
        self._policies.append(policy)

    def _get_source_tables(self, parsed: exp.Select) -> Set[str]:
        """Extract source table names from a SELECT query.
        
        Args:
            parsed: The parsed SELECT statement.
            
        Returns:
            A set of lowercase table names from FROM/JOIN clauses.
        """
        from_tables = set()
        for table in parsed.find_all(exp.Table):
            # Only consider tables in FROM/JOIN clauses, not in column references
            if table.find_ancestor(exp.From) or table.find_ancestor(exp.Join):
                from_tables.add(table.name.lower())
        return from_tables

    def _has_aggregations(self, parsed: exp.Select) -> bool:
        """Check if a SELECT query contains aggregations.
        
        Args:
            parsed: The parsed SELECT statement.
            
        Returns:
            True if the query contains aggregations, False otherwise.
        """
        return any(
            isinstance(expr, exp.AggFunc) or 
            (isinstance(expr, exp.Column) and expr.find_ancestor(exp.AggFunc))
            for expr in parsed.expressions
        )

    def _find_matching_policies(self, source_tables: Set[str]) -> list[DFCPolicy]:
        """Find policies that match the source tables in the query.
        
        Args:
            source_tables: Set of source table names from the query.
            
        Returns:
            List of policies that have a source matching one of the source tables.
        """
        matching = []
        for policy in self._policies:
            if policy.source and policy.source.lower() in source_tables:
                matching.append(policy)
        return matching

    def _apply_policy_constraints_to_aggregation(
        self, 
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
            self._ensure_columns_accessible(parsed, constraint_expr, source_tables)
            
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

    def _ensure_columns_accessible(
        self,
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

    def _apply_policy_constraints_to_scan(
        self,
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
            constraint_expr = self._transform_aggregations_to_columns(
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

    def _transform_aggregations_to_columns(
        self,
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

    def close(self) -> None:
        """Close the DuckDB connection."""
        self.conn.close()

    def __enter__(self) -> "SQLRewriter":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

