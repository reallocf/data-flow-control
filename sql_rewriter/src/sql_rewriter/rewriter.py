"""SQL rewriter that intercepts queries, transforms them, and executes against DuckDB."""

import duckdb
import sqlglot
from sqlglot import exp
from typing import Any, Optional, Set, Union
import tempfile
import os
import threading

from .policy import DFCPolicy, Resolution
from .sqlglot_utils import get_column_name, get_table_name_from_column
from .rewrite_rule import (
    apply_policy_constraints_to_aggregation,
    apply_policy_constraints_to_scan,
    ensure_subqueries_have_constraint_columns,
)


class SQLRewriter:
    """SQL rewriter that intercepts queries, transforms them, and executes against DuckDB."""

    def __init__(
        self, 
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        human_review_enabled: bool = True,
        pending_file_path: Optional[str] = None,
        stream_file_path: Optional[str] = None
    ) -> None:
        """Initialize the SQL rewriter with a DuckDB connection.

        Args:
            conn: Optional DuckDB connection. If None, creates a new in-memory database connection.
            human_review_enabled: If True, enables human review mode for HUMAN resolution policies.
                                 Violating rows will be written to a pending file for review.
            pending_file_path: Optional path for pending file (violating rows). If None, creates a temp file.
            stream_file_path: Optional path for stream file (approved rows). If None, creates a temp file.
        """
        if conn is not None:
            self.conn = conn
        else:
            self.conn = duckdb.connect()
        self._policies: list[DFCPolicy] = []
        
        # Human review configuration
        self._human_review_enabled = human_review_enabled
        if human_review_enabled:
            if pending_file_path is None:
                pending_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
                self._pending_file_path = pending_file.name
                pending_file.close()
            else:
                self._pending_file_path = pending_file_path
            
            if stream_file_path is None:
                stream_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
                self._stream_file_path = stream_file.name
                stream_file.close()
            else:
                self._stream_file_path = stream_file_path
            
            self._violating_rows_count = 0
            self._pending_file_lock = threading.Lock()
        else:
            self._pending_file_path = None
            self._stream_file_path = None
        
        self._register_kill_udf()
        self._register_address_violating_rows_udf()

    def transform_query(self, query: str) -> str:
        """Transform a SQL query according to the rewriter's rules.

        Applies DFC policies to queries over source tables. For aggregation queries,
        policies are applied as HAVING clauses. For non-aggregation queries, policies
        are applied as WHERE clauses with aggregations transformed to columns.
        
        For INSERT statements, policies are matched based on sink table and source tables
        from the SELECT part (if present).

        Args:
            query: The original SQL query string.

        Returns:
            The transformed SQL query string.
        """
        try:
            parsed = sqlglot.parse_one(query, read="duckdb")

            if isinstance(parsed, exp.Select):
                from_tables = self._get_source_tables(parsed)
                
                if from_tables:
                    matching_policies = self._find_matching_policies(
                        source_tables=from_tables, sink_table=None
                    )
                    
                    if matching_policies:
                        for policy in matching_policies:
                            print(f"Matched policy: {policy.get_identifier()}")
                        
                        # Ensure subqueries and CTEs have columns needed for constraints
                        ensure_subqueries_have_constraint_columns(parsed, matching_policies, from_tables)
                        
                        if self._has_aggregations(parsed):
                            apply_policy_constraints_to_aggregation(
                                parsed, matching_policies, from_tables, 
                                stream_file_path=self._stream_file_path if self._human_review_enabled else None
                            )
                        else:
                            apply_policy_constraints_to_scan(
                                parsed, matching_policies, from_tables,
                                stream_file_path=self._stream_file_path if self._human_review_enabled else None
                            )
            
            elif isinstance(parsed, exp.Insert):
                sink_table = self._get_sink_table(parsed)
                source_tables = self._get_insert_source_tables(parsed)
                
                matching_policies = self._find_matching_policies(
                    source_tables=source_tables, sink_table=sink_table
                )
                
                if matching_policies:
                    for policy in matching_policies:
                        print(f"Matched policy: {policy.get_identifier()}")
                    
                    # Check if any matching policy is INVALIDATE with sink
                    has_invalidate_with_sink = any(
                        p.on_fail == Resolution.INVALIDATE and p.sink 
                        for p in matching_policies
                    )
                    
                    # Find the SELECT statement within the INSERT
                    select_expr = parsed.find(exp.Select)
                    if select_expr:
                        # If INVALIDATE policy with sink, add 'valid' column to INSERT column list
                        # This must happen before adding aliases so the mapping is correct
                        if has_invalidate_with_sink and sink_table:
                            self._add_valid_column_to_insert(parsed)
                        
                        # Add aliases to SELECT outputs to match sink column names (if explicit column list)
                        # This ensures sink column references in constraints can be replaced correctly
                        self._add_aliases_to_insert_select_outputs(parsed, select_expr)
                        
                        # Get mapping from sink columns to SELECT output columns
                        sink_to_output_mapping = None
                        if sink_table:
                            sink_to_output_mapping = self._get_insert_column_mapping(parsed, select_expr)
                        
                        # Ensure subqueries and CTEs have columns needed for constraints
                        ensure_subqueries_have_constraint_columns(
                            select_expr, matching_policies, source_tables
                        )
                        
                        # Check if 'valid' is already in INSERT column list (user-provided value)
                        # If so, we should replace it with constraint, not combine
                        insert_has_valid = False
                        if hasattr(parsed, 'this') and isinstance(parsed.this, exp.Schema):
                            if hasattr(parsed.this, 'expressions') and parsed.this.expressions:
                                for col in parsed.this.expressions:
                                    col_name = None
                                    if isinstance(col, exp.Identifier):
                                        col_name = col.name.lower()
                                    elif isinstance(col, exp.Column):
                                        col_name = get_column_name(col).lower()
                                    elif isinstance(col, str):
                                        col_name = col.lower()
                                    if col_name == "valid":
                                        insert_has_valid = True
                                        break
                        
                        if self._has_aggregations(select_expr):
                            apply_policy_constraints_to_aggregation(
                                select_expr, matching_policies, source_tables,
                                stream_file_path=self._stream_file_path if self._human_review_enabled else None,
                                sink_table=sink_table,
                                sink_to_output_mapping=sink_to_output_mapping,
                                replace_existing_valid=insert_has_valid
                            )
                        else:
                            apply_policy_constraints_to_scan(
                                select_expr, matching_policies, source_tables,
                                stream_file_path=self._stream_file_path if self._human_review_enabled else None,
                                sink_table=sink_table,
                                sink_to_output_mapping=sink_to_output_mapping,
                                replace_existing_valid=insert_has_valid
                            )

            return parsed.sql(pretty=True, dialect="duckdb")
        except Exception as e:
            # In production, you might want to log this error
            print("Encountered exception: ", e)
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

    def _get_column_type(self, table_name: str, column_name: str) -> Optional[str]:
        """Get the data type of a column in a table.
        
        Args:
            table_name: The name of the table.
            column_name: The name of the column.
            
        Returns:
            The data type as a string (e.g., 'BOOLEAN', 'INTEGER'), or None if column doesn't exist.
            
        Raises:
            ValueError: If query fails.
        """
        try:
            result = self.conn.execute(
                """
                SELECT data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'main' AND table_name = ? AND column_name = ?
                """,
                [table_name.lower(), column_name.lower()]
            ).fetchone()
            return result[0].upper() if result else None
        except Exception as e:
            raise ValueError(f"Failed to get column type for '{table_name}.{column_name}': {e}")

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
        - For INVALIDATE policies with sink tables, the sink table has a boolean column named 'valid'

        Args:
            policy: The DFCPolicy to register.

        Raises:
            ValueError: If validation fails (table doesn't exist, column doesn't exist, etc.).
        """
        if policy.source:
            self._validate_table_exists(policy.source, "Source")
        if policy.sink:
            self._validate_table_exists(policy.sink, "Sink")

        source_columns: Optional[Set[str]] = None
        sink_columns: Optional[Set[str]] = None

        if policy.source:
            source_columns = self._get_table_columns(policy.source)
        if policy.sink:
            sink_columns = self._get_table_columns(policy.sink)
        
        # For INVALIDATE policies with sink tables, validate that sink has a boolean 'valid' column
        if policy.on_fail == Resolution.INVALIDATE and policy.sink:
            if sink_columns is None:
                raise ValueError(f"Sink table '{policy.sink}' has no columns")
            if "valid" not in sink_columns:
                raise ValueError(
                    f"Sink table '{policy.sink}' must have a boolean column named 'valid' "
                    f"for INVALIDATE resolution policies"
                )
            valid_column_type = self._get_column_type(policy.sink, "valid")
            if valid_column_type != "BOOLEAN":
                raise ValueError(
                    f"Column 'valid' in sink table '{policy.sink}' must be of type BOOLEAN, "
                    f"but found type '{valid_column_type}'"
                )

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

        self._policies.append(policy)

    def get_dfc_policies(self) -> list[DFCPolicy]:
        """Get all registered DFC policies.
        
        Returns:
            List of all registered DFCPolicy objects.
        """
        return self._policies.copy()

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

    def _get_sink_table(self, parsed: exp.Insert) -> Optional[str]:
        """Extract sink table name from an INSERT statement.
        
        Args:
            parsed: The parsed INSERT statement.
            
        Returns:
            The lowercase sink table name, or None if not found.
        """
        if not isinstance(parsed, exp.Insert):
            return None
        
        def _extract_table_name(table_expr) -> Optional[str]:
            """Helper to extract table name from various expression types.
            
            Based on sqlglot structure:
            - When INSERT has column list: parsed.this is a Schema containing a Table
            - When INSERT has no column list: parsed.this is a Table directly
            - Table.name is always a string (not an Identifier)
            """
            # Handle Schema objects (when INSERT has column list: INSERT INTO table (col1, col2))
            if isinstance(table_expr, exp.Schema):
                # Schema.this contains the Table
                if hasattr(table_expr, 'this') and isinstance(table_expr.this, exp.Table):
                    return _extract_table_name(table_expr.this)
            
            # Handle Table expressions
            if isinstance(table_expr, exp.Table):
                # Table.name is always a string in sqlglot
                if hasattr(table_expr, 'name') and table_expr.name:
                    return str(table_expr.name).lower()
                # Fallback to alias_or_name if name is not available
                if hasattr(table_expr, 'alias_or_name'):
                    return str(table_expr.alias_or_name).lower()
            
            return None
        
        # In sqlglot, INSERT statements have the table in parsed.this
        # This can be either a Table (no column list) or Schema (with column list)
        if hasattr(parsed, 'this') and parsed.this:
            result = _extract_table_name(parsed.this)
            if result:
                return result
        
        # Fallback: find Table expressions that are NOT inside a SELECT
        # (to avoid picking up source tables from INSERT ... SELECT)
        # This handles edge cases where parsed.this might not be set correctly
        for table in parsed.find_all(exp.Table):
            # Skip tables that are inside a SELECT statement (these are source tables)
            if table.find_ancestor(exp.Select):
                continue
            # Skip tables that are inside JOIN clauses (these are source tables)
            if table.find_ancestor(exp.Join):
                continue
            # This should be the sink table
            result = _extract_table_name(table)
            if result:
                return result
        
        return None

    def _get_insert_source_tables(self, parsed: exp.Insert) -> Set[str]:
        """Extract source table names from an INSERT ... SELECT statement.
        
        Args:
            parsed: The parsed INSERT statement.
            
        Returns:
            A set of lowercase table names from the SELECT part of the INSERT statement.
        """
        if not isinstance(parsed, exp.Insert):
            return set()
        
        # Check if INSERT has a SELECT statement
        select_expr = parsed.find(exp.Select)
        if select_expr:
            return self._get_source_tables(select_expr)
        
        return set()

    def _get_insert_column_mapping(
        self,
        insert_parsed: exp.Insert,
        select_parsed: exp.Select
    ) -> dict[str, str]:
        """Get mapping from sink table column names to SELECT output column names/aliases.
        
        For INSERT INTO sink (col1, col2) SELECT x, y FROM source:
        - If column list is specified: maps sink column names to SELECT output by position
        - If no column list: maps by position (sink.col1 -> first SELECT output, etc.)
        
        Args:
            insert_parsed: The parsed INSERT statement.
            select_parsed: The parsed SELECT statement within the INSERT.
            
        Returns:
            Dictionary mapping sink column name (lowercase) to SELECT output column name/alias (lowercase).
            The output column name is the alias if present, otherwise the column name.
        """
        mapping = {}
        
        # Get the INSERT column list if specified
        # In sqlglot, INSERT columns might be in different places
        insert_columns = []
        
        # When INSERT has column list, parsed.this is a Schema and columns are in Schema.expressions
        if hasattr(insert_parsed, 'this') and isinstance(insert_parsed.this, exp.Schema):
            if hasattr(insert_parsed.this, 'expressions') and insert_parsed.this.expressions:
                for col in insert_parsed.this.expressions:
                    if isinstance(col, exp.Identifier):
                        insert_columns.append(col.name.lower())
                    elif isinstance(col, exp.Column):
                        insert_columns.append(get_column_name(col).lower())
                    elif isinstance(col, str):
                        insert_columns.append(col.lower())
        
        # Check for columns attribute (common in sqlglot)
        if not insert_columns and hasattr(insert_parsed, 'columns') and insert_parsed.columns:
            for col in insert_parsed.columns:
                if isinstance(col, exp.Identifier):
                    insert_columns.append(col.name.lower())
                elif isinstance(col, exp.Column):
                    insert_columns.append(get_column_name(col).lower())
                elif isinstance(col, str):
                    insert_columns.append(col.lower())
        
        # Also check expressions attribute as fallback
        if not insert_columns and hasattr(insert_parsed, 'expressions') and insert_parsed.expressions:
            for expr in insert_parsed.expressions:
                if isinstance(expr, exp.Identifier):
                    insert_columns.append(expr.name.lower())
                elif isinstance(expr, exp.Column):
                    insert_columns.append(get_column_name(expr).lower())
        
        # Get SELECT output columns (with aliases if present)
        select_outputs = []
        for expr in select_parsed.expressions:
            if isinstance(expr, exp.Alias):
                # Column has an alias - use the alias
                if isinstance(expr.alias, exp.Identifier):
                    alias_name = expr.alias.name.lower()
                elif isinstance(expr.alias, str):
                    alias_name = expr.alias.lower()
                else:
                    alias_name = str(expr.alias).lower()
                select_outputs.append(alias_name)
            elif isinstance(expr, exp.Column):
                # Column without alias - use column name
                select_outputs.append(get_column_name(expr).lower())
            elif isinstance(expr, exp.Star):
                # SELECT * - can't map columns reliably
                return {}
            else:
                # Expression without alias - use position-based name
                # This is a fallback, ideally columns should have aliases
                select_outputs.append(f"col{len(select_outputs) + 1}")
        
        # Map sink columns to SELECT outputs by position
        if insert_columns:
            # Column list specified: map by position
            for i, sink_col in enumerate(insert_columns):
                if i < len(select_outputs):
                    mapping[sink_col] = select_outputs[i]
        else:
            # No column list: map by position
            # We can't know the actual sink column names, so we'll need to map
            # based on the constraint's column references
            # For now, map by position assuming sink columns are referenced by position
            for i, select_output in enumerate(select_outputs):
                # Use position-based mapping
                mapping[f"col{i + 1}"] = select_output
        
        return mapping

    def _add_valid_column_to_insert(self, insert_parsed: exp.Insert) -> None:
        """Add 'valid' column to INSERT column list if not already present.
        
        For INVALIDATE policies with sink tables, the INSERT statement needs to include
        the 'valid' column in its column list so that the SELECT output can be mapped
        to it. This only modifies INSERT statements with explicit column lists.
        
        Args:
            insert_parsed: The parsed INSERT statement to modify.
        """
        # Check if INSERT has an explicit column list
        # When INSERT has column list, parsed.this is a Schema and columns are in Schema.expressions
        if hasattr(insert_parsed, 'this') and isinstance(insert_parsed.this, exp.Schema):
            if hasattr(insert_parsed.this, 'expressions') and insert_parsed.this.expressions:
                # Check if 'valid' is already in the column list
                column_names = []
                for col in insert_parsed.this.expressions:
                    if isinstance(col, exp.Identifier):
                        column_names.append(col.name.lower())
                    elif isinstance(col, exp.Column):
                        column_names.append(get_column_name(col).lower())
                    elif isinstance(col, str):
                        column_names.append(col.lower())
                
                # Add 'valid' column if not already present
                if "valid" not in column_names:
                    valid_identifier = exp.Identifier(this="valid", quoted=False)
                    insert_parsed.this.expressions.append(valid_identifier)
                return
        
        # Check for columns attribute (common in sqlglot)
        if hasattr(insert_parsed, 'columns') and insert_parsed.columns:
            # Check if 'valid' is already in the column list
            column_names = []
            for col in insert_parsed.columns:
                if isinstance(col, exp.Identifier):
                    column_names.append(col.name.lower())
                elif isinstance(col, exp.Column):
                    column_names.append(get_column_name(col).lower())
                elif isinstance(col, str):
                    column_names.append(col.lower())
            
            # Add 'valid' column if not already present
            if "valid" not in column_names:
                valid_identifier = exp.Identifier(this="valid", quoted=False)
                insert_parsed.columns.append(valid_identifier)
            return

    def _add_aliases_to_insert_select_outputs(
        self,
        insert_parsed: exp.Insert,
        select_parsed: exp.Select
    ) -> None:
        """Add aliases to SELECT outputs to match sink column names when INSERT has explicit column list.
        
        This ensures that sink column references in constraints can be replaced with SELECT output
        column references. For example, if INSERT INTO table (col1, col2) SELECT x, y, we add
        aliases: SELECT x AS col1, y AS col2.
        
        Args:
            insert_parsed: The parsed INSERT statement.
            select_parsed: The parsed SELECT statement within the INSERT.
        """
        # Get the INSERT column list if specified
        insert_columns = []
        
        # When INSERT has column list, parsed.this is a Schema and columns are in Schema.expressions
        if hasattr(insert_parsed, 'this') and isinstance(insert_parsed.this, exp.Schema):
            if hasattr(insert_parsed.this, 'expressions') and insert_parsed.this.expressions:
                for col in insert_parsed.this.expressions:
                    if isinstance(col, exp.Identifier):
                        insert_columns.append(col.name.lower())
                    elif isinstance(col, exp.Column):
                        insert_columns.append(get_column_name(col).lower())
                    elif isinstance(col, str):
                        insert_columns.append(col.lower())
        
        # Check for columns attribute (common in sqlglot)
        if not insert_columns and hasattr(insert_parsed, 'columns') and insert_parsed.columns:
            for col in insert_parsed.columns:
                if isinstance(col, exp.Identifier):
                    insert_columns.append(col.name.lower())
                elif isinstance(col, exp.Column):
                    insert_columns.append(get_column_name(col).lower())
                elif isinstance(col, str):
                    insert_columns.append(col.lower())
        
        # Also check expressions attribute as fallback
        if not insert_columns and hasattr(insert_parsed, 'expressions') and insert_parsed.expressions:
            for expr in insert_parsed.expressions:
                if isinstance(expr, exp.Identifier):
                    insert_columns.append(expr.name.lower())
                elif isinstance(expr, exp.Column):
                    insert_columns.append(get_column_name(expr).lower())
        
        # Only add aliases if there's an explicit column list
        if not insert_columns:
            return
        
        # Add aliases to SELECT outputs that don't already have them
        for i, expr in enumerate(select_parsed.expressions):
            if i >= len(insert_columns):
                break
            
            # Skip if already has an alias
            if isinstance(expr, exp.Alias):
                continue
            
            # Skip SELECT * (can't add aliases)
            if isinstance(expr, exp.Star):
                continue
            
            sink_col_name = insert_columns[i]
            
            # Skip if expression is already a Column with the same name as sink column
            # (no need to add redundant alias like "txn_id AS txn_id")
            if isinstance(expr, exp.Column):
                col_name = get_column_name(expr).lower()
                if col_name == sink_col_name:
                    continue
            
            # Add alias matching the sink column name
            alias_expr = exp.Alias(
                this=expr,
                alias=exp.Identifier(this=sink_col_name, quoted=False)
            )
            select_parsed.expressions[i] = alias_expr

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

    def _find_matching_policies(
        self, 
        source_tables: Set[str], 
        sink_table: Optional[str] = None
    ) -> list[DFCPolicy]:
        """Find policies that match the source and sink tables in the query.
        
        Matching rules:
        - If a policy has only a sink, it matches INSERT queries with that sink table.
        - If a policy has only a source, it matches SELECT queries with that source table.
        - If a policy has both sink and source, it matches INSERT INTO sink queries.
          The policy will be applied and will fail if the source is not present in the query.
        
        Args:
            source_tables: Set of source table names from the query.
            sink_table: Optional sink table name from the query (for INSERT statements).
            
        Returns:
            List of policies that match the query's source and sink tables.
        """
        matching = []
        for policy in self._policies:
            policy_source = policy.source.lower() if policy.source else None
            policy_sink = policy.sink.lower() if policy.sink else None
            
            if policy_sink and policy_source:
                # Policy has both sink and source: match INSERT INTO sink queries
                # The policy will fail if source is not present (enforcing that source must be present)
                if sink_table is not None and policy_sink == sink_table:
                    matching.append(policy)
            elif policy_sink:
                # Policy has only sink: query must be INSERT INTO sink
                if sink_table is not None and policy_sink == sink_table:
                    matching.append(policy)
            elif policy_source:
                # Policy has only source: query must be SELECT ... FROM source
                if source_tables and policy_source in source_tables:
                    matching.append(policy)
        
        return matching

    def _register_kill_udf(self) -> None:
        """Register the kill UDF that raises a ValueError when called.
        
        This UDF is used by KILL resolution policies to abort queries
        when policy constraints fail.
        """
        def kill() -> bool:
            """Kill function that raises ValueError to abort the query.
            
            Returns:
                bool: Never returns, always raises ValueError.
                
            Raises:
                ValueError: Always raised with message "KILLing due to dfc policy violation"
            """
            raise ValueError("KILLing due to dfc policy violation")
        
        self.conn.create_function('kill', kill, return_type='BOOLEAN')

    def _register_address_violating_rows_udf(self) -> None:
        """Register the address_violating_rows UDF for HUMAN resolution policies.
        
        This UDF is used by HUMAN resolution policies to handle violating rows
        through a human-in-the-loop mechanism. When human_review_enabled is True,
        violating rows are written to a pending file for review.
        
        Users can override this by registering their own address_violating_rows
        function after creating the SQLRewriter instance.
        """
        if self._human_review_enabled:
            def address_violating_rows(*args) -> bool:
                """address_violating_rows function that writes violating rows to pending file.
                
                Writes violating row data to the pending file in tab-separated format.
                The last argument is the stream_endpoint (path to stream file).
                
                Args:
                    *args: Variable arguments - columns from the constraint plus stream_endpoint.
                          The last argument is the stream_endpoint string.
                
                Returns:
                    bool: False to filter out the violating row.
                """
                if not args:
                    return False
                
                # Last argument is stream_endpoint, rest are column values
                column_values = args[:-1] if len(args) > 1 else args
                stream_endpoint = args[-1] if len(args) > 1 else ''
                
                # Write violating row to pending file
                # Format: tab-separated values
                with self._pending_file_lock:
                    with open(self._pending_file_path, 'a') as f:
                        row_data = '\t'.join(str(val).lower() if isinstance(val, bool) else str(val) for val in column_values)
                        f.write(f"{row_data}\n")
                        f.flush()
                    
                    self._violating_rows_count += 1
                    print(f"[UDF] Violating row #{self._violating_rows_count} written to pending file: {row_data}")
                
                # Return False to filter out from original query (it will come from stream if user passes it)
                return False
        else:
            def address_violating_rows(*args) -> bool:
                """Default address_violating_rows function for HUMAN resolution.
                
                This default implementation returns False to filter out violating rows.
                Enable human_review_enabled to write rows to a file for review.
                
                Args:
                    *args: Variable arguments - columns from the constraint plus stream_endpoint.
                
                Returns:
                    bool: False to filter out the violating row.
                """
                return False
        
        # Register with a flexible signature - DuckDB will handle the variable arguments
        # We use a generic signature that accepts any number of arguments
        self.conn.create_function('address_violating_rows', address_violating_rows, return_type='BOOLEAN')

    def get_pending_file_path(self) -> Optional[str]:
        """Get the path to the pending file containing violating rows.
        
        Returns:
            Path to pending file if human review is enabled, None otherwise.
        """
        return self._pending_file_path if self._human_review_enabled else None
    
    def get_stream_file_path(self) -> Optional[str]:
        """Get the path to the stream file for approved rows.
        
        Returns:
            Path to stream file if human review is enabled, None otherwise.
        """
        return self._stream_file_path if self._human_review_enabled else None
    
    def get_violating_rows_count(self) -> int:
        """Get the number of violating rows collected so far.
        
        Returns:
            Number of violating rows written to the pending file.
        """
        return self._violating_rows_count if self._human_review_enabled else 0
    
    def review_pending_rows(self) -> list[dict]:
        """Read and return all pending violating rows from the pending file.
        
        Returns:
            List of dictionaries, each containing the row data as key-value pairs.
            Returns empty list if human review is not enabled or file doesn't exist.
        """
        if not self._human_review_enabled or not self._pending_file_path:
            return []
        
        if not os.path.exists(self._pending_file_path):
            return []
        
        rows = []
        with open(self._pending_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    parts = line.split('\t')
                    # Convert to dict with indexed keys (col0, col1, etc.)
                    # Users can customize this based on their column names
                    row_dict = {f'col{i}': part for i, part in enumerate(parts)}
                    rows.append(row_dict)
        
        return rows
    
    def approve_row(self, row_data: list) -> None:
        """Approve a row by writing it to the stream file.
        
        Args:
            row_data: List of column values to write to the stream file.
                     Should match the format written to pending file (tab-separated).
        """
        if not self._human_review_enabled or not self._stream_file_path:
            return
        
        with open(self._stream_file_path, 'a') as f:
            row_str = '\t'.join(str(val).lower() if isinstance(val, bool) else str(val) for val in row_data)
            f.write(f"{row_str}\n")
            f.flush()
    
    def close(self) -> None:
        """Close the DuckDB connection."""
        self.conn.close()

    def __enter__(self) -> "SQLRewriter":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

