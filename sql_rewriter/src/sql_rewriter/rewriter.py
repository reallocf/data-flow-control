"""SQL rewriter that intercepts queries, transforms them, and executes against DuckDB."""

import duckdb
import sqlglot
from sqlglot import exp
from typing import Any, Optional, Set, Union, Dict, List
import tempfile
import os
import json
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError, BotoCoreError

from .policy import DFCPolicy, Resolution
from .sqlglot_utils import get_column_name, get_table_name_from_column
from .rewrite_rule import _extract_columns_from_constraint
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
        stream_file_path: Optional[str] = None,
        bedrock_client: Optional[Any] = None,
        bedrock_model_id: Optional[str] = None
    ) -> None:
        """Initialize the SQL rewriter with a DuckDB connection.

        Args:
            conn: Optional DuckDB connection. If None, creates a new in-memory database connection.
            stream_file_path: Optional path for stream file (fixed rows from LLM). If None, creates a temp file.
            bedrock_client: Optional boto3 Bedrock Runtime client for LLM resolution policies.
            bedrock_model_id: Optional Bedrock model ID. Defaults to Claude Haiku if not provided.
        """
        if conn is not None:
            self.conn = conn
        else:
            self.conn = duckdb.connect()
        self._policies: list[DFCPolicy] = []
        
        # Bedrock client for LLM resolution
        self._bedrock_client = bedrock_client
        self._bedrock_model_id = bedrock_model_id or os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-haiku-4-5-20251001-v1:0")
        
        # Stream file for LLM-fixed rows
        if stream_file_path is None:
            stream_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
            self._stream_file_path = stream_file.name
            stream_file.close()
        else:
            self._stream_file_path = stream_file_path
        
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
                        # Ensure subqueries and CTEs have columns needed for constraints
                        ensure_subqueries_have_constraint_columns(parsed, matching_policies, from_tables)
                        
                        if self._has_aggregations(parsed):
                            apply_policy_constraints_to_aggregation(
                                parsed, matching_policies, from_tables, 
                                stream_file_path=self._stream_file_path
                            )
                        else:
                            apply_policy_constraints_to_scan(
                                parsed, matching_policies, from_tables,
                                stream_file_path=self._stream_file_path
                            )
            
            elif isinstance(parsed, exp.Insert):
                sink_table = self._get_sink_table(parsed)
                source_tables = self._get_insert_source_tables(parsed)
                
                matching_policies = self._find_matching_policies(
                    source_tables=source_tables, sink_table=sink_table
                )
                
                if matching_policies:
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
                        
                        # Get INSERT column list to filter which columns should be in SELECT output
                        insert_columns = self._get_insert_column_list(parsed)
                        
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
                                stream_file_path=self._stream_file_path,
                                sink_table=sink_table,
                                sink_to_output_mapping=sink_to_output_mapping,
                                replace_existing_valid=insert_has_valid,
                                insert_columns=insert_columns
                            )
                        else:
                            apply_policy_constraints_to_scan(
                                select_expr, matching_policies, source_tables,
                                stream_file_path=self._stream_file_path,
                                sink_table=sink_table,
                                sink_to_output_mapping=sink_to_output_mapping,
                                replace_existing_valid=insert_has_valid,
                                insert_columns=insert_columns
                            )

            transformed = parsed.sql(pretty=True, dialect="duckdb")
            return transformed
        except Exception as e:
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

    def delete_policy(
        self,
        source: Optional[str] = None,
        sink: Optional[str] = None,
        constraint: str = "",
        on_fail: Optional[Resolution] = None,
        description: Optional[str] = None,
    ) -> bool:
        """Delete a DFC policy from the rewriter by matching all provided parameters.
        
        All provided parameters must match exactly for a policy to be deleted.
        If a parameter is None (for source/sink/description/on_fail) or empty string (for constraint),
        it will match any value for that field. However, at least one of source, sink, or
        constraint must be provided to identify the policy.
        
        Args:
            source: Optional source table name to match. None matches any source.
            sink: Optional sink table name to match. None matches any sink.
            constraint: Constraint SQL expression to match. Empty string matches any constraint.
            on_fail: Optional resolution type to match. None matches any resolution.
            description: Optional description to match. None matches any description.
        
        Returns:
            True if a policy was found and deleted, False otherwise.
            
        Raises:
            ValueError: If neither source, sink, nor constraint is provided.
        """
        if source is None and sink is None and not constraint:
            raise ValueError("At least one of source, sink, or constraint must be provided")
        
        # Find matching policy by comparing each field
        for i, policy in enumerate(self._policies):
            # Compare each field individually, allowing None/empty to match any
            source_match = source is None or policy.source == source
            sink_match = sink is None or policy.sink == sink
            constraint_match = not constraint or policy.constraint == constraint
            on_fail_match = on_fail is None or policy.on_fail == on_fail
            description_match = description is None or policy.description == description
            
            if source_match and sink_match and constraint_match and on_fail_match and description_match:
                # Remove the matching policy
                del self._policies[i]
                return True
        
        return False

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

    def _get_insert_column_list(self, insert_parsed: exp.Insert) -> List[str]:
        """Get the INSERT column list if specified.
        
        Args:
            insert_parsed: The parsed INSERT statement.
            
        Returns:
            List of column names (lowercase) in the INSERT column list, or empty list if no column list.
        """
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
        
        return insert_columns

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
        insert_columns = self._get_insert_column_list(insert_parsed)
        
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

    
    def _call_llm_to_fix_row(
        self, 
        constraint: str,
        description: Optional[str],
        column_values: List[Any],
        column_names: Optional[List[str]] = None
    ) -> Optional[List[Any]]:
        """Call LLM to try to fix a violating row based on the constraint.
        
        Args:
            constraint: The policy constraint that was violated.
            description: Optional policy description.
            column_values: List of column values from the violating row.
            column_names: Optional list of column names corresponding to column_values.
        
        Returns:
            Optional list of fixed column values, or None if LLM couldn't fix it or failed.
        """
        if not self._bedrock_client:
            return None
        
        bedrock_client = self._bedrock_client
        
        # Helper function to convert values to JSON-serializable types
        def make_json_serializable(value):
            """Convert value to JSON-serializable type."""
            if isinstance(value, Decimal):
                return float(value)
            elif isinstance(value, (int, float, str, bool, type(None))):
                return value
            else:
                # For other types, convert to string
                return str(value)
        
        # Build row data dictionary
        row_data = {}
        if column_names and len(column_names) == len(column_values):
            for name, value in zip(column_names, column_values):
                row_data[name] = make_json_serializable(value)
        else:
            # Use generic column names
            for i, value in enumerate(column_values):
                row_data[f"col{i}"] = make_json_serializable(value)
        
        # Build prompt for LLM
        constraint_desc = description or "Policy constraint"
        
        prompt = f"""You are a data quality assistant. A row of data has violated a data flow control policy.

POLICY CONSTRAINT: {constraint}
POLICY DESCRIPTION: {constraint_desc}

VIOLATING ROW DATA:
{json.dumps(row_data, indent=2)}

Your task is to fix the violating row data so it satisfies the policy constraint. Return the fixed row data as a JSON object with the same keys as the input row data. Only modify values that need to be changed to satisfy the constraint. If you cannot fix the row, return null.

Return only the JSON object (or null), no additional text or explanation."""
        
        try:
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 2048,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }
            
            response = bedrock_client.invoke_model(
                modelId=self._bedrock_model_id,
                body=json.dumps(request_body)
            )
            
            response_body = json.loads(response['body'].read())
            
            # Extract text content from response
            text_content = ""
            for content_block in response_body.get('content', []):
                if content_block.get('type') == 'text':
                    text_content += content_block.get('text', '')
            
            if not text_content:
                return None
            
            text_content = text_content.strip()
            
            if text_content.lower() == 'null':
                return None
            
            # Try to extract JSON from response (might be wrapped in markdown code blocks or have extra text)
            json_text = text_content
            # Try to extract JSON from markdown code blocks
            if '```json' in text_content:
                start = text_content.find('```json') + 7
                end = text_content.find('```', start)
                if end != -1:
                    json_text = text_content[start:end].strip()
            elif '```' in text_content:
                start = text_content.find('```') + 3
                end = text_content.find('```', start)
                if end != -1:
                    json_text = text_content[start:end].strip()
            
            # Parse JSON response
            try:
                fixed_row_data = json.loads(json_text)
            except json.JSONDecodeError:
                return None
            
            # Convert back to list of values in the same order
            if column_names:
                fixed_values = [fixed_row_data.get(name, val) for name, val in zip(column_names, column_values)]
            else:
                # Use generic column names
                fixed_values = [fixed_row_data.get(f"col{i}", val) for i, val in enumerate(column_values)]
            
            return fixed_values
            
        except (ClientError, BotoCoreError):
            return None
        except json.JSONDecodeError:
            return None
        except Exception:
            return None
    
    def _register_address_violating_rows_udf(self) -> None:
        """Register the address_violating_rows UDF for LLM resolution policies.
        
        This UDF is used by LLM resolution policies to handle violating rows.
        The LLM is called to try to fix the row data, and fixed rows are written to the stream file.
        
        Users can override this by registering their own address_violating_rows
        function after creating the SQLRewriter instance.
        """
        def address_violating_rows(*args) -> bool:
            """address_violating_rows function that handles violating rows with LLM.
            
            Calls LLM to try to fix the row, writes fixed row to stream if successful.
            
            Args:
                *args: Variable arguments - columns, constraint, description, column_names_json, stream_endpoint.
                      Format: col1, col2, ..., constraint, description, column_names_json, stream_endpoint
                      (stream_endpoint is last for async_rewrite compatibility)
            
            Returns:
                bool: False to filter out the violating row.
            """
            if not args or len(args) < 4:
                return False
            
            # Last four arguments are: constraint, description, column_names_json, stream_endpoint
            # Rest are column values
            column_values = list(args[:-4]) if len(args) >= 4 else []
            constraint = args[-4] if len(args) >= 4 else ''
            description = args[-3] if len(args) >= 3 else ''
            column_names_json = args[-2] if len(args) >= 2 else ''
            stream_endpoint = args[-1] if len(args) >= 1 else ''
            
            # Strip quotes from stream_endpoint if present (SQL string literals include quotes)
            if stream_endpoint:
                stream_endpoint = stream_endpoint.strip().strip("'").strip('"')
            
            # Parse column names from JSON string
            column_names = None
            if column_names_json:
                try:
                    # Strip quotes from JSON string if present
                    column_names_json_cleaned = column_names_json.strip().strip("'").strip('"')
                    column_names = json.loads(column_names_json_cleaned)
                except Exception:
                    column_names = None
            
            # If we have constraint and bedrock client, try to fix with LLM
            if constraint and self._bedrock_client:
                try:
                    fixed_values = self._call_llm_to_fix_row(
                        constraint, 
                        description if description else None,
                        column_values, 
                        column_names
                    )
                    
                    if fixed_values:
                        # Write fixed row to stream file
                        if stream_endpoint:
                            try:
                                # Ensure values are written in the same order as column_names
                                # Format: tab-separated values matching the SELECT output column order
                                row_data = '\t'.join(str(val).lower() if isinstance(val, bool) else str(val) for val in fixed_values)
                                
                                import os
                                with open(stream_endpoint, 'a') as f:
                                    f.write(f"{row_data}\n")
                                    f.flush()
                                    # Force sync to disk
                                    os.fsync(f.fileno())
                            except Exception:
                                pass
                        
                        # Return False to filter out original row (fixed version is in stream)
                        return False
                except Exception:
                    pass
            
            # Return False to filter out from original query
            return False
        
        # Register with a flexible signature - DuckDB will handle the variable arguments
        # We use a generic signature that accepts any number of arguments
        self.conn.create_function('address_violating_rows', address_violating_rows, return_type='BOOLEAN')

    def get_stream_file_path(self) -> Optional[str]:
        """Get the path to the stream file for LLM-fixed rows.
        
        Returns:
            Path to stream file.
        """
        return self._stream_file_path
    
    def reset_stream_file_path(self) -> None:
        """Reset the stream file path by creating a new temporary file.
        
        This clears any existing stream entries and ensures a fresh file for new runs.
        """
        import tempfile
        stream_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
        self._stream_file_path = stream_file.name
        stream_file.close()
    
    def close(self) -> None:
        """Close the DuckDB connection."""
        self.conn.close()

    def __enter__(self) -> "SQLRewriter":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

