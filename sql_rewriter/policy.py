"""Data Flow Control Policy definitions."""

import duckdb
import sqlglot
from sqlglot import exp
from enum import Enum
from typing import Optional


class Resolution(Enum):
    """Action to take when a policy fails."""

    REMOVE = "REMOVE"
    KILL = "KILL"


class DFCPolicy:
    """Data Flow Control Policy.

    A policy defines constraints on data flow between source and sink tables.
    Either source or sink (or both) must be specified.
    """

    def __init__(
        self,
        constraint: str,
        on_fail: Resolution,
        source: Optional[str] = None,
        sink: Optional[str] = None,
    ) -> None:
        """Initialize a DFC policy.

        Args:
            constraint: A SQL expression that must evaluate to true for the policy to pass.
            on_fail: Action to take when the policy fails (REMOVE or KILL).
            source: Optional source table name.
            sink: Optional sink table name.

        Raises:
            ValueError: If neither source nor sink is provided, or if validation fails.
        """
        if source is None and sink is None:
            raise ValueError("Either source or sink must be provided")

        self.source = source
        self.sink = sink
        self.constraint = constraint
        self.on_fail = on_fail

        # Validate the policy
        self._validate()

    def _validate(self) -> None:
        """Validate that source, sink, and constraint are valid SQL and reference real tables/columns."""
        # Validate source table name if provided (must be a valid identifier)
        if self.source:
            try:
                # Try to parse as a table reference in a FROM clause
                # This validates it's a valid SQL identifier
                test_query = f"SELECT * FROM {self.source}"
                parsed = sqlglot.parse_one(test_query, read="duckdb")
                if not isinstance(parsed, sqlglot.exp.Select):
                    raise ValueError(f"Source '{self.source}' is not a valid table identifier")
                # Extract table name to ensure it parsed correctly
                tables = list(parsed.find_all(sqlglot.exp.Table))
                if not tables:
                    raise ValueError(f"Source '{self.source}' does not reference a valid table")
            except sqlglot.errors.ParseError as e:
                raise ValueError(f"Invalid source table name '{self.source}': {e}")
            except Exception as e:
                if "Invalid" not in str(e):
                    raise ValueError(f"Invalid source table '{self.source}': {e}")
                raise

        # Validate sink table name if provided (must be a valid identifier)
        if self.sink:
            try:
                # Try to parse as a table reference in a FROM clause
                test_query = f"SELECT * FROM {self.sink}"
                parsed = sqlglot.parse_one(test_query, read="duckdb")
                if not isinstance(parsed, sqlglot.exp.Select):
                    raise ValueError(f"Sink '{self.sink}' is not a valid table identifier")
                tables = list(parsed.find_all(sqlglot.exp.Table))
                if not tables:
                    raise ValueError(f"Sink '{self.sink}' does not reference a valid table")
            except sqlglot.errors.ParseError as e:
                raise ValueError(f"Invalid sink table name '{self.sink}': {e}")
            except Exception as e:
                if "Invalid" not in str(e):
                    raise ValueError(f"Invalid sink table '{self.sink}': {e}")
                raise

        # Validate constraint SQL expression
        try:
            # First, check if the constraint itself is a SELECT statement
            constraint_parsed = sqlglot.parse_one(self.constraint, read="duckdb")
            if isinstance(constraint_parsed, exp.Select):
                raise ValueError("Constraint must be an expression, not a SELECT statement")
            
            # Try to parse the constraint as an expression
            # Wrap it in a SELECT to validate it's a valid expression
            test_query = f"SELECT {self.constraint} AS test"
            parsed = sqlglot.parse_one(test_query, read="duckdb")
            if not isinstance(parsed, exp.Select):
                raise ValueError("Constraint must be a valid SQL expression")
        except sqlglot.errors.ParseError as e:
            # If parsing fails, check if it's because the constraint is a SELECT statement
            constraint_upper = self.constraint.strip().upper()
            if constraint_upper.startswith("SELECT"):
                raise ValueError("Constraint must be an expression, not a SELECT statement")
            raise ValueError(f"Invalid constraint SQL expression '{self.constraint}': {e}")
        except Exception as e:
            if "Constraint" in str(e) or "must be an expression" in str(e):
                raise
            if "Invalid" not in str(e):
                raise ValueError(f"Invalid constraint SQL expression '{self.constraint}': {e}")
            raise

        # Validate that all columns are qualified (have table names)
        self._validate_column_qualification()

        # Validate aggregations and source column aggregation requirements
        self._validate_aggregation_rules()

        # Validate that the constraint can be used with the specified tables
        # by creating a test query that references both tables
        try:
            if self.source and self.sink:
                # Both tables: constraint can reference columns from both
                test_query = f"SELECT ({self.constraint}) AS policy_check FROM {self.source} s, {self.sink} t"
            elif self.source:
                # Only source: constraint can reference source columns
                test_query = f"SELECT ({self.constraint}) AS policy_check FROM {self.source}"
            else:  # sink only
                # Only sink: constraint can reference sink columns
                test_query = f"SELECT ({self.constraint}) AS policy_check FROM {self.sink}"

            # Parse the test query to validate the constraint works with the tables
            sqlglot.parse_one(test_query, read="duckdb")
        except sqlglot.errors.ParseError as e:
            raise ValueError(
                f"Constraint '{self.constraint}' cannot be evaluated with "
                f"source={self.source}, sink={self.sink}: {e}"
            )

    def _validate_column_qualification(self) -> None:
        """Validate that all columns in the constraint are qualified with table names."""
        # Parse the constraint to find all column references
        constraint_parsed = sqlglot.parse_one(self.constraint, read="duckdb")
        
        # Find all column references in the constraint
        columns = list(constraint_parsed.find_all(exp.Column))
        
        unqualified_columns = []
        for column in columns:
            # Check if the column is qualified with a table name
            # A qualified column has a table attribute
            if not column.table:
                # Get the column name for the error message
                col_name = column.alias_or_name if hasattr(column, "alias_or_name") else (column.name if hasattr(column, "name") else str(column))
                unqualified_columns.append(col_name)
        
        if unqualified_columns:
            raise ValueError(
                f"All columns in constraints must be qualified with table names. "
                f"Unqualified columns found: {', '.join(unqualified_columns)}"
            )

    def _validate_aggregation_rules(self) -> None:
        """Validate aggregation rules: aggregations only reference source, and all source columns are aggregated."""
        # Parse the constraint once for both validations
        constraint_parsed = sqlglot.parse_one(self.constraint, read="duckdb")
        
        # Find all aggregate functions and all columns
        aggregate_funcs = list(constraint_parsed.find_all(exp.AggFunc))
        all_columns = list(constraint_parsed.find_all(exp.Column))
        
        # Validation 1: If there are aggregations, they must only reference the source table
        if aggregate_funcs:
            # If there are aggregations but no source table, that's an error
            if not self.source:
                raise ValueError(
                    "Aggregations in constraints can only reference the source table, "
                    "but no source table is provided"
                )
            
            # Check each aggregate function
            for agg_func in aggregate_funcs:
                # Find all column references within this aggregate function
                columns = list(agg_func.find_all(exp.Column))
                
                for column in columns:
                    # Since we've already validated all columns are qualified, we can safely
                    # get the table name
                    if not column.table:
                        # This shouldn't happen due to _validate_column_qualification,
                        # but check just in case
                        continue
                    
                    table_name = column.table.lower()
                    
                    # Check if it references the sink table (not allowed for aggregations)
                    if self.sink and table_name == self.sink.lower():
                        raise ValueError(
                            f"Aggregation '{agg_func.sql()}' references sink table '{self.sink}', "
                            "but aggregations can only reference the source table"
                        )
                    # Check if it references a table that's not the source
                    if table_name != self.source.lower():
                        raise ValueError(
                            f"Aggregation '{agg_func.sql()}' references table '{table_name}', "
                            f"but aggregations can only reference the source table '{self.source}'"
                        )
        
        # Validation 2: If there's a source table, all source columns must be aggregated
        if self.source:
            # Find all columns that reference the source table
            source_columns = []
            for column in all_columns:
                if not column.table:
                    # This shouldn't happen due to _validate_column_qualification
                    continue
                
                if column.table.lower() == self.source.lower():
                    source_columns.append(column)
            
            if source_columns:
                # Check if each source column is within an aggregate function
                unaggregated_source_columns = []
                for column in source_columns:
                    # Check if this column is within an aggregate function
                    # Use find_ancestor to check if the column is inside an AggFunc
                    parent_agg = column.find_ancestor(exp.AggFunc)
                    if not parent_agg:
                        # Column is not inside any aggregate function
                        col_name = column.alias_or_name if hasattr(column, "alias_or_name") else (column.name if hasattr(column, "name") else str(column))
                        unaggregated_source_columns.append(f"{self.source}.{col_name}")
                
                if unaggregated_source_columns:
                    raise ValueError(
                        f"All columns from source table '{self.source}' must be aggregated. "
                        f"Unaggregated source columns found: {', '.join(unaggregated_source_columns)}"
                    )

    def __repr__(self) -> str:
        """Return a string representation of the policy."""
        parts = []
        if self.source:
            parts.append(f"source={self.source!r}")
        if self.sink:
            parts.append(f"sink={self.sink!r}")
        parts.append(f"constraint={self.constraint!r}")
        parts.append(f"on_fail={self.on_fail.value}")
        return f"DFCPolicy({', '.join(parts)})"

    def __eq__(self, other: object) -> bool:
        """Check if two policies are equal."""
        if not isinstance(other, DFCPolicy):
            return False
        return (
            self.source == other.source
            and self.sink == other.sink
            and self.constraint == other.constraint
            and self.on_fail == other.on_fail
        )

