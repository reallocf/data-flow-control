"""Data Flow Control Policy definitions."""

import duckdb
import sqlglot
from sqlglot import exp
from enum import Enum
from typing import Optional, Set

from .sqlglot_utils import get_column_name, get_table_name_from_column


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

        self._constraint_parsed = self._parse_constraint()
        self._validate()
        self._source_columns_needed = self._calculate_source_columns_needed()

    def _validate(self) -> None:
        """Validate that source, sink, and constraint are valid SQL syntax.
        
        This performs syntax validation only. Database binding validation (checking that
        tables and columns actually exist) should be performed when the policy is
        registered with a SQLRewriter instance.
        """
        if self.source:
            self._validate_table_name(self.source, "Source")
        if self.sink:
            self._validate_table_name(self.sink, "Sink")

        if isinstance(self._constraint_parsed, exp.Select):
            raise ValueError("Constraint must be an expression, not a SELECT statement")
        
        try:
            if self.source and self.sink:
                test_query = f"SELECT ({self.constraint}) AS policy_check FROM {self.source} s, {self.sink} t"
            elif self.source:
                test_query = f"SELECT ({self.constraint}) AS policy_check FROM {self.source}"
            else:
                test_query = f"SELECT ({self.constraint}) AS policy_check FROM {self.sink}"

            sqlglot.parse_one(test_query, read="duckdb")
        except sqlglot.errors.ParseError as e:
            raise ValueError(
                f"Constraint '{self.constraint}' cannot be evaluated with "
                f"source={self.source}, sink={self.sink}: {e}"
            )

        self._validate_column_qualification()
        self._validate_aggregation_rules()

    def _validate_table_name(self, table_name: str, table_type: str) -> None:
        """Validate that a table name is a valid SQL identifier.
        
        Args:
            table_name: The table name to validate.
            table_type: The type of table ("Source" or "Sink") for error messages.
            
        Raises:
            ValueError: If the table name is invalid.
        """
        try:
            test_query = f"SELECT * FROM {table_name}"
            parsed = sqlglot.parse_one(test_query, read="duckdb")
            if not isinstance(parsed, sqlglot.exp.Select):
                raise ValueError(f"{table_type} '{table_name}' is not a valid table identifier")
            tables = list(parsed.find_all(sqlglot.exp.Table))
            if not tables:
                raise ValueError(f"{table_type} '{table_name}' does not reference a valid table")
        except sqlglot.errors.ParseError as e:
            raise ValueError(f"Invalid {table_type.lower()} table name '{table_name}': {e}")
        except Exception as e:
            if "Invalid" not in str(e):
                raise ValueError(f"Invalid {table_type.lower()} table '{table_name}': {e}")
            raise


    def _parse_constraint(self) -> exp.Expression:
        """Parse the constraint SQL expression.
        
        Returns:
            The parsed constraint expression.
            
        Raises:
            ValueError: If the constraint is invalid or is a SELECT statement.
        """
        try:
            constraint_parsed = sqlglot.parse_one(self.constraint, read="duckdb")
            if isinstance(constraint_parsed, exp.Select):
                raise ValueError("Constraint must be an expression, not a SELECT statement")
            
            try:
                test_query = f"SELECT {self.constraint} AS test"
                parsed = sqlglot.parse_one(test_query, read="duckdb")
                if not isinstance(parsed, exp.Select):
                    raise ValueError("Constraint must be a valid SQL expression")
                
                # The first expression is an Alias, and we want the 'this' attribute
                if parsed.expressions and hasattr(parsed.expressions[0], 'this'):
                    return parsed.expressions[0].this
                else:
                    return constraint_parsed
            except sqlglot.errors.ParseError:
                return constraint_parsed
        except sqlglot.errors.ParseError as e:
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


    def _validate_column_qualification(self) -> None:
        """Validate that all columns in the constraint are qualified with table names."""
        columns = list(self._constraint_parsed.find_all(exp.Column))
        unqualified_columns = [
            get_column_name(column)
            for column in columns
            if not column.table
        ]
        
        if unqualified_columns:
            raise ValueError(
                f"All columns in constraints must be qualified with table names. "
                f"Unqualified columns found: {', '.join(unqualified_columns)}"
            )

    def _calculate_source_columns_needed(self) -> Set[str]:
        """Calculate the set of source columns needed after transforming aggregations to columns.
        
        For scan queries, aggregations in constraints are transformed to their underlying columns.
        This method extracts which columns from the source table will be needed after that
        transformation. For example, max(foo.id) > 1 becomes id > 1, so 'id' is needed.
        
        Returns:
            Set of column names (lowercase) needed from the source table.
        """
        if not self.source:
            return set()
        
        needed_columns = set()
        
        # Extract columns from aggregations (these will become the columns after transformation)
        for agg_func in self._constraint_parsed.find_all(exp.AggFunc):
            columns = list(agg_func.find_all(exp.Column))
            for column in columns:
                table_name = get_table_name_from_column(column)
                if table_name == self.source.lower():
                    col_name = get_column_name(column).lower()
                    needed_columns.add(col_name)
        
        # Also extract any non-aggregated source columns
        for column in self._constraint_parsed.find_all(exp.Column):
            # Skip columns that are inside aggregations (already handled above)
            if column.find_ancestor(exp.AggFunc) is not None:
                continue
            
            table_name = get_table_name_from_column(column)
            if table_name == self.source.lower():
                col_name = get_column_name(column).lower()
                needed_columns.add(col_name)
        
        return needed_columns

    def _validate_aggregation_rules(self) -> None:
        """Validate aggregation rules: aggregations only reference source, and all source columns are aggregated."""
        aggregate_funcs = list(self._constraint_parsed.find_all(exp.AggFunc))
        all_columns = list(self._constraint_parsed.find_all(exp.Column))
        
        if aggregate_funcs:
            if not self.source:
                raise ValueError(
                    "Aggregations in constraints can only reference the source table, "
                    "but no source table is provided"
                )
            
            for agg_func in aggregate_funcs:
                columns = list(agg_func.find_all(exp.Column))
                
                for column in columns:
                    table_name = get_table_name_from_column(column)
                    if table_name is None:
                        continue
                    
                    if self.sink and table_name == self.sink.lower():
                        raise ValueError(
                            f"Aggregation '{agg_func.sql()}' references sink table '{self.sink}', "
                            "but aggregations can only reference the source table"
                        )
                    if table_name != self.source.lower():
                        raise ValueError(
                            f"Aggregation '{agg_func.sql()}' references table '{table_name}', "
                            f"but aggregations can only reference the source table '{self.source}'"
                        )
        
        if self.source:
            source_columns = [
                column
                for column in all_columns
                if get_table_name_from_column(column) == self.source.lower()
            ]
            
            if source_columns:
                unaggregated_source_columns = [
                    f"{self.source}.{get_column_name(column)}"
                    for column in source_columns
                    if column.find_ancestor(exp.AggFunc) is None
                ]
                
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

