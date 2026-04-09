"""Data Flow Control Policy definitions."""

from enum import Enum
import re
from typing import Optional

import sqlglot
from sqlglot import exp

from .sqlglot_utils import get_column_name, get_table_name_from_column


class Resolution(Enum):
    """Action to take when a policy fails."""

    REMOVE = "REMOVE"
    KILL = "KILL"
    INVALIDATE = "INVALIDATE"
    INVALIDATE_MESSAGE = "INVALIDATE_MESSAGE"
    LLM = "LLM"


class DFCPolicy:
    """Data Flow Control Policy.

    A policy defines constraints on data flow between source and sink tables.
    Either source or sink (or both) must be specified.

    Disaggregation
    State changes while processing.
    Agent that runs a query, reads the data, uses the data to do another step.
    """

    def __init__(
        self,
        constraint: str,
        on_fail: Resolution,
        sources: list[str],
        sink: Optional[str] = None,
        sink_alias: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        """Initialize a DFC policy.

        Args:
            constraint: A SQL expression that must evaluate to true for the policy to pass.
            on_fail: Action to take when the policy fails
                (REMOVE, KILL, INVALIDATE, INVALIDATE_MESSAGE, or LLM).
            sources: List of source table names (use an empty list for no sources).
            sink: Optional sink table name.
            sink_alias: Optional alias name that may be used to reference the sink table
                within the constraint.
            description: Optional description of the policy.

        Raises:
            ValueError: If neither source nor sink is provided, or if validation fails.
        """
        if sources is None:
            raise ValueError("Sources must be provided (use an empty list for no sources)")
        if not sources and sink is None:
            raise ValueError("Either sources or sink must be provided")
        if sink_alias is not None and sink is None:
            raise ValueError("sink_alias requires sink to be provided")
        if not isinstance(sources, list):
            raise ValueError("Sources must be provided as a list of table names")
        if any(source is None for source in sources):
            raise ValueError("Sources cannot contain None values")

        seen_sources = set()
        normalized_sources = []
        for source in sources:
            if not isinstance(source, str) or not source.strip():
                raise ValueError("Sources must be non-empty strings")
            source_stripped = source.strip()
            source_lower = source_stripped.lower()
            if source_lower in seen_sources:
                raise ValueError(f"Duplicate source table '{source_stripped}' in sources list")
            seen_sources.add(source_lower)
            normalized_sources.append(source_stripped)

        self.sources = normalized_sources
        self.sink = sink
        self.sink_alias = sink_alias.strip() if isinstance(sink_alias, str) else sink_alias
        self.constraint = constraint
        self.on_fail = on_fail
        self.description = description
        self._sources_lower = {source.lower() for source in self.sources}
        self._sink_reference_names = set()
        sink_overlaps_source = self.sink and self.sink.lower() in self._sources_lower
        if self.sink and not (sink_overlaps_source and self.sink_alias):
            self._sink_reference_names.add(self.sink.lower())
        if self.sink_alias:
            if not self.sink_alias:
                raise ValueError("sink_alias must be a non-empty string")
            self._sink_reference_names.add(self.sink_alias.lower())

        self._constraint_parsed = self._parse_constraint()
        self._validate()
        self._source_columns_needed = self._calculate_source_columns_needed()

    @classmethod
    def from_policy_str(cls, policy_str: str) -> "DFCPolicy":
        """Create a DFCPolicy from a policy string.

        Parses a policy string in the format:
        SOURCES <source1, source2> SINK <sink> CONSTRAINT <constraint> ON FAIL <on_fail> [DESCRIPTION <description>]

        Fields can be separated by any whitespace (spaces, tabs, newlines).
        The constraint value can contain spaces.
        DESCRIPTION is optional and can appear anywhere in the string.

        Args:
            policy_str: The policy string to parse

        Returns:
            DFCPolicy: A new DFCPolicy instance

        Raises:
            ValueError: If the policy string cannot be parsed or is invalid
        """
        if not policy_str or not policy_str.strip():
            raise ValueError("Policy text is empty")

        # Normalize whitespace: replace all whitespace sequences with single spaces
        normalized = re.sub(r"\s+", " ", policy_str.strip())

        sources: list[str] = []
        sink = None
        constraint = None
        on_fail = None
        description = None

        # Find positions of all keywords (case-insensitive)
        # Handle "ON FAIL" as a special case since it's two words
        keyword_positions = []

        # Find single-word keywords
        for keyword in ["SOURCES", "SINK", "CONSTRAINT", "DESCRIPTION"]:
            pattern = r"\b" + re.escape(keyword) + r"\b"
            for match in re.finditer(pattern, normalized, re.IGNORECASE):
                keyword_positions.append((match.start(), keyword.upper()))

        # Find "ON FAIL" (two words)
        for match in re.finditer(r"\bON\s+FAIL\b", normalized, re.IGNORECASE):
            keyword_positions.append((match.start(), "ON FAIL"))

        # Sort by position
        keyword_positions.sort()

        # Extract values between keywords
        for i, (pos, keyword) in enumerate(keyword_positions):
            # Find the start of the value (after the keyword and whitespace)
            value_start = pos + 7 if keyword == "ON FAIL" else pos + len(keyword)
            # Skip whitespace after keyword
            while value_start < len(normalized) and normalized[value_start] == " ":
                value_start += 1

            # Find the end of the value (start of next keyword or end of string)
            if i + 1 < len(keyword_positions):
                value_end = keyword_positions[i + 1][0]
                # Back up to remove trailing whitespace
                while value_end > value_start and normalized[value_end - 1] == " ":
                    value_end -= 1
            else:
                value_end = len(normalized)

            value = normalized[value_start:value_end].strip()

            if keyword == "SOURCES":
                if not value or value.upper() == "NONE":
                    sources = []
                else:
                    sources = [item.strip() for item in value.split(",") if item.strip()]
            elif keyword == "SINK":
                sink = value if value and value.upper() != "NONE" else None
            elif keyword == "CONSTRAINT":
                constraint = value
            elif keyword == "ON FAIL":
                try:
                    on_fail = Resolution(value.upper())
                except ValueError as e:
                    raise ValueError(
                        f"Invalid ON FAIL value '{value}'. Must be 'REMOVE', 'KILL', "
                        f"'INVALIDATE', 'INVALIDATE_MESSAGE', or 'LLM'"
                    ) from e
            elif keyword == "DESCRIPTION":
                description = value if value else None

        # Validate required fields
        if constraint is None:
            raise ValueError("CONSTRAINT is required but not found in policy text")

        if on_fail is None:
            raise ValueError("ON FAIL is required but not found in policy text")

        if not sources and sink is None:
            raise ValueError("Either SOURCES or SINK must be provided")

        # Create and return the policy
        return cls(
            constraint=constraint,
            on_fail=on_fail,
            sources=sources,
            sink=sink,
            description=description
        )

    def _validate(self) -> None:
        """Validate that source, sink, and constraint are valid SQL syntax.

        This performs syntax validation only. Database binding validation (checking that
        tables and columns actually exist) should be performed when the policy is
        registered with a SQLRewriter instance.
        """
        for source in self.sources:
            self._validate_table_name(source, "Source")
        if self.sink:
            self._validate_table_name(self.sink, "Sink")
        if self.sink_alias:
            self._validate_identifier_name(self.sink_alias, "Sink alias")

        if isinstance(self._constraint_parsed, exp.Select):
            raise ValueError("Constraint must be an expression, not a SELECT statement")

        try:
            if self.sources and self.sink:
                sources_from = ", ".join(self.sources)
                sink_ref = self.sink
                if self.sink_alias:
                    sink_ref = f"{self.sink} AS {self.sink_alias}"
                test_query = f"SELECT ({self.constraint}) AS policy_check FROM {sources_from}, {sink_ref}"
            elif self.sources:
                sources_from = ", ".join(self.sources)
                test_query = f"SELECT ({self.constraint}) AS policy_check FROM {sources_from}"
            else:
                sink_ref = self.sink
                if self.sink_alias:
                    sink_ref = f"{self.sink} AS {self.sink_alias}"
                test_query = f"SELECT ({self.constraint}) AS policy_check FROM {sink_ref}"

            sqlglot.parse_one(test_query, read="duckdb")
        except sqlglot.errors.ParseError as e:
            raise ValueError(
                f"Constraint '{self.constraint}' cannot be evaluated with "
                f"sources={self.sources}, sink={self.sink}: {e}"
            ) from e

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
            raise ValueError(f"Invalid {table_type.lower()} table name '{table_name}': {e}") from e
        except Exception as e:
            if "Invalid" not in str(e):
                raise ValueError(f"Invalid {table_type.lower()} table '{table_name}': {e}") from e
            raise

    def _validate_identifier_name(self, identifier: str, identifier_type: str) -> None:
        """Validate that an identifier is syntactically valid."""
        try:
            sqlglot.parse_one(
                f"SELECT 1 FROM dummy_table AS {identifier}",
                read="duckdb",
            )
        except sqlglot.errors.ParseError as e:
            raise ValueError(f"Invalid {identifier_type.lower()} '{identifier}': {e}") from e

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
                if parsed.expressions and hasattr(parsed.expressions[0], "this"):
                    return parsed.expressions[0].this
                return constraint_parsed
            except sqlglot.errors.ParseError:
                return constraint_parsed
        except sqlglot.errors.ParseError as e:
            constraint_upper = self.constraint.strip().upper()
            if constraint_upper.startswith("SELECT"):
                raise ValueError("Constraint must be an expression, not a SELECT statement") from e
            raise ValueError(f"Invalid constraint SQL expression '{self.constraint}': {e}") from e
        except Exception as e:
            if "Constraint" in str(e) or "must be an expression" in str(e):
                raise
            if "Invalid" not in str(e):
                raise ValueError(f"Invalid constraint SQL expression '{self.constraint}': {e}") from e
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

    def _calculate_source_columns_needed(self) -> dict[str, set[str]]:
        """Calculate the set of source columns needed after transforming aggregations to columns.

        For scan queries, aggregations in constraints are transformed to their underlying columns.
        This method extracts which columns from the source table will be needed after that
        transformation. For example, max(foo.id) > 1 becomes id > 1, so 'id' is needed.

        Returns:
            Mapping of source table names (lowercase) to needed column names (lowercase).
        """
        if not self.sources:
            return {}

        needed_columns: dict[str, set[str]] = {source.lower(): set() for source in self.sources}

        # Extract columns from aggregations (these will become the columns after transformation)
        for agg_func in self._constraint_parsed.find_all(exp.AggFunc):
            columns = list(agg_func.find_all(exp.Column))
            for column in columns:
                table_name = get_table_name_from_column(column)
                if table_name in self._sources_lower:
                    col_name = get_column_name(column).lower()
                    needed_columns[table_name].add(col_name)

        # Also extract any non-aggregated source columns
        for column in self._constraint_parsed.find_all(exp.Column):
            # Skip columns that are inside aggregations (already handled above)
            if column.find_ancestor(exp.AggFunc) is not None:
                continue

            table_name = get_table_name_from_column(column)
            if table_name in self._sources_lower:
                col_name = get_column_name(column).lower()
                needed_columns[table_name].add(col_name)

        return needed_columns

    def _validate_aggregation_rules(self) -> None:
        """Validate aggregation rules: aggregations only reference source, and all source columns are aggregated."""
        aggregate_funcs = list(self._constraint_parsed.find_all(exp.AggFunc))
        all_columns = list(self._constraint_parsed.find_all(exp.Column))

        if aggregate_funcs:
            if not self.sources:
                raise ValueError(
                    "Aggregations in constraints can only reference the source tables, "
                    "but no sources are provided"
                )

            for agg_func in aggregate_funcs:
                columns = list(agg_func.find_all(exp.Column))

                for column in columns:
                    table_name = get_table_name_from_column(column)
                    if table_name is None:
                        continue

                    if table_name in self._sink_reference_names:
                        raise ValueError(
                            f"Aggregation '{agg_func.sql()}' references sink table '{table_name}', "
                            "but aggregations can only reference source tables"
                        )
                    if table_name not in self._sources_lower:
                        raise ValueError(
                            f"Aggregation '{agg_func.sql()}' references table '{table_name}', "
                            f"but aggregations can only reference source tables {self.sources}"
                        )

        if self.sources:
            unaggregated_source_columns = []
            for column in all_columns:
                table_name = get_table_name_from_column(column)
                if table_name in self._sources_lower and column.find_ancestor(exp.AggFunc) is None:
                    unaggregated_source_columns.append(f"{table_name}.{get_column_name(column)}")

            if unaggregated_source_columns:
                raise ValueError(
                    "All columns from source tables must be aggregated. "
                    f"Unaggregated source columns found: {', '.join(unaggregated_source_columns)}"
                )

    def get_identifier(self) -> str:
        """Get a descriptive identifier for a policy for logging purposes.

        Returns:
            A string identifier for the policy.
        """
        parts = []
        if self.sources:
            parts.append(f"sources={self.sources}")
        if self.sink:
            parts.append(f"sink={self.sink}")
        if self.sink_alias:
            parts.append(f"sink_alias={self.sink_alias}")
        parts.append(f"constraint={self.constraint}")
        return f"DFCPolicy({', '.join(parts)})"

    def __repr__(self) -> str:
        """Return a string representation of the policy."""
        parts = []
        if self.sources:
            parts.append(f"sources={self.sources!r}")
        if self.sink:
            parts.append(f"sink={self.sink!r}")
        if self.sink_alias:
            parts.append(f"sink_alias={self.sink_alias!r}")
        parts.append(f"constraint={self.constraint!r}")
        parts.append(f"on_fail={self.on_fail.value}")
        if self.description:
            parts.append(f"description={self.description!r}")
        return f"DFCPolicy({', '.join(parts)})"

    def __eq__(self, other: object) -> bool:
        """Check if two policies are equal."""
        if not isinstance(other, DFCPolicy):
            return False
        return (
            self.sources == other.sources
            and self.sink == other.sink
            and self.sink_alias == other.sink_alias
            and self.constraint == other.constraint
            and self.on_fail == other.on_fail
            and self.description == other.description
        )


class AggregateDFCPolicy:
    """Aggregate Data Flow Control Policy.

    Similar to DFCPolicy but uses inner/outer aggregation patterns:
    - Source columns must be aggregated (inner aggregate during query, outer aggregate during finalize)
    - Sink columns can be aggregated or unaggregated (aggregated once during finalize)
    - Constraints are evaluated after all data is processed via the finalize() method
    """

    def __init__(
        self,
        constraint: str,
        on_fail: Resolution,
        sources: list[str],
        sink: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        """Initialize an Aggregate DFC policy.

        Args:
            constraint: A SQL expression that must evaluate to true for the policy to pass.
            on_fail: Action to take when the policy fails (currently only INVALIDATE supported).
            sources: List of source table names (use an empty list for no sources).
            sink: Optional sink table name.
            description: Optional description of the policy.

        Raises:
            ValueError: If neither source nor sink is provided, or if validation fails.
        """
        if sources is None:
            raise ValueError("Sources must be provided (use an empty list for no sources)")
        if not sources and sink is None:
            raise ValueError("Either sources or sink must be provided")
        if not isinstance(sources, list):
            raise ValueError("Sources must be provided as a list of table names")
        if any(source is None for source in sources):
            raise ValueError("Sources cannot contain None values")

        # Only INVALIDATE is supported initially
        if on_fail != Resolution.INVALIDATE:
            raise ValueError(
                f"AggregateDFCPolicy currently only supports INVALIDATE resolution, "
                f"but got {on_fail.value}"
            )

        seen_sources = set()
        normalized_sources = []
        for source in sources:
            if not isinstance(source, str) or not source.strip():
                raise ValueError("Sources must be non-empty strings")
            source_stripped = source.strip()
            source_lower = source_stripped.lower()
            if source_lower in seen_sources:
                raise ValueError(f"Duplicate source table '{source_stripped}' in sources list")
            seen_sources.add(source_lower)
            normalized_sources.append(source_stripped)

        self.sources = normalized_sources
        self.sink = sink
        self.constraint = constraint
        self.on_fail = on_fail
        self.description = description
        self._sources_lower = {source.lower() for source in self.sources}

        self._constraint_parsed = self._parse_constraint()
        self._validate()
        self._source_columns_needed = self._calculate_source_columns_needed()

    @classmethod
    def from_policy_str(cls, policy_str: str) -> "AggregateDFCPolicy":
        """Create an AggregateDFCPolicy from a policy string.

        Parses a policy string in the format:
        AGGREGATE SOURCES <source1, source2> SINK <sink> CONSTRAINT <constraint> ON FAIL <on_fail> [DESCRIPTION <description>]

        Fields can be separated by any whitespace (spaces, tabs, newlines).
        The constraint value can contain spaces.
        DESCRIPTION is optional and can appear anywhere in the string.

        Args:
            policy_str: The policy string to parse

        Returns:
            AggregateDFCPolicy: A new AggregateDFCPolicy instance

        Raises:
            ValueError: If the policy string cannot be parsed or is invalid
        """
        if not policy_str or not policy_str.strip():
            raise ValueError("Policy text is empty")

        # Normalize whitespace: replace all whitespace sequences with single spaces
        normalized = re.sub(r"\s+", " ", policy_str.strip())

        # Check for AGGREGATE keyword at the start (case-insensitive)
        if not re.match(r"\bAGGREGATE\b", normalized, re.IGNORECASE):
            raise ValueError(
                "AggregateDFCPolicy requires 'AGGREGATE' keyword at the start of the policy string"
            )

        # Remove AGGREGATE keyword from the start only
        normalized = re.sub(r"^\s*\bAGGREGATE\b\s+", "", normalized, flags=re.IGNORECASE).strip()

        sources: list[str] = []
        sink = None
        constraint = None
        on_fail = None
        description = None

        # Find positions of all keywords (case-insensitive)
        keyword_positions = []

        # Find single-word keywords
        for keyword in ["SOURCES", "SINK", "CONSTRAINT", "DESCRIPTION"]:
            pattern = r"\b" + re.escape(keyword) + r"\b"
            for match in re.finditer(pattern, normalized, re.IGNORECASE):
                keyword_positions.append((match.start(), keyword.upper()))

        # Find "ON FAIL" (two words)
        for match in re.finditer(r"\bON\s+FAIL\b", normalized, re.IGNORECASE):
            keyword_positions.append((match.start(), "ON FAIL"))

        # Sort by position
        keyword_positions.sort()

        # Extract values between keywords
        for i, (pos, keyword) in enumerate(keyword_positions):
            # Find the start of the value (after the keyword and whitespace)
            value_start = pos + 7 if keyword == "ON FAIL" else pos + len(keyword)
            # Skip whitespace after keyword
            while value_start < len(normalized) and normalized[value_start] == " ":
                value_start += 1

            # Find the end of the value (start of next keyword or end of string)
            if i + 1 < len(keyword_positions):
                value_end = keyword_positions[i + 1][0]
                # Back up to remove trailing whitespace
                while value_end > value_start and normalized[value_end - 1] == " ":
                    value_end -= 1
            else:
                value_end = len(normalized)

            value = normalized[value_start:value_end].strip()

            if keyword == "SOURCES":
                if not value or value.upper() == "NONE":
                    sources = []
                else:
                    sources = [item.strip() for item in value.split(",") if item.strip()]
            elif keyword == "SINK":
                sink = value if value and value.upper() != "NONE" else None
            elif keyword == "CONSTRAINT":
                constraint = value
            elif keyword == "ON FAIL":
                try:
                    on_fail = Resolution(value.upper())
                except ValueError as e:
                    raise ValueError(
                        f"Invalid ON FAIL value '{value}'. Must be 'REMOVE', 'KILL', "
                        f"'INVALIDATE', 'INVALIDATE_MESSAGE', or 'LLM'"
                    ) from e
            elif keyword == "DESCRIPTION":
                description = value if value else None

        # Validate required fields
        if constraint is None:
            raise ValueError("CONSTRAINT is required but not found in policy text")

        if on_fail is None:
            raise ValueError("ON FAIL is required but not found in policy text")

        if not sources and sink is None:
            raise ValueError("Either SOURCES or SINK must be provided")

        # Create and return the policy
        return cls(
            constraint=constraint,
            on_fail=on_fail,
            sources=sources,
            sink=sink,
            description=description
        )

    def _validate(self) -> None:
        """Validate that source, sink, and constraint are valid SQL syntax.

        This performs syntax validation only. Database binding validation (checking that
        tables and columns actually exist) should be performed when the policy is
        registered with a SQLRewriter instance.
        """
        for source in self.sources:
            self._validate_table_name(source, "Source")
        if self.sink:
            self._validate_table_name(self.sink, "Sink")

        if isinstance(self._constraint_parsed, exp.Select):
            raise ValueError("Constraint must be an expression, not a SELECT statement")

        try:
            if self.sources and self.sink:
                sources_from = ", ".join(self.sources)
                test_query = f"SELECT ({self.constraint}) AS policy_check FROM {sources_from}, {self.sink}"
            elif self.sources:
                sources_from = ", ".join(self.sources)
                test_query = f"SELECT ({self.constraint}) AS policy_check FROM {sources_from}"
            else:
                test_query = f"SELECT ({self.constraint}) AS policy_check FROM {self.sink}"

            sqlglot.parse_one(test_query, read="duckdb")
        except sqlglot.errors.ParseError as e:
            raise ValueError(
                f"Constraint '{self.constraint}' cannot be evaluated with "
                f"sources={self.sources}, sink={self.sink}: {e}"
            ) from e

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
            raise ValueError(f"Invalid {table_type.lower()} table name '{table_name}': {e}") from e
        except Exception as e:
            if "Invalid" not in str(e):
                raise ValueError(f"Invalid {table_type.lower()} table '{table_name}': {e}") from e
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
                if parsed.expressions and hasattr(parsed.expressions[0], "this"):
                    return parsed.expressions[0].this
                return constraint_parsed
            except sqlglot.errors.ParseError:
                return constraint_parsed
        except sqlglot.errors.ParseError as e:
            constraint_upper = self.constraint.strip().upper()
            if constraint_upper.startswith("SELECT"):
                raise ValueError("Constraint must be an expression, not a SELECT statement") from e
            raise ValueError(f"Invalid constraint SQL expression '{self.constraint}': {e}") from e
        except Exception as e:
            if "Constraint" in str(e) or "must be an expression" in str(e):
                raise
            if "Invalid" not in str(e):
                raise ValueError(f"Invalid constraint SQL expression '{self.constraint}': {e}") from e
            raise

    def _validate_column_qualification(self) -> None:
        """Validate that all columns in the constraint are qualified with table names.

        Exceptions:
        - Columns inside FILTER clauses are allowed to be unqualified as they're
          in a WHERE context and may reference the sink table
        - Columns that are direct arguments to aggregate functions AND match the sink
          table name are allowed (as shorthand for table reference)
        """
        columns = list(self._constraint_parsed.find_all(exp.Column))
        unqualified_columns = []

        for column in columns:
            if not column.table:
                col_name = get_column_name(column).lower()

                # Check if this column is inside a FILTER clause
                # FILTER clauses have their own WHERE context
                if column.find_ancestor(exp.Filter) is not None:
                    # Column is inside a FILTER clause - allow unqualified
                    continue

                # Check if this column is the direct argument of an aggregate function
                # AND it matches the sink table name (as a shorthand)
                parent = column.parent
                if (
                    isinstance(parent, exp.AggFunc)
                    and hasattr(parent, "this")
                    and parent.this == column
                    and self.sink
                    and col_name == self.sink.lower()
                ):
                    # This is a shorthand for the sink table - allow unqualified
                    continue

                # Otherwise, it's an unqualified column that should be flagged
                unqualified_columns.append(col_name)

        if unqualified_columns:
            raise ValueError(
                f"All columns in constraints must be qualified with table names. "
                f"Unaggregated columns found: {', '.join(unqualified_columns)}"
            )

    def _calculate_source_columns_needed(self) -> dict[str, set[str]]:
        """Calculate the set of source columns needed.

        Returns:
            Mapping of source table names (lowercase) to needed column names (lowercase).
        """
        if not self.sources:
            return {}

        needed_columns: dict[str, set[str]] = {source.lower(): set() for source in self.sources}

        # Extract columns from aggregations
        for agg_func in self._constraint_parsed.find_all(exp.AggFunc):
            columns = list(agg_func.find_all(exp.Column))
            for column in columns:
                table_name = get_table_name_from_column(column)
                if table_name in self._sources_lower:
                    col_name = get_column_name(column).lower()
                    needed_columns[table_name].add(col_name)

        # Also extract any non-aggregated source columns
        for column in self._constraint_parsed.find_all(exp.Column):
            # Skip columns that are inside aggregations (already handled above)
            if column.find_ancestor(exp.AggFunc) is not None:
                continue

            table_name = get_table_name_from_column(column)
            if table_name in self._sources_lower:
                col_name = get_column_name(column).lower()
                needed_columns[table_name].add(col_name)

        return needed_columns

    def _validate_aggregation_rules(self) -> None:
        """Validate aggregation rules: source columns must be aggregated, sink columns can be aggregated or not."""
        list(self._constraint_parsed.find_all(exp.AggFunc))
        all_columns = list(self._constraint_parsed.find_all(exp.Column))

        # Source columns must be aggregated
        if self.sources:
            unaggregated_source_columns = []
            for column in all_columns:
                table_name = get_table_name_from_column(column)
                if table_name in self._sources_lower and column.find_ancestor(exp.AggFunc) is None:
                    unaggregated_source_columns.append(f"{table_name}.{get_column_name(column)}")

            if unaggregated_source_columns:
                raise ValueError(
                    "All columns from source tables must be aggregated. "
                    f"Unaggregated source columns found: {', '.join(unaggregated_source_columns)}"
                )

    def get_identifier(self) -> str:
        """Get a descriptive identifier for a policy for logging purposes.

        Returns:
            A string identifier for the policy.
        """
        parts = []
        if self.sources:
            parts.append(f"sources={self.sources}")
        if self.sink:
            parts.append(f"sink={self.sink}")
        parts.append(f"constraint={self.constraint}")
        return f"AggregateDFCPolicy({', '.join(parts)})"

    def __repr__(self) -> str:
        """Return a string representation of the policy."""
        parts = []
        if self.sources:
            parts.append(f"sources={self.sources!r}")
        if self.sink:
            parts.append(f"sink={self.sink!r}")
        parts.append(f"constraint={self.constraint!r}")
        parts.append(f"on_fail={self.on_fail.value}")
        if self.description:
            parts.append(f"description={self.description!r}")
        return f"AggregateDFCPolicy({', '.join(parts)})"

    def __eq__(self, other: object) -> bool:
        """Check if two policies are equal."""
        if not isinstance(other, AggregateDFCPolicy):
            return False
        return (
            self.sources == other.sources
            and self.sink == other.sink
            and self.constraint == other.constraint
            and self.on_fail == other.on_fail
            and self.description == other.description
        )
