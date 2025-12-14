"""SQL rewriter that intercepts queries, transforms them, and executes against DuckDB."""

import duckdb
import sqlglot
from sqlglot import exp
from typing import Any, Optional


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

    def transform_query(self, query: str) -> str:
        """Transform a SQL query according to the rewriter's rules.

        Currently adds column "bar" to SELECT statements that query table "foo".

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
                # Check if table "foo" is in the FROM clause
                from_tables = []
                for table in parsed.find_all(exp.Table):
                    # Only consider tables in FROM/JOIN clauses, not in column references
                    if table.find_ancestor(exp.From) or table.find_ancestor(exp.Join):
                        from_tables.append(table.name.lower())

                if "foo" in from_tables:
                    # Skip transformation for aggregate queries (e.g., COUNT(*), SUM(*))
                    has_aggregate = any(
                        isinstance(expr, exp.AggFunc) or 
                        (isinstance(expr, exp.Column) and expr.find_ancestor(exp.AggFunc))
                        for expr in parsed.expressions
                    )
                    
                    if has_aggregate:
                        # Don't modify aggregate queries
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

    def execute(self, query: str) -> Any:
        """Execute a SQL query after transforming it.

        Args:
            query: The SQL query string to execute.

        Returns:
            The result of executing the query.
        """
        transformed_query = self.transform_query(query)
        return self.conn.execute(transformed_query)

    def fetchall(self, query: str) -> list[tuple]:
        """Execute a query and fetch all results.

        Args:
            query: The SQL query string to execute.

        Returns:
            List of tuples containing the query results.
        """
        transformed_query = self.transform_query(query)
        return self.conn.execute(transformed_query).fetchall()

    def fetchone(self, query: str) -> Optional[tuple]:
        """Execute a query and fetch one result.

        Args:
            query: The SQL query string to execute.

        Returns:
            A single tuple containing one row of results, or None if no results.
        """
        transformed_query = self.transform_query(query)
        return self.conn.execute(transformed_query).fetchone()

    def close(self) -> None:
        """Close the DuckDB connection."""
        self.conn.close()

    def __enter__(self) -> "SQLRewriter":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

