"""Utility functions for working with sqlglot expressions."""

from typing import Optional

from sqlglot import exp


def get_column_name(column: exp.Column) -> str:
    """Extract column name from a column expression.
    
    Args:
        column: The column expression.
        
    Returns:
        The column name as a string.
    """
    if hasattr(column, "alias_or_name"):
        return column.alias_or_name
    if hasattr(column, "name"):
        return column.name
    return str(column)


def get_table_name_from_column(column: exp.Column) -> Optional[str]:
    """Extract table name from a column expression, handling different types.
    
    Args:
        column: The column expression to extract the table name from.
        
    Returns:
        The table name as a lowercase string, or None if not qualified.
    """
    if not column.table:
        return None
    if isinstance(column.table, exp.Identifier):
        return column.table.name.lower()
    if isinstance(column.table, str):
        return column.table.lower()
    # Fallback for any other type - convert to string and lowercase
    # This ensures we don't silently skip validation for unexpected types
    return str(column.table).lower()

