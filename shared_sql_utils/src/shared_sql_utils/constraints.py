"""Constraint helpers for SQL expressions."""

import sqlglot
from sqlglot import exp


def combine_constraints_balanced_expr(
    constraints: list[str],
    dialect: str = "duckdb",
) -> exp.Expression:
    """Combine constraints into a balanced AND expression object.

    Args:
        constraints: List of SQL constraint expressions.
        dialect: SQL dialect for parsing/formatting.

    Returns:
        Combined SQL expression.
    """
    parsed_constraints = [sqlglot.parse_one(constraint, read=dialect) for constraint in constraints]
    wrapped = [exp.Paren(this=expr) for expr in parsed_constraints]

    if not wrapped:
        return exp.Literal(this="true", is_string=False)
    if len(wrapped) == 1:
        return wrapped[0]

    nodes = wrapped
    while len(nodes) > 1:
        next_nodes = []
        it = iter(nodes)
        for left in it:
            right = next(it, None)
            if right is None:
                next_nodes.append(left)
            else:
                next_nodes.append(exp.And(this=left, expression=right))
        nodes = next_nodes

    return nodes[0]


def combine_constraints_balanced(constraints: list[str], dialect: str = "duckdb") -> str:
    """Combine constraints into a balanced AND expression string.

    Args:
        constraints: List of SQL constraint expressions.
        dialect: SQL dialect for parsing/formatting.

    Returns:
        Combined SQL expression string.
    """
    if not constraints:
        return "TRUE"
    return combine_constraints_balanced_expr(constraints, dialect=dialect).sql(dialect=dialect)
