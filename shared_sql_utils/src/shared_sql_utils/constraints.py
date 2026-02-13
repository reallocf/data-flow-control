"""Constraint helpers for SQL expressions."""

import sqlglot
from sqlglot import exp


def combine_expressions_balanced_expr(
    expressions: list[str],
    operator: type[exp.Expression],
    dialect: str = "duckdb",
    empty_fallback: exp.Expression | None = None,
) -> exp.Expression:
    """Combine expressions into a balanced tree using the provided operator.

    Args:
        expressions: List of SQL expressions.
        operator: sqlglot expression class (e.g., exp.And, exp.Or, exp.Add).
        dialect: SQL dialect for parsing/formatting.
        empty_fallback: Expression to use when expressions is empty.

    Returns:
        Combined SQL expression.
    """
    parsed = [sqlglot.parse_one(expr, read=dialect) for expr in expressions]
    wrapped = [exp.Paren(this=expr) for expr in parsed]

    if not wrapped:
        if empty_fallback is not None:
            return empty_fallback
        raise ValueError("expressions must contain at least one element")
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
                combined = operator(this=left, expression=right)
                next_nodes.append(exp.Paren(this=combined))
        nodes = next_nodes

    return nodes[0]


def combine_constraints_balanced_expr(
    constraints: list[str],
    dialect: str = "duckdb",
) -> exp.Expression:
    """Combine constraints into a balanced AND expression object."""
    return combine_expressions_balanced_expr(
        constraints,
        exp.And,
        dialect=dialect,
        empty_fallback=exp.Literal(this="true", is_string=False),
    )


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


def combine_expressions_balanced(
    expressions: list[str],
    operator: type[exp.Expression],
    dialect: str = "duckdb",
    empty_fallback: str | None = None,
) -> str:
    """Combine expressions into a balanced tree and return SQL."""
    expr = combine_expressions_balanced_expr(
        expressions,
        operator,
        dialect=dialect,
        empty_fallback=sqlglot.parse_one(empty_fallback, read=dialect) if empty_fallback else None,
    )
    return expr.sql(dialect=dialect)
