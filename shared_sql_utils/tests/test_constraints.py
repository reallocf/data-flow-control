"""Tests for constraint helpers."""

import math

import sqlglot
from sqlglot import exp

from shared_sql_utils import (
    combine_constraints_balanced,
    combine_constraints_balanced_expr,
    combine_expressions_balanced,
    combine_expressions_balanced_expr,
)


def _and_depth(expr: exp.Expression) -> int:
    if isinstance(expr, exp.And):
        return 1 + max(_and_depth(expr.this), _and_depth(expr.expression))
    if isinstance(expr, exp.Paren):
        return _and_depth(expr.this)
    return 0


def _or_depth(expr: exp.Expression) -> int:
    if isinstance(expr, exp.Or):
        return 1 + max(_or_depth(expr.this), _or_depth(expr.expression))
    if isinstance(expr, exp.Paren):
        return _or_depth(expr.this)
    return 0


def _add_depth(expr: exp.Expression) -> int:
    if isinstance(expr, exp.Add):
        return 1 + max(_add_depth(expr.this), _add_depth(expr.expression))
    if isinstance(expr, exp.Paren):
        return _add_depth(expr.this)
    return 0


def test_combine_constraints_empty_returns_true():
    assert combine_constraints_balanced([]) == "TRUE"
    expr = combine_constraints_balanced_expr([])
    assert isinstance(expr, exp.Literal)
    assert expr.sql(dialect="duckdb").upper() == "TRUE"


def test_combine_constraints_single_preserves_constraint():
    constraint = "max(lineitem.l_quantity) >= 1"
    combined = combine_constraints_balanced([constraint])
    parsed = sqlglot.parse_one(combined, read="duckdb")
    assert isinstance(parsed, exp.Paren)
    assert parsed.this.sql(dialect="duckdb") == sqlglot.parse_one(constraint, read="duckdb").sql(
        dialect="duckdb"
    )


def test_combine_constraints_multiple_contains_all_predicates():
    constraints = ["a = 1", "b = 2", "c = 3", "d = 4"]
    combined = combine_constraints_balanced(constraints)
    for constraint in constraints:
        needle = sqlglot.parse_one(constraint, read="duckdb").sql(dialect="duckdb")
        assert needle in combined
    expr = combine_constraints_balanced_expr(constraints)
    assert _and_depth(expr) <= 3


def test_combine_constraints_odd_count_balances():
    constraints = [f"col_{i} = {i}" for i in range(7)]
    expr = combine_constraints_balanced_expr(constraints)
    max_depth = math.ceil(math.log2(len(constraints))) + 1
    assert _and_depth(expr) <= max_depth


def test_combine_constraints_dialect_round_trip():
    constraints = ["x >= 10", "y < 5", "z IS NOT NULL"]
    combined = combine_constraints_balanced(constraints, dialect="duckdb")
    parsed = sqlglot.parse_one(combined, read="duckdb")
    assert parsed.sql(dialect="duckdb") == combined


def test_combine_expressions_balanced_or_contains_all_predicates():
    expressions = ["a = 1", "b = 2", "c = 3", "d = 4", "e = 5"]
    combined = combine_expressions_balanced(expressions, exp.Or, dialect="duckdb")
    for expr in expressions:
        needle = sqlglot.parse_one(expr, read="duckdb").sql(dialect="duckdb")
        assert needle in combined
    parsed = combine_expressions_balanced_expr(expressions, exp.Or, dialect="duckdb")
    assert _or_depth(parsed) <= 3


def test_combine_expressions_balanced_add_contains_all_terms():
    terms = ["col1", "col2", "col3", "col4", "col5", "col6"]
    combined = combine_expressions_balanced(terms, exp.Add, dialect="duckdb")
    for term in terms:
        needle = sqlglot.parse_one(term, read="duckdb").sql(dialect="duckdb")
        assert needle in combined
    parsed = combine_expressions_balanced_expr(terms, exp.Add, dialect="duckdb")
    assert _add_depth(parsed) <= 3


def test_combine_expressions_balanced_empty_fallback():
    combined = combine_expressions_balanced([], exp.Or, dialect="duckdb", empty_fallback="FALSE")
    assert combined.upper() == "FALSE"
