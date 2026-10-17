"""Single-statement DFC baseline over ProvSQL provenance circuits.

The original query is evaluated once in a materialized CTE.  The remainder of
the same SQL statement traverses its row and aggregate-value circuits, resolves
source tuples, evaluates the policy, and filters the original output.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re
from typing import Any

import sqlglot
from sqlglot import exp

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SUPPORTED_AGGREGATES = {"MAX", "MIN", "COUNT", "SUM", "AVG"}
_SUPPORTED_COMPARISONS = {"=", "<>", "<", "<=", ">", ">="}
_QUERY_CTE = "__provsql_dfc_query"
_ROW_TOKEN = "__provsql_dfc_row_token"


class ProvSQLBaselineError(ValueError):
    """Raised when the monolithic ProvSQL baseline cannot be generated."""


def _quote_identifier(identifier: str) -> str:
    if not _IDENTIFIER.fullmatch(identifier):
        raise ProvSQLBaselineError(f"Unsafe SQL identifier: {identifier!r}")
    return f'"{identifier}"'


def _decode_agg_token(value: Any) -> Any:
    if not isinstance(value, str) or not value.endswith(" (*)"):
        return value

    scalar = value[: -len(" (*)")]
    if scalar.upper() == "NULL" or not scalar:
        return None
    try:
        return Decimal(scalar)
    except InvalidOperation:
        return scalar


def _column_names(description: Any) -> list[str]:
    return [column.name if hasattr(column, "name") else column[0] for column in description]


def normalize_provsql_results(
    description: Any,
    rows: list[tuple[Any, ...]],
) -> list[tuple[Any, ...]]:
    """Remove ProvSQL's automatic annotation and decode aggregate values."""

    names = _column_names(description)
    visible_indexes = [index for index, name in enumerate(names) if name != "provsql"]
    return [tuple(_decode_agg_token(row[index]) for index in visible_indexes) for row in rows]


def _contains_own_aggregate(expression: exp.Expression) -> bool:
    return any(
        isinstance(node, exp.AggFunc)
        for node in expression.walk(
            prune=lambda candidate: candidate is not expression and isinstance(candidate, exp.Query)
        )
    )


def _projection_names(select: exp.Select) -> list[str]:
    names: list[str] = []
    for index, projection in enumerate(list(select.expressions)):
        name = projection.alias_or_name
        if name == "*":
            raise ProvSQLBaselineError(
                "Monolithic ProvSQL baseline requires explicitly named output columns"
            )
        if not name:
            name = f"__provsql_dfc_output_{index}"
            projection.replace(exp.alias_(projection.copy(), name))
        names.append(name)
    if len({name.lower() for name in names}) != len(names):
        raise ProvSQLBaselineError("Monolithic ProvSQL baseline requires unique output names")
    return names


class ProvSQLNaiveBaseline:
    """Generate and execute one provenance-postprocessing SQL statement."""

    def __init__(
        self,
        *,
        source_table: str,
        source_column: str,
        aggregate: str = "MAX",
        comparison: str = ">=",
        threshold: Decimal | int | float = 1,
    ) -> None:
        aggregate = aggregate.upper()
        if aggregate not in _SUPPORTED_AGGREGATES:
            raise ProvSQLBaselineError(f"Unsupported policy aggregate: {aggregate}")
        if comparison not in _SUPPORTED_COMPARISONS:
            raise ProvSQLBaselineError(f"Unsupported policy comparison: {comparison}")

        self.source_table = _quote_identifier(source_table)
        self.source_column = _quote_identifier(source_column)
        self.aggregate = aggregate
        self.comparison = comparison
        self.threshold = threshold

    def prepare(self, connection: Any) -> None:
        """Install the aggregate-token UUID adapter once for this database.

        PostgreSQL resolves a CTE aggregate column to its ordinary SQL result
        type before ProvSQL's planner hook substitutes ``agg_token``.  The
        polymorphic signature lets the hook pass that carrier to ProvSQL's
        existing UUID extractor without a parse-time type error.
        """

        cursor = connection.cursor()
        cursor.execute(
            "CREATE OR REPLACE FUNCTION provsql.agg_token_uuid_any(anyelement) "
            "RETURNS text AS 'provsql', 'agg_token_cast' "
            "LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE"
        )
        cursor.execute(
            "CREATE OR REPLACE FUNCTION provsql.descendant_tokens(root UUID) "
            "RETURNS TABLE(token UUID) LANGUAGE SQL STABLE PARALLEL SAFE "
            "SET provsql.active=off AS $$ "
            "WITH RECURSIVE walk(token) AS ("
            "SELECT root UNION SELECT child FROM walk "
            "CROSS JOIN LATERAL unnest(provsql.get_children(walk.token)) "
            "AS children(child)) SELECT token FROM walk $$"
        )
        cursor.close()

    def rewrite(self, query: str) -> str:
        """Return one SQL statement that runs Q and enforces the policy."""

        tree = sqlglot.parse_one(query, read="postgres")
        if not isinstance(tree, exp.Select):
            raise ProvSQLBaselineError(
                f"ProvSQL baseline requires a SELECT root, got {type(tree).__name__}"
            )

        output_names = _projection_names(tree)
        is_scalar_aggregate = tree.args.get("group") is None and any(
            _contains_own_aggregate(projection) for projection in tree.expressions
        )
        aggregate_names = (
            [
                projection.alias_or_name
                for projection in tree.expressions
                if _contains_own_aggregate(projection)
            ]
            if is_scalar_aggregate
            else []
        )
        original_order = tree.args.get("order")
        annotated = tree.copy().select(
            exp.alias_(exp.Anonymous(this="provenance"), _ROW_TOKEN),
            append=True,
            copy=False,
        )
        annotated_sql = annotated.sql(dialect="postgres")

        roots = [f'q."{_ROW_TOKEN}"']
        roots.extend(
            f"provsql.agg_token_uuid_any(q.{_quote_identifier(name)})::UUID"
            for name in aggregate_names
        )
        root_values_sql = ", ".join(f"({root})" for root in roots)

        visible = ", ".join(f"q.{_quote_identifier(name)}" for name in output_names)
        threshold_sql = sqlglot.parse_one(str(self.threshold), read="postgres").sql(
            dialect="postgres"
        )
        order_sql = f" {original_order.sql(dialect='postgres')}" if original_order else ""

        return (
            f'WITH "{_QUERY_CTE}" AS MATERIALIZED ({annotated_sql}) '
            f'SELECT {visible} FROM "{_QUERY_CTE}" AS q '
            "CROSS JOIN LATERAL ("
            f"SELECT {self.aggregate}(source.{self.source_column}) AS policy_value "
            "FROM (SELECT DISTINCT descendants.token "
            f"FROM (VALUES {root_values_sql}) AS roots(root_token) "
            "CROSS JOIN LATERAL provsql.descendant_tokens(roots.root_token) "
            "AS descendants) AS reachable "
            f"JOIN {self.source_table} AS source ON source.provsql = reachable.token"
            ") AS policy "
            f'WHERE present(q."{_ROW_TOKEN}") AND '
            f"policy.policy_value {self.comparison} {threshold_sql}"
            f"{order_sql}"
        )

    def execute(
        self,
        connection: Any,
        query: str,
        *,
        rewritten_sql: str | None = None,
    ) -> tuple[list[tuple[Any, ...]], str]:
        """Execute exactly one rewritten statement and return visible results."""

        submitted_sql = rewritten_sql or self.rewrite(query)
        cursor = connection.cursor()
        cursor.execute(submitted_sql)
        rows = cursor.fetchall()
        description = cursor.description
        cursor.close()
        return normalize_provsql_results(description, rows), submitted_sql
