from decimal import Decimal
import uuid

import pytest

from vldb_experiments.baselines.provsql_baseline import (
    ProvSQLBaselineError,
    ProvSQLNaiveBaseline,
    normalize_provsql_results,
)


def test_scalar_aggregate_rewrite_uses_row_and_aggregate_tokens() -> None:
    baseline = ProvSQLNaiveBaseline(
        source_table="lineitem",
        source_column="l_quantity",
    )
    query = "SELECT COUNT(*) AS n FROM lineitem"

    assert baseline.rewrite(query) == (
        'WITH "__provsql_dfc_query" AS MATERIALIZED '
        "(SELECT COUNT(*) AS n, PROVENANCE() AS __provsql_dfc_row_token "
        'FROM lineitem) SELECT q."n" FROM "__provsql_dfc_query" AS q '
        'CROSS JOIN LATERAL (SELECT MAX(source."l_quantity") AS policy_value '
        "FROM (SELECT DISTINCT descendants.token FROM "
        '(VALUES (q."__provsql_dfc_row_token"), '
        '(provsql.agg_token_uuid_any(q."n")::UUID)) AS roots(root_token) '
        "CROSS JOIN LATERAL provsql.descendant_tokens(roots.root_token) "
        'AS descendants) AS reachable JOIN "lineitem" AS source '
        "ON source.provsql = reachable.token) AS policy "
        'WHERE present(q."__provsql_dfc_row_token") AND '
        "policy.policy_value >= 1"
    )


def test_grouped_aggregate_rewrite_needs_only_group_row_token() -> None:
    baseline = ProvSQLNaiveBaseline(
        source_table="lineitem",
        source_column="l_quantity",
    )

    assert baseline.rewrite(
        "SELECT l_returnflag, COUNT(*) AS n FROM lineitem GROUP BY l_returnflag"
    ) == (
        'WITH "__provsql_dfc_query" AS MATERIALIZED '
        "(SELECT l_returnflag, COUNT(*) AS n, "
        "PROVENANCE() AS __provsql_dfc_row_token FROM lineitem "
        'GROUP BY l_returnflag) SELECT q."l_returnflag", q."n" '
        'FROM "__provsql_dfc_query" AS q CROSS JOIN LATERAL '
        '(SELECT MAX(source."l_quantity") AS policy_value FROM '
        "(SELECT DISTINCT descendants.token FROM "
        '(VALUES (q."__provsql_dfc_row_token")) AS roots(root_token) '
        "CROSS JOIN LATERAL provsql.descendant_tokens(roots.root_token) "
        'AS descendants) AS reachable JOIN "lineitem" AS source '
        "ON source.provsql = reachable.token) AS policy "
        'WHERE present(q."__provsql_dfc_row_token") AND '
        "policy.policy_value >= 1"
    )


def test_rejects_unsafe_source_identifier() -> None:
    with pytest.raises(ProvSQLBaselineError, match="Unsafe SQL identifier"):
        ProvSQLNaiveBaseline(
            source_table="lineitem; DROP TABLE lineitem",
            source_column="l_quantity",
        )


def test_normalize_strips_annotation_and_decodes_aggregate_tokens() -> None:
    class Column:
        def __init__(self, name: str) -> None:
            self.name = name

    annotation = uuid.uuid4()
    rows = [("A", "12.50 (*)", annotation), ("B", "NULL (*)", annotation)]

    assert normalize_provsql_results(
        [Column("label"), Column("total"), Column("provsql")], rows
    ) == [("A", Decimal("12.50")), ("B", None)]
