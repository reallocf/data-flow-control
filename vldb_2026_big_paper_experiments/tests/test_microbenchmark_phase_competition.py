"""Sanity tests for phase-competition rewrite SQL text and result equivalence."""

from __future__ import annotations

from sql_rewriter import DFCPolicy, Resolution, SQLRewriter
import sqlglot

from vldb_experiments.correctness import compare_results_exact
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck

BASE_AGGREGATE_COLUMNS = 256
POLICY_START_COLUMN = 256
MAX_POLICY_COLUMN_COUNT_FOR_TEST = 8
TEST_JOIN_FANOUT = 4


def _normalize_sql(sql: str) -> str:
    return sqlglot.parse_one(sql, read="duckdb").sql(dialect="duckdb")


def _setup_wide_data_1000_rows() -> tuple[object, SQLRewriter]:
    max_col = POLICY_START_COLUMN + MAX_POLICY_COLUMN_COUNT_FOR_TEST - 1
    value_cols = ",\n            ".join(
        [
            f"CAST(((i + {idx}) % 1000) + 1 AS DOUBLE) AS c{idx}"
            for idx in range(1, max_col + 1)
        ]
    )
    conn = _ensure_smokedduck().connect(":memory:")
    conn.execute("SET max_expression_depth TO 100000")
    conn.execute(
        f"""
        CREATE TABLE wide_data AS
        SELECT
            i AS pk,
            {value_cols}
        FROM range(1, 1001) t(i)
        """
    )
    conn.execute(
        f"""
        CREATE TABLE join_data AS
        SELECT
            k AS fk,
            r AS replica,
            CAST((((k * 13) + r) % 1000) + 1 AS DOUBLE) AS jv
        FROM range(1, 1001) keys(k)
        CROSS JOIN range(1, {TEST_JOIN_FANOUT + 1}) reps(r)
        """
    )
    return conn, SQLRewriter(conn=conn)


def _build_query_and_policy(policy_column_count: int) -> tuple[str, DFCPolicy]:
    base_expr = " + ".join(
        f"wide_data.c{i}" for i in range(1, BASE_AGGREGATE_COLUMNS + 1)
    )
    policy_start = POLICY_START_COLUMN
    policy_end = policy_start + policy_column_count - 1
    policy_expr = " + ".join(
        f"wide_data.c{i}" for i in range(policy_start, policy_end + 1)
    )

    query = (
        f"SELECT SUM({base_expr}) AS base_sum "
        "FROM wide_data "
        "JOIN join_data ON wide_data.pk = join_data.fk"
    )
    policy = DFCPolicy(
        sources=["wide_data"],
        constraint=f"sum({policy_expr}) >= 0",
        on_fail=Resolution.REMOVE,
        description=f"phase_competition_policy_cols_{policy_column_count}",
    )
    return query, policy


def _expected_query_sql() -> str:
    base_expr = " + ".join(
        f"wide_data.c{i}" for i in range(1, BASE_AGGREGATE_COLUMNS + 1)
    )
    return (
        f"SELECT SUM({base_expr}) AS base_sum "
        "FROM wide_data "
        "JOIN join_data ON wide_data.pk = join_data.fk"
    )


def _expected_policy_constraint(policy_column_count: int) -> str:
    policy_expr = " + ".join(
        f"wide_data.c{i}"
        for i in range(POLICY_START_COLUMN, POLICY_START_COLUMN + policy_column_count)
    )
    return f"sum({policy_expr}) >= 0"


def _expected_dfc_1phase_sql(policy_column_count: int) -> str:
    return (
        f"{_expected_query_sql()} "
        f"HAVING (SUM("
        + " + ".join(
            f"wide_data.c{i}"
            for i in range(POLICY_START_COLUMN, POLICY_START_COLUMN + policy_column_count)
        )
        + ") >= 0)"
    )


def _expected_dfc_2phase_sql(policy_column_count: int) -> str:
    policy_sum_expr = " + ".join(
        f"wide_data.c{i}"
        for i in range(POLICY_START_COLUMN, POLICY_START_COLUMN + policy_column_count)
    )
    return (
        "WITH base_query AS ("
        f"{_expected_query_sql()}"
        "), policy_eval AS ("
        "SELECT 1 AS __dfc_two_phase_key FROM wide_data "
        "JOIN join_data ON wide_data.pk = join_data.fk "
        f"HAVING (SUM({policy_sum_expr}) >= 0)"
        ") SELECT base_query.* FROM base_query CROSS JOIN policy_eval"
    )


def _clear_policies(rewriter: SQLRewriter) -> None:
    for old_policy in rewriter.get_dfc_policies():
        rewriter.delete_policy(
            sources=old_policy.sources,
            constraint=old_policy.constraint,
            on_fail=old_policy.on_fail,
        )


def _assert_rewrite_and_results_for_policy_column_count(policy_column_count: int) -> None:
    conn, rewriter = _setup_wide_data_1000_rows()
    try:
        _clear_policies(rewriter)
        query, policy = _build_query_and_policy(policy_column_count)
        rewriter.register_policy(policy)

        dfc_1phase_sql = _normalize_sql(rewriter.transform_query(query))
        dfc_2phase_sql = _normalize_sql(rewriter.transform_query(query, use_two_phase=True))

        actual_query_sql = _normalize_sql(query)
        expected_query_sql = _normalize_sql(_expected_query_sql())
        assert actual_query_sql == expected_query_sql, (
            "Base query SQL mismatch.\n"
            f"Expected:\n{expected_query_sql}\n\n"
            f"Actual:\n{actual_query_sql}"
        )
        expected_policy_constraint = _expected_policy_constraint(policy_column_count)
        assert policy.constraint == expected_policy_constraint, (
            "Policy constraint mismatch.\n"
            f"Expected:\n{expected_policy_constraint}\n\n"
            f"Actual:\n{policy.constraint}"
        )
        expected_dfc_1phase_sql = _normalize_sql(_expected_dfc_1phase_sql(policy_column_count))
        assert dfc_1phase_sql == expected_dfc_1phase_sql, (
            "1Phase SQL mismatch.\n"
            f"Expected:\n{expected_dfc_1phase_sql}\n\n"
            f"Actual:\n{dfc_1phase_sql}"
        )
        expected_dfc_2phase_sql = _normalize_sql(_expected_dfc_2phase_sql(policy_column_count))
        assert dfc_2phase_sql == expected_dfc_2phase_sql, (
            "2Phase SQL mismatch.\n"
            f"Expected:\n{expected_dfc_2phase_sql}\n\n"
            f"Actual:\n{dfc_2phase_sql}"
        )

        dfc_1phase_results = conn.execute(dfc_1phase_sql).fetchall()
        dfc_2phase_results = conn.execute(dfc_2phase_sql).fetchall()
        match, error = compare_results_exact(dfc_1phase_results, dfc_2phase_results)
        assert match, f"Result mismatch for policy_column_count={policy_column_count}: {error}"
    finally:
        conn.execute("DROP TABLE IF EXISTS join_data")
        conn.execute("DROP TABLE IF EXISTS wide_data")
        rewriter.close()
        conn.close()


def test_phase_competition_rewrite_sql_and_results_policy_columns_2() -> None:
    _assert_rewrite_and_results_for_policy_column_count(2)


def test_phase_competition_rewrite_sql_and_results_policy_columns_4() -> None:
    _assert_rewrite_and_results_for_policy_column_count(4)


def test_phase_competition_rewrite_sql_and_results_policy_columns_8() -> None:
    _assert_rewrite_and_results_for_policy_column_count(8)
