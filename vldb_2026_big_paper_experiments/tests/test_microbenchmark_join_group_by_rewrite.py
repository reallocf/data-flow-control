"""Snapshot tests for JOIN->GROUP_BY microbenchmark SQL expansion and rewrites."""

import re

from sql_rewriter import SQLRewriter
import sqlglot

from vldb_experiments.baselines.logical_baseline import (
    rewrite_query_logical,
    rewrite_query_logical_multi,
)
from vldb_experiments.baselines.physical_baseline import execute_query_physical_detailed
from vldb_experiments.baselines.physical_rewriter import rewrite_query_physical
from vldb_experiments.correctness import compare_results_exact
from vldb_experiments.data_setup import setup_test_data_with_join_group_by
from vldb_experiments.policy_setup import create_test_policies, create_test_policy
from vldb_experiments.strategies.microbenchmark_strategy import (
    MicrobenchmarkStrategy,
    _ensure_smokedduck,
)

EXPECTED_DFC_SQL = {
    1: "SELECT test_data.category, COUNT(*), SUM(test_data.amount + j1.amount) AS total_amount FROM test_data JOIN join_data_1 AS j1 ON test_data.id = j1.id GROUP BY test_data.category HAVING (MAX(test_data.value) > 100)",
    10: "SELECT test_data.category, COUNT(*), SUM(test_data.amount + j1.amount + j2.amount + j3.amount + j4.amount + j5.amount + j6.amount + j7.amount + j8.amount + j9.amount + j10.amount) AS total_amount FROM test_data JOIN join_data_1 AS j1 ON test_data.id = j1.id JOIN join_data_2 AS j2 ON test_data.id = j2.id JOIN join_data_3 AS j3 ON test_data.id = j3.id JOIN join_data_4 AS j4 ON test_data.id = j4.id JOIN join_data_5 AS j5 ON test_data.id = j5.id JOIN join_data_6 AS j6 ON test_data.id = j6.id JOIN join_data_7 AS j7 ON test_data.id = j7.id JOIN join_data_8 AS j8 ON test_data.id = j8.id JOIN join_data_9 AS j9 ON test_data.id = j9.id JOIN join_data_10 AS j10 ON test_data.id = j10.id GROUP BY test_data.category HAVING (MAX(test_data.value) > 100)",
}

EXPECTED_LOGICAL_SQL = {
    1: "WITH base_query AS (SELECT test_data.category, COUNT(*) AS count, SUM(test_data.amount + j1.amount) AS total_amount FROM test_data JOIN join_data_1 AS j1 ON test_data.id = j1.id GROUP BY test_data.category) SELECT base_query.category, MAX(base_query.count), MAX(base_query.total_amount) AS total_amount FROM base_query, test_data JOIN join_data_1 AS j1 ON test_data.id = j1.id WHERE base_query.category = test_data.category GROUP BY base_query.category HAVING MAX(test_data.value) > 100",
    10: "WITH base_query AS (SELECT test_data.category, COUNT(*) AS count, SUM(test_data.amount + j1.amount + j2.amount + j3.amount + j4.amount + j5.amount + j6.amount + j7.amount + j8.amount + j9.amount + j10.amount) AS total_amount FROM test_data JOIN join_data_1 AS j1 ON test_data.id = j1.id JOIN join_data_2 AS j2 ON test_data.id = j2.id JOIN join_data_3 AS j3 ON test_data.id = j3.id JOIN join_data_4 AS j4 ON test_data.id = j4.id JOIN join_data_5 AS j5 ON test_data.id = j5.id JOIN join_data_6 AS j6 ON test_data.id = j6.id JOIN join_data_7 AS j7 ON test_data.id = j7.id JOIN join_data_8 AS j8 ON test_data.id = j8.id JOIN join_data_9 AS j9 ON test_data.id = j9.id JOIN join_data_10 AS j10 ON test_data.id = j10.id GROUP BY test_data.category) SELECT base_query.category, MAX(base_query.count), MAX(base_query.total_amount) AS total_amount FROM base_query, test_data JOIN join_data_1 AS j1 ON test_data.id = j1.id JOIN join_data_2 AS j2 ON test_data.id = j2.id JOIN join_data_3 AS j3 ON test_data.id = j3.id JOIN join_data_4 AS j4 ON test_data.id = j4.id JOIN join_data_5 AS j5 ON test_data.id = j5.id JOIN join_data_6 AS j6 ON test_data.id = j6.id JOIN join_data_7 AS j7 ON test_data.id = j7.id JOIN join_data_8 AS j8 ON test_data.id = j8.id JOIN join_data_9 AS j9 ON test_data.id = j9.id JOIN join_data_10 AS j10 ON test_data.id = j10.id WHERE base_query.category = test_data.category GROUP BY base_query.category HAVING MAX(test_data.value) > 100",
}

EXPECTED_PHYSICAL_SQL = {
    1: 'WITH lineage AS (SELECT "output_id" AS out_index, "opid_0_test_data" AS "test_data" FROM READ_BLOCK(0)) SELECT generated_table."category", generated_table."count", generated_table."total_amount" FROM temp_table_name AS generated_table JOIN lineage ON CAST(generated_table.rowid AS BIGINT) = CAST(lineage.out_index AS BIGINT) JOIN test_data ON CAST(test_data.rowid AS BIGINT) = CAST(lineage.test_data AS BIGINT) GROUP BY generated_table.rowid, generated_table."category", generated_table."count", generated_table."total_amount" HAVING MAX(test_data.value) > 100',
    10: 'WITH lineage AS (SELECT "output_id" AS out_index, "opid_0_test_data" AS "test_data" FROM READ_BLOCK(0)) SELECT generated_table."category", generated_table."count", generated_table."total_amount" FROM temp_table_name AS generated_table JOIN lineage ON CAST(generated_table.rowid AS BIGINT) = CAST(lineage.out_index AS BIGINT) JOIN test_data ON CAST(test_data.rowid AS BIGINT) = CAST(lineage.test_data AS BIGINT) GROUP BY generated_table.rowid, generated_table."category", generated_table."count", generated_table."total_amount" HAVING MAX(test_data.value) > 100',
}

EXPECTED_DFC_SQL_10_POLICIES = {
    1: "SELECT test_data.category, COUNT(*), SUM(test_data.amount + j1.amount) AS total_amount FROM test_data JOIN join_data_1 AS j1 ON test_data.id = j1.id GROUP BY test_data.category HAVING (MAX(test_data.value) > 108) AND (MAX(test_data.value) > 106) AND (MAX(test_data.value) > 104) AND (MAX(test_data.value) > 102) AND (MAX(test_data.value) > 100) AND (MAX(test_data.value) > 101) AND (MAX(test_data.value) > 103) AND (MAX(test_data.value) > 105) AND (MAX(test_data.value) > 107) AND (MAX(test_data.value) > 109)",
    10: "SELECT test_data.category, COUNT(*), SUM(test_data.amount + j1.amount + j2.amount + j3.amount + j4.amount + j5.amount + j6.amount + j7.amount + j8.amount + j9.amount + j10.amount) AS total_amount FROM test_data JOIN join_data_1 AS j1 ON test_data.id = j1.id JOIN join_data_2 AS j2 ON test_data.id = j2.id JOIN join_data_3 AS j3 ON test_data.id = j3.id JOIN join_data_4 AS j4 ON test_data.id = j4.id JOIN join_data_5 AS j5 ON test_data.id = j5.id JOIN join_data_6 AS j6 ON test_data.id = j6.id JOIN join_data_7 AS j7 ON test_data.id = j7.id JOIN join_data_8 AS j8 ON test_data.id = j8.id JOIN join_data_9 AS j9 ON test_data.id = j9.id JOIN join_data_10 AS j10 ON test_data.id = j10.id GROUP BY test_data.category HAVING (MAX(test_data.value) > 108) AND (MAX(test_data.value) > 106) AND (MAX(test_data.value) > 104) AND (MAX(test_data.value) > 102) AND (MAX(test_data.value) > 100) AND (MAX(test_data.value) > 101) AND (MAX(test_data.value) > 103) AND (MAX(test_data.value) > 105) AND (MAX(test_data.value) > 107) AND (MAX(test_data.value) > 109)",
}

EXPECTED_LOGICAL_SQL_10_POLICIES = {
    1: "WITH base_query AS (SELECT test_data.category, COUNT(*) AS count, SUM(test_data.amount + j1.amount) AS total_amount FROM test_data JOIN join_data_1 AS j1 ON test_data.id = j1.id GROUP BY test_data.category) SELECT base_query.category, MAX(base_query.count), MAX(base_query.total_amount) AS total_amount FROM base_query, test_data JOIN join_data_1 AS j1 ON test_data.id = j1.id WHERE base_query.category = test_data.category GROUP BY base_query.category HAVING (((((MAX(test_data.value) > 100) AND (MAX(test_data.value) > 101)) AND ((MAX(test_data.value) > 102) AND (MAX(test_data.value) > 103))) AND (((MAX(test_data.value) > 104) AND (MAX(test_data.value) > 105)) AND ((MAX(test_data.value) > 106) AND (MAX(test_data.value) > 107)))) AND ((MAX(test_data.value) > 108) AND (MAX(test_data.value) > 109)))",
    10: "WITH base_query AS (SELECT test_data.category, COUNT(*) AS count, SUM(test_data.amount + j1.amount + j2.amount + j3.amount + j4.amount + j5.amount + j6.amount + j7.amount + j8.amount + j9.amount + j10.amount) AS total_amount FROM test_data JOIN join_data_1 AS j1 ON test_data.id = j1.id JOIN join_data_2 AS j2 ON test_data.id = j2.id JOIN join_data_3 AS j3 ON test_data.id = j3.id JOIN join_data_4 AS j4 ON test_data.id = j4.id JOIN join_data_5 AS j5 ON test_data.id = j5.id JOIN join_data_6 AS j6 ON test_data.id = j6.id JOIN join_data_7 AS j7 ON test_data.id = j7.id JOIN join_data_8 AS j8 ON test_data.id = j8.id JOIN join_data_9 AS j9 ON test_data.id = j9.id JOIN join_data_10 AS j10 ON test_data.id = j10.id GROUP BY test_data.category) SELECT base_query.category, MAX(base_query.count), MAX(base_query.total_amount) AS total_amount FROM base_query, test_data JOIN join_data_1 AS j1 ON test_data.id = j1.id JOIN join_data_2 AS j2 ON test_data.id = j2.id JOIN join_data_3 AS j3 ON test_data.id = j3.id JOIN join_data_4 AS j4 ON test_data.id = j4.id JOIN join_data_5 AS j5 ON test_data.id = j5.id JOIN join_data_6 AS j6 ON test_data.id = j6.id JOIN join_data_7 AS j7 ON test_data.id = j7.id JOIN join_data_8 AS j8 ON test_data.id = j8.id JOIN join_data_9 AS j9 ON test_data.id = j9.id JOIN join_data_10 AS j10 ON test_data.id = j10.id WHERE base_query.category = test_data.category GROUP BY base_query.category HAVING (((((MAX(test_data.value) > 100) AND (MAX(test_data.value) > 101)) AND ((MAX(test_data.value) > 102) AND (MAX(test_data.value) > 103))) AND (((MAX(test_data.value) > 104) AND (MAX(test_data.value) > 105)) AND ((MAX(test_data.value) > 106) AND (MAX(test_data.value) > 107)))) AND ((MAX(test_data.value) > 108) AND (MAX(test_data.value) > 109)))",
}

EXPECTED_PHYSICAL_SQL_10_POLICIES = {
    1: 'WITH lineage AS (SELECT "output_id" AS out_index, "opid_0_test_data" AS "test_data" FROM READ_BLOCK(0)) SELECT generated_table."category", generated_table."count", generated_table."total_amount" FROM temp_table_name AS generated_table JOIN lineage ON CAST(generated_table.rowid AS BIGINT) = CAST(lineage.out_index AS BIGINT) JOIN test_data ON CAST(test_data.rowid AS BIGINT) = CAST(lineage.test_data AS BIGINT) GROUP BY generated_table.rowid, generated_table."category", generated_table."count", generated_table."total_amount" HAVING (MAX(test_data.value) > 100) AND (MAX(test_data.value) > 101) AND (MAX(test_data.value) > 102) AND (MAX(test_data.value) > 103) AND (MAX(test_data.value) > 104) AND (MAX(test_data.value) > 105) AND (MAX(test_data.value) > 106) AND (MAX(test_data.value) > 107) AND (MAX(test_data.value) > 108) AND (MAX(test_data.value) > 109)',
    10: 'WITH lineage AS (SELECT "output_id" AS out_index, "opid_0_test_data" AS "test_data" FROM READ_BLOCK(0)) SELECT generated_table."category", generated_table."count", generated_table."total_amount" FROM temp_table_name AS generated_table JOIN lineage ON CAST(generated_table.rowid AS BIGINT) = CAST(lineage.out_index AS BIGINT) JOIN test_data ON CAST(test_data.rowid AS BIGINT) = CAST(lineage.test_data AS BIGINT) GROUP BY generated_table.rowid, generated_table."category", generated_table."count", generated_table."total_amount" HAVING (MAX(test_data.value) > 100) AND (MAX(test_data.value) > 101) AND (MAX(test_data.value) > 102) AND (MAX(test_data.value) > 103) AND (MAX(test_data.value) > 104) AND (MAX(test_data.value) > 105) AND (MAX(test_data.value) > 106) AND (MAX(test_data.value) > 107) AND (MAX(test_data.value) > 108) AND (MAX(test_data.value) > 109)',
}


def _normalize_sql(sql: str) -> str:
    safe_sql = sql.replace("{temp_table_name}", "temp_table_name")
    safe_sql = re.sub(r"read_block\(\d+\)", "read_block(0)", safe_sql, flags=re.IGNORECASE)
    return sqlglot.parse_one(safe_sql, read="duckdb").sql(dialect="duckdb")


def _build_join_group_by_query(join_count: int) -> str:
    return MicrobenchmarkStrategy()._build_join_group_by_query(join_count)


def _build_rewrites(join_count: int) -> tuple[str, str, str]:
    conn = _ensure_smokedduck().connect(":memory:")
    setup_test_data_with_join_group_by(conn, join_count=join_count, num_rows=10)
    query = _build_join_group_by_query(join_count)
    policy = create_test_policy()

    rewriter = SQLRewriter(conn=conn)
    rewriter.register_policy(policy)
    dfc_sql = rewriter.transform_query(query)
    logical_sql = rewrite_query_logical(query, policy)
    _, physical_template, _ = rewrite_query_physical(
        query,
        policy,
        lineage_query='SELECT "output_id" AS out_index, "opid_0_test_data" AS "test_data" FROM read_block(0)',
        output_columns=["category", "count", "total_amount"],
    )
    conn.close()
    return dfc_sql, logical_sql, physical_template


def _build_rewrites_multi_policy(join_count: int, policy_count: int) -> tuple[str, str, str]:
    conn = _ensure_smokedduck().connect(":memory:")
    setup_test_data_with_join_group_by(conn, join_count=join_count, num_rows=10)
    query = _build_join_group_by_query(join_count)
    policies = create_test_policies(policy_count=policy_count)

    rewriter = SQLRewriter(conn=conn)
    for policy in policies:
        rewriter.register_policy(policy)
    dfc_sql = rewriter.transform_query(query)
    logical_sql = rewrite_query_logical_multi(query, policies)
    _, physical_template, _ = rewrite_query_physical(
        query,
        policies,
        lineage_query='SELECT "output_id" AS out_index, "opid_0_test_data" AS "test_data" FROM read_block(0)',
        output_columns=["category", "count", "total_amount"],
    )
    conn.close()
    return dfc_sql, logical_sql, physical_template


def _execute_rewrites(
    join_count: int,
    policy_count: int = 1,
) -> tuple[list[tuple], list[tuple], list[tuple]]:
    query = _build_join_group_by_query(join_count)
    policies = create_test_policies(threshold=0, policy_count=policy_count)
    local_duckdb = _ensure_smokedduck()

    dfc_conn = local_duckdb.connect(":memory:")
    logical_conn = local_duckdb.connect(":memory:")
    physical_conn = local_duckdb.connect(":memory:")
    for conn in [dfc_conn, logical_conn, physical_conn]:
        conn.execute("SET max_expression_depth TO 20000")
        setup_test_data_with_join_group_by(conn, join_count=join_count, num_rows=20)

    rewriter = SQLRewriter(conn=dfc_conn)
    for policy in policies:
        rewriter.register_policy(policy)
    dfc_sql = rewriter.transform_query(query)
    dfc_results = dfc_conn.execute(dfc_sql).fetchall()

    if len(policies) == 1:
        logical_sql = rewrite_query_logical(query, policies[0])
    else:
        logical_sql = rewrite_query_logical_multi(query, policies)
    logical_results = logical_conn.execute(logical_sql).fetchall()

    physical_results, _timing, physical_error, _base, _filter = execute_query_physical_detailed(
        physical_conn,
        query,
        policies if len(policies) > 1 else policies[0],
    )
    assert physical_error is None, physical_error

    for conn in [dfc_conn, logical_conn, physical_conn]:
        conn.close()

    return dfc_results, logical_results, physical_results


def test_join_group_by_query_expansion_for_1_and_10() -> None:
    q1 = _build_join_group_by_query(1)
    q10 = _build_join_group_by_query(10)

    assert q1.count("JOIN join_data_") == 1
    assert "JOIN join_data_1 j1" in q1
    assert "j1.amount" in q1

    assert q10.count("JOIN join_data_") == 10
    assert "JOIN join_data_10 j10" in q10
    assert "j10.amount" in q10


def test_join_group_by_rewrite_sql_snapshots_for_1_and_10() -> None:
    for join_count in [1, 10]:
        dfc_sql, logical_sql, physical_template = _build_rewrites(join_count)

        assert _normalize_sql(dfc_sql) == EXPECTED_DFC_SQL[join_count]
        assert _normalize_sql(logical_sql) == EXPECTED_LOGICAL_SQL[join_count]
        assert _normalize_sql(physical_template) == EXPECTED_PHYSICAL_SQL[join_count]


def test_join_group_by_rewrite_sql_snapshots_for_1_and_10_with_10_policies() -> None:
    for join_count in [1, 10]:
        dfc_sql, logical_sql, physical_template = _build_rewrites_multi_policy(
            join_count=join_count,
            policy_count=10,
        )

        assert _normalize_sql(dfc_sql) == EXPECTED_DFC_SQL_10_POLICIES[join_count]
        assert _normalize_sql(logical_sql) == EXPECTED_LOGICAL_SQL_10_POLICIES[join_count]
        assert _normalize_sql(physical_template) == EXPECTED_PHYSICAL_SQL_10_POLICIES[join_count]


def test_join_group_by_results_match_for_1_and_10() -> None:
    for join_count in [1, 10]:
        dfc_results, logical_results, physical_results = _execute_rewrites(join_count)

        logical_match, logical_error = compare_results_exact(dfc_results, logical_results)
        assert logical_match, f"Logical mismatch for join_count={join_count}: {logical_error}"

        physical_match, physical_error = compare_results_exact(dfc_results, physical_results)
        assert physical_match, f"Physical mismatch for join_count={join_count}: {physical_error}"


def test_join_group_by_results_match_for_1_and_10_with_10_policies() -> None:
    for join_count in [1, 10]:
        dfc_results, logical_results, physical_results = _execute_rewrites(
            join_count=join_count,
            policy_count=10,
        )

        logical_match, logical_error = compare_results_exact(dfc_results, logical_results)
        assert logical_match, f"Logical mismatch for join_count={join_count}, policies=10: {logical_error}"

        physical_match, physical_error = compare_results_exact(dfc_results, physical_results)
        assert physical_match, f"Physical mismatch for join_count={join_count}, policies=10: {physical_error}"
