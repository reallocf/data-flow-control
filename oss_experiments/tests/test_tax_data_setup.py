"""Tests for deterministic tax receipt dataset and query catalog."""

from __future__ import annotations

import duckdb

from oss_experiments.data_setup import (
    DEFAULT_BATCH_ROW_COUNT,
    DEFAULT_INPUT_ROW_COUNT,
    EXPERIMENT_MODE_AGG,
    FAIL_BIZ_USE,
    MERCHANT_NAMES,
    QUERY_SET_HUNDRED_ROW,
    QUERY_SET_ONE_ROW,
    QUERY_SET_ROW_COUNTS,
    QUERY_SET_TEN_ROW,
    QUERY_SETS,
    build_query_catalog,
    evaluate_dfc_truth,
    execute_raw_candidate_rows,
    fetch_all_input_relations,
    fetch_all_receipt_rows,
    fetch_input_receipt_rows,
    reset_dataset,
)


def test_receipt_row_count_and_batches():
    conn = duckdb.connect()
    reset_dataset(conn, mode=EXPERIMENT_MODE_AGG)
    total = conn.execute("SELECT COUNT(*) FROM Receipts").fetchone()[0]
    assert total == DEFAULT_INPUT_ROW_COUNT
    batches = conn.execute(
        "SELECT batch_id, COUNT(*) FROM Receipts GROUP BY batch_id ORDER BY batch_id"
    ).fetchall()
    assert len(batches) == 10
    for batch_id, count in batches:
        assert count == DEFAULT_BATCH_ROW_COUNT
        start = batch_id * DEFAULT_BATCH_ROW_COUNT + 1
        end = (batch_id + 1) * DEFAULT_BATCH_ROW_COUNT
        ids = conn.execute(
            "SELECT MIN(id), MAX(id) FROM Receipts WHERE batch_id = ?",
            [batch_id],
        ).fetchone()
        assert ids == (start, end)


def test_receipt_merchants_are_vague_rotating_sets():
    conn = duckdb.connect()
    reset_dataset(conn, mode=EXPERIMENT_MODE_AGG)
    assert len(MERCHANT_NAMES) == 10

    merchant_names = {
        row[0] for row in conn.execute("SELECT DISTINCT merchant FROM Receipts").fetchall()
    }

    assert {name.rsplit(" ", 1)[0] for name in merchant_names} == set(MERCHANT_NAMES)
    mixed_merchants = conn.execute(
        "SELECT COUNT(*) FROM ("
        "SELECT merchant FROM Receipts GROUP BY merchant HAVING COUNT(DISTINCT cat) > 1"
        ")"
    ).fetchone()[0]
    assert mixed_merchants == 0

    leaky_terms = (
        "meal",
        "food",
        "cafe",
        "restaurant",
        "diner",
        "grill",
        "kitchen",
        "bistro",
        "pizza",
        "burger",
        "taco",
        "coffee",
        "lunch",
        "dinner",
        "breakfast",
    )
    for name in merchant_names:
        lower = name.lower()
        assert not lower.startswith(("meal-", "item-"))
        assert all(term not in lower for term in leaky_terms)


def test_receipts_and_expenses_are_not_itemized():
    conn = duckdb.connect()
    reset_dataset(conn, mode=EXPERIMENT_MODE_AGG)
    receipt_columns = {row[1] for row in conn.execute("PRAGMA table_info('Receipts')").fetchall()}
    expense_columns = {row[1] for row in conn.execute("PRAGMA table_info('Expenses')").fetchall()}
    assert "item" not in receipt_columns
    assert "item" not in expense_columns
    assert "merchant" in receipt_columns
    assert "merchant" in expense_columns


def test_query_catalog_has_thirty_definitions():
    queries = build_query_catalog(mode=EXPERIMENT_MODE_AGG)
    assert len(queries) == 30
    for query_set in QUERY_SETS:
        subset = [q for q in queries if q.query_set == query_set]
        assert len(subset) == 10
        assert {q.query_id for q in subset} == {f"q{i:02d}" for i in range(10)}


def test_query_sql_uses_rollup_joins_and_case_when_biz_use_mix():
    for query in build_query_catalog(mode=EXPERIMENT_MODE_AGG):
        assert "WITH selected_receipts AS" in query.sql
        assert "candidate_receipts AS" in query.sql
        assert "rollups AS" in query.sql
        assert "FROM Receipts" in query.sql
        assert "JOIN MerchantMap" not in query.sql
        assert "FROM Reports" not in query.sql
        assert "ROW_NUMBER" not in query.sql
        assert "duplicate_rank" not in query.sql
        assert "GROUP BY report_id, merchant" in query.sql
        assert "CASE " in query.sql
        assert "WHEN merchant IN (" in query.sql
        assert " END AS biz_use" in query.sql


def test_one_row_queries_use_distinct_receipt_ids():
    queries = [q for q in build_query_catalog() if q.query_set == QUERY_SET_ONE_ROW]
    ids = [q.input_receipt_id_start for q in queries]
    assert len(ids) == len(set(ids))


def test_multi_row_queries_use_non_overlapping_ranges():
    for query_set in (QUERY_SET_TEN_ROW, QUERY_SET_HUNDRED_ROW):
        queries = [q for q in build_query_catalog() if q.query_set == query_set]
        assert all(len(q.input_receipt_ids) == QUERY_SET_ROW_COUNTS[query_set] for q in queries)
        for index, query in enumerate(queries):
            other_queries = queries[:index] + queries[index + 1 :]
            assert all(
                set(query.input_receipt_ids).isdisjoint(other.input_receipt_ids)
                for other in other_queries
            )
        if query_set == QUERY_SET_TEN_ROW:
            for query in queries:
                assert "selected_receipts(selection_position, receipt_id)" in query.sql
                assert "JOIN selected_receipts" in query.sql
                assert query.expected_expense_rows == 1
                assert query.input_receipt_ids != tuple(
                    range(query.input_receipt_id_start, query.input_receipt_id_end + 1)
                )
        else:
            assert all(q.expected_expense_rows == 10 for q in queries)
            for query in queries:
                assert query.input_receipt_ids != tuple(
                    range(query.input_receipt_id_start, query.input_receipt_id_end + 1)
                )


def test_fetch_all_receipt_rows_returns_full_input_relation():
    conn = duckdb.connect()
    reset_dataset(conn, mode=EXPERIMENT_MODE_AGG)
    cols, rows = fetch_all_receipt_rows(conn)
    assert len(rows) == DEFAULT_INPUT_ROW_COUNT
    assert "cat" in cols
    assert "employee_id" in cols
    assert conn.execute("SELECT COUNT(*) FROM Receipts").fetchone()[0] == len(rows)


def test_fetch_all_input_relations_returns_query_inputs():
    conn = duckdb.connect()
    reset_dataset(conn, mode=EXPERIMENT_MODE_AGG)
    relations = fetch_all_input_relations(conn)
    assert [name for name, _, _ in relations] == ["Receipts"]
    row_counts = {name: len(rows) for name, _, rows in relations}
    assert row_counts == {
        "Receipts": DEFAULT_INPUT_ROW_COUNT,
    }


def test_fetch_input_receipt_rows_still_returns_query_selection():
    conn = duckdb.connect()
    reset_dataset(conn, mode=EXPERIMENT_MODE_AGG)
    query = next(
        q for q in build_query_catalog(mode=EXPERIMENT_MODE_AGG) if q.query_set == QUERY_SET_TEN_ROW
    )
    _cols, rows = fetch_input_receipt_rows(conn, query)
    assert len(rows) == query.rows_selected
    assert {row[0] for row in rows} == set(query.input_receipt_ids)


def test_raw_execution_row_counts():
    conn = duckdb.connect()
    for query in build_query_catalog(mode=EXPERIMENT_MODE_AGG):
        cols, rows = execute_raw_candidate_rows(conn, query)
        assert len(rows) == query.expected_expense_rows
        assert cols


def test_each_batch_has_one_meal_merchant():
    conn = duckdb.connect()
    reset_dataset(conn, mode=EXPERIMENT_MODE_AGG)
    for batch_id in range(10):
        meal_merchant_count = conn.execute(
            "SELECT COUNT(DISTINCT merchant) FROM Receipts WHERE batch_id = ? AND cat = 'Meal'",
            [batch_id],
        ).fetchone()[0]
        meal_receipt_count = conn.execute(
            "SELECT COUNT(*) FROM Receipts WHERE batch_id = ? AND cat = 'Meal'",
            [batch_id],
        ).fetchone()[0]
        assert meal_merchant_count == 1
        assert meal_receipt_count == 10


def test_raw_execution_uses_high_biz_use_for_selected_merchants():
    conn = duckdb.connect()
    for query in build_query_catalog(mode=EXPERIMENT_MODE_AGG):
        if query.query_set != QUERY_SET_TEN_ROW:
            continue
        cols, rows = execute_raw_candidate_rows(conn, query)
        biz_use_idx = cols.index("biz_use")
        biz_use_values = {row[biz_use_idx] for row in rows}
        assert biz_use_values == {FAIL_BIZ_USE}


def test_meal_policy_pass_fail_split():
    for query_set in QUERY_SETS:
        queries = [q for q in build_query_catalog() if q.query_set == query_set]
        kills = sum(1 for q in queries if q.expected_meal_limit_kill)
        passes = sum(1 for q in queries if not q.expected_meal_limit_kill)
        assert kills == 5
        assert passes == 5


def test_passing_and_failing_ten_row_outputs_are_not_self_evident():
    conn = duckdb.connect()
    passing_query = next(
        q
        for q in build_query_catalog(mode=EXPERIMENT_MODE_AGG)
        if q.query_set == QUERY_SET_TEN_ROW and not q.expected_meal_limit_kill
    )
    failing_query = next(
        q
        for q in build_query_catalog(mode=EXPERIMENT_MODE_AGG)
        if q.query_set == QUERY_SET_TEN_ROW and q.expected_meal_limit_kill
    )
    pass_cols, pass_rows = execute_raw_candidate_rows(conn, passing_query)
    fail_cols, fail_rows = execute_raw_candidate_rows(conn, failing_query)
    assert pass_cols == fail_cols
    pass_biz_use_idx = pass_cols.index("biz_use")
    fail_biz_use_idx = fail_cols.index("biz_use")
    assert len(pass_rows) == len(fail_rows) == 1
    assert pass_rows[0][pass_biz_use_idx] == FAIL_BIZ_USE
    assert fail_rows[0][fail_biz_use_idx] == FAIL_BIZ_USE


def test_dfc_truth_matches_expected_for_all_queries():
    conn = duckdb.connect()
    for query in build_query_catalog(mode=EXPERIMENT_MODE_AGG):
        if query.query_set != QUERY_SET_TEN_ROW:
            continue
        result = evaluate_dfc_truth(conn, query)
        assert result["dfc_truth_matches_expected"] is True
