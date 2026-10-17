"""Tests for row-level select-mode tax dataset and query catalog."""

from __future__ import annotations

import duckdb

from oss_experiments.data_setup import (
    EXPERIMENT_MODE_SELECT,
    FAIL_BIZ_USE,
    PASS_BIZ_USE,
    QUERY_SET_ONE_ROW,
    QUERY_SET_TEN_ROW,
    build_query_catalog,
    evaluate_dfc_truth,
    execute_raw_candidate_rows,
    reset_dataset,
)


def test_select_queries_use_simple_insert_select():
    for query in build_query_catalog(mode=EXPERIMENT_MODE_SELECT):
        assert query.experiment_mode == EXPERIMENT_MODE_SELECT
        assert "INSERT INTO Expenses" in query.sql
        assert "SELECT" in query.sql
        assert "FROM Receipts" in query.sql
        assert "WITH selected_receipts AS" not in query.sql
        assert "rollups AS" not in query.sql
        assert "CASE " in query.sql
        assert "WHEN Receipts.id = " in query.sql


def test_select_ten_row_queries_use_scattered_ids():
    queries = [
        q
        for q in build_query_catalog(mode=EXPERIMENT_MODE_SELECT)
        if q.query_set == QUERY_SET_TEN_ROW
    ]
    for query in queries:
        assert "Receipts.id IN (" in query.sql
        assert len(query.input_receipt_ids) == 10
        span = query.input_receipt_id_end - query.input_receipt_id_start + 1
        assert span > 10


def test_select_ten_row_biz_use_mix_and_meal_positions():
    conn = duckdb.connect()
    passing = next(
        q
        for q in build_query_catalog(mode=EXPERIMENT_MODE_SELECT)
        if q.query_set == QUERY_SET_TEN_ROW and not q.expected_meal_limit_kill
    )
    for query in build_query_catalog(mode=EXPERIMENT_MODE_SELECT):
        if query.query_set != QUERY_SET_TEN_ROW:
            continue
        cols, rows = execute_raw_candidate_rows(conn, query, mode=EXPERIMENT_MODE_SELECT)
        assert len(rows) == 10
        biz_use_idx = cols.index("biz_use")
        assert {row[biz_use_idx] for row in rows} == {PASS_BIZ_USE, FAIL_BIZ_USE}

    pass_cols, pass_rows = execute_raw_candidate_rows(conn, passing, mode=EXPERIMENT_MODE_SELECT)
    id_idx = pass_cols.index("id")
    pass_meal_positions = [
        index
        for index, row in enumerate(pass_rows, start=1)
        if conn.execute(
            "SELECT cat = 'Meal' FROM Receipts WHERE id = ?",
            [row[id_idx]],
        ).fetchone()[0]
    ]
    assert pass_meal_positions == [2]

    failing_queries = [
        q
        for q in build_query_catalog(mode=EXPERIMENT_MODE_SELECT)
        if q.query_set == QUERY_SET_TEN_ROW and q.expected_meal_limit_kill
    ]
    observed_violation_positions = []
    for query in failing_queries:
        cols, rows = execute_raw_candidate_rows(conn, query, mode=EXPERIMENT_MODE_SELECT)
        id_idx = cols.index("id")
        biz_use_idx = cols.index("biz_use")
        violating_positions = [
            position
            for position, row in enumerate(rows, start=1)
            if conn.execute(
                "SELECT cat = 'Meal' FROM Receipts WHERE id = ?",
                [row[id_idx]],
            ).fetchone()[0]
            and row[biz_use_idx] == FAIL_BIZ_USE
        ]
        assert len(violating_positions) == 1
        observed_violation_positions.append(violating_positions[0])
    assert observed_violation_positions == [2, 4, 6, 8, 10]


def test_select_one_row_queries_are_single_meal_rows():
    conn = duckdb.connect()
    for query in build_query_catalog(mode=EXPERIMENT_MODE_SELECT):
        if query.query_set != QUERY_SET_ONE_ROW:
            continue
        cols, rows = execute_raw_candidate_rows(conn, query, mode=EXPERIMENT_MODE_SELECT)
        assert len(rows) == 1
        id_idx = cols.index("id")
        is_meal = conn.execute(
            "SELECT cat = 'Meal' FROM Receipts WHERE id = ?",
            [rows[0][id_idx]],
        ).fetchone()[0]
        assert is_meal


def test_select_dfc_truth_matches_expected_for_all_queries():
    conn = duckdb.connect()
    reset_dataset(conn, mode=EXPERIMENT_MODE_SELECT)
    for query in build_query_catalog(mode=EXPERIMENT_MODE_SELECT):
        result = evaluate_dfc_truth(conn, query, mode=EXPERIMENT_MODE_SELECT)
        assert result["dfc_truth_matches_expected"] is True
