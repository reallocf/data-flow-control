"""Tests for tax policy catalog."""

from __future__ import annotations

from data_flow_control import dfc
import duckdb

from oss_experiments.data_setup_common import EXPERIMENT_MODE_AGG, EXPERIMENT_MODE_SELECT
from oss_experiments.policies import (
    MEAL_DEDUCTION_LIMIT_PGN_AGG,
    MEAL_DEDUCTION_LIMIT_PGN_SELECT,
    build_meal_deduction_limit_policy,
    build_policy_set,
    policy_catalog,
    policy_descriptions,
)


def test_policy_catalog_entries():
    catalog = policy_catalog()
    assert set(catalog) == {"meal_deduction_limit"}


def test_select_mode_uses_row_level_meal_policy():
    meal = build_meal_deduction_limit_policy(mode=EXPERIMENT_MODE_SELECT)
    assert meal.constraint == "Expenses.biz_use <= 50 OR Receipts.cat != 'Meal'"
    assert "MIN(" not in meal.constraint


def test_agg_mode_uses_aggregate_meal_policy():
    meal = build_meal_deduction_limit_policy(mode=EXPERIMENT_MODE_AGG)
    assert meal.constraint == "MIN(Receipts.cat) != 'Meal' OR Expenses.biz_use <= 50.0"
    assert (
        meal.constraint != build_meal_deduction_limit_policy(mode=EXPERIMENT_MODE_SELECT).constraint
    )


def test_policy_descriptions_differ_by_mode():
    select_desc = policy_descriptions(mode=EXPERIMENT_MODE_SELECT)
    agg_desc = policy_descriptions(mode=EXPERIMENT_MODE_AGG)
    assert select_desc != agg_desc
    assert "aggregated" not in select_desc[0].lower()


def test_from_pgn_round_trip():
    meal = build_meal_deduction_limit_policy()
    assert meal.sources == ["Receipts"]
    assert meal.sink == "Expenses"


def test_register_policies_on_dfc_connection():
    conn = dfc(duckdb.connect())
    conn.execute("CREATE TABLE Receipts (id INTEGER, merchant VARCHAR, cat VARCHAR, cost DOUBLE)")
    conn.execute(
        "CREATE TABLE Expenses (id INTEGER, merchant VARCHAR, cost DOUBLE, biz_use DOUBLE)"
    )
    meal = build_meal_deduction_limit_policy(mode=EXPERIMENT_MODE_SELECT)
    conn.register_policy(meal)
    assert len(build_policy_set(mode=EXPERIMENT_MODE_SELECT)) == 1
    assert meal.constraint in MEAL_DEDUCTION_LIMIT_PGN_SELECT
    assert "MIN(" in MEAL_DEDUCTION_LIMIT_PGN_AGG
