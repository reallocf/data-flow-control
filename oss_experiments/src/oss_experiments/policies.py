"""DFC policy catalog for tax receipt experiments."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from data_flow_control import Policy

from oss_experiments.data_setup_common import (
    DEFAULT_POLICY_SET_ID,
    EXPERIMENT_MODE_AGG,
    EXPERIMENT_MODE_SELECT,
    normalize_experiment_mode,
)

MEAL_DEDUCTION_LIMIT_PGN_AGG = (
    "SOURCE Receipts SINK Expenses "
    "CONSTRAINT MIN(Receipts.cat) != 'Meal' OR Expenses.biz_use <= 50.0 ON FAIL KILL "
    "DESCRIPTION meal_deduction_limit"
)

MEAL_DEDUCTION_LIMIT_PGN_SELECT = (
    "SOURCE Receipts SINK Expenses "
    "CONSTRAINT Expenses.biz_use <= 50 OR Receipts.cat != 'Meal' ON FAIL KILL "
    "DESCRIPTION meal_deduction_limit"
)

__all__ = [
    "DEFAULT_POLICY_SET_ID",
    "MEAL_DEDUCTION_LIMIT_PGN_AGG",
    "MEAL_DEDUCTION_LIMIT_PGN_SELECT",
    "PolicyEntry",
    "build_meal_deduction_limit_policy",
    "build_policy_set",
    "policy_catalog",
    "policy_descriptions",
    "register_policy_set",
]


@dataclass(frozen=True)
class PolicyEntry:
    policy_id: str
    description: str
    policy: Policy


def _meal_pgn(mode: str) -> str:
    """Select mode uses the original row-level constraint; agg uses MIN(cat)."""
    if normalize_experiment_mode(mode) == EXPERIMENT_MODE_SELECT:
        return MEAL_DEDUCTION_LIMIT_PGN_SELECT
    return MEAL_DEDUCTION_LIMIT_PGN_AGG


def build_meal_deduction_limit_policy(*, mode: str = EXPERIMENT_MODE_AGG) -> Policy:
    return Policy.from_pgn(_meal_pgn(mode))


def policy_catalog(*, mode: str = EXPERIMENT_MODE_AGG) -> dict[str, PolicyEntry]:
    meal = build_meal_deduction_limit_policy(mode=mode)
    if normalize_experiment_mode(mode) == EXPERIMENT_MODE_SELECT:
        description = "Meal expenses may deduct at most 50% (biz_use <= 50)."
    else:
        description = "If the aggregated receipt category is Meal, biz_use must be at most 50%."
    return {
        "meal_deduction_limit": PolicyEntry(
            policy_id="meal_deduction_limit",
            description=description,
            policy=meal,
        ),
    }


def build_policy_set(
    policy_ids: list[str] | None = None,
    *,
    mode: str = EXPERIMENT_MODE_AGG,
) -> list[Policy]:
    catalog = policy_catalog(mode=mode)
    selected = policy_ids or ["meal_deduction_limit"]
    return [catalog[policy_id].policy for policy_id in selected]


def register_policy_set(
    conn: Any,
    policy_ids: list[str] | None = None,
    *,
    mode: str = EXPERIMENT_MODE_AGG,
) -> list[Policy]:
    policies = build_policy_set(policy_ids, mode=mode)
    for policy in policies:
        conn.register_policy(policy)
    return policies


def policy_descriptions(
    policy_ids: list[str] | None = None,
    *,
    mode: str = EXPERIMENT_MODE_AGG,
) -> list[str]:
    catalog = policy_catalog(mode=mode)
    selected = policy_ids or ["meal_deduction_limit"]
    return [catalog[policy_id].description for policy_id in selected]
