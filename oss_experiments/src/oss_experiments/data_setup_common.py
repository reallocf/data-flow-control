"""Shared constants and types for tax experiment data setup."""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_INPUT_ROW_COUNT = 1000
DEFAULT_BATCH_ROW_COUNT = 100
DEFAULT_NUM_BATCHES = 10

EXPERIMENT_MODE_AGG = "agg"
EXPERIMENT_MODE_SELECT = "select"
EXPERIMENT_MODES = (EXPERIMENT_MODE_AGG, EXPERIMENT_MODE_SELECT)

QUERY_SET_ONE_ROW = "1_row_queries"
QUERY_SET_TEN_ROW = "10_row_queries"
QUERY_SET_HUNDRED_ROW = "100_row_queries"
QUERY_SET_ROW_COUNTS = {
    QUERY_SET_ONE_ROW: 1,
    QUERY_SET_TEN_ROW: 10,
    QUERY_SET_HUNDRED_ROW: 100,
}
QUERY_SETS = tuple(QUERY_SET_ROW_COUNTS)
MAX_QUERY_ROW_COUNT = max(QUERY_SET_ROW_COUNTS.values())

PASS_BIZ_USE = 50.0
FAIL_BIZ_USE = 100.0

DEFAULT_POLICY_SET_ID = "receipt_processing_kill"


@dataclass(frozen=True)
class QueryRecord:
    query_set: str
    query_id: str
    sql: str
    batch_id: int
    input_receipt_id_start: int
    input_receipt_id_end: int
    rows_selected: int
    expected_meal_limit_kill: bool
    expected_dfc_kill: bool
    input_receipt_ids: tuple[int, ...] = ()
    selected_merchants: tuple[str, ...] = ()
    meal_merchant: str = ""
    expected_expense_rows: int = 0
    experiment_mode: str = EXPERIMENT_MODE_AGG
    policy_set_id: str = DEFAULT_POLICY_SET_ID
    policy_ids: tuple[str, ...] = ("meal_deduction_limit",)
    query_kind: str = "insert_select"

    @property
    def key(self) -> tuple[str, str]:
        return (self.query_set, self.query_id)

    def __post_init__(self) -> None:
        if self.expected_expense_rows <= 0:
            object.__setattr__(self, "expected_expense_rows", self.rows_selected)


def normalize_experiment_mode(mode: str) -> str:
    normalized = mode.strip().lower()
    if normalized not in EXPERIMENT_MODES:
        raise ValueError(f"unsupported experiment mode: {mode!r}")
    return normalized


def default_dataset_label(mode: str, input_row_count: int = DEFAULT_INPUT_ROW_COUNT) -> str:
    normalized = normalize_experiment_mode(mode)
    suffix = "rollup" if normalized == EXPERIMENT_MODE_AGG else "select"
    return f"tax_receipts_{input_row_count}_{suffix}"
