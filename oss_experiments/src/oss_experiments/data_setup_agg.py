"""Aggregation-oriented query catalog for tax LLM validation."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import duckdb  # noqa: TC002

from oss_experiments.data_setup_common import (
    DEFAULT_BATCH_ROW_COUNT,
    DEFAULT_INPUT_ROW_COUNT,
    DEFAULT_NUM_BATCHES,
    EXPERIMENT_MODE_AGG,
    FAIL_BIZ_USE,
    MAX_QUERY_ROW_COUNT,
    PASS_BIZ_USE,
    QUERY_SET_ROW_COUNTS,
    QUERY_SET_TEN_ROW,
    QueryRecord,
)
from oss_experiments.policies import DEFAULT_POLICY_SET_ID

MERCHANTS_PER_BATCH = 10
RECEIPTS_PER_MERCHANT = DEFAULT_BATCH_ROW_COUNT // MERCHANTS_PER_BATCH

RECEIPTS_DDL = """
CREATE TABLE Receipts (
    id INTEGER,
    uid INTEGER,
    batch_id INTEGER,
    employee_id INTEGER,
    merchant VARCHAR,
    cat VARCHAR,
    cost DOUBLE,
    receipt_date DATE
)
"""

EXPENSES_DDL = """
CREATE TABLE Expenses (
    id INTEGER,
    merchant VARCHAR,
    cost DOUBLE,
    biz_use DOUBLE,
    source_note VARCHAR
)
"""


MERCHANT_NAMES = (
    "Alder Point",
    "Blue Meridian",
    "Cedar House",
    "Harbor Nine",
    "Juniper Room",
    "Lantern Row",
    "Northstar",
    "Silver Atrium",
    "Stonebridge",
    "Vista Lane",
)


def _batch_id_range(batch_id: int, batch_row_count: int) -> tuple[int, int]:
    start = batch_id * batch_row_count + 1
    end = (batch_id + 1) * batch_row_count
    return start, end


def _meal_batch_passes(batch_id: int) -> bool:
    return batch_id % 2 == 0


def _query_passes_meal_policy(query_index: int) -> bool:
    return query_index % 2 == 0


def _failing_query_ordinal(query_index: int) -> int:
    return query_index // 2


def _violating_output_position(rows_selected: int, query_index: int) -> int:
    step = rows_selected // 5
    return 2 + _failing_query_ordinal(query_index) * step


def _merchant_name(batch_id: int, merchant_slot: int) -> str:
    prefix = MERCHANT_NAMES[(batch_id + merchant_slot - 1) % len(MERCHANT_NAMES)]
    return f"{prefix} {batch_id:02d}-{merchant_slot:02d}"


def _meal_merchant_slot(batch_id: int) -> int:
    if _meal_batch_passes(batch_id):
        return 1
    return 2 + _failing_query_ordinal(batch_id) * 2


def _receipt_rows(
    input_row_count: int,
    batch_row_count: int,
) -> list[tuple[Any, ...]]:
    if input_row_count % batch_row_count != 0:
        raise ValueError("input_row_count must be divisible by batch_row_count")
    num_batches = input_row_count // batch_row_count
    rows: list[tuple[Any, ...]] = []
    base_date = date(2024, 1, 1)
    for batch_id in range(num_batches):
        id_start, _ = _batch_id_range(batch_id, batch_row_count)
        meal_merchant_slot = _meal_merchant_slot(batch_id)
        employee_id = 10_000 + batch_id
        receipt_date = base_date + timedelta(days=batch_id * 7 + 1)
        for offset in range(batch_row_count):
            receipt_id = id_start + offset
            uid = receipt_id
            merchant_slot = (offset % MERCHANTS_PER_BATCH) + 1
            cat = "Travel" if merchant_slot % 5 == 1 else "Office"
            cost = 100.0
            merchant = _merchant_name(batch_id, merchant_slot)
            is_meal = merchant_slot == meal_merchant_slot
            if is_meal:
                cat = "Meal"
            rows.append((receipt_id, uid, batch_id, employee_id, merchant, cat, cost, receipt_date))
    return rows


def _merchant_for_slot(batch_id: int, merchant_slot: int) -> str:
    return _merchant_name(batch_id, merchant_slot)


def _receipt_ids_for_merchant_slot(
    batch_id: int,
    batch_row_count: int,
    merchant_slot: int,
) -> tuple[int, ...]:
    id_start, _ = _batch_id_range(batch_id, batch_row_count)
    return tuple(
        id_start + (merchant_slot - 1) + receipt_index * MERCHANTS_PER_BATCH
        for receipt_index in range(RECEIPTS_PER_MERCHANT)
    )


def _selected_receipt_ids(
    *,
    batch_id: int,
    batch_row_count: int,
    rows_selected: int,
) -> tuple[int, ...]:
    selected_slot = _meal_merchant_slot(batch_id)
    if _query_passes_meal_policy(batch_id):
        selected_slot = 2 if selected_slot != 2 else 3
    if rows_selected == 1:
        return (_receipt_ids_for_merchant_slot(batch_id, batch_row_count, selected_slot)[0],)
    if rows_selected == QUERY_SET_ROW_COUNTS[QUERY_SET_TEN_ROW]:
        return _receipt_ids_for_merchant_slot(batch_id, batch_row_count, selected_slot)
    return tuple(
        receipt_id
        for merchant_slot in range(1, MERCHANTS_PER_BATCH + 1)
        for receipt_id in _receipt_ids_for_merchant_slot(
            batch_id,
            batch_row_count,
            merchant_slot,
        )
    )


def _selected_receipts_values(receipt_ids: tuple[int, ...]) -> str:
    values = ",\n        ".join(
        f"({position}, {receipt_id})" for position, receipt_id in enumerate(receipt_ids, start=1)
    )
    return f"(VALUES\n        {values}\n    ) AS selected_receipts(selection_position, receipt_id)"


def _selected_merchants_for_receipts(
    *,
    batch_id: int,
    batch_row_count: int,
    receipt_ids: tuple[int, ...],
) -> tuple[str, ...]:
    merchants: list[str] = []
    id_start, _ = _batch_id_range(batch_id, batch_row_count)
    for receipt_id in receipt_ids:
        merchant_slot = ((receipt_id - id_start) % MERCHANTS_PER_BATCH) + 1
        merchant = _merchant_for_slot(batch_id, merchant_slot)
        if merchant not in merchants:
            merchants.append(merchant)
    return tuple(merchants)


def _build_insert_sql(
    query_set: str,
    query_id: str,
    *,
    receipt_ids: tuple[int, ...],
    high_biz_use_merchants: tuple[str, ...],
) -> str:
    source_note = f"'{query_set}:{query_id}'"
    high_merchant_sql = ", ".join(f"'{merchant}'" for merchant in high_biz_use_merchants) or "''"
    return f"""
INSERT INTO Expenses (id, merchant, cost, biz_use, source_note)
WITH selected_receipts AS (
    SELECT * FROM {_selected_receipts_values(receipt_ids)}
),
candidate_receipts AS (
    SELECT
        Receipts.batch_id AS report_id,
        Receipts.id AS receipt_id,
        Receipts.cat,
        Receipts.merchant,
        Receipts.cost,
        selected_receipts.selection_position
    FROM Receipts
    JOIN selected_receipts
        ON selected_receipts.receipt_id = Receipts.id
),
rollups AS (
    SELECT
        report_id,
        merchant,
        MIN(selection_position) AS output_position,
        SUM(cost) AS cost
    FROM candidate_receipts
    GROUP BY report_id, merchant
)
SELECT
    report_id * 1000 + output_position AS id,
    merchant,
    cost,
    CASE WHEN merchant IN ({high_merchant_sql}) THEN {FAIL_BIZ_USE} ELSE {PASS_BIZ_USE} END AS biz_use,
    {source_note} AS source_note
FROM rollups
ORDER BY output_position
""".strip()


def build_query_catalog(
    *,
    batch_row_count: int = DEFAULT_BATCH_ROW_COUNT,
    num_batches: int = DEFAULT_NUM_BATCHES,
) -> list[QueryRecord]:
    if batch_row_count < MAX_QUERY_ROW_COUNT:
        raise ValueError(
            f"batch_row_count must be at least {MAX_QUERY_ROW_COUNT} for the configured query sets"
        )
    queries: list[QueryRecord] = []
    for query_index in range(num_batches):
        query_id = f"q{query_index:02d}"
        batch_id = query_index
        meal_kill = not _query_passes_meal_policy(query_index)
        meal_merchant = _merchant_for_slot(batch_id, _meal_merchant_slot(batch_id))
        for query_set, rows_selected in QUERY_SET_ROW_COUNTS.items():
            input_receipt_ids = _selected_receipt_ids(
                batch_id=batch_id,
                batch_row_count=batch_row_count,
                rows_selected=rows_selected,
            )
            selected_merchants = _selected_merchants_for_receipts(
                batch_id=batch_id,
                batch_row_count=batch_row_count,
                receipt_ids=input_receipt_ids,
            )
            sql = _build_insert_sql(
                query_set,
                query_id,
                receipt_ids=input_receipt_ids,
                high_biz_use_merchants=selected_merchants,
            )
            queries.append(
                QueryRecord(
                    query_set=query_set,
                    query_id=query_id,
                    sql=sql,
                    batch_id=batch_id,
                    input_receipt_id_start=min(input_receipt_ids),
                    input_receipt_id_end=max(input_receipt_ids),
                    input_receipt_ids=input_receipt_ids,
                    selected_merchants=selected_merchants,
                    meal_merchant=meal_merchant,
                    expected_expense_rows=len(selected_merchants),
                    rows_selected=rows_selected,
                    expected_meal_limit_kill=meal_kill,
                    expected_dfc_kill=meal_kill,
                    experiment_mode=EXPERIMENT_MODE_AGG,
                    policy_set_id=DEFAULT_POLICY_SET_ID,
                )
            )
    return queries


def reset_dataset(
    conn: duckdb.DuckDBPyConnection,
    *,
    input_row_count: int = DEFAULT_INPUT_ROW_COUNT,
    batch_row_count: int = DEFAULT_BATCH_ROW_COUNT,
) -> None:
    conn.execute("DROP TABLE IF EXISTS Expenses")
    conn.execute("DROP TABLE IF EXISTS Receipts")
    conn.execute(RECEIPTS_DDL)
    conn.execute(EXPENSES_DDL)
    rows = _receipt_rows(input_row_count, batch_row_count)
    conn.executemany(
        "INSERT INTO Receipts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def execute_raw_candidate_rows(
    conn: duckdb.DuckDBPyConnection,
    query: QueryRecord,
    *,
    input_row_count: int = DEFAULT_INPUT_ROW_COUNT,
    batch_row_count: int = DEFAULT_BATCH_ROW_COUNT,
) -> tuple[list[str], list[tuple[Any, ...]]]:
    reset_dataset(conn, input_row_count=input_row_count, batch_row_count=batch_row_count)
    conn.execute(query.sql)
    cursor = conn.execute(
        "SELECT id, merchant, cost, biz_use, source_note FROM Expenses "
        "WHERE source_note = ? ORDER BY id",
        [f"{query.query_set}:{query.query_id}"],
    )
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description or []]
    return columns, rows


def fetch_all_receipt_rows(
    conn: duckdb.DuckDBPyConnection,
) -> tuple[list[str], list[tuple[Any, ...]]]:
    cursor = conn.execute(f"SELECT {_receipt_query_columns()} FROM Receipts ORDER BY id")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description or []]
    return columns, rows


def fetch_all_input_relations(
    conn: duckdb.DuckDBPyConnection,
) -> list[tuple[str, list[str], list[tuple[Any, ...]]]]:
    receipt_cols, receipt_rows = fetch_all_receipt_rows(conn)
    return [
        ("Receipts", receipt_cols, receipt_rows),
    ]


def _receipt_query_columns() -> str:
    return "id, uid, batch_id, employee_id, merchant, cat, cost, receipt_date"


def fetch_input_receipt_rows(
    conn: duckdb.DuckDBPyConnection,
    query: QueryRecord,
) -> tuple[list[str], list[tuple[Any, ...]]]:
    if query.input_receipt_ids:
        placeholders = ", ".join("?" for _ in query.input_receipt_ids)
        cursor = conn.execute(
            f"SELECT {_receipt_query_columns()} "
            f"FROM Receipts WHERE id IN ({placeholders}) ORDER BY id",
            list(query.input_receipt_ids),
        )
    else:
        cursor = conn.execute(
            f"SELECT {_receipt_query_columns()} "
            "FROM Receipts WHERE id >= ? AND id <= ? ORDER BY id",
            [query.input_receipt_id_start, query.input_receipt_id_end],
        )
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description or []]
    return columns, rows
