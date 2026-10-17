"""Row-level INSERT..SELECT query catalog for tax LLM validation."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import duckdb  # noqa: TC002

from oss_experiments.data_setup_common import (
    DEFAULT_BATCH_ROW_COUNT,
    DEFAULT_NUM_BATCHES,
    EXPERIMENT_MODE_SELECT,
    FAIL_BIZ_USE,
    MAX_QUERY_ROW_COUNT,
    PASS_BIZ_USE,
    QUERY_SET_ROW_COUNTS,
    QUERY_SET_TEN_ROW,
    QueryRecord,
)
from oss_experiments.policies import DEFAULT_POLICY_SET_ID

MEAL_MERCHANT_NAMES = (
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

NON_MEAL_MERCHANT_NAMES = (
    "Atlas Point",
    "Briar House",
    "Crescent Nine",
    "Ember Room",
    "Granite Atrium",
    "Keystone Row",
    "Pioneer Lane",
    "Riverstone",
    "Sable Meridian",
    "Willow Bridge",
)

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


def _violating_receipt_offset(query_index: int) -> int:
    return _violating_output_position(MAX_QUERY_ROW_COUNT, query_index) - 1


def _rotating_merchant_name(names: tuple[str, ...], batch_id: int, offset: int) -> str:
    return names[(batch_id + offset) % len(names)]


def _meal_receipt_id(batch_id: int, batch_row_count: int) -> int:
    id_start, _ = _batch_id_range(batch_id, batch_row_count)
    if _meal_batch_passes(batch_id):
        return id_start + 1
    return id_start + _violating_receipt_offset(batch_id)


def _receipt_rows(input_row_count: int, batch_row_count: int) -> list[tuple[Any, ...]]:
    if input_row_count % batch_row_count != 0:
        raise ValueError("input_row_count must be divisible by batch_row_count")
    num_batches = input_row_count // batch_row_count
    rows: list[tuple[Any, ...]] = []
    base_date = date(2024, 1, 1)
    for batch_id in range(num_batches):
        id_start, _ = _batch_id_range(batch_id, batch_row_count)
        batch_passes = _meal_batch_passes(batch_id)
        violating_offset = None if batch_passes else _violating_receipt_offset(batch_id)
        employee_id = 10_000 + batch_id
        for offset in range(batch_row_count):
            receipt_id = id_start + offset
            uid = receipt_id
            cat = "Travel" if offset % 5 == 0 else "Office"
            cost = 100.0
            merchant = _rotating_merchant_name(NON_MEAL_MERCHANT_NAMES, batch_id, offset)
            is_meal = offset == (1 if batch_passes else violating_offset)
            if is_meal:
                cat = "Meal"
                merchant = _rotating_merchant_name(MEAL_MERCHANT_NAMES, batch_id, offset)
            receipt_date = base_date + timedelta(days=receipt_id % 365)
            rows.append((receipt_id, uid, batch_id, employee_id, merchant, cat, cost, receipt_date))
    return rows


def _one_row_receipt_id(batch_id: int, batch_row_count: int, query_index: int) -> int:
    id_start, _ = _batch_id_range(batch_id, batch_row_count)
    if _query_passes_meal_policy(query_index):
        return id_start + 1
    return id_start + _violating_receipt_offset(query_index)


def _multi_row_receipt_range(
    batch_id: int,
    batch_row_count: int,
    query_index: int,
    rows_selected: int,
) -> tuple[int, int]:
    id_start, id_end = _batch_id_range(batch_id, batch_row_count)
    if _query_passes_meal_policy(query_index):
        return id_start, id_start + rows_selected - 1
    violation_id = id_start + _violating_receipt_offset(query_index)
    violation_position = _violating_output_position(rows_selected, query_index)
    range_start = violation_id - violation_position + 1
    range_end = range_start + rows_selected - 1
    if range_start < id_start or range_end > id_end:
        raise ValueError(
            f"{rows_selected}-row query range for q{query_index:02d} "
            f"falls outside batch {batch_id}: {range_start}-{range_end} not in {id_start}-{id_end}"
        )
    return range_start, range_end


def _pick_spaced_receipt_ids(
    id_low: int,
    id_high: int,
    count: int,
    *,
    exclude: int | None = None,
) -> list[int]:
    if count == 0:
        return []
    if id_high < id_low:
        raise ValueError(f"cannot pick {count} ids from empty range {id_low}-{id_high}")
    available = [receipt_id for receipt_id in range(id_low, id_high + 1) if receipt_id != exclude]
    if len(available) < count:
        raise ValueError(
            f"cannot pick {count} ids from range {id_low}-{id_high} excluding {exclude}"
        )
    span = available[-1] - available[0]
    step = max(1, span // max(count - 1, 1))
    picked: list[int] = []
    cursor = 0
    while len(picked) < count and cursor < len(available):
        picked.append(available[cursor])
        cursor += step
    if len(picked) < count:
        for receipt_id in available:
            if receipt_id not in picked:
                picked.append(receipt_id)
            if len(picked) == count:
                break
    return picked[:count]


def _scattered_receipt_ids(
    batch_id: int,
    batch_row_count: int,
    query_index: int,
    rows_selected: int,
) -> list[int]:
    id_start, id_end = _batch_id_range(batch_id, batch_row_count)
    meal_id = _meal_receipt_id(batch_id, batch_row_count)
    if _query_passes_meal_policy(query_index):
        non_meal_ids = _pick_spaced_receipt_ids(
            id_start,
            id_end,
            rows_selected - 1,
            exclude=meal_id,
        )
        return sorted([*non_meal_ids, meal_id])

    violation_position = _violating_output_position(rows_selected, query_index)
    below_count = violation_position - 1
    above_count = rows_selected - violation_position
    below_ids = _pick_spaced_receipt_ids(id_start, meal_id - 1, below_count, exclude=meal_id)
    above_ids = _pick_spaced_receipt_ids(meal_id + 1, id_end, above_count, exclude=meal_id)
    return sorted([*below_ids, meal_id, *above_ids])


def _where_clause_for_receipt_ids(receipt_ids: list[int]) -> str:
    id_list = ", ".join(str(receipt_id) for receipt_id in receipt_ids)
    return f"Receipts.id IN ({id_list})"


def _biz_use_expr(
    *,
    meal_kill: bool,
    violation_receipt_id: int | None,
    receipt_ids: list[int],
    meal_receipt_id: int,
) -> str:
    meal_biz_use = FAIL_BIZ_USE if meal_kill else PASS_BIZ_USE
    opposite_biz_use = PASS_BIZ_USE + FAIL_BIZ_USE - meal_biz_use
    meal_in_selection = meal_receipt_id in receipt_ids
    non_meal_ids = [receipt_id for receipt_id in receipt_ids if receipt_id != meal_receipt_id]
    opposite_count = (len(non_meal_ids) + 1) // 2
    opposite_ids = non_meal_ids[:opposite_count]
    same_ids = non_meal_ids[opposite_count:]

    clauses: list[str] = []
    if meal_kill:
        if violation_receipt_id is None:
            raise ValueError("violation_receipt_id is required for failing queries")
        clauses.append(f"WHEN Receipts.id = {violation_receipt_id} THEN {meal_biz_use}")
    elif meal_in_selection:
        clauses.append(f"WHEN Receipts.id = {meal_receipt_id} THEN {meal_biz_use}")
    for receipt_id in opposite_ids:
        clauses.append(f"WHEN Receipts.id = {receipt_id} THEN {opposite_biz_use}")
    for receipt_id in same_ids:
        clauses.append(f"WHEN Receipts.id = {receipt_id} THEN {meal_biz_use}")
    return "CASE " + " ".join(clauses) + " END"


def _violation_receipt_id(
    receipt_ids: list[int],
    query_index: int,
    rows_selected: int,
    *,
    meal_receipt_id: int,
    meal_kill: bool,
) -> int:
    if not meal_kill:
        raise ValueError("violation_receipt_id requested for a passing query")
    if rows_selected == 1:
        return receipt_ids[0]
    violation_position = _violating_output_position(rows_selected, query_index)
    violation_id = sorted(receipt_ids)[violation_position - 1]
    if violation_id != meal_receipt_id:
        raise ValueError(
            f"scattered ids for q{query_index:02d} place meal {meal_receipt_id} "
            f"at violation position {violation_position}, got {violation_id}"
        )
    return violation_id


def _build_insert_sql(
    query_set: str,
    query_id: str,
    *,
    where_clause: str,
    biz_use_expr: str,
) -> str:
    source_note = f"'{query_set}:{query_id}'"
    return f"""
INSERT INTO Expenses (id, merchant, cost, biz_use, source_note)
SELECT
    Receipts.id,
    Receipts.merchant,
    Receipts.cost,
    {biz_use_expr} AS biz_use,
    {source_note} AS source_note
FROM Receipts
WHERE {where_clause}
ORDER BY Receipts.id
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
        _, id_end = _batch_id_range(batch_id, batch_row_count)
        meal_kill = not _query_passes_meal_policy(query_index)
        meal_receipt_id = _meal_receipt_id(batch_id, batch_row_count)
        for query_set, rows_selected in QUERY_SET_ROW_COUNTS.items():
            if rows_selected == 1:
                receipt_ids = [_one_row_receipt_id(batch_id, batch_row_count, query_index)]
                where_clause = f"Receipts.id = {receipt_ids[0]}"
            elif rows_selected == QUERY_SET_ROW_COUNTS[QUERY_SET_TEN_ROW]:
                receipt_ids = _scattered_receipt_ids(
                    batch_id,
                    batch_row_count,
                    query_index,
                    rows_selected,
                )
                where_clause = _where_clause_for_receipt_ids(receipt_ids)
            else:
                range_start, range_end = _multi_row_receipt_range(
                    batch_id,
                    batch_row_count,
                    query_index,
                    rows_selected,
                )
                receipt_ids = list(range(range_start, range_end + 1))
                where_clause = f"Receipts.id >= {range_start} AND Receipts.id <= {range_end}"
            range_start = min(receipt_ids)
            range_end = max(receipt_ids)
            if range_end > id_end or range_start < batch_id * batch_row_count + 1:
                raise ValueError(
                    f"{query_set} selects beyond batch {batch_id}: "
                    f"{range_start}-{range_end} outside batch bounds"
                )
            sql = _build_insert_sql(
                query_set,
                query_id,
                where_clause=where_clause,
                biz_use_expr=_biz_use_expr(
                    meal_kill=meal_kill,
                    violation_receipt_id=(
                        _violation_receipt_id(
                            receipt_ids,
                            query_index,
                            rows_selected,
                            meal_receipt_id=meal_receipt_id,
                            meal_kill=meal_kill,
                        )
                        if meal_kill
                        else None
                    ),
                    receipt_ids=receipt_ids,
                    meal_receipt_id=meal_receipt_id,
                ),
            )
            queries.append(
                QueryRecord(
                    query_set=query_set,
                    query_id=query_id,
                    sql=sql,
                    batch_id=batch_id,
                    input_receipt_id_start=range_start,
                    input_receipt_id_end=range_end,
                    input_receipt_ids=tuple(receipt_ids),
                    rows_selected=rows_selected,
                    expected_expense_rows=rows_selected,
                    expected_meal_limit_kill=meal_kill,
                    expected_dfc_kill=meal_kill,
                    experiment_mode=EXPERIMENT_MODE_SELECT,
                    policy_set_id=DEFAULT_POLICY_SET_ID,
                )
            )
    return queries


def reset_dataset(
    conn: duckdb.DuckDBPyConnection,
    *,
    input_row_count: int,
    batch_row_count: int,
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
    input_row_count: int,
    batch_row_count: int,
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
    cursor = conn.execute(
        "SELECT id, uid, batch_id, employee_id, merchant, cat, cost, receipt_date "
        "FROM Receipts ORDER BY id"
    )
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description or []]
    return columns, rows


def fetch_all_input_relations(
    conn: duckdb.DuckDBPyConnection,
) -> list[tuple[str, list[str], list[tuple[Any, ...]]]]:
    receipt_cols, receipt_rows = fetch_all_receipt_rows(conn)
    return [("Receipts", receipt_cols, receipt_rows)]


def fetch_input_receipt_rows(
    conn: duckdb.DuckDBPyConnection,
    query: QueryRecord,
) -> tuple[list[str], list[tuple[Any, ...]]]:
    if query.input_receipt_ids:
        placeholders = ", ".join("?" for _ in query.input_receipt_ids)
        cursor = conn.execute(
            "SELECT id, uid, batch_id, employee_id, merchant, cat, cost, receipt_date "
            f"FROM Receipts WHERE id IN ({placeholders}) ORDER BY id",
            list(query.input_receipt_ids),
        )
    else:
        cursor = conn.execute(
            "SELECT id, uid, batch_id, employee_id, merchant, cat, cost, receipt_date "
            "FROM Receipts WHERE id >= ? AND id <= ? ORDER BY id",
            [query.input_receipt_id_start, query.input_receipt_id_end],
        )
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description or []]
    return columns, rows
