from __future__ import annotations

from decimal import Decimal

import duckdb


def _run_tied_allocation(insertion_order: tuple[str, str]) -> tuple[str, tuple[object, ...]]:
    rows = {
        "disputed": (
            "BAL-DISPUTED",
            "ACC-001",
            "cash_advance",
            Decimal("25.000"),
            Decimal("100.00"),
            "open",
        ),
        "undisputed": (
            "BAL-UNDISPUTED",
            "ACC-001",
            "cash_advance",
            Decimal("25.000"),
            Decimal("100.00"),
            "none",
        ),
    }
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(
            "CREATE TABLE balance_components ("
            "component_id VARCHAR PRIMARY KEY, account_id VARCHAR, "
            "balance_category VARCHAR, apr DECIMAL(5, 3), "
            "balance_amount DECIMAL(10, 2), dispute_status VARCHAR)"
        )
        conn.executemany(
            "INSERT INTO balance_components VALUES (?, ?, ?, ?, ?, ?)",
            [rows[key] for key in insertion_order],
        )
        selected_component = conn.execute(
            "SELECT component_id FROM balance_components ORDER BY apr DESC LIMIT 1"
        ).fetchone()[0]
        visible_allocation = conn.execute(
            "SELECT account_id, balance_category, "
            "LEAST(100.00, balance_amount) AS allocated_amount "
            "FROM balance_components ORDER BY apr DESC LIMIT 1"
        ).fetchone()
        return selected_component, visible_allocation
    finally:
        conn.close()


def test_tied_top_one_follows_physical_insertion_order_but_hides_provenance() -> None:
    disputed_first = [_run_tied_allocation(("disputed", "undisputed")) for _ in range(25)]
    undisputed_first = [_run_tied_allocation(("undisputed", "disputed")) for _ in range(25)]

    expected_visible_allocation = (
        "ACC-001",
        "cash_advance",
        Decimal("100.00"),
    )
    assert disputed_first == [("BAL-DISPUTED", expected_visible_allocation)] * 25
    assert undisputed_first == [("BAL-UNDISPUTED", expected_visible_allocation)] * 25
