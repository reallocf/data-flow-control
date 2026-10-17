"""Full-catalog evidence for both possible contributors to each v3 allocation.

Neither the expected selection nor the oracle label is used in these derivations.
The benchmark query joins the context for the contributor it actually selects.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any

from vldb_experiments.strategies.regulation_z_53_runtime import (
    SCHEMA_DDL,
    create_schema,
    derive_allocation_context,
)

if TYPE_CHECKING:
    from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
        PaymentAllocationCase,
    )

EVIDENCE_TABLES = (*(ddl.split()[2] for ddl in SCHEMA_DDL), "regz_context")


def reset_full_evidence(conn: Any, workload: list[PaymentAllocationCase]) -> None:
    for table in EVIDENCE_TABLES:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    create_schema(conn)
    for case in workload:
        load_case_evidence(conn, case)


def load_case_evidence(conn: Any, case: PaymentAllocationCase) -> None:
    """Add real terms/history and derive a complete proposal for each candidate."""
    account, payment = case.account_id, case.payment_transaction_id
    minimum_lot, promo_lot = f"{account}-MIN", f"{account}-PROMO"
    program = f"{account}-PROGRAM"
    conn.execute("INSERT INTO regz_accounts VALUES (?, true, true, true, false)", [account])
    conn.execute(
        "INSERT INTO regz_payments VALUES (?, ?, ?, ?, ?, ?)",
        [
            payment,
            account,
            case.payment_date,
            case.cycle_end,
            round(case.total_payment * 100),
            round(case.minimum_payment_portion * 100),
        ],
    )
    conn.execute(
        "INSERT INTO regz_practices VALUES (?, ?, 'cycle_end', 0, NULL)",
        [account, case.cycle_start],
    )
    # An active deferred program outside its priority window exercises program
    # classification and zero allocation APR without changing the tied top-1.
    expiration = case.cycle_start + timedelta(days=149)
    window_start = case.cycle_start + timedelta(days=60)
    conn.execute(
        "INSERT INTO regz_programs VALUES (?, ?, ?, ?, true, true, false, false, true, true)",
        [program, account, case.cycle_start, expiration],
    )
    for index in range(5):
        start = case.cycle_start + timedelta(days=30 * index)
        end = start + timedelta(days=29)
        conn.execute(
            "INSERT INTO regz_cycles VALUES (?, ?, ?, ?)",
            [account, start, end, end + timedelta(days=10)],
        )
    charges = sorted(
        (row for row in case.transactions if row.transaction_type == "cash_advance"),
        key=lambda row: row.transaction_id,
    )
    for snapshot in (case.cycle_end, case.payment_date):
        conn.execute("INSERT INTO regz_snapshot_sets VALUES (?, ?)", [account, snapshot])
        for row in charges:
            amount = round(row.amount * 100)
            conn.execute(
                "INSERT INTO regz_balances VALUES (?, ?, ?, ?, ?, ?, false, NULL)",
                [
                    account,
                    row.transaction_id,
                    snapshot,
                    amount,
                    row.apr_applied,
                    amount if row.is_disputed else 0,
                ],
            )
        conn.execute(
            "INSERT INTO regz_balances VALUES (?, ?, ?, ?, 10, 0, false, NULL)",
            [account, minimum_lot, snapshot, round(case.minimum_payment_portion * 100)],
        )
        conn.execute(
            "INSERT INTO regz_balances VALUES (?, ?, ?, 5000, 15, 0, false, ?)",
            [account, promo_lot, snapshot, program],
        )
    for selected in charges:
        batch = selected.transaction_id
        conn.execute(
            "INSERT INTO regz_proposals VALUES (?, ?, ?, ?, 'default', NULL, NULL)",
            [batch, payment, case.cycle_end, case.payment_date],
        )
        conn.execute(
            "INSERT INTO regz_program_classifications VALUES (?, ?, true)", [batch, program]
        )
        for row in charges:
            excess = round((case.total_payment - case.minimum_payment_portion) * 100)
            conn.execute(
                "INSERT INTO regz_proposal_lines VALUES (?, ?, 0, ?, ?, NULL)",
                [batch, row.transaction_id, excess if row == selected else 0, row.apr_applied],
            )
        conn.execute(
            "INSERT INTO regz_proposal_lines VALUES (?, ?, ?, 0, 10, NULL)",
            [batch, minimum_lot, round(case.minimum_payment_portion * 100)],
        )
        conn.execute(
            "INSERT INTO regz_proposal_lines VALUES (?, ?, 0, 0, 0, ?)",
            [batch, promo_lot, window_start],
        )
        context = derive_allocation_context(conn, batch)
        columns = list(context)
        conn.execute(
            f"INSERT INTO regz_context ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
            [context[column] for column in columns],
        )


def full_evidence_for_prompt(conn: Any) -> dict[str, list[dict]]:
    """Stable logical ordering exposes both candidates, never their physical order."""
    evidence = {}
    for table in EVIDENCE_TABLES:
        cursor = conn.execute(f"SELECT * FROM {table} ORDER BY ALL")
        names = [column[0] for column in cursor.description]
        evidence[table] = [dict(zip(names, row)) for row in cursor.fetchall()]
    return evidence
