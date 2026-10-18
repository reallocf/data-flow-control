"""Execute inline policies, including correction evidence and sink binding."""

from datetime import date

from data_flow_control.adapters.duckdb import DuckDBAdapter
from data_flow_control.connection import Connection
import duckdb
import pytest

from vldb_experiments.strategies.regulation_z_53_runtime import (
    create_schema,
    derive_correction_context,
)
from vldb_experiments.strategies.regulation_z_expanded import (
    build_expanded_policies,
    correction_facts_sql,
    expanded_update,
)
from vldb_experiments.strategies.regulation_z_expanded_benchmark import prepare_expanded_database
from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
    generate_payment_allocation_workload,
)


@pytest.mark.parametrize("mutation", ["none", "missing", "recipient", "late", "blank", "reversal"])
def test_correction_execution(mutation):
    with duckdb.connect() as conn:
        create_schema(conn)
        conn.execute(
            "INSERT INTO regz_error_obligations VALUES ('E','2026-01-01','2026-02-01','customer')"
        )
        conn.execute(
            "INSERT INTO regz_error_items VALUES ('E','principal',20000),('E','charge',1000)"
        )
        conn.execute(
            "INSERT INTO regz_credits VALUES ('C1','E','principal',20000,'2026-01-15'),('C2','E','charge',1000,'2026-01-15')"
        )
        conn.execute(
            "INSERT INTO regz_dispatches VALUES ('D','E','customer','correction','2026-01-15','document')"
        )
        conn.execute("INSERT INTO regz_correction_completions VALUES ('E','2026-02-03')")
        changes = {
            "missing": "DELETE FROM regz_credits WHERE credit_id='C2'",
            "recipient": "UPDATE regz_dispatches SET recipient='other'",
            "late": "UPDATE regz_dispatches SET sent_date='2026-02-02'",
            "blank": "UPDATE regz_dispatches SET content_evidence=' '",
            "reversal": "INSERT INTO regz_credits VALUES ('C3','E','principal',-1,'2026-01-16')",
        }
        if mutation in changes:
            conn.execute(changes[mutation])
        expected = derive_correction_context(conn, "E", date(2026, 2, 3))
        assert conn.execute(correction_facts_sql()).fetchone() == (
            "correction:E",
            expected["missing_credit_cents"],
            expected["notice_count"],
        )
        conn.execute("DROP TABLE regz_context")
        rewriter = Connection(DuckDBAdapter(conn))
        for policy in build_expanded_policies(binding="correction"):
            rewriter.register_policy(policy)
        conn.execute(
            rewriter.transform_query(
                "INSERT INTO regz_acceptances SELECT 'correction:' || completion.error_id, 'correction' FROM regz_correction_completions AS completion"
            )
        )
        assert conn.execute("SELECT COUNT(*) FROM regz_acceptances").fetchone()[0] == (
            mutation == "none"
        )


@pytest.mark.parametrize("no_ties", [False, True])
def test_updates_without_materialized_tables(no_ties):
    workload = generate_payment_allocation_workload(num_accounts=4, num_updates=4, no_ties=no_ties)
    with duckdb.connect() as conn:
        prepare_expanded_database(conn, workload)
        assert {r[0] for r in conn.execute("SHOW TABLES").fetchall()}.isdisjoint(
            {"regz_context", "balance_components", "allocation_snapshot_context"}
        )
        rewriter = Connection(DuckDBAdapter(conn))
        for policy in build_expanded_policies():
            rewriter.register_policy(policy)
        for case in workload:
            conn.execute(rewriter.transform_query(expanded_update(case.sql)))
            value = conn.execute(
                "SELECT cash_advance_allocation FROM payment_allocations WHERE allocation_id=?",
                [case.allocation_id],
            ).fetchone()[0]
            assert (value > 0) == case.is_valid


def test_proposed_amount_and_fresh_dispute_evidence():
    workload = generate_payment_allocation_workload(num_accounts=4, num_updates=4)
    case = next(c for c in workload if c.is_valid)
    with duckdb.connect() as conn:
        prepare_expanded_database(conn, workload)
        rewriter = Connection(DuckDBAdapter(conn))
        for policy in build_expanded_policies():
            rewriter.register_policy(policy)
        query = expanded_update(case.sql)
        changed = query.replace("LEAST(", "LEAST(99, ", 1)
        assert changed != query
        conn.execute(rewriter.transform_query(changed))
        assert (
            conn.execute(
                "SELECT cash_advance_allocation FROM payment_allocations WHERE allocation_id=?",
                [case.allocation_id],
            ).fetchone()[0]
            == 0
        )
        conn.execute(
            "UPDATE regz_balances SET protected_cents=amount_cents WHERE account_id=? AND balance_id=?",
            [case.account_id, case.expected_selected_transaction_id],
        )
        conn.execute(rewriter.transform_query(query))
        assert (
            conn.execute(
                "SELECT cash_advance_allocation FROM payment_allocations WHERE allocation_id=?",
                [case.allocation_id],
            ).fetchone()[0]
            == 0
        )
