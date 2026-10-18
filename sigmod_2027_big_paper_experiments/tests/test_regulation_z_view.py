"""The unchanged policy catalog must read fresh view results through Rust."""

from data_flow_control.adapters.duckdb import DuckDBAdapter
from data_flow_control.connection import Connection
import duckdb
import pytest

from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
    build_regulation_z_53_policies,
    generate_payment_allocation_workload,
    reset_payment_allocation_database,
)
from vldb_experiments.strategies.regulation_z_view_benchmark import (
    ViewCatalogAdapter,
    prepare_view_database,
)


@pytest.mark.parametrize("no_ties", [False, True])
def test_view_equivalence_and_freshness(no_ties):
    workload = generate_payment_allocation_workload(num_accounts=4, num_updates=4, no_ties=no_ties)
    with duckdb.connect() as table, duckdb.connect() as view:
        reset_payment_allocation_database(table, workload)
        prepare_view_database(view, workload)
        assert view.execute(
            "SELECT table_type FROM information_schema.tables WHERE table_name='regz_context'"
        ).fetchone() == ("VIEW",)
        columns = [r[0] for r in table.execute("DESCRIBE regz_context").fetchall()]
        query = "SELECT " + ", ".join(columns) + " FROM regz_context ORDER BY batch_id"
        assert table.execute(query).fetchall() == view.execute(query).fetchall()
        engines = [Connection(DuckDBAdapter(table)), Connection(ViewCatalogAdapter(view))]
        for engine in engines:
            for policy in build_regulation_z_53_policies():
                engine.register_policy(policy)
        for case in workload:
            for conn, engine in zip((table, view), engines, strict=True):
                conn.execute(engine.transform_query(case.sql))
                value = conn.execute(
                    "SELECT cash_advance_allocation FROM payment_allocations WHERE allocation_id=?",
                    [case.allocation_id],
                ).fetchone()[0]
                assert (value > 0) == case.is_valid
        case = next(c for c in workload if c.is_valid)
        view.execute("UPDATE payment_allocations SET cash_advance_allocation=0")
        view.execute(
            "UPDATE regz_balances SET protected_cents=amount_cents WHERE account_id=? AND balance_id=?",
            [case.account_id, case.expected_selected_transaction_id],
        )
        view.execute(engines[1].transform_query(case.sql))
        assert (
            view.execute(
                "SELECT cash_advance_allocation FROM payment_allocations WHERE allocation_id=?",
                [case.allocation_id],
            ).fetchone()[0]
            == 0
        )
