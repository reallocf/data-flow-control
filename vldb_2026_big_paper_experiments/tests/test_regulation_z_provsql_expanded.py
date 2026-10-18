"""Verify raw-evidence exports and optional execution in the actual ProvSQL server."""

import json
import os
import subprocess

import duckdb
import pytest

from vldb_experiments.strategies import regulation_z_v3_evidence as evidence
from vldb_experiments.strategies.regulation_z_53_full_policy_catalog import (
    FULL_REGULATION_Z_53_POLICY_SPECS,
)
from vldb_experiments.strategies.regulation_z_expanded import decision_sql
from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
    generate_payment_allocation_workload,
    reset_payment_allocation_database,
)
from vldb_experiments.strategies.regulation_z_provsql import (
    export_provsql_workload,
    run_provsql_workload,
)


def test_expanded_export_does_not_derive_or_export_summaries(tmp_path, monkeypatch):
    def forbidden(*_args):
        pytest.fail("Expanded export must not compute stored context")

    monkeypatch.setattr(evidence, "derive_allocation_context", forbidden)
    workload = generate_payment_allocation_workload(num_accounts=4, num_updates=4)
    export_provsql_workload(tmp_path, workload, expanded=True)
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["expanded"] is True
    assert set(manifest["tables"]).isdisjoint(
        {"regz_context", "balance_components", "allocation_snapshot_context"}
    )
    assert manifest["tables"]["regz_proposals"] == 8


@pytest.mark.parametrize("no_ties", [False, True])
def test_live_expanded_updates_and_evidence_mutations(tmp_path, no_ties):
    container = os.environ.get("PROVSQL_TEST_CONTAINER")
    if not container:
        pytest.skip("Set PROVSQL_TEST_CONTAINER to run actual PostgreSQL/ProvSQL")
    workload = generate_payment_allocation_workload(num_accounts=4, num_updates=4, no_ties=no_ties)
    export_provsql_workload(tmp_path, workload, expanded=True)
    result = run_provsql_workload(tmp_path, container=container)
    assert sorted(c["decision"] for c in result["cases"]) == ["PASS", "PASS", "REJECT", "REJECT"]
    assert all(c["verified"] for c in result["cases"])
    allowed = next(c for c in result["cases"] if c["decision"] == "PASS")
    allocation_id = allowed["allocation_id"]
    selected = allowed["selected_transaction_id"]
    script = (tmp_path / f"{allocation_id}.sql").read_text()

    def execute(sql):
        return subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                container,
                "psql",
                "-X",
                "-qAt",
                "-U",
                "postgres",
                "-d",
                result["database"],
                "-v",
                "ON_ERROR_STOP=1",
            ],
            input="SET jit=off; SET statement_timeout='30s';\n" + sql,
            text=True,
            capture_output=True,
            timeout=45,
        )

    tables = execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
    )
    assert tables.returncode == 0, tables.stderr
    assert set(tables.stdout.splitlines()).isdisjoint(
        {"regz_context", "balance_components", "allocation_snapshot_context"}
    )
    # Compare every policy for both candidate proposals, not only the P17 path
    # exercised by the chosen UPDATE. No expected labels enter the live query.
    with duckdb.connect() as reference:
        reset_payment_allocation_database(reference, workload)
        for spec in FULL_REGULATION_Z_53_POLICY_SPECS:
            if spec.constraint is None:
                continue
            expected = reference.execute(
                "SELECT batch_id FROM regz_context WHERE " + spec.constraint + " ORDER BY batch_id"
            ).fetchall()
            actual = execute(
                "SET provsql.active=off; SELECT batch_id FROM ("
                + decision_sql(spec.number)
                + ") eligible ORDER BY batch_id;"
            )
            assert actual.returncode == 0, actual.stderr
            assert actual.stdout.splitlines() == [row[0] for row in expected], spec.proposition_id
    for mutation, expected_error in (
        (
            f"UPDATE regz_balances SET protected_cents=amount_cents WHERE balance_id='{selected}';",
            "Regulation Z proposal rejected; inspect policy results",
        ),
        (
            f"DELETE FROM regz_proposal_lines WHERE batch_id='{selected}';",
            "Invalid raw allocation evidence",
        ),
        (
            "UPDATE transactions SET amount=99 WHERE transaction_type='cash_advance' "
            f"AND account_id=(SELECT account_id FROM payment_allocations WHERE allocation_id='{allocation_id}');",
            "UPDATE does not match complete raw proposal",
        ),
    ):
        # Inject into the checked transaction so the expected error rolls back
        # both the adversarial evidence change and any attempted sink write.
        altered = script.replace(
            "SET LOCAL provsql.update_provenance = off;",
            "SET LOCAL provsql.update_provenance = off;\n" + mutation,
        )
        run = execute(altered)
        assert run.returncode != 0
        assert run.stderr.splitlines()[0] == "ERROR:  " + expected_error
        state = execute(
            f"SELECT cash_advance_allocation FROM payment_allocations WHERE allocation_id='{allocation_id}';"
        )
        assert state.returncode == 0, state.stderr
        assert state.stdout.strip() == "100.00"
