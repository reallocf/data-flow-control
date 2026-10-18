from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from vldb_experiments.strategies.regulation_z_gprom import (
    build_gprom_candidate_query,
    build_gprom_reenact,
    compose_gprom_policy_query,
    export_gprom_workload,
    run_gprom_workload,
)
from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
    generate_payment_allocation_workload,
)


def test_export_and_candidate_preserve_cap_and_evidence(tmp_path: Path) -> None:
    workload = generate_payment_allocation_workload(num_accounts=4, num_updates=4, no_ties=True)
    export_gprom_workload(tmp_path, workload)
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    with sqlite3.connect(tmp_path / "data.sqlite") as conn:
        counts = {
            name: conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            for name in manifest["tables"]
        }
        assert counts == manifest["tables"]
        assert counts["regz_context"] == 8
        for case in workload:
            query = (tmp_path / f"{case.allocation_id}.candidate.sql").read_text()
            row = conn.execute(query).fetchone()
            assert row == (
                case.allocation_id,
                case.account_id,
                case.statement_id,
                case.payment_transaction_id,
                case.expected_selected_transaction_id,
                100,
            )
            # Catch sqlglot's silent SQLite LEAST -> first operand translation.
            conn.execute(
                "UPDATE transactions SET amount = 40 WHERE transaction_id = ?",
                [case.expected_selected_transaction_id],
            )
            assert conn.execute(query).fetchone() == (*row[:-1], 40)
            conn.execute(
                "UPDATE transactions SET amount = NULL WHERE transaction_id = ?",
                [case.expected_selected_transaction_id],
            )
            assert conn.execute(query).fetchone() == row
            assert (tmp_path / f"{case.allocation_id}.update.sql").read_text() == case.sql + ";\n"
    before = hashlib.sha256((tmp_path / "data.sqlite").read_bytes()).hexdigest()
    with pytest.raises(FileExistsError):
        export_gprom_workload(tmp_path, workload)
    assert hashlib.sha256((tmp_path / "data.sqlite").read_bytes()).hexdigest() == before


def test_reject_unsupported_update_and_unsafe_generated_sql() -> None:
    with pytest.raises(ValueError, match="Expected the single-assignment"):
        build_gprom_candidate_query("UPDATE payment_allocations SET purchase_allocation = 10")
    with pytest.raises(ValueError, match="exactly one read-only query"):
        compose_gprom_policy_query(
            "DELETE FROM payment_allocations", "{{REENACT_GENERATED_SELECT}}"
        )
    with pytest.raises(ValueError, match="exactly one read-only query"):
        compose_gprom_policy_query(
            "SELECT 1; DELETE FROM payment_allocations", "{{REENACT_GENERATED_SELECT}}"
        )
    assert build_gprom_reenact({"allocation_id": "a'b", "proposed_cash_dollars": 100}) == (
        "REENACT WITH PROVENANCE (\n"
        "    UPDATE payment_allocations\n"
        "    SET cash_advance_allocation = 100\n"
        "    WHERE allocation_id = 'a''b';\n);\n"
    )


@pytest.mark.parametrize("no_ties", [False, True])
def test_live_gprom_full_query_is_read_only(tmp_path: Path, no_ties: bool) -> None:
    if os.environ.get("GPROM_TEST_DOCKER") != "1":
        pytest.skip("Set GPROM_TEST_DOCKER=1 to run the actual GProM image")
    workload = generate_payment_allocation_workload(num_accounts=4, num_updates=4, no_ties=no_ties)
    export_gprom_workload(tmp_path, workload)
    summary = run_gprom_workload(tmp_path)
    assert summary["source_unchanged"] is True
    assert sorted(case["decision"] for case in summary["cases"]) == [
        "ALLOW",
        "ALLOW",
        "REJECT",
        "REJECT",
    ]
    with sqlite3.connect(tmp_path / "data.sqlite") as conn:
        conn.execute("PRAGMA query_only=ON")
        for case in workload:
            stem = case.allocation_id
            params = json.loads((tmp_path / f"{stem}.parameters.json").read_text())
            if no_ties:
                assert params["batch_id"] == case.expected_selected_transaction_id
            generated = (tmp_path / f"{stem}.generated.sql").read_text()
            cursor = conn.execute(generated)
            names = [c[0] for c in cursor.description]
            proposed = [dict(zip(names, row)) for row in cursor.fetchall()]
            target = next(row for row in proposed if row["ALLOCATION_ID"] == stem)
            assert target["CASH_ADVANCE_ALLOCATION"] == 100
            assert target["PROV_PAYMENT__ALLOCATIONS_CASH__ADVANCE__ALLOCATION"] == 0
            rows = json.loads((tmp_path / f"{stem}.result.json").read_text())
            assert [r["proposition_id"] for r in rows] == [f"P{n:02}" for n in range(1, 41)]
            query = (tmp_path / f"{stem}.sql").read_text()
            invalid = conn.execute(query, {**params, "batch_id": "missing"}).fetchall()
            assert {r[2] for r in invalid} == {"INPUT_ERROR"}
        assert (
            conn.execute("SELECT cash_advance_allocation FROM payment_allocations").fetchall()
            == [(0,)] * 4
        )
