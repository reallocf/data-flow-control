"""Export the v3 generator and execute checked UPDATEs on a fresh ProvSQL database.

DuckDB derives the evidence; PostgreSQL freezes the selection and checks policies.
Only context evidence is provenance-enabled, avoiding experimental DML tracking.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import json
import math
from pathlib import Path
import subprocess
import time
from typing import TYPE_CHECKING
from uuid import uuid4

import duckdb
import sqlglot
from sqlglot import exp

from vldb_experiments.strategies.regulation_z_53_full_policy_catalog import (
    FULL_REGULATION_Z_53_POLICY_SPECS,
)
from vldb_experiments.strategies.regulation_z_expanded import (
    UPDATE_BINDING,
    decision_sql,
    expanded_update,
)
from vldb_experiments.strategies.regulation_z_expanded_validation import VALID_ALLOCATION_SQL
from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
    reset_payment_allocation_database,
)

if TYPE_CHECKING:
    from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
        PaymentAllocationCase,
    )


def _literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return str(value).upper()
    if isinstance(value, (int, Decimal)):
        return str(value)
    if isinstance(value, float) and math.isfinite(value):
        return repr(value)
    if isinstance(value, (date, datetime)):
        return "'" + value.isoformat() + "'"
    if isinstance(value, str):
        return "E'" + value.replace("\\", "\\\\").replace("'", "''") + "'"
    raise ValueError(f"Unsupported export value: {value!r}")


def build_provsql_query(case: PaymentAllocationCase, *, expanded: bool = False) -> str:
    """Expand the checked transaction around the generator's exact selection."""
    sql = expanded_update(case.sql) if expanded else case.sql
    update = sqlglot.parse_one(sql, read="duckdb")
    if (
        not isinstance(update, exp.Update)
        or update.this.name != "payment_allocations"
        or len(update.expressions) != 1
        or update.expressions[0].this.name != "cash_advance_allocation"
        or not update.args.get("from_")
        or not update.args.get("where")
    ):
        raise ValueError("Expected the generator's single-assignment allocation UPDATE")
    candidate = (
        "SELECT allocation.allocation_id, "
        "selected.transaction_id AS selected_transaction_id, "
        f"{update.expressions[0].expression.sql(dialect='postgres')} AS proposed_cash\n"
        f"FROM {update.this.sql(dialect='postgres')}, "
        f"{update.args['from_'].this.sql(dialect='postgres')}\n"
        f"{update.args['where'].sql(dialect='postgres')}"
    )
    template_path = Path(__file__).with_suffix(".sql")
    if expanded:
        template_path = template_path.with_name("regulation_z_provsql_expanded.sql")
    template = template_path.read_text()
    policies = [spec for spec in FULL_REGULATION_Z_53_POLICY_SPECS if spec.constraint is not None]
    values = ",\n".join(
        f"    ('{spec.proposition_id}', ({spec.constraint.replace('regz_context.', 'c.')}))"
        for spec in policies
    )
    if expanded:
        values = ",\n".join(
            f"('{spec.proposition_id}', (f.selected_transaction_id IN (\n"
            f"{VALID_ALLOCATION_SQL}\n) AND f.selected_transaction_id IN (\n"
            f"{decision_sql(spec.number)}\n)))"
            for spec in policies
        )
        binding = UPDATE_BINDING.replace(
            "allocation.cash_advance_allocation", "f.proposed_cash"
        ).replace("selected.transaction_id", "f.selected_transaction_id")
        template = template.replace("{{update_binding}}", binding).replace(
            "{{valid_input}}", VALID_ALLOCATION_SQL
        )
    return (
        template.replace("{{allocation_id_literal}}", _literal(case.allocation_id))
        .replace("{{candidate_query}}", candidate)
        .replace("{{policy_values}}", values)
        .replace("{{policy_count}}", str(len(policies)))
    )


def export_provsql_workload(
    output_dir: Path, workload: list[PaymentAllocationCase], *, expanded: bool = False
) -> None:
    """Preserve schema, exact decimal values, and all generated evidence tables."""
    output_dir.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect()
    try:
        reset_payment_allocation_database(conn, workload, materialize_context=not expanded)
        if expanded:
            for table in ("regz_context", "balance_components", "allocation_snapshot_context"):
                conn.execute(f"DROP TABLE {table}")
        counts = {}
        with (output_dir / "data.sql").open("x") as stream:
            stream.write("BEGIN;\nSET standard_conforming_strings = on;\n")
            for name, ddl in conn.execute(
                "SELECT table_name, sql FROM duckdb_tables() ORDER BY table_name"
            ).fetchall():
                stream.write(sqlglot.transpile(ddl, read="duckdb", write="postgres")[0] + ";\n")
                rows = conn.execute(f'SELECT * FROM "{name}"').fetchall()
                counts[name] = len(rows)
                for row in rows:
                    stream.write(
                        f'INSERT INTO "{name}" VALUES ({", ".join(map(_literal, row))});\n'
                    )
            provenance_table = "regz_proposals" if expanded else "regz_context"
            stream.write(
                "CREATE EXTENSION provsql CASCADE;\n"
                f"SELECT provsql.add_provenance('{provenance_table}');\nCOMMIT;\n"
            )
    finally:
        conn.close()
    cases = []
    for case in workload:
        (output_dir / f"{case.allocation_id}.sql").write_text(
            build_provsql_query(case, expanded=expanded)
        )
        (output_dir / f"{case.allocation_id}.original.sql").write_text(case.sql + ";\n")
        cases.append(
            {
                "allocation_id": case.allocation_id,
                "disputed_transaction_id": case.disputed_transaction_id,
                "undisputed_transaction_id": case.undisputed_transaction_id,
                "generator_expected_selection": case.expected_selected_transaction_id,
                "candidate_cash": {
                    row.transaction_id: str(
                        min(
                            Decimal(str(case.total_payment))
                            - Decimal(str(case.minimum_payment_portion)),
                            Decimal(str(row.amount)),
                        )
                    )
                    for row in case.transactions
                    if row.transaction_type == "cash_advance"
                },
            }
        )
    (output_dir / "manifest.json").write_text(
        json.dumps({"expanded": expanded, "tables": counts, "cases": cases}, indent=2) + "\n"
    )


def run_provsql_workload(output_dir: Path, *, container: str = "provsql-tpch-dfc") -> dict:
    """Execute exported transactions, preserving a fresh database and run logs."""
    database = "regz_provsql_" + uuid4().hex[:12]

    def execute(sql: str, *, db: str = database) -> subprocess.CompletedProcess:
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
                db,
                "-v",
                "ON_ERROR_STOP=1",
            ],
            input="SET statement_timeout='90s'; SET jit=off;\n" + sql,
            text=True,
            capture_output=True,
            timeout=120,
        )

    created = execute(f'CREATE DATABASE "{database}";', db="postgres")
    if created.returncode:
        raise RuntimeError(created.stderr)
    (output_dir / "database.txt").write_text(database + "\n")
    loaded = execute((output_dir / "data.sql").read_text())
    (output_dir / "load.log").write_text(loaded.stdout + loaded.stderr)
    if loaded.returncode:
        raise RuntimeError(f"ProvSQL load failed in {database}: {loaded.stderr}")
    manifest = json.loads((output_dir / "manifest.json").read_text())
    for table, count in manifest["tables"].items():
        loaded_count = execute(f'SET provsql.active = off; SELECT count(*) FROM "{table}";')
        if loaded_count.returncode or int(loaded_count.stdout.strip()) != count:
            raise RuntimeError(f"Row count mismatch after loading {table}")
    cases = []
    for case in manifest["cases"]:
        allocation_id = case["allocation_id"]
        start = time.perf_counter()
        run = execute((output_dir / f"{allocation_id}.sql").read_text())
        elapsed_ms = (time.perf_counter() - start) * 1000
        (output_dir / f"{allocation_id}.log").write_text(run.stdout + run.stderr)
        checks = [
            line.split("|")
            for line in run.stdout.splitlines()
            if len(line.split("|")) == 4 and line.split("|")[2].startswith("P")
        ]
        rejected = run.returncode != 0 and (
            "Regulation Z proposal rejected; inspect policy results" in run.stderr
        )
        policy_ids = {
            spec.proposition_id
            for spec in FULL_REGULATION_Z_53_POLICY_SPECS
            if spec.constraint is not None
        }
        if (
            len(checks) != len(policy_ids)
            or {row[2] for row in checks} != policy_ids
            or len({(row[0], row[1]) for row in checks}) != 1
            or (run.returncode and not rejected)
        ):
            raise RuntimeError(f"Unexpected query failure for {allocation_id}: {run.stderr}")
        selected = checks[0][1]
        if selected not in (case["disputed_transaction_id"], case["undisputed_transaction_id"]):
            raise RuntimeError(f"Unexpected contributor: {selected}")
        state = execute(
            "SELECT cash_advance_allocation FROM payment_allocations "
            f"WHERE allocation_id = {_literal(allocation_id)};"
        )
        if state.returncode:
            raise RuntimeError(state.stderr)
        actual = Decimal(state.stdout.strip())
        expected_rejection = selected == case["disputed_transaction_id"]
        expected_cash = Decimal(case["candidate_cash"][selected])
        verified = rejected == expected_rejection and actual == (0 if rejected else expected_cash)
        cases.append(
            {
                **case,
                "selected_transaction_id": selected,
                "decision": "REJECT" if rejected else "PASS",
                "failed_propositions": [row[2] for row in checks if row[3] != "t"],
                "cash_advance_allocation": str(actual),
                "verified": verified,
                "elapsed_ms_including_client": elapsed_ms,
            }
        )
    result = {
        "database": database,
        "container": container,
        "expanded": manifest.get("expanded", False),
        "jit": False,
        "cases": cases,
    }
    (output_dir / "results.json").write_text(json.dumps(result, indent=2) + "\n")
    if not all(case["verified"] for case in cases):
        raise RuntimeError("ProvSQL results disagree with selected-contributor or sink checks")
    return result
