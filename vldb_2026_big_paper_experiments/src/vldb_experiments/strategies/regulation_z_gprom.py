"""Export v3 evidence and evaluate prospective allocations with GProM/SQLite.

GProM supplies the reenacted target state. The backend evaluates the full policy
query; selection is frozen separately because UPDATE FROM is unsupported by the
installed GProM frontend. This runner never applies the proposed UPDATE.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import time
from typing import TYPE_CHECKING

import duckdb
import sqlglot
from sqlglot import exp

from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
    reset_payment_allocation_database,
)

if TYPE_CHECKING:
    from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
        PaymentAllocationCase,
    )


DEFAULT_GPROM_IMAGE = "iitdbgroup/gprom:latest"


def build_gprom_candidate_query(update_sql: str) -> str:
    """Project the actual UPDATE selection and assignment in SQLite's dialect."""
    update = sqlglot.parse_one(update_sql, read="duckdb")
    if (
        not isinstance(update, exp.Update)
        or update.this.name != "payment_allocations"
        or len(update.expressions) != 1
        or update.expressions[0].this.name != "cash_advance_allocation"
        or not update.args.get("from_")
        or not update.args.get("where")
    ):
        raise ValueError("Expected the single-assignment payment allocation UPDATE FROM")
    assignment = update.expressions[0].expression
    # This sqlglot version silently drops LEAST's second operand for SQLite.
    # Explicit CASE also preserves DuckDB's NULL-ignoring two-argument semantics.
    if not isinstance(assignment, exp.Least) or len(assignment.expressions) != 1:
        raise ValueError("Expected the benchmark's two-argument LEAST assignment")
    left = assignment.this.sql(dialect="sqlite")
    right = assignment.expressions[0].sql(dialect="sqlite")
    value = (
        f"CASE WHEN ({left}) IS NULL THEN ({right}) "
        f"WHEN ({right}) IS NULL THEN ({left}) ELSE MIN({left}, {right}) END"
    )
    return (
        "SELECT allocation.allocation_id, allocation.account_id, "
        "allocation.statement_id, allocation.transaction_id AS payment_id, "
        "selected.transaction_id AS batch_id, "
        f"{value} AS proposed_cash_dollars\n"
        f"FROM {update.this.sql(dialect='sqlite')} CROSS JOIN "
        f"{update.args['from_'].this.sql(dialect='sqlite')}\n"
        f"{update.args['where'].sql(dialect='sqlite')};\n"
    )


def _sqlite_value(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        # NUMERIC affinity retains integral cents/whole dollars as integers.
        # Non-integral numeric values use SQLite's normal floating representation.
        return str(value)
    return value


def export_gprom_workload(
    output_dir: Path, workload: list[PaymentAllocationCase], *, expanded: bool = False
) -> None:
    """Generate a new SQLite database from the existing trusted DuckDB generator."""
    output_dir.mkdir(parents=True, exist_ok=True)
    database = output_dir / "data.sqlite"
    # Do not erase another experiment when the user reuses an output path.
    with database.open("xb"):
        pass
    source = duckdb.connect()
    try:
        reset_payment_allocation_database(source, workload, materialize_context=not expanded)
        if expanded:
            for name in ("regz_context", "balance_components", "allocation_snapshot_context"):
                source.execute(f'DROP TABLE "{name}"')
        counts = {}
        with sqlite3.connect(database) as target:
            for (name,) in source.execute("SHOW TABLES").fetchall():
                columns = source.execute(f'DESCRIBE "{name}"').fetchall()
                definitions = []
                primary = []
                for col, dtype, nullable, key, *_ in columns:
                    affinity = (
                        "TEXT"
                        if dtype in {"VARCHAR", "DATE", "TIMESTAMP"}
                        else "NUMERIC"
                        if dtype.startswith("DECIMAL")
                        else "INTEGER"
                        if dtype in {"BOOLEAN", "BIGINT", "INTEGER"}
                        else None
                    )
                    if affinity is None:
                        raise ValueError(f"Unsupported SQLite export type: {dtype}")
                    definitions.append(
                        f'"{col}" {affinity}' + (" NOT NULL" if nullable == "NO" else "")
                    )
                    if key == "PRI":
                        primary.append(f'"{col}"')
                if primary:
                    definitions.append(f"PRIMARY KEY ({', '.join(primary)})")
                target.execute(f'CREATE TABLE "{name}" ({", ".join(definitions)})')
                rows = source.execute(f'SELECT * FROM "{name}"').fetchall()
                target.executemany(
                    f'INSERT INTO "{name}" VALUES ({", ".join("?" for _ in columns)})',
                    [tuple(_sqlite_value(v) for v in row) for row in rows],
                )
                counts[name] = len(rows)
    finally:
        source.close()
    cases = []
    for case in workload:
        stem = case.allocation_id
        candidate_update = case.sql
        if expanded:
            from vldb_experiments.strategies.regulation_z_expanded import expanded_update

            candidate_update = expanded_update(case.sql)
        (output_dir / f"{stem}.candidate.sql").write_text(
            build_gprom_candidate_query(candidate_update)
        )
        (output_dir / f"{stem}.update.sql").write_text(case.sql + ";\n")
        cases.append(
            {
                "allocation_id": stem,
                "disputed_transaction_id": case.disputed_transaction_id,
                "undisputed_transaction_id": case.undisputed_transaction_id,
                "generator_expected_selection": case.expected_selected_transaction_id,
            }
        )
    if expanded:
        from vldb_experiments.strategies.regulation_z_expanded_gprom import export_standalone_sql

        export_standalone_sql(output_dir)
    else:
        (output_dir / "policy.template.sql").write_text(
            Path(__file__).with_suffix(".sql").read_text()
        )
    (output_dir / "manifest.json").write_text(
        json.dumps(
            {
                "tables": counts,
                "cases": cases,
                "note": "Judge the actual backend-selected contributor, not portable tie ordering.",
            },
            indent=2,
        )
        + "\n"
    )


def build_gprom_reenact(candidate: dict) -> str:
    """Specialize only the frozen target and amount, not a predicted selection."""
    amount = Decimal(str(candidate["proposed_cash_dollars"]))
    if not amount.is_finite() or amount < 0:
        raise ValueError("Proposed cash allocation must be finite and nonnegative")
    target = str(candidate["allocation_id"]).replace("'", "''")
    return (
        "REENACT WITH PROVENANCE (\n"
        "    UPDATE payment_allocations\n"
        f"    SET cash_advance_allocation = {amount}\n"
        f"    WHERE allocation_id = '{target}';\n);\n"
    )


def compose_gprom_policy_query(generated_sql: str, template: str) -> str:
    """Insert the actual backend SELECT; fail rather than accepting an error log."""
    generated_sql = re.sub(r"\x1b\[[0-9;]*m", "", generated_sql).strip().rstrip(";")
    statements = sqlglot.parse(generated_sql, read="sqlite")
    if len(statements) != 1 or not isinstance(statements[0], exp.Query):
        raise ValueError("GProM must generate exactly one read-only query")
    if template.count("{{REENACT_GENERATED_SELECT}}") != 1:
        raise ValueError("Expected exactly one reenactment placeholder")
    return template.replace("{{REENACT_GENERATED_SELECT}}", generated_sql)


def run_gprom_workload(output_dir: Path, *, image: str = DEFAULT_GPROM_IMAGE) -> dict:
    """Invoke actual GProM, evaluate its generated query, and verify no source writes."""
    output_dir = output_dir.resolve()
    database = output_dir / "data.sqlite"
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    manifest = json.loads((output_dir / "manifest.json").read_text())
    template = (output_dir / "policy.template.sql").read_text()
    base = [
        "docker",
        "run",
        "--rm",
        "--network",
        "none",
        "-v",
        f"{output_dir}:/research:ro",
        image,
        "-backend",
        "sqlite",
        "-db",
        "/research/data.sqlite",
    ]
    results = []
    connection = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        for case in manifest["cases"]:
            stem = case["allocation_id"]
            candidates = connection.execute(
                (output_dir / f"{stem}.candidate.sql").read_text()
            ).fetchall()
            if len(candidates) != 1:
                raise ValueError(f"{stem}: expected one frozen candidate, got {len(candidates)}")
            candidate = dict(candidates[0])
            reenact = build_gprom_reenact(candidate)
            (output_dir / f"{stem}.gprom.sql").write_text(reenact)
            command = [*base, "-queryFile", f"/research/{stem}.gprom.sql"]
            # Execute REENACT itself as well as requesting generated SQL, so the
            # experiment records the real engine result rather than a mock rewrite.
            preview = subprocess.run(
                command, check=True, capture_output=True, text=True, timeout=60
            )
            (output_dir / f"{stem}.gprom.txt").write_text(preview.stdout + preview.stderr)
            generated = subprocess.run(
                [*command, "-Pexecutor", "sql"],
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
            (output_dir / f"{stem}.generated.sql").write_text(generated.stdout)
            query = compose_gprom_policy_query(generated.stdout, template)
            (output_dir / f"{stem}.sql").write_text(query)
            (output_dir / f"{stem}.parameters.json").write_text(
                json.dumps(candidate, indent=2) + "\n"
            )
            started = time.perf_counter()
            rows = [dict(row) for row in connection.execute(query, candidate)]
            evaluation_ms = (time.perf_counter() - started) * 1000
            if [row["proposition_id"] for row in rows] != [f"P{n:02}" for n in range(1, 41)]:
                raise ValueError(f"{stem}: incomplete 40-proposition result")
            decisions = {row["decision"] for row in rows}
            selected = candidate["batch_id"]
            if selected not in {case["disputed_transaction_id"], case["undisputed_transaction_id"]}:
                raise ValueError(f"{stem}: unexpected selected balance {selected}")
            expected = "REJECT" if selected == case["disputed_transaction_id"] else "ALLOW"
            if decisions != {expected}:
                raise ValueError(f"{stem}: expected {expected}, got {decisions}")
            (output_dir / f"{stem}.result.json").write_text(json.dumps(rows, indent=2) + "\n")
            results.append(
                {
                    "allocation_id": stem,
                    "selected_balance": selected,
                    "decision": expected,
                    "policy_evaluation_ms": evaluation_ms,
                }
            )
        if connection.execute(
            "SELECT COUNT(*) FROM payment_allocations WHERE cash_advance_allocation <> 0"
        ).fetchone()[0]:
            raise ValueError("Reenactment unexpectedly changed allocation values")
    finally:
        connection.close()
    after = hashlib.sha256(database.read_bytes()).hexdigest()
    if before != after:
        raise ValueError("Source database changed during evaluation")
    image_id = subprocess.run(
        ["docker", "image", "inspect", image, "--format", "{{.Id}}"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    ).stdout.strip()
    summary = {
        "image": image,
        "image_id": image_id,
        "source_unchanged": True,
        "sha256_before": before,
        "sha256_after": after,
        "cases": results,
    }
    (output_dir / "results.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary
