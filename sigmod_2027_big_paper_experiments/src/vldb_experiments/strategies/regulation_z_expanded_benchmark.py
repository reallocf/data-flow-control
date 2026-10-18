"""DFC-only paired experiment: materialized summaries versus inline constraints."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
import time

from data_flow_control.adapters.duckdb import DuckDBAdapter
from data_flow_control.connection import Connection
import duckdb
from experiment_harness import ExperimentResult, ExperimentStrategy

from vldb_experiments.strategies.regulation_z_expanded import (
    build_expanded_policies,
    expanded_update,
)
from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
    build_regulation_z_53_policies,
    generate_payment_allocation_workload,
    reset_payment_allocation_database,
)


def prepare_expanded_database(conn, workload):
    reset_payment_allocation_database(conn, workload, materialize_context=False)
    for table in ("regz_context", "balance_components", "allocation_snapshot_context"):
        conn.execute(f"DROP TABLE {table}")


def export_expanded_policies(output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    rows = []
    for policy in build_expanded_policies():
        pid = policy.description.split(":", 1)[0]
        text = (
            "SOURCE transactions AS selected\nSINK payment_allocations AS allocation\n"
            f"CONSTRAINT\n{policy.constraint}\nON FAIL REMOVE\n"
        )
        # PGN parsing is prohibitively slow for deeply nested constraints.
        # Execution uses the validated Policy constructor, as the benchmark does.
        (output / f"{pid}.pgn").write_text(text)
        rows.append(
            {
                "id": pid,
                "description": policy.description,
                "constraint": policy.constraint,
                "bytes": len(text.encode()),
                "sha256": hashlib.sha256(text.encode()).hexdigest(),
            }
        )
    (output / "policies.json").write_text(json.dumps(rows, indent=2) + "\n")
    correction_dir = output / "correction"
    correction_dir.mkdir(exist_ok=True)
    for policy in build_expanded_policies(binding="correction"):
        pid = policy.description.split(":", 1)[0]
        (correction_dir / f"{pid}.pgn").write_text(
            "SOURCE regz_correction_completions AS completion\nSINK regz_acceptances\n"
            f"CONSTRAINT\n{policy.constraint}\nON FAIL REMOVE\n"
        )


def audit_expanded_policies(workload, output: Path):
    rows = []
    with duckdb.connect() as conn:
        prepare_expanded_database(conn, workload)
        expected_blocked = {c.allocation_id for c in workload if not c.is_valid}
        for policy in build_expanded_policies():
            pid = policy.description.split(":", 1)[0]
            conn.execute("UPDATE payment_allocations SET cash_advance_allocation=0")
            rewriter = Connection(DuckDBAdapter(conn))
            rewriter.register_policy(policy)
            for case in workload:
                conn.execute(rewriter.transform_query(expanded_update(case.sql)))
            blocked = {
                r[0]
                for r in conn.execute(
                    "SELECT allocation_id FROM payment_allocations WHERE cash_advance_allocation=0"
                ).fetchall()
            }
            expected = expected_blocked if pid == "P17" else set()
            rows.append(
                {
                    "policy": pid,
                    "executions": len(workload),
                    "blocked": len(blocked),
                    "expected_blocked": len(expected),
                    "matches_expected_ids": blocked == expected,
                }
            )
            print(
                f"  {pid}: {len(blocked)} blocked; exact IDs match={blocked == expected}",
                flush=True,
            )
    with output.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    if not all(r["matches_expected_ids"] for r in rows):
        raise AssertionError("Expanded policy-isolation audit failed")
    return rows


class ExpandedRegulationZStrategy(ExperimentStrategy):
    def setup(self, context):
        config = context.strategy_config
        self.workload = generate_payment_allocation_workload(
            num_accounts=config["num_accounts"],
            num_updates=config["num_accounts"],
            no_ties=config["no_ties"],
        )
        self.connections = {name: duckdb.connect() for name in ("materialized", "expanded")}
        self.rewriters = {}
        for name, conn in self.connections.items():
            if name == "expanded":
                prepare_expanded_database(conn, self.workload)
                policies = build_expanded_policies()
            else:
                reset_payment_allocation_database(conn, self.workload)
                policies = build_regulation_z_53_policies()
            self.rewriters[name] = Connection(DuckDBAdapter(conn))
            for policy in policies:
                self.rewriters[name].register_policy(policy)
        self.output = Path(config["output"])

    def execute(self, context):
        rows = []
        metrics = {
            "num_updates": len(self.workload),
            "no_ties": context.strategy_config["no_ties"],
            "policy_count": 31,
        }
        decisions = {}
        for name, conn in self.connections.items():
            conn.execute("UPDATE payment_allocations SET cash_advance_allocation=0")
            rewrite_ms = execute_ms = 0.0
            decisions[name] = []
            for index, case in enumerate(self.workload):
                query = expanded_update(case.sql) if name == "expanded" else case.sql
                start = time.perf_counter()
                rewritten = self.rewriters[name].transform_query(query)
                rewrite_time = (time.perf_counter() - start) * 1000
                start = time.perf_counter()
                conn.execute(rewritten)
                execution_time = (time.perf_counter() - start) * 1000
                applied = (
                    conn.execute(
                        "SELECT cash_advance_allocation FROM payment_allocations WHERE allocation_id=?",
                        [case.allocation_id],
                    ).fetchone()[0]
                    != 0
                )
                decisions[name].append(applied)
                rewrite_ms += rewrite_time
                execute_ms += execution_time
                rows.append(
                    {
                        "approach": name,
                        "allocation_id": case.allocation_id,
                        "expected_allow": case.is_valid,
                        "actual_allow": applied,
                        "rewrite_ms": rewrite_time,
                        "execute_ms": execution_time,
                        "rewritten_bytes": len(rewritten.encode()),
                    }
                )
                if index == 0:
                    (self.output / f"{name}.example.sql").write_text(rewritten + ";\n")
                if (index + 1) % 20 == 0:
                    print(f"{name}: {index + 1}/{len(self.workload)}", flush=True)
            correct = sum(
                a == c.is_valid for a, c in zip(decisions[name], self.workload, strict=True)
            )
            metrics.update(
                {
                    f"{name}_correct": correct,
                    f"{name}_rewrite_ms": rewrite_ms,
                    f"{name}_execute_ms": execute_ms,
                    f"{name}_total_ms": rewrite_ms + execute_ms,
                }
            )
        mode = "warmup" if context.is_warmup else "run"
        with (self.output / f"{mode}-{context.execution_number}.decisions.csv").open(
            "w", newline=""
        ) as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        assert decisions["materialized"] == decisions["expanded"]
        assert all(
            a == c.is_valid for a, c in zip(decisions["expanded"], self.workload, strict=True)
        )
        return ExperimentResult(duration_ms=metrics["expanded_total_ms"], custom_metrics=metrics)

    def teardown(self, _context):
        for conn in self.connections.values():
            conn.close()
