#!/usr/bin/env python3
"""Run only the Rust-backed DFC path for the Regulation Z v2 benchmark."""

from __future__ import annotations

import argparse
import csv
from importlib.metadata import version
from pathlib import Path
import re
import sys
import time

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from vldb_experiments.strategies.regulation_z_statement_benchmark import (  # noqa: E402
    DEFAULT_CATALOG_PATH,
    DEFAULT_NUM_ACCOUNTS,
    build_regulation_z_statement_policies,
    generate_statement_workload,
    reset_statement_database,
)
from vldb_experiments.strategies.regulation_z_v2_data import load_product_terms  # noqa: E402
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck  # noqa: E402

_SOURCE_AGGREGATE = re.compile(r"max\((statement_reconciliation\.[A-Za-z_][A-Za-z0-9_]*)\)")

PROGRESS_FIELDS = [
    "update_number",
    "publication_id",
    "case_type",
    "expected_allow",
    "actual_allow",
    "correct",
    "rewrite_ms",
    "sql_execution_ms",
    "elapsed_ms",
]


def _rust_constraint(constraint: str) -> str:
    """Remove aggregation required by the legacy rewriter from a one-row source relation."""
    rewritten = _SOURCE_AGGREGATE.sub(r"\1", constraint)
    if "max(statement_reconciliation." in rewritten:
        raise ValueError(f"Unsupported aggregate in Rust policy constraint: {constraint}")
    return rewritten


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-accounts", type=int, default=DEFAULT_NUM_ACCOUNTS)
    parser.add_argument("--num-statements", type=int, default=100)
    parser.add_argument("--catalog-path", type=Path, default=DEFAULT_CATALOG_PATH)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/regulation_z_v2_rust_dfc_100.csv"),
    )
    parser.add_argument(
        "--progress-output",
        type=Path,
        default=Path("results/regulation_z_v2_rust_dfc_100.progress.csv"),
    )
    parser.add_argument("--passant-commit", default="unknown")
    args = parser.parse_args()

    try:
        from data_flow_control import Policy, Resolution
        from data_flow_control.adapters.duckdb import DuckDBAdapter
        from data_flow_control.connection import Connection
    except ImportError as error:
        raise RuntimeError(
            "Install the Rust Passant package into this environment before running this script"
        ) from error

    workload = generate_statement_workload(
        load_product_terms(args.catalog_path),
        num_accounts=args.num_accounts,
        num_updates=args.num_statements,
    )
    duckdb_module = _ensure_smokedduck()
    raw_conn = duckdb_module.connect(":memory:")
    reset_statement_database(raw_conn, workload)

    class ViewAwareDuckDBAdapter(DuckDBAdapter):
        """Include user views in the catalog snapshot supplied to the Rust planner."""

        def _list_catalog_tables(self) -> list[str]:
            names = super()._list_catalog_tables()
            rows = self.execute(
                "SELECT schema_name, view_name FROM duckdb_views() "
                "WHERE NOT internal AND NOT temporary"
            ).fetchall()
            for schema, name in rows:
                qualified = (
                    f"{schema}.{name}"
                    if schema and schema.lower() not in ("main", "temp")
                    else name
                )
                if qualified not in names:
                    names.append(qualified)
            return names

    rust_conn = Connection(ViewAwareDuckDBAdapter(raw_conn))
    rust_conn.register_policies(
        [
            Policy(
                sources=policy.sources,
                sink=policy.sink,
                sink_alias=policy.sink_alias,
                constraint=_rust_constraint(policy.constraint),
                on_fail=Resolution.REMOVE,
                description=policy.description,
            )
            for policy in build_regulation_z_statement_policies()
        ]
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.progress_output.parent.mkdir(parents=True, exist_ok=True)
    rewrite_ms = 0.0
    execution_ms = 0.0
    correct = 0
    with args.progress_output.open("w", encoding="utf-8", newline="", buffering=1) as file:
        writer = csv.DictWriter(file, fieldnames=PROGRESS_FIELDS)
        writer.writeheader()
        for update_number, case in enumerate(workload, start=1):
            rewrite_start = time.perf_counter()
            rewritten = rust_conn.transform_query(case.sql)
            case_rewrite_ms = (time.perf_counter() - rewrite_start) * 1000.0
            execution_start = time.perf_counter()
            raw_conn.execute(rewritten)
            case_execution_ms = (time.perf_counter() - execution_start) * 1000.0
            actual_allow = (
                raw_conn.execute(
                    "SELECT status FROM statement_publications WHERE publication_id = ?",
                    [case.publication_id],
                ).fetchone()[0]
                == "published"
            )
            is_correct = actual_allow == case.is_valid
            rewrite_ms += case_rewrite_ms
            execution_ms += case_execution_ms
            correct += int(is_correct)
            writer.writerow(
                {
                    "update_number": update_number,
                    "publication_id": case.publication_id,
                    "case_type": case.case_type,
                    "expected_allow": case.is_valid,
                    "actual_allow": actual_allow,
                    "correct": is_correct,
                    "rewrite_ms": case_rewrite_ms,
                    "sql_execution_ms": case_execution_ms,
                    "elapsed_ms": case_rewrite_ms + case_execution_ms,
                }
            )
            file.flush()

    total_ms = rewrite_ms + execution_ms
    summary_fields = [
        "passant_version",
        "passant_commit",
        "num_updates",
        "dfc_correct",
        "dfc_accuracy",
        "dfc_rewrite_time_ms",
        "dfc_execution_time_ms",
        "dfc_time_ms",
        "dfc_average_rewrite_ms",
        "dfc_average_execution_ms",
        "dfc_average_time_ms",
    ]
    with args.output.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=summary_fields)
        writer.writeheader()
        writer.writerow(
            {
                "passant_version": version("data-flow-control"),
                "passant_commit": args.passant_commit,
                "num_updates": len(workload),
                "dfc_correct": correct,
                "dfc_accuracy": correct / len(workload),
                "dfc_rewrite_time_ms": rewrite_ms,
                "dfc_execution_time_ms": execution_ms,
                "dfc_time_ms": total_ms,
                "dfc_average_rewrite_ms": rewrite_ms / len(workload),
                "dfc_average_execution_ms": execution_ms / len(workload),
                "dfc_average_time_ms": total_ms / len(workload),
            }
        )
    raw_conn.close()
    print(f"Rust DFC correctness: {correct}/{len(workload)}")
    print(
        f"Rust rewrite time: {rewrite_ms:.3f} ms total ({rewrite_ms / len(workload):.3f} ms/update)"
    )
    print(
        f"SQL execution time: {execution_ms:.3f} ms total "
        f"({execution_ms / len(workload):.3f} ms/update)"
    )
    print(f"Full DFC time: {total_ms:.3f} ms total ({total_ms / len(workload):.3f} ms/update)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
