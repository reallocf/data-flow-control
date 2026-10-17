#!/usr/bin/env python3
"""Compare naive output-provenance policy evaluation with Passant."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys
import time

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from data_flow_control import Policy, Resolution, dfc  # noqa: E402

from vldb_experiments.baselines.provsql_baseline import ProvSQLNaiveBaseline  # noqa: E402
from vldb_experiments.correctness import compare_results_approx  # noqa: E402
from vldb_experiments.multi_db.provsql import ProvSQLClient  # noqa: E402
from vldb_experiments.strategies.tpch_strategy import (  # noqa: E402
    TPCH_QUERIES,
    load_tpch_query,
)


def _write_results(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Test naive ProvSQL output-provenance filtering against Passant"
    )
    parser.add_argument("--sf", type=float, default=0.01)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/tpch_provsql_monolithic_correctness_sf001.csv"),
    )
    args = parser.parse_args()

    policy = Policy(
        sources=["lineitem"],
        constraint="max(lineitem.l_quantity) >= 1",
        on_fail=Resolution.REMOVE,
    )

    duckdb_conn = duckdb.connect(":memory:")
    duckdb_conn.execute("INSTALL tpch")
    duckdb_conn.execute("LOAD tpch")
    duckdb_conn.execute(f"CALL dbgen(sf={args.sf})")
    passant = dfc(duckdb_conn, dialect="duckdb")
    passant.register_policy(policy)

    data_dir = Path("results") / "provsql_tpch" / f"sf{args.sf:g}"
    provsql = ProvSQLClient(data_dir, scale_factor=args.sf)
    provsql.start()
    provsql.wait_ready()
    provsql_conn = provsql.connect()
    provsql.ensure_tpch_data(duckdb_conn)
    provsql_baseline = ProvSQLNaiveBaseline(
        source_table="lineitem",
        source_column="l_quantity",
        aggregate="MAX",
        comparison=">=",
        threshold=1,
    )
    provsql_baseline.prepare(provsql_conn)

    output_rows: list[dict[str, object]] = []
    failures: list[int] = []
    try:
        for query_number in TPCH_QUERIES:
            query = load_tpch_query(query_number)

            passant_rewrite_start = time.perf_counter()
            passant_sql = passant.transform_query(query)
            passant_rewrite_ms = (time.perf_counter() - passant_rewrite_start) * 1000
            passant_execute_start = time.perf_counter()
            passant_rows = duckdb_conn.execute(passant_sql).fetchall()
            passant_execute_ms = (time.perf_counter() - passant_execute_start) * 1000

            provsql_rewrite_start = time.perf_counter()
            provsql_sql = provsql_baseline.rewrite(query)
            provsql_rewrite_ms = (time.perf_counter() - provsql_rewrite_start) * 1000
            provsql_execute_start = time.perf_counter()
            provsql_rows, _ = provsql_baseline.execute(
                provsql_conn,
                query,
                rewritten_sql=provsql_sql,
            )
            provsql_execute_ms = (time.perf_counter() - provsql_execute_start) * 1000

            match, error = compare_results_approx(passant_rows, provsql_rows)
            if error:
                error = error.replace("dfc=", "passant=").replace("logical=", "provsql_naive=")
            if not match:
                failures.append(query_number)
            print(
                f"Q{query_number:02d}: {'PASS' if match else 'FAIL'} "
                f"({len(passant_rows)} rows, ProvSQL {provsql_execute_ms:.1f} ms)"
            )
            if error:
                print(f"  {error}")

            output_rows.append(
                {
                    "query_num": query_number,
                    "tpch_sf": args.sf,
                    "correctness_match": match,
                    "correctness_error": error or "",
                    "passant_rows": len(passant_rows),
                    "provsql_rows": len(provsql_rows),
                    "passant_rewrite_time_ms": passant_rewrite_ms,
                    "passant_exec_time_ms": passant_execute_ms,
                    "provsql_rewrite_time_ms": provsql_rewrite_ms,
                    "provsql_exec_time_ms": provsql_execute_ms,
                    "passant_sql": passant_sql,
                    "provsql_sql": provsql_sql,
                }
            )
    finally:
        provsql.close()
        passant.close()

    _write_results(args.output, output_rows)
    print(f"Wrote {args.output}")
    if failures:
        print(f"Mismatches: {failures}")
        return 1
    print(f"All {len(TPCH_QUERIES)} queries match")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
