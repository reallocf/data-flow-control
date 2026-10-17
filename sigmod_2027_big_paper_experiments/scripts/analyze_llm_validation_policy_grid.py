#!/usr/bin/env python3
"""Generate policy-by-query violation grid for llm_validation policies on TPC-H."""

from __future__ import annotations

import argparse
import contextlib
import csv
from pathlib import Path

import duckdb
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

from vldb_experiments.strategies.llm_validation_common import (
    POLICY_VARIANT_GROWING,
    SUPPORTED_POLICY_VARIANTS,
    policy_specs_for_variant,
)


def _find_tpch_queries_dir() -> Path:
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        candidate = parent / "benchmarks" / "tpch" / "queries"
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Could not find benchmarks/tpch/queries from script location")


TPCH_QUERIES_DIR = _find_tpch_queries_dir()
EXCLUDED_QUERIES = [2, 11, 13, 15, 16, 17, 20, 21, 22]
DEFAULT_QUERIES = [q for q in range(1, 23) if q not in EXCLUDED_QUERIES]


def load_tpch_query(query_num: int) -> str:
    query_file = TPCH_QUERIES_DIR / f"q{query_num:02d}.sql"
    if not query_file.exists():
        raise FileNotFoundError(f"TPC-H query {query_num} not found at {query_file}")
    return query_file.read_text()


def _ensure_tpch_data(conn: duckdb.DuckDBPyConnection, sf: float) -> None:
    try:
        conn.execute("LOAD tpch")
    except Exception:
        with contextlib.suppress(Exception):
            conn.execute("INSTALL tpch")
        conn.execute("LOAD tpch")

    table_exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'lineitem'"
    ).fetchone()[0]
    if table_exists == 0:
        print(f"Generating TPC-H data at sf={sf} ...")
        conn.execute(f"CALL dbgen(sf={sf})")
        print("TPC-H data generation complete.")


def _query_has_violation(
    conn: duckdb.DuckDBPyConnection, rewriter: SQLRewriter, query_num: int
) -> bool:
    query = load_tpch_query(query_num)
    transformed = rewriter.transform_query(query, use_two_phase=False)
    cursor = conn.execute(transformed)
    rows = cursor.fetchall()
    columns = [d[0] for d in (cursor.description or [])]
    lower_columns = [c.lower() for c in columns]
    if "valid" not in lower_columns:
        return False
    valid_idx = lower_columns.index("valid")
    return any(
        (row[valid_idx] is False) or (str(row[valid_idx]).lower() == "false") for row in rows
    )


def _clear_policies(rewriter: SQLRewriter) -> None:
    for old in rewriter.get_dfc_policies():
        rewriter.delete_policy(
            sources=old.sources,
            constraint=old.constraint,
            on_fail=old.on_fail,
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Analyze llm_validation policy violations across TPC-H queries."
    )
    parser.add_argument(
        "--tpch-sf",
        type=float,
        default=1.0,
        help="TPC-H scale factor (default: 1.0).",
    )
    parser.add_argument(
        "--queries",
        type=int,
        nargs="+",
        default=DEFAULT_QUERIES,
        help=(
            "TPC-H query numbers to test "
            "(default excludes non-monotonic/unsupported: 2,11,13,15,16,17,20,21,22)."
        ),
    )
    parser.add_argument(
        "--policy-variant",
        choices=[variant.replace("_", "-") for variant in SUPPORTED_POLICY_VARIANTS],
        default=POLICY_VARIANT_GROWING.replace("_", "-"),
        help="Policy catalog variant to analyze.",
    )
    parser.add_argument(
        "--db-path",
        default="./results/llm_validation_sf1.0.db",
        help="DuckDB path to use/create (default: ./results/llm_validation_sf1.0.db).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail immediately on the first query/policy error.",
    )
    parser.add_argument(
        "--output-filename",
        default="llm_validation_policy_query_grid.csv",
        help="Output CSV filename under results/ (default: llm_validation_policy_query_grid.csv).",
    )
    args = parser.parse_args()

    output_path = Path("./results") / args.output_filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    Path(args.db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(args.db_path)
    try:
        _ensure_tpch_data(conn, args.tpch_sf)
        rewriter = SQLRewriter(conn=conn)
        policies = policy_specs_for_variant(args.policy_variant.replace("-", "_"))

        header = (
            ["policy_index", "policy_constraint"]
            + [f"q{q:02d}" for q in args.queries]
            + ["fails_any"]
        )
        rows: list[list[object]] = []

        for idx, spec in enumerate(policies, start=1):
            _clear_policies(rewriter)
            policy = DFCPolicy(
                sources=list(spec.sources),
                constraint=spec.constraint,
                on_fail=Resolution.INVALIDATE,
            )
            rewriter.register_policy(policy)

            query_flags: list[bool] = []
            for query_num in args.queries:
                try:
                    violated = _query_has_violation(conn, rewriter, query_num)
                except Exception as exc:
                    print(f"ERROR policy={idx:02d} q{query_num:02d}: {type(exc).__name__}: {exc}")
                    if args.strict:
                        raise
                    violated = False
                query_flags.append(violated)

            fails_any = any(query_flags)
            rows.append([idx, spec.constraint, *query_flags, fails_any])
            print(f"Policy {idx:02d}: fails_any={fails_any}")

        with output_path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(rows)

        never_fail = [int(row[0]) for row in rows if not bool(row[-1])]
        print(f"\nWrote grid: {output_path}")
        print(f"Policies never failing any tested query: {never_fail}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
