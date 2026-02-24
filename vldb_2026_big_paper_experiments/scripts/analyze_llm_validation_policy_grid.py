#!/usr/bin/env python3
"""Generate policy-by-query violation grid for llm_validation policies on TPC-H."""

from __future__ import annotations

import argparse
import contextlib
import csv
from pathlib import Path

import duckdb
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter


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


def _policy_catalog() -> list[tuple[str, str]]:
    return [
        ("avg(lineitem.l_quantity) <= 30", "Average quantity should remain at or below 30."),
        ("avg(lineitem.l_quantity) >= 20", "Average quantity should be at least 20."),
        ("max(lineitem.l_quantity) <= 50", "No line item quantity should exceed 50 units."),
        ("min(lineitem.l_quantity) >= 1", "All line item quantities must be at least 1."),
        ("avg(lineitem.l_discount) <= 0.06", "Average discount should not exceed 6%."),
        ("max(lineitem.l_discount) <= 0.07", "No single discount should exceed 7%."),
        ("min(lineitem.l_discount) >= 0.00", "Discount should be non-negative."),
        ("avg(lineitem.l_tax) <= 0.06", "Average tax rate should stay under 6%."),
        ("max(lineitem.l_tax) <= 0.06", "No single tax rate should exceed 6%."),
        ("min(lineitem.l_tax) >= 0.00", "Tax should be non-negative."),
        ("min(lineitem.l_extendedprice) >= 0", "Extended prices should be non-negative."),
        ("avg(lineitem.l_extendedprice) <= 60000", "Average extended price should not exceed 60,000."),
        ("max(lineitem.l_extendedprice) <= 90000", "Single extended price should not exceed 90,000."),
        ("count(lineitem.l_orderkey) >= 1", "There should be at least one line item."),
        ("count(distinct lineitem.l_shipmode) <= 5", "Distinct ship modes should not exceed 5."),
        ("count(distinct lineitem.l_returnflag) <= 2", "Distinct return flags should not exceed 2."),
        ("count(distinct lineitem.l_linestatus) <= 2", "Distinct line statuses should not exceed 2."),
        ("max(lineitem.l_linenumber) <= 5", "Line numbers should stay within 1..5."),
        ("min(lineitem.l_linenumber) >= 1", "Line numbers should be at least 1."),
        ("min(lineitem.l_shipdate) >= DATE '1994-01-01'", "Ship dates should be on/after 1994-01-01."),
        ("max(lineitem.l_shipdate) <= DATE '1997-12-31'", "Ship dates should be on/before 1997-12-31."),
        ("min(lineitem.l_receiptdate) >= DATE '1994-01-01'", "Receipt dates should be on/after 1994-01-01."),
        ("max(lineitem.l_receiptdate) <= DATE '1997-12-31'", "Receipt dates should be on/before 1997-12-31."),
        ("min(lineitem.l_commitdate) >= DATE '1994-01-01'", "Commit dates should be on/after 1994-01-01."),
        ("max(lineitem.l_commitdate) <= DATE '1997-12-31'", "Commit dates should be on/before 1997-12-31."),
        ("sum(lineitem.l_discount) <= 180000", "Total discounts should stay below 180,000."),
        ("sum(lineitem.l_quantity) <= 80000000", "Total quantity should stay below 80M."),
        ("min(lineitem.l_orderkey) >= 1", "Order keys should be positive."),
        ("max(lineitem.l_orderkey) <= 3000000", "Order keys should remain within expected TPCH range."),
        ("min(lineitem.l_partkey) >= 1", "Part keys should be positive."),
        ("max(lineitem.l_partkey) <= 100000", "Part keys should remain within expected TPCH range."),
        ("max(lineitem.l_suppkey) <= 5000", "Supplier keys should remain within expected TPCH range."),
    ]


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


def _query_has_violation(conn: duckdb.DuckDBPyConnection, rewriter: SQLRewriter, query_num: int) -> bool:
    query = load_tpch_query(query_num)
    transformed = rewriter.transform_query(query, use_two_phase=False)
    cursor = conn.execute(transformed)
    rows = cursor.fetchall()
    columns = [d[0] for d in (cursor.description or [])]
    lower_columns = [c.lower() for c in columns]
    if "valid" not in lower_columns:
        return False
    valid_idx = lower_columns.index("valid")
    return any((row[valid_idx] is False) or (str(row[valid_idx]).lower() == "false") for row in rows)


def _clear_policies(rewriter: SQLRewriter) -> None:
    for old in rewriter.get_dfc_policies():
        rewriter.delete_policy(
            sources=old.sources,
            constraint=old.constraint,
            on_fail=old.on_fail,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze llm_validation policy violations across TPC-H queries.")
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
        policies = _policy_catalog()

        header = (
            ["policy_index", "policy_constraint"]
            + [f"q{q:02d}" for q in args.queries]
            + ["fails_any"]
        )
        rows: list[list[object]] = []

        for idx, (constraint, _description) in enumerate(policies, start=1):
            _clear_policies(rewriter)
            policy = DFCPolicy(
                sources=["lineitem"],
                constraint=constraint,
                on_fail=Resolution.INVALIDATE,
            )
            rewriter.register_policy(policy)

            query_flags: list[bool] = []
            for query_num in args.queries:
                try:
                    violated = _query_has_violation(conn, rewriter, query_num)
                except Exception as exc:
                    print(
                        f"ERROR policy={idx:02d} q{query_num:02d}: {type(exc).__name__}: {exc}"
                    )
                    if args.strict:
                        raise
                    violated = False
                query_flags.append(violated)

            fails_any = any(query_flags)
            rows.append([idx, constraint, *query_flags, fails_any])
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
