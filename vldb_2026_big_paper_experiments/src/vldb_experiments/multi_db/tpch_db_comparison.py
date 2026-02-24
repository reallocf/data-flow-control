"""Compare TPC-H query results across DuckDB and an external engine."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

from vldb_experiments.correctness import compare_results_approx, rows_equal_approx
from vldb_experiments.multi_db import DataFusionClient, PostgresClient, SQLServerClient, UmbraClient
from vldb_experiments.strategies.tpch_strategy import TPCH_QUERIES, load_tpch_query

MULTI_DB_DATA_DIR = Path("results") / "multi_db"

if TYPE_CHECKING:
    from collections.abc import Iterable


def _parse_queries(raw: str) -> list[int]:
    if raw.lower() == "all":
        return list(TPCH_QUERIES)
    values = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        values.append(int(part))
    return values


def _format_row(row: tuple) -> str:
    return "(" + ", ".join(repr(item) for item in row) + ")"


def _normalize_approx_results(results: list[tuple], precision: int = 6) -> list[tuple]:
    from decimal import Decimal

    normalized = []
    for row in results:
        normalized_row = []
        for val in row:
            if val is None:
                normalized_row.append(None)
            elif isinstance(val, Decimal):
                normalized_row.append(round(float(val), precision))
            elif isinstance(val, float):
                normalized_row.append(round(val, precision))
            else:
                normalized_row.append(val)
        normalized.append(tuple(normalized_row))
    normalized.sort()
    return normalized


def _print_diff(duckdb_results: list[tuple], engine_results: list[tuple], max_rows: int) -> None:
    norm_duckdb = _normalize_approx_results(duckdb_results)
    norm_engine = _normalize_approx_results(engine_results)

    if len(norm_duckdb) != len(norm_engine):
        print(f"  Row count mismatch: duckdb={len(norm_duckdb)} engine={len(norm_engine)}")

    limit = min(len(norm_duckdb), len(norm_engine))
    shown = 0
    for idx in range(limit):
        if not rows_equal_approx(norm_duckdb[idx], norm_engine[idx]):
            print(f"  Row {idx} mismatch:")
            print(f"    duckdb: {_format_row(norm_duckdb[idx])}")
            print(f"    engine: {_format_row(norm_engine[idx])}")
            shown += 1
            if shown >= max_rows:
                break

    if shown == 0 and len(norm_duckdb) != len(norm_engine):
        extra = norm_duckdb[len(norm_engine) : len(norm_engine) + max_rows]
        if extra:
            print("  Extra duckdb rows:")
            for row in extra:
                print(f"    {_format_row(row)}")
        extra_engine = norm_engine[len(norm_duckdb) : len(norm_duckdb) + max_rows]
        if extra_engine:
            print("  Extra engine rows:")
            for row in extra_engine:
                print(f"    {_format_row(row)}")


def _get_client(engine: str, data_dir: Path):
    engine = engine.lower()
    if engine == "umbra":
        return UmbraClient(data_dir)
    if engine == "postgres":
        return PostgresClient(data_dir)
    if engine == "datafusion":
        return DataFusionClient(data_dir)
    if engine == "sqlserver":
        return SQLServerClient(data_dir)
    raise ValueError(f"Unsupported engine: {engine}")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare TPC-H query results across engines.")
    parser.add_argument(
        "--engine",
        default="umbra",
        choices=["umbra", "postgres", "datafusion", "sqlserver"],
        help="External engine to compare against DuckDB.",
    )
    parser.add_argument(
        "--queries",
        required=True,
        help="Comma-separated query numbers (e.g., 1,3,6) or 'all'.",
    )
    parser.add_argument(
        "--sf",
        type=float,
        required=True,
        help="TPC-H scale factor.",
    )
    parser.add_argument(
        "--max-diffs",
        type=int,
        default=5,
        help="Maximum differing rows to print per query.",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    query_numbers = _parse_queries(args.queries)
    data_dir = MULTI_DB_DATA_DIR / f"sf{args.sf}" / args.engine
    client = _get_client(args.engine, data_dir)

    print(f"Comparing DuckDB vs {args.engine} at SF={args.sf} for queries: {query_numbers}")

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL tpch")
    conn.execute("LOAD tpch")
    conn.execute(f"CALL dbgen(sf={args.sf})")

    client.start()
    client.wait_ready(timeout_s=120)
    client.connect()
    client.ensure_tpch_data(conn)

    correct = []
    incorrect = []

    for query_num in query_numbers:
        query = load_tpch_query(query_num)
        print(f"\nQ{query_num:02d}")
        try:
            duckdb_results = conn.execute(query).fetchall()
        except Exception as exc:
            print(f"  DuckDB error: {exc}")
            incorrect.append(query_num)
            continue

        try:
            engine_results = client.fetchall(query)
        except Exception as exc:
            print(f"  Engine error: {exc}")
            incorrect.append(query_num)
            continue

        match, error = compare_results_approx(duckdb_results, engine_results)
        if match:
            print("  ✅ Results match")
            correct.append(query_num)
        else:
            print(f"  ❌ Results differ: {error}")
            _print_diff(duckdb_results, engine_results, args.max_diffs)
            incorrect.append(query_num)

    client.close()
    conn.close()

    print("\nSummary")
    print(f"  Correct: {correct}")
    print(f"  Incorrect: {incorrect}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
