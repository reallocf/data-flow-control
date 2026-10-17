#!/usr/bin/env python3
"""Generate PostgreSQL plans for the project's three internal TPC-H rewrites."""

from __future__ import annotations

import ast
from pathlib import Path
import subprocess

import duckdb
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter
import sqlglot

REPO_ROOT = Path(__file__).resolve().parents[3]
QUERY_DIR = REPO_ROOT / "benchmarks" / "tpch" / "queries"
OUTPUT_DIR = Path(__file__).resolve().parent / "results"
DUCKDB_PATH = REPO_ROOT / "vldb_2026_big_paper_experiments" / "results" / "tpch_sf1.db"
QUERY_NUMBERS = [1, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 18, 19]


def load_verified_logical_sql() -> dict[int, str]:
    test_path = (
        REPO_ROOT
        / "vldb_2026_big_paper_experiments"
        / "tests"
        / "test_tpch_rewriting.py"
    )
    module = ast.parse(test_path.read_text())
    for node in module.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "LOGICAL_EXPECTED_SQL":
                    return ast.literal_eval(node.value)
    raise RuntimeError("LOGICAL_EXPECTED_SQL was not found in test_tpch_rewriting.py")


def explain(sql: str) -> str:
    completed = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            "gprom-tpch-postgres",
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "postgres",
            "-d",
            "tpch",
            "-X",
            "-P",
            "pager=off",
        ],
        input="EXPLAIN (COSTS, VERBOSE, FORMAT TEXT)\n" + sql.rstrip(";") + ";\n",
        text=True,
        capture_output=True,
        check=False,
        timeout=300,
    )
    return completed.stdout + completed.stderr


def to_postgres(sql: str) -> str:
    return sqlglot.parse_one(sql, read="duckdb").sql(pretty=True, dialect="postgres")


def main() -> int:
    connection = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    rewriter = SQLRewriter(conn=connection)
    policy = DFCPolicy(
        sources=["lineitem"],
        constraint="avg(lineitem.l_quantity) >= 30",
        on_fail=Resolution.REMOVE,
    )
    rewriter.register_policy(policy)
    logical_sql = load_verified_logical_sql()

    for query_number in QUERY_NUMBERS:
        query = (QUERY_DIR / f"q{query_number:02d}.sql").read_text()
        rewrites = {
            "internal_full": rewriter.transform_query(query),
            "internal_partial": rewriter.transform_query(query, use_two_phase=True),
            "internal_logical": logical_sql[query_number],
        }
        query_dir = OUTPUT_DIR / f"q{query_number:02d}"
        for name, rewritten in rewrites.items():
            postgres_sql = to_postgres(rewritten)
            (query_dir / f"{name}.sql").write_text(postgres_sql + ";\n")
            (query_dir / f"{name}_explain.txt").write_text(explain(postgres_sql))

    rewriter.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
