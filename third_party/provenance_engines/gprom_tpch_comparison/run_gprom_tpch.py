#!/usr/bin/env python3
"""Generate GProM provenance-policy rewrites and PostgreSQL plans for TPC-H."""

from __future__ import annotations

from pathlib import Path
import re
import subprocess

REPO_ROOT = Path(__file__).resolve().parents[3]
QUERY_DIR = REPO_ROOT / "benchmarks" / "tpch" / "queries"
OUTPUT_DIR = Path(__file__).resolve().parent / "results"
GPROM_IMAGE = "iitdbgroup/gprom:latest"
POSTGRES_CONTAINER = "gprom-tpch-postgres"
QUERY_OUTPUTS = {
    1: [
        "l_returnflag",
        "l_linestatus",
        "sum_qty",
        "sum_base_price",
        "sum_disc_price",
        "sum_charge",
        "avg_qty",
        "avg_price",
        "avg_disc",
        "count_order",
    ],
    3: ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"],
    4: ["o_orderpriority", "order_count"],
    5: ["n_name", "revenue"],
    6: ["revenue"],
    7: ["supp_nation", "cust_nation", "l_year", "revenue"],
    8: ["o_year", "mkt_share"],
    9: ["nation", "o_year", "sum_profit"],
    10: [
        "c_custkey",
        "c_name",
        "revenue",
        "c_acctbal",
        "n_name",
        "c_address",
        "c_phone",
        "c_comment",
    ],
    12: ["l_shipmode", "high_line_count", "low_line_count"],
    14: ["promo_revenue"],
    18: [
        "c_name",
        "c_custkey",
        "o_orderkey",
        "o_orderdate",
        "o_totalprice",
        "sum_l_quantity",
    ],
    19: ["revenue"],
}


def run(
    command: list[str], *, stdin: str | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        input=stdin,
        text=True,
        capture_output=True,
        check=False,
        timeout=300,
    )


def strip_ansi(value: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", value)


def normalize_for_gprom(query_number: int, original: str) -> str:
    """Apply semantics-preserving syntax changes required by GProM's Oracle frontend."""
    original = re.sub(
        r"extract\(year FROM ([^)]+)\)",
        r"date_part('year', \1)",
        original,
        flags=re.IGNORECASE,
    )
    original = re.sub(
        r"date\s+'([^']+)'",
        r"CAST('\1' AS date)",
        original,
        flags=re.IGNORECASE,
    )
    original = original.replace(
        "l_discount BETWEEN 0.05\n    AND 0.07",
        "l_discount * 100 >= 5\n    AND l_discount * 100 <= 7",
    )
    original = original.replace(
        "l_shipdate BETWEEN CAST('1995-01-01' AS date)\n        AND CAST('1996-12-31' AS date)",
        "l_shipdate >= CAST('1995-01-01' AS date)\n        AND l_shipdate <= CAST('1996-12-31' AS date)",
    )
    original = original.replace(
        "o_orderdate BETWEEN CAST('1995-01-01' AS date)\n        AND CAST('1996-12-31' AS date)",
        "o_orderdate >= CAST('1995-01-01' AS date)\n        AND o_orderdate <= CAST('1996-12-31' AS date)",
    )

    if query_number == 4:
        return """SELECT
    o_orderpriority,
    count(*) AS order_count
FROM orders
JOIN (
    SELECT l_orderkey
    FROM lineitem
    WHERE l_commitdate < l_receiptdate
    GROUP BY l_orderkey
) AS qualifying_lineitem
  ON o_orderkey = qualifying_lineitem.l_orderkey
WHERE o_orderdate >= CAST('1993-07-01' AS date)
  AND o_orderdate < CAST('1993-10-01' AS date)
GROUP BY o_orderpriority
ORDER BY o_orderpriority"""

    if query_number == 18:
        return """SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(outer_lineitem.l_quantity) AS sum_l_quantity
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem AS outer_lineitem ON o_orderkey = outer_lineitem.l_orderkey
JOIN (
    SELECT l_orderkey
    FROM lineitem
    GROUP BY l_orderkey
    HAVING sum(l_quantity) > 300
) AS large_orders ON o_orderkey = large_orders.l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100"""

    if query_number == 19:
        replacements = {
            "p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')": "(p_container = 'SM CASE' OR p_container = 'SM BOX' OR "
            "p_container = 'SM PACK' OR p_container = 'SM PKG')",
            "p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')": "(p_container = 'MED BAG' OR p_container = 'MED BOX' OR "
            "p_container = 'MED PKG' OR p_container = 'MED PACK')",
            "p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')": "(p_container = 'LG CASE' OR p_container = 'LG BOX' OR "
            "p_container = 'LG PACK' OR p_container = 'LG PKG')",
            "l_shipmode IN ('AIR', 'AIR REG')": "(l_shipmode = 'AIR' OR l_shipmode = 'AIR REG')",
            "p_size BETWEEN 1 AND 5": "p_size >= 1 AND p_size <= 5",
            "p_size BETWEEN 1 AND 10": "p_size >= 1 AND p_size <= 10",
            "p_size BETWEEN 1 AND 15": "p_size >= 1 AND p_size <= 15",
        }
        for source, replacement in replacements.items():
            original = original.replace(source, replacement)

    return original


def original_query(query_number: int) -> str:
    original = (QUERY_DIR / f"q{query_number:02d}.sql").read_text().strip().rstrip(";")
    return normalize_for_gprom(query_number, original)


def compose_query(query_number: int, original: str, policy_columns: list[str]) -> str:
    outputs = ", ".join(QUERY_OUTPUTS[query_number])
    policy = " AND ".join(f"AVG({column}) >= 30" for column in policy_columns)
    return (
        "WITH provenance_result AS (\n"
        "  PROVENANCE OF (\n"
        f"{original}\n"
        "  )\n"
        ")\n"
        f"SELECT {outputs}\n"
        "FROM provenance_result\n"
        f"GROUP BY {outputs}\n"
        f"HAVING {policy};\n"
    )


def gprom_rewrite(input_path: Path) -> subprocess.CompletedProcess[str]:
    container_path = f"/workspace/{input_path.relative_to(REPO_ROOT)}"
    return run(
        [
            "docker",
            "run",
            "--rm",
            "-i",
            "-v",
            f"{REPO_ROOT}:/workspace",
            "-w",
            "/workspace",
            GPROM_IMAGE,
            "-backend",
            "postgres",
            "-host",
            "host.docker.internal",
            "-port",
            "55432",
            "-db",
            "tpch",
            "-user",
            "postgres",
            "-passwd",
            "postgres",
            "-Pexecutor",
            "sql",
            "-queryFile",
            container_path,
        ]
    )


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary = ["query,status,detail"]

    for query_number in QUERY_OUTPUTS:
        query_name = f"q{query_number:02d}"
        query_dir = OUTPUT_DIR / query_name
        query_dir.mkdir(parents=True, exist_ok=True)
        original = original_query(query_number)
        provenance_only_path = query_dir / "provenance_only_input.sql"
        provenance_only_path.write_text(f"PROVENANCE OF ({original});\n")
        provenance_only = gprom_rewrite(provenance_only_path)
        provenance_sql = strip_ansi(provenance_only.stdout)
        provenance_error = strip_ansi(provenance_only.stderr)
        (query_dir / "provenance_only_rewritten.sql").write_text(provenance_sql)
        (query_dir / "provenance_only_stderr.txt").write_text(provenance_error)
        if provenance_only.returncode != 0 or not provenance_sql.strip():
            detail = (
                (provenance_error or provenance_sql).strip().replace("\n", " ")[:200]
            )
            summary.append(
                f'{query_name},provenance_rewrite_failed,"'
                f'{detail.replace(chr(34), chr(34) * 2)}"'
            )
            continue

        policy_columns = sorted(
            {
                match.lower()
                for match in re.findall(
                    r"(prov_lineitem(?:_[0-9]+)?_l__quantity)",
                    provenance_sql,
                    flags=re.IGNORECASE,
                )
            }
        )
        if not policy_columns:
            summary.append(f"{query_name},policy_column_missing,")
            continue

        composed = compose_query(query_number, original, policy_columns)
        input_path = query_dir / "gprom_input.sql"
        input_path.write_text(composed)
        gprom = gprom_rewrite(input_path)
        generated_sql = strip_ansi(gprom.stdout)
        error = strip_ansi(gprom.stderr)
        (query_dir / "gprom_stdout.txt").write_text(generated_sql)
        (query_dir / "gprom_stderr.txt").write_text(error)

        if gprom.returncode != 0 or not generated_sql.strip():
            detail = (error or generated_sql).strip().replace("\n", " ")[:200]
            summary.append(
                f'{query_name},rewrite_failed,"{detail.replace(chr(34), chr(34) * 2)}"'
            )
            continue

        rewritten_path = query_dir / "gprom_rewritten.sql"
        rewritten_path.write_text(generated_sql)
        explain = run(
            [
                "docker",
                "exec",
                "-i",
                POSTGRES_CONTAINER,
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
            stdin="EXPLAIN (COSTS, VERBOSE, FORMAT TEXT)\n" + generated_sql,
        )
        plan = explain.stdout + explain.stderr
        (query_dir / "postgres_explain.txt").write_text(plan)
        if explain.returncode == 0:
            summary.append(f"{query_name},ok,")
        else:
            detail = plan.strip().replace("\n", " ")[:200]
            summary.append(
                f'{query_name},explain_failed,"{detail.replace(chr(34), chr(34) * 2)}"'
            )

    (OUTPUT_DIR / "summary.csv").write_text("\n".join(summary) + "\n")
    print("\n".join(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
