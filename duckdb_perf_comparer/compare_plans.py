#!/usr/bin/env python3
"""Compare DuckDB EXPLAIN ANALYZE (format json) outputs for two queries."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


def _load_query(arg: str | None, file_arg: str | None) -> str:
    if arg and file_arg:
        raise ValueError("Provide either a query string or a query file, not both.")
    if file_arg:
        return Path(file_arg).read_text()
    if arg:
        return arg
    raise ValueError("Must provide a query string or file.")


def _explain_analyze_json(conn: duckdb.DuckDBPyConnection, query: str) -> dict[str, Any]:
    result = conn.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {query}").fetchone()
    if not result:
        raise RuntimeError("No EXPLAIN ANALYZE output returned.")
    plan_json = result[1] if len(result) > 1 else result[0]
    if isinstance(plan_json, str):
        plan_json = json.loads(plan_json)
    if isinstance(plan_json, dict) and "result" in plan_json:
        result_payload = plan_json["result"]
        if isinstance(result_payload, list) and result_payload:
            return result_payload[0]
    if isinstance(plan_json, dict):
        return plan_json
    raise TypeError(f"Unexpected EXPLAIN output type: {type(plan_json)}")


def _node_name(node: dict[str, Any]) -> str:
    return str(
        node.get("name")
        or node.get("operator_name")
        or node.get("operator")
        or node.get("type")
        or "unknown"
    )


def _node_time_ms(node: dict[str, Any]) -> float:
    for key in ("operator_timing", "timing", "time", "total_time", "latency"):
        if key in node:
            try:
                return float(node[key]) * (1000.0 if key == "operator_timing" or key == "latency" else 1.0)
            except (TypeError, ValueError):
                pass
    return 0.0


def _node_rows(node: dict[str, Any]) -> float:
    for key in ("cardinality", "rows", "row_count"):
        if key in node:
            try:
                return float(node[key])
            except (TypeError, ValueError):
                pass
    return 0.0


def _children(node: dict[str, Any]) -> list[dict[str, Any]]:
    children = node.get("children")
    if isinstance(children, list):
        return children
    return []


def _flatten_plan(node: dict[str, Any], path: str = "") -> list[dict[str, Any]]:
    name = _node_name(node)
    current_path = f"{path}/{name}" if path else name
    flattened = [
        {
            "path": current_path,
            "name": name,
            "time_ms": _node_time_ms(node),
            "rows": _node_rows(node),
            "extra": node.get("extra_info") or {},
            "raw": node,
        }
    ]
    for child in _children(node):
        flattened.extend(_flatten_plan(child, current_path))
    return flattened


def _total_time_ms(plan: dict[str, Any]) -> float:
    for key in ("latency", "total_time", "timing"):
        if key in plan:
            try:
                value = float(plan[key])
                return value * (1000.0 if key == "latency" else 1.0)
            except (TypeError, ValueError):
                continue
    return 0.0


def _summarize(flat: list[dict[str, Any]], top_n: int) -> list[dict[str, Any]]:
    return sorted(flat, key=lambda n: n["time_ms"], reverse=True)[:top_n]


def _format_ms(value: float) -> str:
    return f"{value:.3f} ms"


def _format_ratio(a: float, b: float) -> str:
    if b == 0:
        return "n/a"
    return f"{a / b:.2f}x"


def compare_plans(plan_a: dict[str, Any], plan_b: dict[str, Any], top_n: int) -> str:
    total_a = _total_time_ms(plan_a)
    total_b = _total_time_ms(plan_b)

    root_a = plan_a.get("children", [plan_a])[0] if isinstance(plan_a.get("children"), list) else plan_a
    root_b = plan_b.get("children", [plan_b])[0] if isinstance(plan_b.get("children"), list) else plan_b
    flat_a = _flatten_plan(root_a)
    flat_b = _flatten_plan(root_b)

    if total_a == 0.0:
        total_a = sum(node["time_ms"] for node in flat_a)
    if total_b == 0.0:
        total_b = sum(node["time_ms"] for node in flat_b)

    summary = []
    summary.append("Overview")
    summary.append(f"- Query A total time: {_format_ms(total_a)}")
    summary.append(f"- Query B total time: {_format_ms(total_b)}")
    summary.append(
        f"- Speed ratio (A/B): {_format_ratio(total_a, total_b)} "
        f"(A {'slower' if total_a > total_b else 'faster'})"
    )

    summary.append("\nTop Operators (Query A)")
    for node in _summarize(flat_a, top_n):
        summary.append(
            f"- {_format_ms(node['time_ms'])} | {node['name']} | rows={int(node['rows'])}"
        )

    summary.append("\nTop Operators (Query B)")
    for node in _summarize(flat_b, top_n):
        summary.append(
            f"- {_format_ms(node['time_ms'])} | {node['name']} | rows={int(node['rows'])}"
        )

    summary.append("\nLargest Time Deltas (A - B by operator path)")
    by_path_b = {node["path"]: node for node in flat_b}
    deltas = []
    for node in flat_a:
        other = by_path_b.get(node["path"])
        if not other:
            continue
        delta = node["time_ms"] - other["time_ms"]
        deltas.append((delta, node, other))
    for delta, node, other in sorted(deltas, key=lambda x: abs(x[0]), reverse=True)[:top_n]:
        summary.append(
            f"- {node['path']}: {delta:+.3f} ms (A {node['time_ms']:.3f} | B {other['time_ms']:.3f})"
        )

    return "\n".join(summary)


def _format_extra(extra: dict[str, Any]) -> str:
    if not extra:
        return ""
    parts = []
    for key in sorted(extra.keys()):
        parts.append(f"{key}={extra[key]}")
    return "; ".join(parts)


def _summarize_children(node: dict[str, Any]) -> str:
    children = _children(node)
    if not children:
        return "no children"
    summaries = []
    for child in children:
        name = _node_name(child)
        rows = _node_rows(child)
        time_ms = _node_time_ms(child)
        summaries.append(f"{name} rows={int(rows)} time={_format_ms(time_ms)}")
    return " | ".join(summaries)


def _join_diagnostics(flat: list[dict[str, Any]], top_n: int) -> list[str]:
    joins = [n for n in flat if "JOIN" in n["name"].upper()]
    joins = sorted(joins, key=lambda n: n["time_ms"], reverse=True)[:top_n]
    lines: list[str] = []
    for node in joins:
        extra = _format_extra(node["extra"])
        child_summary = _summarize_children(node["raw"])
        lines.append(
            f"- {_format_ms(node['time_ms'])} | {node['name']} | rows={int(node['rows'])}\n"
            f"  extra: {extra or 'n/a'}\n"
            f"  children: {child_summary}"
        )
    return lines


def _compare_results(
    conn: duckdb.DuckDBPyConnection, query_a: str, query_b: str
) -> tuple[bool, str]:
    res_a = conn.execute(query_a).fetchall()
    res_b = conn.execute(query_b).fetchall()
    if len(res_a) != len(res_b):
        return False, f"Row count mismatch: A={len(res_a)} B={len(res_b)}"
    res_a_sorted = sorted(res_a)
    res_b_sorted = sorted(res_b)
    for idx, (row_a, row_b) in enumerate(zip(res_a_sorted, res_b_sorted)):
        if row_a != row_b:
            return False, f"Row {idx} mismatch: {row_a} != {row_b}"
    return True, "Results match (exact)"


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare DuckDB query performance via EXPLAIN ANALYZE JSON.")
    parser.add_argument("--query-a", help="Query A SQL string")
    parser.add_argument("--query-b", help="Query B SQL string")
    parser.add_argument("--file-a", help="Path to file containing Query A")
    parser.add_argument("--file-b", help="Path to file containing Query B")
    parser.add_argument("--db", default=":memory:", help="DuckDB database path (default: in-memory)")
    parser.add_argument(
        "--tpch-sf",
        type=float,
        default=1.0,
        help="TPC-H scale factor to load (default: 1.0)",
    )
    parser.add_argument("--top", type=int, default=5, help="Number of top operators to show")
    parser.add_argument(
        "--join-details",
        type=int,
        default=0,
        help="Show top-N join operator details (0 disables).",
    )
    args = parser.parse_args()

    query_a = _load_query(args.query_a, args.file_a)
    query_b = _load_query(args.query_b, args.file_b)

    conn = duckdb.connect(args.db)
    try:
        conn.execute("INSTALL tpch")
        conn.execute("LOAD tpch")
        if args.db == ":memory:":
            conn.execute(f"CALL dbgen(sf={args.tpch_sf})")
        else:
            table_exists = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'lineitem'"
            ).fetchone()[0]
            if table_exists == 0:
                conn.execute(f"CALL dbgen(sf={args.tpch_sf})")
        same, msg = _compare_results(conn, query_a, query_b)
        plan_a = _explain_analyze_json(conn, query_a)
        plan_b = _explain_analyze_json(conn, query_b)
    finally:
        conn.close()

    print(compare_plans(plan_a, plan_b, args.top))
    if args.join_details > 0:
        root_a = plan_a.get("children", [plan_a])[0] if isinstance(plan_a.get("children"), list) else plan_a
        root_b = plan_b.get("children", [plan_b])[0] if isinstance(plan_b.get("children"), list) else plan_b
        flat_a = _flatten_plan(root_a)
        flat_b = _flatten_plan(root_b)
        print("\nTop Join Operators (Query A)")
        print("\n".join(_join_diagnostics(flat_a, args.join_details)))
        print("\nTop Join Operators (Query B)")
        print("\n".join(_join_diagnostics(flat_b, args.join_details)))
    print(f"\nResultset comparison: {msg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
