#!/usr/bin/env python3
"""Show a unified diff between DFC and Logical SQL for a policy count."""

from __future__ import annotations

import argparse
import difflib
from pathlib import Path
import re

DEFAULT_DFC_TEST = Path("..") / "sql_rewriter" / "test_tpch_policy_count.py"
DEFAULT_LOGICAL_TEST = Path("tests") / "test_tpch_policy_count_rewrite.py"


def _extract_expected_sql(test_path: Path, policy_count: int) -> str:
    text = test_path.read_text()
    match = re.search(r"EXPECTED_SQL = \{([\s\S]*?)\n\}\n", text)
    if not match:
        raise ValueError(f"EXPECTED_SQL block not found in {test_path}")
    block = match.group(1)
    entry = re.search(
        rf"\n\s*{policy_count}:\s*r\"\"\"([\s\S]*?)\"\"\",",
        block,
    )
    if not entry:
        raise ValueError(f"Policy count {policy_count} not found in {test_path}")
    return entry.group(1)


def main() -> int:
    parser = argparse.ArgumentParser(description="Diff DFC vs Logical SQL for a policy count.")
    parser.add_argument("policy_count", type=int, help="Policy count to compare")
    parser.add_argument(
        "--dfc-test",
        type=Path,
        default=DEFAULT_DFC_TEST,
        help="Path to sql_rewriter test file with EXPECTED_SQL",
    )
    parser.add_argument(
        "--logical-test",
        type=Path,
        default=DEFAULT_LOGICAL_TEST,
        help="Path to logical test file with EXPECTED_SQL",
    )
    args = parser.parse_args()

    dfc_sql = _extract_expected_sql(args.dfc_test, args.policy_count)
    logical_sql = _extract_expected_sql(args.logical_test, args.policy_count)

    dfc_lines = dfc_sql.splitlines(keepends=True)
    logical_lines = logical_sql.splitlines(keepends=True)

    diff = difflib.unified_diff(
        dfc_lines,
        logical_lines,
        fromfile=f"DFC({args.policy_count})",
        tofile=f"Logical({args.policy_count})",
    )

    print("".join(diff))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
